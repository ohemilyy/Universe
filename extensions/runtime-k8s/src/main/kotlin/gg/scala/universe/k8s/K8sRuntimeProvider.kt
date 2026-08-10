package gg.scala.universe.k8s

import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.runtime.RuntimeProvider
import gg.scala.universe.runtime.RuntimeRecoveryInspector
import gg.scala.universe.runtime.RuntimeResourceState
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVarBuilder
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodSpecBuilder
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder
import io.fabric8.kubernetes.api.model.TolerationBuilder
import io.fabric8.kubernetes.api.model.VolumeBuilder
import io.fabric8.kubernetes.api.model.VolumeMountBuilder
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.KubernetesClientException
import com.google.gson.Gson
import gg.scala.universe.schema.Template
import gg.scala.universe.schema.TemplateInstallationConfig
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream
import kotlin.time.Duration.Companion.seconds

/**
 * [RuntimeProvider] implementation using Kubernetes Pods.
 *
 * Each instance runs in a dedicated Kubernetes Pod. Commands are executed
 * via `kubectl exec` equivalent through the Fabric8 client.
 */
class K8sRuntimeProvider private constructor(
    private val config: K8sConfig,
    providedClient: KubernetesClient?,
    @Suppress("UNUSED_PARAMETER") useProvidedClient: Boolean
) : RuntimeProvider, RuntimeRecoveryInspector {

    constructor(config: K8sConfig) : this(config, null, false)

    internal constructor(config: K8sConfig, client: KubernetesClient) : this(config, client, true)

    private var client: KubernetesClient? = null
    private val podNames = ConcurrentHashMap<String, String>()

    init {
        try {
            client = providedClient ?: createKubernetesClient()
            log("Connected to Kubernetes cluster", LogLevel.SUCCESS)

            // Clean up orphaned services from previous crashes
            client?.let { cleanupOrphanedServices(it, config.namespace, config.service) }
        } catch (e: Exception) {
            log("Failed to connect to Kubernetes cluster: ${e.message}. K8s runtime will be unavailable.", LogLevel.WARNING)
        }
    }

    private fun requireClient(): KubernetesClient {
        return client ?: throw IllegalStateException(
            "Kubernetes client is not available. Ensure the node is running in a cluster or kubeConfigPath/masterUrl is configured."
        )
    }

    override fun start(
        instanceId: String,
        workingDir: Path,
        port: Int,
        command: String,
        ramMB: Int,
        cpu: Int,
        configuration: gg.scala.universe.schema.Configuration,
        environmentVariables: Map<String, String>?,
    ): ProcessHandle {
        val k8s = requireClient()
        val podName = "universe-$instanceId"
        val namespace = config.namespace

        // Remove stale pod if it exists
        removePodIfExists(k8s, namespace, podName)

        val labels = mutableMapOf(
            "app" to "universe",
            "universe-instance-id" to instanceId,
            "configuration" to configuration.name
        )
        labels.putAll(config.labels)

        val containerBuilder = ContainerBuilder()
            .withName("main")
            .withImage(resolveImage(config.image, environmentVariables))
            .withImagePullPolicy(config.imagePullPolicy)
            .withCommand("sh", "-c", command)
            .withWorkingDir(config.workingDir)
            .withStdin(true)
            .withTty(true)
            .withPorts(K8sPortSpec.build(port, configuration.additionalPorts, config.hostPortBindAddress))

        // Add additional ports from the instance configuration (e.g. voice chat, metrics)
        configuration.additionalPorts.forEach { ap ->
            val proto = if (ap.protocol.equals("udp", ignoreCase = true)) "UDP" else "TCP"
            log("K8s pod '$podName' additional port: ${ap.port}/$proto${if (ap.name.isNotBlank()) " (${ap.name})" else ""}")
        }

        // Working directory volume: hostPath for local, emptyDir for cloud
        val workVolumeName = "universe-workdir-$instanceId"
        val workVolume: io.fabric8.kubernetes.api.model.Volume?

        // Check if user-provided volumeMounts already cover the working directory
        val userCoversWorkDir = config.volumeMounts.any { vm ->
            val mountPath = vm.mountPath.removeSuffix("/")
            val workDir = config.workingDir.removeSuffix("/")
            workDir == mountPath || workDir.startsWith("$mountPath/")
        }

        if (userCoversWorkDir) {
            // User is providing their own storage for the working directory (e.g., shared PVC)
            workVolume = null
            log("User-provided volumeMount covers working directory '${config.workingDir}'. " +
                "Skipping default work volume.")
        } else if (config.hostDataPath != null) {
            // Local mode: Universe is in Docker on the same host as the K8s node
            val hostWorkingDir = workingDir.toAbsolutePath().normalize().toString().replaceFirst(
                "/data", config.hostDataPath, ignoreCase = false
            )
            workVolume = VolumeBuilder()
                .withName(workVolumeName)
                .withNewHostPath()
                    .withPath(hostWorkingDir)
                .endHostPath()
                .build()
            containerBuilder.addNewVolumeMount()
                .withName(workVolumeName)
                .withMountPath(config.workingDir)
                .endVolumeMount()
            log("Mounting host working dir '$hostWorkingDir' into pod at '${config.workingDir}' (local mode)")
        } else {
            // Cloud mode: K8s nodes are separate from Universe host. Use emptyDir.
            workVolume = VolumeBuilder()
                .withName(workVolumeName)
                .withNewEmptyDir()
                .endEmptyDir()
                .build()
            containerBuilder.addNewVolumeMount()
                .withName(workVolumeName)
                .withMountPath(config.workingDir)
                .endVolumeMount()
            log("Using emptyDir for pod working directory at '${config.workingDir}' (cloud mode). " +
                "Ensure your container image or init container provides the required files.", LogLevel.WARNING)
        }

        // Resource limits
        if (ramMB > 0 || cpu > 0) {
            val limits = java.util.HashMap<String, Quantity>()
            val requests = java.util.HashMap<String, Quantity>()
            if (ramMB > 0) {
                val q = Quantity("${ramMB}M")
                limits["memory"] = q
                requests["memory"] = q
                log("K8s pod '$podName' memory limit: ${ramMB}M")
            }
            if (cpu > 0) {
                // cpu units: 100 = 1 core. K8s uses millicores (1000m = 1 core)
                val millicores = cpu * 10
                val q = Quantity("${millicores}m")
                limits["cpu"] = q
                requests["cpu"] = q
                log("K8s pod '$podName' CPU limit: $cpu units (${millicores}m / ${millicores / 1000.0} cores)")
            }
            val resources = ResourceRequirementsBuilder().apply {
                limits.forEach { (k, v) -> addToLimits(k, v) }
                requests.forEach { (k, v) -> addToRequests(k, v) }
            }.build()
            containerBuilder.withResources(resources)
        }

        val hostPortBindAddress = K8sPortSpec.resolveHostIp(config.hostPortBindAddress)
        log("K8s pod '$podName' hostPort bind address: ${hostPortBindAddress ?: "wildcard (hostIP omitted)"}")

        // Environment variables
        val allEnv = mutableMapOf<String, String>()
        allEnv.putAll(config.env)
        if (!environmentVariables.isNullOrEmpty()) {
            allEnv.putAll(environmentVariables)
        }
        if (allEnv.isNotEmpty()) {
            containerBuilder.withEnv(allEnv.map { (k, v) ->
                EnvVarBuilder().withName(k).withValue(v).build()
            })
        }

        // Volume mounts
        if (config.volumeMounts.isNotEmpty()) {
            containerBuilder.withVolumeMounts(config.volumeMounts.map { vm ->
                VolumeMountBuilder()
                    .withName(vm.name)
                    .withMountPath(vm.mountPath)
                    .withReadOnly(vm.readOnly)
                    .build()
            })
        }

        val container = containerBuilder.build()

        // Build init containers for S3 template download in cloud mode
        val initContainers = buildS3InitContainers(configuration.templateInstallationConfig, workVolumeName, workVolume)

        val volumes = config.volumes.map { vol ->
            VolumeBuilder().withName(vol.name).apply {
                when {
                    vol.hostPath != null -> withNewHostPath()
                        .withPath(vol.hostPath)
                        .endHostPath()
                    vol.emptyDir -> withNewEmptyDir().endEmptyDir()
                    vol.configMapName != null -> withNewConfigMap()
                        .withName(vol.configMapName)
                        .endConfigMap()
                    vol.secretName != null -> withNewSecret()
                        .withSecretName(vol.secretName)
                        .endSecret()
                    vol.claimName != null -> withNewPersistentVolumeClaim()
                        .withClaimName(vol.claimName)
                        .endPersistentVolumeClaim()
                }
            }.build()
        }

        val podSpecBuilder = PodSpecBuilder()
            .withRestartPolicy(config.restartPolicy)
            .withContainers(container)
            .withVolumes(listOfNotNull(workVolume) + volumes)
        if (initContainers.isNotEmpty()) {
            podSpecBuilder.withInitContainers(initContainers)
            log("Added ${initContainers.size} S3 template init container(s) for instance $instanceId")
        }
        if (config.serviceAccount != null) {
            podSpecBuilder.withServiceAccountName(config.serviceAccount)
        }

        if (config.tolerations.isNotEmpty()) {
            podSpecBuilder.withTolerations(config.tolerations.map { t ->
                TolerationBuilder()
                    .withKey(t.key)
                    .withOperator(t.operator)
                    .withValue(t.value)
                    .withEffect(t.effect)
                    .build()
            })
        }

        val podSpec = podSpecBuilder.build()
        if (config.nodeSelector.isNotEmpty()) {
            podSpec.nodeSelector = java.util.HashMap<String, String>(config.nodeSelector)
        }

        val podBuilder = PodBuilder()
            .withNewMetadata()
                .withName(podName)
                .withNamespace(namespace)
                .addToLabels(labels)
                .addToAnnotations(config.annotations)
            .endMetadata()
            .withSpec(podSpec)

        val pod = k8s.resource(podBuilder.build()).create()
        podNames[instanceId] = podName

        // Create Service for in-cluster connectivity (DNS / routing)
        createInstanceService(k8s, config.service, namespace, podName, instanceId, port, labels, pod.metadata.uid)

        // Wait for pod to be running
        val started = waitForPodPhase(k8s, namespace, podName, "Running", config.timeoutSeconds)
        if (!started) {
            // Try to fetch pod status for diagnostics
            val status = k8s.pods().inNamespace(namespace).withName(podName).get()?.status
            val phase = status?.phase ?: "unknown"
            val containerStatus = status?.containerStatuses?.firstOrNull()
            val reason = containerStatus?.state?.waiting?.reason ?: containerStatus?.state?.terminated?.reason ?: "unknown"
            val message = containerStatus?.state?.waiting?.message ?: containerStatus?.state?.terminated?.message ?: ""
            throw RuntimeException(
                "K8s pod '$podName' failed to start within ${config.timeoutSeconds}s. " +
                "Phase: $phase, Reason: $reason${if (message.isNotBlank()) ", Message: $message" else ""}. " +
                "Command: $command"
            )
        }

        log("Started K8s pod '$podName' for instance $instanceId on port $port", LogLevel.SUCCESS)

        return K8sProcessHandle(podName, namespace, k8s)
    }

    override fun stop(instanceId: String) {
        val k8s = requireClient()
        val podName = discoverPodName(k8s, instanceId)
        if (podName != null) {
            var gracefulFailure: Exception? = null
            try {
                k8s.pods().inNamespace(config.namespace).withName(podName)
                    .withTimeoutInMillis(60.seconds.inWholeMilliseconds).delete()
            } catch (e: Exception) {
                gracefulFailure = e
                log("Pod '$podName' didn't terminate gracefully, force deleting...", LogLevel.WARNING)
                try {
                    k8s.pods().inNamespace(config.namespace).withName(podName)
                        .withGracePeriod(0).delete()
                } catch (forceFailure: Exception) {
                    throw IllegalStateException(
                        "Failed to delete K8s pod '$podName' for instance $instanceId",
                        forceFailure
                    ).also { it.addSuppressed(gracefulFailure) }
                }
            }

            if (!waitForPodAbsent(k8s, config.namespace, podName, config.timeoutSeconds)) {
                try {
                    k8s.pods().inNamespace(config.namespace).withName(podName)
                        .withGracePeriod(0).delete()
                } catch (forceFailure: Exception) {
                    throw IllegalStateException("K8s pod '$podName' still exists and force deletion failed", forceFailure)
                }
                check(waitForPodAbsent(k8s, config.namespace, podName, config.timeoutSeconds)) {
                    "K8s pod '$podName' still exists after confirmed deletion attempts"
                }
            }
            podNames.remove(instanceId, podName)
        }

        val serviceName = "universe-$instanceId"
        try {
            k8s.services().inNamespace(config.namespace).withName(serviceName).delete()
        } catch (failure: Exception) {
            throw IllegalStateException("Failed to delete K8s service '$serviceName'", failure)
        }
        check(waitForServiceAbsent(k8s, config.namespace, serviceName, config.timeoutSeconds)) {
            "K8s service '$serviceName' still exists after deletion"
        }
        log("Stopped K8s resources for instance $instanceId")
    }

    override fun inspectRecovered(
        instanceId: String,
        processPid: Long?,
        workingDirectory: Path
    ): RuntimeResourceState {
        val k8s = try { requireClient() } catch (_: Exception) { return RuntimeResourceState.UNKNOWN }
        return try {
            val podName = discoverPodName(k8s, instanceId)
                ?: return inspectRecoveredService(k8s, instanceId)
            val pod = k8s.pods().inNamespace(config.namespace).withName(podName).get()
                ?: return inspectRecoveredService(k8s, instanceId)
            podNames[instanceId] = podName
            when (pod.status?.phase) {
                "Running" -> RuntimeResourceState.RUNNING
                "Pending" -> RuntimeResourceState.PRESENT_TRANSITIONAL
                "Succeeded", "Failed" -> RuntimeResourceState.TERMINAL
                else -> RuntimeResourceState.UNKNOWN
            }
        } catch (_: Exception) {
            RuntimeResourceState.UNKNOWN
        }
    }

    private fun inspectRecoveredService(
        k8s: KubernetesClient,
        instanceId: String
    ): RuntimeResourceState {
        val service = k8s.services().inNamespace(config.namespace)
            .withName("universe-$instanceId").get()
        return if (service == null) RuntimeResourceState.ABSENT
        else RuntimeResourceState.CLEANUP_REQUIRED
    }

    override fun getHostAddress(instanceId: String): String {
        val k8s = client ?: return ""
        val podName = try { discoverPodName(k8s, instanceId) } catch (_: Exception) { null } ?: return ""
        return try {
            k8s.pods().inNamespace(config.namespace).withName(podName).get()
                ?.status?.podIP ?: ""
        } catch (_: Exception) {
            ""
        }
    }

    override fun getLogs(instanceId: String, lines: Int): List<String> {
        val podName = podNames[instanceId] ?: return emptyList()
        val k8s = client ?: return emptyList()
        return try {
            k8s.pods().inNamespace(config.namespace).withName(podName)
                .tailingLines(lines)
                .log
                .lines()
                .filter { it.isNotBlank() }
        } catch (e: KubernetesClientException) {
            // A missing pod (404) is expected once an instance has stopped or restarted — its
            // logs are simply gone. Log at DEBUG so a live-log stream tailing a just-deleted pod
            // cannot flood the console with the same "not found" warning on every poll. Genuine
            // failures (auth, connectivity, etc.) still surface as warnings.
            if (e.code == 404) {
                log("No K8s logs for instance $instanceId: pod '$podName' is gone", LogLevel.DEBUG)
            } else {
                log("Failed to get K8s logs for instance $instanceId: ${e.message}", LogLevel.WARNING)
            }
            emptyList()
        } catch (e: Exception) {
            log("Failed to get K8s logs for instance $instanceId: ${e.message}", LogLevel.WARNING)
            emptyList()
        }
    }

    override fun executeCommand(instanceId: String, command: String) {
        val podName = podNames[instanceId]
            ?: return log("No K8s pod found for instance $instanceId", LogLevel.WARNING)
        val k8s = client ?: return

        try {
            val commandBytes = (command + "\n").toByteArray()
            val inputStream = java.io.ByteArrayInputStream(commandBytes)

            k8s.pods().inNamespace(config.namespace).withName(podName)
                .readingInput(inputStream)
                .withTTY()
                .attach()
                .use { Thread.sleep(500) }

            log("Executed command in K8s pod '$podName' for instance $instanceId: $command")
        } catch (e: Exception) {
            log("Failed to execute command in K8s pod '$podName' for instance $instanceId: ${e.message}", LogLevel.ERROR)
        }
    }

    override fun isRunning(instanceId: String): Boolean {
        val k8s = requireClient()
        val podName = discoverPodName(k8s, instanceId) ?: return false
        val pod = k8s.pods().inNamespace(config.namespace).withName(podName).get()
        if (pod == null) {
            podNames.remove(instanceId, podName)
            return false
        }
        return pod.status?.phase == "Running"
    }

    override fun listRunningInstances(): List<String> {
        val k8s = requireClient()
        return k8s.pods().inNamespace(config.namespace)
                .withLabel("app", "universe")
                .list().items
                .mapNotNull { pod ->
                    pod.metadata?.labels?.get("universe-instance-id")?.also { id ->
                        pod.metadata?.name?.let { podName -> podNames[id] = podName }
                    }
                }
    }

    /**
     * Resolves the container image to use. If `CUSTOM_IMAGE` is present in
     * the environment variables, it overrides the configured image.
     */
    private fun resolveImage(default: String, envVars: Map<String, String>?): String {
        val customImage = envVars?.get("CUSTOM_IMAGE") ?: return default
        log("CUSTOM_IMAGE override detected: '$customImage'")
        return customImage
    }

    private fun removePodIfExists(k8s: KubernetesClient, namespace: String, name: String) {
        val existing = k8s.pods().inNamespace(namespace).withName(name).get()
        if (existing != null) {
            k8s.pods().inNamespace(namespace).withName(name).delete()
            if (!waitForPodAbsent(k8s, namespace, name, config.timeoutSeconds)) {
                throw IllegalStateException("Stale K8s pod '$name' could not be removed before startup")
            }
        }
    }

    private fun discoverPodName(k8s: KubernetesClient, instanceId: String): String? {
        podNames[instanceId]?.let { mapped ->
            if (k8s.pods().inNamespace(config.namespace).withName(mapped).get() != null) return mapped
            podNames.remove(instanceId, mapped)
        }
        val canonical = "universe-$instanceId"
        if (k8s.pods().inNamespace(config.namespace).withName(canonical).get() != null) {
            podNames[instanceId] = canonical
            return canonical
        }
        val discovered = k8s.pods().inNamespace(config.namespace)
            .withLabel("app", "universe")
            .withLabel("universe-instance-id", instanceId)
            .list().items.firstOrNull()?.metadata?.name
        if (discovered != null) podNames[instanceId] = discovered
        return discovered
    }

    private fun waitForPodAbsent(
        k8s: KubernetesClient,
        namespace: String,
        name: String,
        timeoutSeconds: Int
    ): Boolean {
        val deadline = System.currentTimeMillis() + timeoutSeconds.coerceAtLeast(1) * 1000L
        do {
            if (k8s.pods().inNamespace(namespace).withName(name).get() == null) return true
            Thread.sleep(100)
        } while (System.currentTimeMillis() < deadline)
        return false
    }

    private fun waitForServiceAbsent(
        k8s: KubernetesClient,
        namespace: String,
        name: String,
        timeoutSeconds: Int
    ): Boolean {
        val deadline = System.currentTimeMillis() + timeoutSeconds.coerceAtLeast(1) * 1000L
        do {
            if (k8s.services().inNamespace(namespace).withName(name).get() == null) return true
            Thread.sleep(100)
        } while (System.currentTimeMillis() < deadline)
        return k8s.services().inNamespace(namespace).withName(name).get() == null
    }

    private fun waitForPodPhase(k8s: KubernetesClient, namespace: String, name: String, targetPhase: String, timeoutSeconds: Int): Boolean {
        val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
        while (System.currentTimeMillis() < deadline) {
            val pod = k8s.pods().inNamespace(namespace).withName(name).get()
            val phase = pod?.status?.phase
            if (phase == targetPhase) {
                return true
            }
            if (phase == "Failed" || phase == "Succeeded") {
                return false
            }
            Thread.sleep(500)
        }
        return false
    }

    private fun createKubernetesClient(): KubernetesClient {
        val baseConfig = when {
            config.kubeConfigPath != null -> {
                val content = java.nio.file.Files.readString(java.nio.file.Path.of(config.kubeConfigPath))
                io.fabric8.kubernetes.client.Config.fromKubeconfig(content)
            }
            config.masterUrl != null -> {
                ConfigBuilder().withMasterUrl(config.masterUrl).build()
            }
            else -> {
                // Autoconfigure: tries KUBECONFIG env var, then ~/.kube/config, then in-cluster config
                io.fabric8.kubernetes.client.Config.autoConfigure(null)
            }
        }

        // When running inside Docker, localhost/127.0.0.1 in the kubeconfig points to the
        // container itself, not the host. Rewrite to host.docker.internal so the container
        // can reach the K8s API server running on the host (e.g., Docker Desktop K8s, minikube tunnel).
        val originalMasterUrl = baseConfig.masterUrl
        val isLocalCluster = originalMasterUrl.contains("127.0.0.1") || originalMasterUrl.contains("localhost")
        if (isRunningInsideDocker() && isLocalCluster) {
            val rewritten = originalMasterUrl
                .replace("127.0.0.1", "host.docker.internal")
                .replace("localhost", "host.docker.internal")
            baseConfig.masterUrl = rewritten
            log("Running inside Docker. Rewrote K8s API server from $originalMasterUrl to $rewritten")
        }

        // Local clusters (minikube, Docker Desktop, kind) use self-signed certs that won't
        // match host.docker.internal after rewrite. Trust all certs for local clusters.
        if (isLocalCluster) {
            baseConfig.isTrustCerts = true
            log("Local K8s cluster detected. Disabling certificate verification.", LogLevel.WARNING)
        }

        // Fail fast: no retries, short connection timeout to avoid retry storm spam
        baseConfig.requestRetryBackoffLimit = 0
        baseConfig.connectionTimeout = 5000
        baseConfig.requestTimeout = 10000
        return KubernetesClientBuilder().withConfig(baseConfig).build()
    }

    /**
     * Detects whether Universe is running inside a Docker container.
     * Checks for the presence of `/.dockerenv` or `docker` in `/proc/1/cgroup`.
     */
    private fun isRunningInsideDocker(): Boolean {
        return try {
            java.nio.file.Files.exists(java.nio.file.Path.of("/.dockerenv")) ||
            java.nio.file.Files.readString(java.nio.file.Path.of("/proc/1/cgroup")).contains("docker")
        } catch (_: Exception) {
            false
        }
    }

    /**
     * Builds init containers that download templates from S3 into the pod's working directory.
     *
     * Only active when:
     * - Cloud mode (hostDataPath is null)
     * - s3TemplateInit is enabled
     * - templateConfig is provided and has templates to install
     *
     * Reads S3 credentials from `./extensions/s3/config.json` if present.
     */
    private fun buildS3InitContainers(
        templateConfig: TemplateInstallationConfig?,
        workVolumeName: String,
        workVolume: io.fabric8.kubernetes.api.model.Volume?
    ): List<Container> {
        if (config.hostDataPath != null || !config.s3TemplateInit || templateConfig == null) {
            return emptyList()
        }

        val templates = resolveTemplateList(templateConfig)
        if (templates.isEmpty()) {
            return emptyList()
        }

        // Read S3 config from the S3 extension's config file
        val s3Config = readS3Config()
        val bucket = config.s3Bucket ?: s3Config?.bucket ?: run {
            log("S3 template init enabled but no S3 bucket configured. " +
                "Set s3Bucket in K8s config or enable the S3 storage extension.", LogLevel.WARNING)
            return emptyList()
        }
        val prefix = config.s3Prefix ?: s3Config?.prefix ?: "templates/"

        // Build AWS env vars from S3 config
        val awsEnv = mutableListOf(
            EnvVarBuilder().withName("AWS_DEFAULT_REGION").withValue(s3Config?.region ?: "us-east-1").build()
        )
        if (s3Config?.accessKey != null) {
            awsEnv.add(EnvVarBuilder().withName("AWS_ACCESS_KEY_ID").withValue(s3Config.accessKey).build())
        }
        if (s3Config?.secretKey != null) {
            awsEnv.add(EnvVarBuilder().withName("AWS_SECRET_ACCESS_KEY").withValue(s3Config.secretKey).build())
        }
        if (s3Config?.endpoint != null) {
            awsEnv.add(EnvVarBuilder().withName("AWS_ENDPOINT_URL").withValue(s3Config.endpoint).build())
        }

        return templates.map { template ->
            val s3Key = "${prefix}${template.group}/${template.name}.zip"
            val zipPath = "/tmp/${template.group}-${template.name}.zip"
            val extractDir = "${config.workingDir}/${template.group}/${template.name}"

            val script = buildString {
                append("mkdir -p '$extractDir' && ")
                append("aws s3 cp 's3://$bucket/$s3Key' '$zipPath' && ")
                append("unzip -o '$zipPath' -d '$extractDir' && ")
                append("rm '$zipPath'")
            }

            ContainerBuilder()
                .withName("init-template-${template.group}-${template.name}")
                .withImage(config.s3InitImage)
                .withImagePullPolicy("IfNotPresent")
                .withCommand("sh", "-c", script)
                .withEnv(awsEnv)
                .apply {
                    // Mount the same work volume so files land in the right place
                    if (workVolume != null) {
                        addNewVolumeMount()
                            .withName(workVolumeName)
                            .withMountPath(config.workingDir)
                            .endVolumeMount()
                    }
                    // Also mount any user-provided volumes (they might contain shared PVC data)
                    config.volumeMounts.forEach { vm ->
                        addNewVolumeMount()
                            .withName(vm.name)
                            .withMountPath(vm.mountPath)
                            .withReadOnly(vm.readOnly)
                            .endVolumeMount()
                    }
                }
                .build()
        }
    }

    /**
     * Resolves the flat list of templates from a [TemplateInstallationConfig].
     */
    private fun resolveTemplateList(config: TemplateInstallationConfig): List<Template> {
        val result = mutableListOf<Template>()
        result.addAll(config.allOf)
        config.oneOf.firstOrNull()?.let { result.add(it) }
        return result.filter { it.storage == "s3" || it.storage == "local" }
    }

    /**
     * Reads S3 configuration from `./extensions/s3/config.json` if it exists.
     * Returns null if the file is missing or unreadable.
     */
    private fun readS3Config(): S3Credentials? {
        return try {
            val path = java.nio.file.Path.of("./extensions/s3/config.json")
            if (!java.nio.file.Files.exists(path)) return null
            val json = java.nio.file.Files.readString(path)
            Gson().fromJson(json, S3Credentials::class.java)
        } catch (e: Exception) {
            log("Failed to read S3 config for init container: ${e.message}", LogLevel.WARNING)
            null
        }
    }

    /**
     * Minimal data class for reading S3 credentials from JSON.
     * Mirrors the structure of [gg.scala.universe.s3.S3Config].
     */
    private data class S3Credentials(
        val bucket: String = "universe-templates",
        val region: String = "us-east-1",
        val endpoint: String? = null,
        val accessKey: String? = null,
        val secretKey: String? = null,
        val prefix: String = "templates/"
    )
}

/**
 * A synthetic [ProcessHandle] that delegates lifecycle operations to a Kubernetes Pod.
 */
private class K8sProcessHandle(
    private val podName: String,
    private val namespace: String,
    private val client: KubernetesClient
) : ProcessHandle {

    override fun pid(): Long {
        // Pods don't have numeric PIDs — hash the pod name for a stable synthetic value.
        return podName.hashCode().toLong()
    }

    override fun parent(): Optional<ProcessHandle> = Optional.empty()
    override fun children(): Stream<ProcessHandle> = Stream.empty()
    override fun descendants(): Stream<ProcessHandle> = Stream.empty()
    override fun info(): ProcessHandle.Info = object : ProcessHandle.Info {
        override fun command(): Optional<String> = Optional.of("k8s:$podName")
        override fun commandLine(): Optional<String> = Optional.of("k8s:$podName")
        override fun arguments(): Optional<Array<String>> = Optional.empty()
        override fun startInstant(): Optional<Instant> = Optional.empty()
        override fun totalCpuDuration(): Optional<Duration> = Optional.empty()
        override fun user(): Optional<String> = Optional.empty()
    }

    override fun onExit(): CompletableFuture<ProcessHandle> {
        val future = CompletableFuture<ProcessHandle>()
        future.complete(this)
        return future
    }

    override fun supportsNormalTermination(): Boolean = true

    override fun destroy(): Boolean {
        return try {
            client.pods().inNamespace(namespace).withName(podName).delete()
            true
        } catch (_: Exception) {
            false
        }
    }

    override fun destroyForcibly(): Boolean {
        return destroy()
    }

    override fun isAlive(): Boolean {
        return try {
            val pod = client.pods().inNamespace(namespace).withName(podName).get()
            pod?.status?.phase == "Running"
        } catch (_: Exception) {
            false
        }
    }

    override fun compareTo(other: ProcessHandle): Int {
        return pid().compareTo(other.pid())
    }
}
