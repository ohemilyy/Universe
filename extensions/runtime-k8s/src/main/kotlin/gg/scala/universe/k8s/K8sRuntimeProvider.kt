package gg.scala.universe.k8s

import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.runtime.AdoptedResource
import gg.scala.universe.runtime.NodeAllocatable
import gg.scala.universe.runtime.ReconcileAction
import gg.scala.universe.runtime.ResourceSnapshot
import gg.scala.universe.runtime.ResourceOwnership
import gg.scala.universe.runtime.RuntimeProvider
import gg.scala.universe.runtime.RuntimeReconcile
import gg.scala.universe.runtime.RuntimeReconcileReport
import gg.scala.universe.runtime.RuntimeResources
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

/**
 * [RuntimeProvider] implementation using Kubernetes Pods.
 *
 * Each instance runs in a dedicated Kubernetes Pod. Commands are executed
 * via `kubectl exec` equivalent through the Fabric8 client.
 */
class K8sRuntimeProvider(
    private val config: K8sConfig,
    private val clusterName: String = "",
    private val nodeId: String = ""
) : RuntimeProvider {

    private var client: KubernetesClient? = null
    private val podNames = ConcurrentHashMap<String, String>()

    private companion object {
        // Stable ownership labels. The legacy `app`/`universe-instance-id`/`configuration` labels
        // are kept alongside these so resources created before this scheme are still discovered.
        const val LABEL_MANAGED_BY = "app.kubernetes.io/managed-by"
        const val LABEL_CLUSTER = "universe.cluster"
        const val LABEL_NODE = "universe.node"
        const val LABEL_INSTANCE = "universe.instance"
        const val LABEL_CONFIG = "universe.config"
        const val LABEL_RUNTIME = "universe.runtime"

        const val LEGACY_LABEL_INSTANCE = "universe-instance-id"
        const val LEGACY_LABEL_CONFIG = "configuration"
        const val POD_NAME_PREFIX = "universe-"
    }

    private fun instanceIdOf(labels: Map<String, String>, name: String?): String? =
        ResourceOwnership.instanceId(labels, name, LABEL_INSTANCE, LEGACY_LABEL_INSTANCE, POD_NAME_PREFIX)

    private fun ownedByThisCluster(labels: Map<String, String>): Boolean =
        ResourceOwnership.belongsToCluster(labels, LABEL_CLUSTER, clusterName)

    init {
        try {
            client = createKubernetesClient()
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
            // Legacy labels (kept for backward-compatible discovery of pre-existing resources).
            "app" to "universe",
            LEGACY_LABEL_INSTANCE to instanceId,
            LEGACY_LABEL_CONFIG to configuration.name,
            // Stable ownership labels used for list/adopt/delete scoping.
            LABEL_MANAGED_BY to "universe",
            LABEL_INSTANCE to instanceId,
            LABEL_CONFIG to configuration.name,
            LABEL_RUNTIME to config.factoryName
        )
        if (clusterName.isNotBlank()) labels[LABEL_CLUSTER] = clusterName
        if (nodeId.isNotBlank()) labels[LABEL_NODE] = nodeId
        labels.putAll(config.labels)

        val containerBuilder = ContainerBuilder()
            .withName("main")
            .withImage(resolveImage(config.image, environmentVariables))
            .withImagePullPolicy(config.imagePullPolicy)
            .withCommand("sh", "-c", command)
            .withWorkingDir(config.workingDir)
            .withStdin(true)
            .withTty(true)
            .addNewPort()
                .withContainerPort(port)
                .withHostPort(port)
                .withProtocol("TCP")
            .endPort()

        // Add additional ports from the instance configuration (e.g. voice chat, metrics)
        configuration.additionalPorts.forEach { ap ->
            val proto = if (ap.protocol.equals("udp", ignoreCase = true)) "UDP" else "TCP"
            containerBuilder.addNewPort()
                .withContainerPort(ap.port)
                .withHostPort(ap.port)
                .withProtocol(proto)
                .withName(ap.name.ifBlank { "port-${ap.port}" })
                .endPort()
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
                // cpu units: 100 = 1 core. K8s uses millicores (1000m = 1 core).
                val millicores = RuntimeResources.cpuUnitsToMillicores(cpu)
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

        // Wait for the pod to be Running AND Ready before declaring success. Gating on the
        // Ready condition (not just phase) avoids marking an instance ONLINE before it can
        // actually serve traffic.
        val ready = waitForPodReady(k8s, namespace, podName, config.timeoutSeconds)
        if (!ready) {
            // Capture diagnostics, then delete the pod+service we just created so a failed
            // start never leaks a hostPort-holding zombie that blocks future deploys.
            val status = k8s.pods().inNamespace(namespace).withName(podName).get()?.status
            val phase = status?.phase ?: "unknown"
            val containerStatus = status?.containerStatuses?.firstOrNull()
            val reason = containerStatus?.state?.waiting?.reason ?: containerStatus?.state?.terminated?.reason ?: "unknown"
            val message = containerStatus?.state?.waiting?.message ?: containerStatus?.state?.terminated?.message ?: ""

            log(
                "K8s pod '$podName' (instance $instanceId) did not become ready in ${config.timeoutSeconds}s " +
                "(phase=$phase, reason=$reason). Deleting pod+service to release its port.",
                LogLevel.ERROR
            )
            // Wait for the pod to actually be gone before returning, so the freed hostPort is
            // really available when the caller releases the port and a retry reuses it.
            deletePodServiceAndWait(k8s, namespace, instanceId, podName)
            podNames.remove(instanceId)

            throw RuntimeException(
                "K8s pod '$podName' failed to become ready within ${config.timeoutSeconds}s. " +
                "Phase: $phase, Reason: $reason${if (message.isNotBlank()) ", Message: $message" else ""}. " +
                "Command: $command"
            )
        }

        log("Started K8s pod '$podName' for instance $instanceId on port $port", LogLevel.SUCCESS)

        return K8sProcessHandle(podName, namespace, k8s)
    }

    override fun stop(instanceId: String) {
        val k8s = client ?: return
        // Derive the deterministic pod name when the in-memory cache misses — after a restart,
        // or when a stop races a deploy that has not populated the cache yet — so stop still
        // deletes the real pod/service instead of silently no-op'ing and leaking them.
        val podName = podNames.remove(instanceId) ?: "universe-$instanceId"
        val namespace = config.namespace

        // Delete pod + service and wait for the pod (and its hostPort) to actually be gone.
        deletePodServiceAndWait(k8s, namespace, instanceId, podName)
        log("Stopped K8s pod '$podName' for instance $instanceId")
    }

    override fun getHostAddress(instanceId: String): String {
        val k8s = client ?: return ""
        val podName = podNames[instanceId] ?: "universe-$instanceId"
        return try {
            k8s.pods().inNamespace(config.namespace).withName(podName).get()
                ?.status?.podIP ?: ""
        } catch (_: Exception) {
            ""
        }
    }

    override fun getLogs(instanceId: String, lines: Int): List<String> {
        val k8s = client ?: return emptyList()
        val podName = podNames[instanceId] ?: "universe-$instanceId"
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
        val k8s = client ?: return
        val podName = podNames[instanceId] ?: "universe-$instanceId"

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
        val k8s = client ?: return false
        // Don't depend on the in-memory cache (empty after a restart); the pod name is
        // deterministic. A pod being deleted (deletionTimestamp set) is not "running".
        val podName = podNames[instanceId] ?: "universe-$instanceId"
        return try {
            val pod = k8s.pods().inNamespace(config.namespace).withName(podName).get()
            pod?.status?.phase == "Running" && pod.metadata?.deletionTimestamp == null
        } catch (_: Exception) {
            false
        }
    }

    override fun listRunningInstances(): List<String> {
        val k8s = client ?: return emptyList()
        return try {
            k8s.pods().inNamespace(config.namespace)
                .withLabel("app", "universe")
                .list().items
                .filter { ownedByThisCluster(it.metadata?.labels ?: emptyMap()) }
                .mapNotNull { pod -> instanceIdOf(pod.metadata?.labels ?: emptyMap(), pod.metadata?.name) }
        } catch (_: Exception) {
            emptyList()
        }
    }

    /**
     * Reconciles live Kubernetes pods against the instances Universe still tracks.
     *
     * Orphaned pods (Universe forgot about them after a restart) and dead pods (failed or
     * stuck pending) are deleted so they stop holding hostPorts and node resources. Running,
     * ready pods that Universe still tracks are adopted back into [podNames].
     */
    override fun reconcile(trackedInstanceIds: Set<String>): RuntimeReconcileReport {
        val k8s = client ?: return RuntimeReconcileReport()
        val namespace = config.namespace

        val pods = try {
            k8s.pods().inNamespace(namespace).withLabel("app", "universe").list().items
        } catch (e: Exception) {
            log("K8s reconcile: failed to list pods in '$namespace': ${e.message}", LogLevel.WARNING)
            return RuntimeReconcileReport()
        }

        log("K8s reconcile: examining ${pods.size} universe pod(s) in '$namespace' against ${trackedInstanceIds.size} tracked instance(s)")

        val adopted = mutableListOf<AdoptedResource>()
        val dead = mutableSetOf<String>()
        var orphans = 0
        var deadDeleted = 0
        val now = System.currentTimeMillis()
        val graceMs = config.pendingGraceSeconds * 1000L

        for (pod in pods) {
            val podName = pod.metadata?.name ?: continue
            val labels = pod.metadata?.labels ?: emptyMap()
            // Never touch a pod owned by a different Universe cluster sharing this namespace.
            if (!ownedByThisCluster(labels)) continue
            val id = instanceIdOf(labels, podName)
            val snapshot = podSnapshot(pod, now)
            val tracked = id != null && id in trackedInstanceIds

            when (RuntimeReconcile.classify(snapshot, tracked, graceMs)) {
                ReconcileAction.ADOPT -> if (id != null) {
                    podNames[id] = podName
                    adopted.add(
                        AdoptedResource(
                            instanceId = id,
                            configurationName = labels[LABEL_CONFIG] ?: labels[LEGACY_LABEL_CONFIG],
                            hostAddress = pod.status?.podIP ?: "",
                            port = pod.spec?.containers?.firstOrNull()?.ports?.firstOrNull()?.containerPort ?: 0
                        )
                    )
                }
                ReconcileAction.WAIT -> if (id != null && tracked) {
                    podNames[id] = podName
                }
                ReconcileAction.DELETE_ORPHAN -> {
                    val phase = pod.status?.phase ?: "unknown"
                    deletePodAndService(k8s, namespace, id, podName)
                    orphans++
                    // Per-resource detail is DEBUG so a namespace full of leftovers can't spam
                    // the console with hundreds of lines; the summary below carries the counts.
                    log("K8s reconcile: deleted orphan pod '$podName' (instance '${id ?: "?"}' not tracked, phase=$phase)", LogLevel.DEBUG)
                }
                ReconcileAction.DELETE_DEAD -> {
                    val phase = pod.status?.phase ?: "unknown"
                    deletePodAndService(k8s, namespace, id, podName)
                    deadDeleted++
                    if (id != null) {
                        dead.add(id)
                        podNames.remove(id)
                    }
                    log("K8s reconcile: deleted dead pod '$podName' (instance '${id ?: "?"}', phase=$phase)", LogLevel.DEBUG)
                }
            }
        }

        if (orphans > 0 || deadDeleted > 0) {
            log("K8s reconcile complete: adopted ${adopted.size}, deleted $orphans orphan(s) and $deadDeleted dead pod(s)", LogLevel.WARNING)
        } else {
            log("K8s reconcile complete: adopted ${adopted.size}, nothing to clean up")
        }
        return RuntimeReconcileReport(adopted, dead, orphans, deadDeleted)
    }

    override fun queryNodeAllocatable(): NodeAllocatable? {
        val k8s = client ?: return null
        return try {
            var cpuMillis = 0L
            var memMB = 0L
            for (node in k8s.nodes().list().items) {
                if (node.spec?.unschedulable == true) continue
                val alloc = node.status?.allocatable ?: continue
                cpuMillis += quantityToMillicores(alloc["cpu"])
                memMB += quantityToMB(alloc["memory"])
            }

            // Subtract what live (non-terminated) pods already request — never live usage.
            var usedCpu = 0L
            var usedMem = 0L
            for (pod in k8s.pods().inAnyNamespace().list().items) {
                val phase = pod.status?.phase
                if (phase == "Succeeded" || phase == "Failed") continue
                if (pod.metadata?.deletionTimestamp != null) continue
                pod.spec?.containers?.forEach { c ->
                    val requests = c.resources?.requests
                    usedCpu += quantityToMillicores(requests?.get("cpu"))
                    usedMem += quantityToMB(requests?.get("memory"))
                }
            }
            NodeAllocatable(cpuMillis, memMB, usedCpu, usedMem)
        } catch (e: Exception) {
            log("K8s: failed to query node allocatable: ${e.message}", LogLevel.WARNING)
            null
        }
    }

    private fun podSnapshot(pod: Pod, nowMs: Long): ResourceSnapshot {
        val id = instanceIdOf(pod.metadata?.labels ?: emptyMap(), pod.metadata?.name)
        val phase = pod.status?.phase
        val ready = isPodReady(pod)
        val terminating = pod.metadata?.deletionTimestamp != null
        // Unknown creation time -> treat as young so we never delete a pod we can't age.
        val ageMs = pod.metadata?.creationTimestamp?.let { ts ->
            try { nowMs - java.time.Instant.parse(ts).toEpochMilli() } catch (_: Exception) { 0L }
        } ?: 0L
        return ResourceSnapshot(id, phase, ready, terminating, ageMs)
    }

    private fun quantityToMillicores(q: Quantity?): Long {
        if (q == null) return 0L
        return try {
            (Quantity.getAmountInBytes(q).toDouble() * 1000.0).toLong()
        } catch (_: Exception) {
            0L
        }
    }

    private fun quantityToMB(q: Quantity?): Long {
        if (q == null) return 0L
        return try {
            (Quantity.getAmountInBytes(q).toDouble() / 1_000_000.0).toLong()
        } catch (_: Exception) {
            0L
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
        try {
            val existing = k8s.pods().inNamespace(namespace).withName(name).get()
            if (existing != null) {
                k8s.pods().inNamespace(namespace).withName(name).delete()
                // Wait briefly for deletion
                var attempts = 0
                while (attempts < 20) {
                    if (k8s.pods().inNamespace(namespace).withName(name).get() == null) break
                    Thread.sleep(250)
                    attempts++
                }
            }
        } catch (_: Exception) {
            // ignored
        }
    }

    /**
     * Waits until the pod is Running and reports the Ready condition true, or returns false
     * if it fails/terminates or the timeout elapses.
     */
    private fun waitForPodReady(k8s: KubernetesClient, namespace: String, name: String, timeoutSeconds: Int): Boolean {
        val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
        while (System.currentTimeMillis() < deadline) {
            val pod = k8s.pods().inNamespace(namespace).withName(name).get()
            val phase = pod?.status?.phase
            if (phase == "Running" && isPodReady(pod)) {
                return true
            }
            if (phase == "Failed" || phase == "Succeeded") {
                return false
            }
            Thread.sleep(500)
        }
        return false
    }

    /**
     * Polls until the named pod no longer exists (or is unreadable), up to [timeoutMs].
     * Returns true if the pod is confirmed gone.
     */
    private fun waitForPodGone(k8s: KubernetesClient, namespace: String, name: String, timeoutMs: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val pod = try {
                k8s.pods().inNamespace(namespace).withName(name).get()
            } catch (_: Exception) {
                return true
            }
            if (pod == null) return true
            Thread.sleep(250)
        }
        return false
    }

    private fun isPodReady(pod: Pod): Boolean {
        return pod.status?.conditions?.any { it.type == "Ready" && it.status == "True" } ?: false
    }

    /**
     * Deletes the pod+service and blocks until the pod is actually gone, so its hostPort is
     * released before the caller reuses the port. Force-deletes if the pod is still Terminating
     * after the grace window — a stuck-Terminating pod otherwise holds the hostPort indefinitely.
     * Used by [stop] and the failed-start cleanup path. [reconcile] uses the non-waiting variant
     * so a namespace full of leftovers doesn't serialise hundreds of deletes during startup.
     */
    private fun deletePodServiceAndWait(k8s: KubernetesClient, namespace: String, instanceId: String?, podName: String) {
        deletePodAndService(k8s, namespace, instanceId, podName)
        if (!waitForPodGone(k8s, namespace, podName, 60_000L)) {
            log("Pod '$podName' still terminating after grace window, force deleting...", LogLevel.WARNING)
            try {
                k8s.pods().inNamespace(namespace).withName(podName).withGracePeriod(0).delete()
                waitForPodGone(k8s, namespace, podName, 15_000L)
            } catch (e: Exception) {
                log("Failed to force-delete pod '$podName'${if (instanceId != null) " for instance $instanceId" else ""}: ${e.message}", LogLevel.ERROR)
            }
        }
    }

    /**
     * Best-effort deletion of both the pod and its service for an instance. Used by [reconcile]
     * and as the first step of [deletePodServiceAndWait].
     */
    private fun deletePodAndService(k8s: KubernetesClient, namespace: String, instanceId: String?, podName: String) {
        try {
            k8s.pods().inNamespace(namespace).withName(podName).delete()
        } catch (e: Exception) {
            log("Failed to delete pod '$podName': ${e.message}", LogLevel.WARNING)
        }
        if (instanceId != null) {
            try {
                k8s.services().inNamespace(namespace).withName("universe-$instanceId").delete()
            } catch (_: Exception) {
                // Service may already be gone via ownerReference garbage collection.
            }
        }
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
