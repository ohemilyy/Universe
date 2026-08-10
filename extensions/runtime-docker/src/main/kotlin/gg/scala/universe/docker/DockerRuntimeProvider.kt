package gg.scala.universe.docker

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.exception.NotModifiedException
import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientImpl
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.runtime.RuntimeProvider
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

/**
 * [RuntimeProvider] implementation using Docker containers.
 *
 * Each instance runs in a dedicated Docker container. The working directory
 * is bind-mounted into the container. Commands are executed via `docker exec`.
 */
class DockerRuntimeProvider private constructor(
    private val config: DockerConfig,
    providedClient: DockerClient?,
    @Suppress("UNUSED_PARAMETER") initializedWithProvidedClient: Boolean
) : RuntimeProvider {

    constructor(config: DockerConfig) : this(config, null, false)

    internal constructor(
        config: DockerConfig,
        dockerClient: DockerClient
    ) : this(config, dockerClient, true)

    private val dockerClient: DockerClient = providedClient ?: createDockerClient()
    private val containerIds = ConcurrentHashMap<String, String>()

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
        val containerName = "universe-$instanceId"

        // Ensure no stale container with this name exists
        removeContainerIfExists(containerName)

        val imageRef = buildImageRef(resolveImage(config.javaImage, environmentVariables))

        // Ensure image is available locally — pull if missing
        ensureImageAvailable(imageRef)

        // Build port bindings
        val exposedPorts = mutableListOf<ExposedPort>()
        val portBindings = Ports()


        // Determine the bind IP from configuration. Only bind to 0.0.0.0 if explicitly requested.
        val bindIp = configuration.hostAddress.takeIf { it.isNotBlank() } ?: "127.0.0.1"
        val bindToAll = bindIp == "0.0.0.0"

        // Always bind the allocated port (maps host port to same container port)
        val allocatedExposed = ExposedPort.tcp(port)
        exposedPorts.add(allocatedExposed)
        val allocatedBinding = if (bindToAll) {
            Ports.Binding.bindPort(port)
        } else {
            Ports.Binding.bindIpAndPort(bindIp, port)
        }
        portBindings.bind(allocatedExposed, allocatedBinding)

        // Add extra exposed ports from config
        config.exposedPorts.forEach { ep ->
            val exposed = if (ep.protocol.equals("udp", ignoreCase = true)) {
                ExposedPort.udp(ep.containerPort)
            } else {
                ExposedPort.tcp(ep.containerPort)
            }
            exposedPorts.add(exposed)
            portBindings.bind(exposed, Ports.Binding.empty())
        }

        // Add additional ports from the instance configuration (e.g. voice chat, metrics)
        configuration.additionalPorts.forEach { ap ->
            val exposed = if (ap.protocol.equals("udp", ignoreCase = true)) {
                ExposedPort.udp(ap.port)
            } else {
                ExposedPort.tcp(ap.port)
            }
            exposedPorts.add(exposed)
            val additionalBinding = if (bindToAll) {
                Ports.Binding.bindPort(ap.port)
            } else {
                Ports.Binding.bindIpAndPort(bindIp, ap.port)
            }
            portBindings.bind(exposed, additionalBinding)
            log("Docker container '$containerName' additional port: ${ap.port}/${ap.protocol}${if (ap.name.isNotBlank()) " (${ap.name})" else ""}")
        }

        // Build volume binds
        val binds = mutableListOf<Bind>()
        val hostWorkingDir = if (config.hostDataPath != null) {
            // Universe is running inside Docker — bind mounts are resolved on the host filesystem
            workingDir.toAbsolutePath().normalize().toString().replaceFirst(
                "/data", config.hostDataPath, ignoreCase = false
            )
        } else {
            workingDir.toAbsolutePath().normalize().toString()
        }
        binds.add(Bind(hostWorkingDir, Volume(config.containerWorkDir)))

        config.volumes.forEach { vol ->
            binds.add(Bind(vol.hostPath, Volume(vol.containerPath), vol.readOnly))
        }

        config.binds.forEach { bindStr ->
            // Format: "hostPath:containerPath" or "hostPath:containerPath:ro"
            val parts = bindStr.split(":")
            when (parts.size) {
                2 -> binds.add(Bind(parts[0], Volume(parts[1])))
                3 -> binds.add(Bind(parts[0], Volume(parts[1]), parts[2] == "ro"))
            }
        }

        val hostConfig = HostConfig.newHostConfig()
            .withBinds(*binds.toTypedArray())
            .withPortBindings(portBindings)
            .withAutoRemove(config.autoRemove)
            .withNetworkMode(config.network)

        // Apply resource limits if configured (>0)
        if (ramMB > 0) {
            val bytes = ramMB * 1024L * 1024L
            hostConfig.withMemory(bytes)
            hostConfig.withMemorySwap(bytes) // Disable swap
            log("Docker container '$containerName' memory limit: ${ramMB}MB")
        }
        if (cpu > 0) {
            // cpu units: 100 = 1 core worth of CPU time
            val nanoCpus = cpu * 10_000_000L
            hostConfig.withNanoCPUs(nanoCpus)
            log("Docker container '$containerName' CPU limit: $cpu units (${nanoCpus / 1_000_000_000.0} cores)")
        }

        // Wrap command with FIFO pipe so executeCommand can send stdin to the process.
        // exec 3<> keeps the write end open so cat never sees EOF.
        val escaped = command.replace("'", "'\\''")
        val wrapperScript = "rm -f /tmp/universe-cmdpipe 2>/dev/null; mkfifo /tmp/universe-cmdpipe; exec 3<> /tmp/universe-cmdpipe; cat <&3 | $escaped"

        val createCmd = dockerClient.createContainerCmd(imageRef)
            .withName(containerName)
            .withWorkingDir(config.containerWorkDir)
            .withCmd("sh", "-c", wrapperScript)
            .withExposedPorts(*exposedPorts.toTypedArray())
            .withHostConfig(hostConfig)
            .withTty(true)
            .withAttachStdout(true)
            .withAttachStderr(true)

        if (!environmentVariables.isNullOrEmpty()) {
            createCmd.withEnv(environmentVariables.map { (k, v) -> "$k=$v" })
        }

        if (config.user != null) {
            createCmd.withUser(config.user)
        }

        val createResponse = createCmd.exec()

        dockerClient.startContainerCmd(createResponse.id).exec()
        containerIds[instanceId] = createResponse.id

        // Verify the container is actually running (not immediately exited)
        Thread.sleep(1500) // brief grace period for container to start
        val containerInfo = dockerClient.inspectContainerCmd(createResponse.id).exec()
        if (containerInfo.state.running != true) {
            val exitCode = containerInfo.state.exitCodeLong?.toInt() ?: -1
            val logHint = if (config.autoRemove) {
                "Set autoRemove=false in docker config to inspect the failed container."
            } else {
                "Check logs with: docker logs $containerName"
            }
            throw RuntimeException(
                "Docker container '$containerName' exited immediately (code $exitCode). " +
                "Command: $command. " +
                "Common causes: missing jar, wrong working directory, or missing dependencies. " +
                logHint
            )
        }

        log("Started Docker container '$containerName' (id=${createResponse.id}) for instance $instanceId on port $port", LogLevel.SUCCESS)

        // Return a synthetic ProcessHandle that delegates to the container
        return DockerProcessHandle(createResponse.id, dockerClient)
    }

    override fun stop(instanceId: String) {
        var containerId = containerIds[instanceId] ?: findContainerId(instanceId) ?: return
        try {
            deleteContainer(containerId)
            val rediscovered = findContainerId(instanceId)
            if (rediscovered != null) {
                containerId = rediscovered
                deleteContainer(rediscovered)
            }
            check(findContainerId(instanceId) == null) {
                "Docker container 'universe-$instanceId' still exists after deletion"
            }
            containerIds.remove(instanceId)
            log("Stopped Docker container for instance $instanceId")
        } catch (e: Exception) {
            log("Failed to stop Docker container for instance $instanceId: ${e.message}", LogLevel.ERROR)
            throw IllegalStateException(
                "Failed to confirm Docker container teardown for instance $instanceId",
                e
            )
        }
    }

    private fun deleteContainer(containerId: String) {
        try {
            dockerClient.stopContainerCmd(containerId).withTimeout(config.stopTimeout).exec()
        } catch (_: NotModifiedException) {
            // Already stopped; removal still confirms absence.
        } catch (_: NotFoundException) {
            // A canonical-name rediscovery is performed by the caller.
        }
        try {
            dockerClient.removeContainerCmd(containerId).withForce(true).exec()
        } catch (_: NotFoundException) {
            // A canonical-name rediscovery is performed by the caller.
        }
    }

    override fun getHostAddress(instanceId: String): String {
        // Docker containers on the same network can reach each other by container name.
        // External access requires explicit port mapping. Return empty to use configured hostAddress.
        return ""
    }

    override fun getLogs(instanceId: String, lines: Int): List<String> {
        val containerId = containerIds[instanceId] ?: return emptyList()
        return try {
            val logStream = dockerClient.logContainerCmd(containerId)
                .withStdOut(true)
                .withStdErr(true)
                .withTail(lines)
                .exec(object : com.github.dockerjava.api.async.ResultCallback.Adapter<com.github.dockerjava.api.model.Frame>() {
                    val buffer = StringBuilder()
                    override fun onNext(frame: com.github.dockerjava.api.model.Frame) {
                        buffer.append(String(frame.payload, Charsets.UTF_8))
                    }
                })
            logStream.awaitCompletion(5, java.util.concurrent.TimeUnit.SECONDS)
            logStream.close()
            logStream.buffer.toString().lines().filter { it.isNotBlank() }
        } catch (e: Exception) {
            log("Failed to get Docker logs for instance $instanceId: ${e.message}", LogLevel.WARNING)
            emptyList()
        }
    }

    override fun executeCommand(instanceId: String, command: String) {
        val containerId = containerIds[instanceId]
            ?: return log("No Docker container found for instance $instanceId", LogLevel.WARNING)

        try {
            // Write command to the FIFO pipe that feeds stdin to the main process
            val escaped = command.replace("'", "'\\''")
            val exec = dockerClient.execCreateCmd(containerId)
                .withCmd("sh", "-c", "printf '%s\\n' '$escaped' > /tmp/universe-cmdpipe")
                .withAttachStdout(true)
                .withAttachStderr(true)
                .exec()
            dockerClient.execStartCmd(exec.id).exec(com.github.dockerjava.api.async.ResultCallback.Adapter())
            log("Executed command in Docker container for instance $instanceId: $command")
        } catch (e: Exception) {
            log("Failed to execute command in Docker container for instance $instanceId: ${e.message}", LogLevel.ERROR)
        }
    }

    override fun isRunning(instanceId: String): Boolean {
        var containerId = containerIds[instanceId] ?: findContainerId(instanceId) ?: return false
        val info = try {
            dockerClient.inspectContainerCmd(containerId).exec()
        } catch (_: NotFoundException) {
            containerIds.remove(instanceId, containerId)
            containerId = findContainerId(instanceId) ?: return false
            dockerClient.inspectContainerCmd(containerId).exec()
        }
        containerIds[instanceId] = containerId
        return info.state.running ?: error(
            "Docker returned an unknown running state for container 'universe-$instanceId'"
        )
    }

    override fun listRunningInstances(): List<String> {
        return try {
            dockerClient.listContainersCmd()
                .withShowAll(false)
                .exec()
                .mapNotNull { container ->
                    container.names?.firstOrNull()?.removePrefix("/")?.takeIf { it.startsWith("universe-") }
                        ?.removePrefix("universe-")
                }
        } catch (failure: Exception) {
            throw IllegalStateException("Failed to discover Docker instances", failure)
        }
    }

    private fun resolveImage(default: DockerImageConfig, envVars: Map<String, String>?): DockerImageConfig {
        val customImage = envVars?.get("CUSTOM_IMAGE") ?: return default
        log("CUSTOM_IMAGE override detected: '$customImage'")
        // Parse the custom image string into a DockerImageConfig
        val parts = customImage.split("/")
        val (registry, repoWithTag) = if (parts.size >= 3 && parts[0].contains(".")) {
            // Has registry: registry.example.com/org/image:tag
            parts[0] to parts.drop(1).joinToString("/")
        } else {
            // No registry: org/image:tag or just image:tag
            null to parts.joinToString("/")
        }
        val (repo, tag) = if (repoWithTag.contains(":")) {
            val idx = repoWithTag.indexOf(":")
            repoWithTag.substring(0, idx) to repoWithTag.substring(idx + 1)
        } else {
            repoWithTag to "latest"
        }
        return DockerImageConfig(
            repository = repo,
            tag = tag,
            registry = registry
        )
    }

    private fun buildImageRef(image: DockerImageConfig): String {
        val repo = image.repository
        val tag = image.tag
        return if (image.registry != null) {
            "${image.registry}/$repo:$tag"
        } else {
            "$repo:$tag"
        }
    }

    private fun removeContainerIfExists(name: String) {
        try {
            dockerClient.listContainersCmd()
                .withShowAll(true)
                .withNameFilter(listOf(name))
                .exec()
                .firstOrNull()
                ?.let {
                    dockerClient.removeContainerCmd(it.id).withForce(true).exec()
                }
        } catch (_: Exception) {
            // ignored
        }
    }

    private fun findContainerId(instanceId: String): String? {
        val expectedName = "universe-$instanceId"
        return dockerClient.listContainersCmd()
            .withShowAll(true)
            .withNameFilter(listOf(expectedName))
            .exec()
            .firstOrNull { container ->
                container.names?.any { it.removePrefix("/") == expectedName } == true
            }
            ?.id
    }

    internal fun rememberContainerForTest(instanceId: String, containerId: String) {
        containerIds[instanceId] = containerId
    }

    /**
     * Checks if the image exists locally. If not, pulls it from the registry.
     */
    private fun ensureImageAvailable(imageRef: String) {
        val exists = try {
            val images = dockerClient.listImagesCmd().exec()
            images.any { img ->
                img.repoTags?.any { it == imageRef || it.startsWith("$imageRef:") } == true
            }
        } catch (_: Exception) {
            false
        }

        if (exists) {
            return
        }

        log("Image '$imageRef' not found locally, pulling...")

        val latch = java.util.concurrent.CountDownLatch(1)
        var pullError: Throwable? = null

        dockerClient.pullImageCmd(imageRef)
            .exec(object : com.github.dockerjava.api.async.ResultCallback.Adapter<com.github.dockerjava.api.model.PullResponseItem>() {
                override fun onNext(item: com.github.dockerjava.api.model.PullResponseItem) {
                    // Pull progress — silently ignore to avoid spam
                }

                override fun onError(throwable: Throwable) {
                    pullError = throwable
                    latch.countDown()
                }

                override fun onComplete() {
                    latch.countDown()
                }
            })

        latch.await()

        if (pullError != null) {
            throw RuntimeException("Failed to pull Docker image '$imageRef': ${pullError.message}", pullError)
        }

        log("Image '$imageRef' pulled successfully", LogLevel.SUCCESS)
    }

    private fun createDockerClient(): DockerClient {
        val builder = DefaultDockerClientConfig.createDefaultConfigBuilder()

        val dockerHost = config.dockerHost ?: "unix:///var/run/docker.sock"
        builder.withDockerHost(dockerHost)

        if (config.dockerCertPath != null) {
            builder.withDockerCertPath(config.dockerCertPath)
                .withDockerTlsVerify(true)
        }
        if (config.registryUsername != null) {
            builder.withRegistryUsername(config.registryUsername)
        }
        if (config.registryPassword != null) {
            builder.withRegistryPassword(config.registryPassword)
        }
        if (config.registryEmail != null) {
            builder.withRegistryEmail(config.registryEmail)
        }
        if (config.registryUrl != null) {
            builder.withRegistryUrl(config.registryUrl)
        }

        val dockerConfig = builder.build()

        val httpClient = ApacheDockerHttpClient.Builder()
            .dockerHost(dockerConfig.dockerHost)
            .sslConfig(dockerConfig.sslConfig)
            .maxConnections(100)
            .connectionTimeout(Duration.ofSeconds(30))
            .responseTimeout(Duration.ofSeconds(45))
            .build()

        return DockerClientImpl.getInstance(dockerConfig, httpClient)
    }
}

/**
 * A synthetic [ProcessHandle] that delegates lifecycle operations to a Docker container.
 */
private class DockerProcessHandle(
    private val containerId: String,
    private val dockerClient: DockerClient
) : ProcessHandle {

    override fun pid(): Long {
        // Docker container IDs are hex strings, not numeric PIDs.
        // We hash the first 8 chars to produce a stable synthetic PID.
        return containerId.take(8).toLongOrNull(16) ?: 0L
    }

    override fun parent(): Optional<ProcessHandle> = Optional.empty()
    override fun children(): Stream<ProcessHandle> = Stream.empty()
    override fun descendants(): Stream<ProcessHandle> = Stream.empty()
    override fun info(): ProcessHandle.Info = object : ProcessHandle.Info {
        override fun command(): Optional<String> = Optional.of("docker:$containerId")
        override fun commandLine(): Optional<String> = Optional.of("docker:$containerId")
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
            dockerClient.stopContainerCmd(containerId).withTimeout(10).exec()
            true
        } catch (_: Exception) {
            false
        }
    }

    override fun destroyForcibly(): Boolean {
        return try {
            dockerClient.killContainerCmd(containerId).exec()
            true
        } catch (_: Exception) {
            false
        }
    }

    override fun isAlive(): Boolean {
        return try {
            val container = dockerClient.inspectContainerCmd(containerId).exec()
            container.state.running ?: false
        } catch (_: Exception) {
            false
        }
    }

    override fun compareTo(other: ProcessHandle): Int {
        return pid().compareTo(other.pid())
    }
}
