package gg.scala.universe.hz.task

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.stableNodeId
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.task.DeployInstanceTask
import gg.scala.universe.task.ExecuteCommandTask
import gg.scala.universe.task.ShutdownNodeTask
import gg.scala.universe.task.StopInstanceTask
import gg.scala.universe.task.UniverseTask
import gg.scala.universe.template.TemplateManager
import gg.scala.universe.template.TemplateVariableRegistry
import gg.scala.universe.util.json.Serializers
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Comparator

@Singleton
class TaskRouter @Inject constructor(
    private val runtimeRegistry: RuntimeRegistry,
    private val clusterStateService: ClusterStateService,
    private val portAllocator: PortAllocator,
    private val templateManager: TemplateManager,
    private val variableRegistry: TemplateVariableRegistry,
    private val hazelcastInstance: HazelcastInstance
) {
    fun route(task: UniverseTask) {
        when (task) {
            is DeployInstanceTask -> handleDeploy(task)
            is StopInstanceTask -> handleStop(task)
            is ExecuteCommandTask -> handleExecute(task)
            is ShutdownNodeTask -> handleShutdown(task)
        }
    }

    private fun handleDeploy(task: DeployInstanceTask) {
        log("Routing deploy task for instance ${task.instanceId}")

        val configuration = clusterStateService.getConfiguration(task.configurationName)
            ?: return log("Configuration ${task.configurationName} not found for instance ${task.instanceId}", LogLevel.ERROR)


        val runtimeProvider = runtimeRegistry.get(configuration.runtime)
            ?: return log("Runtime '${configuration.runtime}' not registered for instance ${task.instanceId}", LogLevel.ERROR)

        val allocatedPort = portAllocator.allocate(configuration.availablePorts)
        if (allocatedPort == null) {
            // No port available: do NOT leave the instance stuck in CREATING. Move it to
            // OFFLINE so it stops counting as active and the enforcer retries it shortly.
            log(
                "No available ports for instance ${task.instanceId} in range " +
                "${configuration.availablePorts.min}-${configuration.availablePorts.max}; marking OFFLINE for retry",
                LogLevel.ERROR
            )
            clusterStateService.updateInstanceState(task.instanceId, InstanceState.OFFLINE)
            return
        }

        val workingDir = if (configuration.static) {
            Paths.get("./static/${configuration.name}").toAbsolutePath().normalize()
        } else {
            Paths.get("./running/${task.instanceId}").toAbsolutePath().normalize()
        }
        workingDir.toFile().mkdirs()

        if (!configuration.static) {
            templateManager.installTemplates(
                configuration = configuration,
                instanceId = task.instanceId,
                allocatedPort = allocatedPort,
                targetDir = workingDir
            )
        }

        // Build variable map for env var replacement
        val variables = variableRegistry.collectVariables(configuration, task.instanceId, allocatedPort)
        val envVars = configuration.environmentVariables.mapValues { (_, value) ->
            var replaced = value
            variables.forEach { (placeholder, replacement) ->
                replaced = replaced.replace(placeholder, replacement)
            }
            replaced
        }

        // Resolve template variables in hostAddress BEFORE passing to runtime provider.
        // Docker/K8s runtimes need the actual IP for port binding, not raw placeholders.
        val resolvedHostAddress = run {
            var addr = configuration.hostAddress
            variables.forEach { (placeholder, replacement) ->
                addr = addr.replace(placeholder, replacement)
            }
            addr
        }
        val resolvedConfiguration = configuration.copy(hostAddress = resolvedHostAddress)

        // Persist the allocated port and resolved host onto the still-CREATING record now,
        // before the blocking start(). Two reasons: `instances list` shows the real port
        // instead of 0 while the deploy is in flight, and — critically — a concurrent stop or
        // the stale-CREATING watchdog releases instance.allocatedPort, which must be the real
        // port rather than the 0 sentinel, otherwise the port leaks for the JVM's lifetime.
        clusterStateService.getInstance(task.instanceId)?.let { existing ->
            clusterStateService.putInstance(
                existing.copy(
                    allocatedPort = allocatedPort,
                    hostAddress = resolvedHostAddress,
                    lastHeartbeat = System.currentTimeMillis()
                )
            )
        }

        val processHandle = try {
            runtimeProvider.start(
                instanceId = task.instanceId,
                workingDir = workingDir,
                port = allocatedPort,
                command = configuration.command,
                ramMB = configuration.ramMB,
                cpu = configuration.cpu,
                configuration = resolvedConfiguration,
                environmentVariables = envVars,
            )
        } catch (e: Exception) {
            val cause = e.cause ?: e
            val reason = "${cause.javaClass.simpleName}: ${cause.message ?: "no details"}"
            log("Failed to start instance ${task.instanceId}: $reason", LogLevel.ERROR)

            // Clean up: remove instance record
            clusterStateService.removeInstance(task.instanceId)

            // Clean up working directory (skip for static instances)
            if (!configuration.static) {
                try {
                    if (Files.exists(workingDir)) {
                        Files.walk(workingDir)
                            .sorted(Comparator.reverseOrder())
                            .forEach { Files.deleteIfExists(it) }
                        log("Cleaned up working directory for instance ${task.instanceId}")
                    }
                } catch (cleanupEx: Exception) {
                    log("Failed to clean up working directory for instance ${task.instanceId}: ${cleanupEx.message}", LogLevel.WARNING)
                }
            }

            portAllocator.release(allocatedPort)
            return
        }

        // Runtime may override the resolved host address (e.g. K8s pod IP).
        val finalHostAddress = runtimeProvider.getHostAddress(task.instanceId)
            .ifBlank { resolvedHostAddress }

        // Update instance info in Hazelcast
        val existing = clusterStateService.getInstance(task.instanceId)
        if (existing != null) {
            clusterStateService.putInstance(
                existing.copy(
                    state = InstanceState.ONLINE,
                    allocatedPort = allocatedPort,
                    processPid = processHandle.pid(),
                    hostAddress = finalHostAddress,
                    runtime = configuration.runtime
                )
            )
        }

        // Track node resources
        val nodeId = existing?.wrapperNodeId ?: task.instanceId
        clusterStateService.addNodeResources(nodeId, configuration.ramMB, configuration.cpu)

        // Write state file for recovery after node restart
        writeStateFile(workingDir, existing?.copy(
            hostAddress = resolvedHostAddress,
            runtime = configuration.runtime
        ) ?: InstanceInfo(
            id = task.instanceId,
            configurationName = configuration.name,
            wrapperNodeId = nodeId,
            hostAddress = resolvedHostAddress,
            allocatedPort = allocatedPort,
            state = InstanceState.ONLINE,
            lastHeartbeat = System.currentTimeMillis(),
            processPid = processHandle.pid(),
            allocatedRamMB = configuration.ramMB,
            allocatedCpu = configuration.cpu,
            runtime = configuration.runtime
        ))

        log("Instance ${task.instanceId} deployed with PID ${processHandle.pid()}", LogLevel.SUCCESS)
    }

    private fun handleStop(task: StopInstanceTask) {
        log("Routing stop task for instance ${task.instanceId}")

        val instance = clusterStateService.getInstance(task.instanceId)
            ?: return log("Instance ${task.instanceId} not found", LogLevel.WARNING)

        // Use the runtime that was stored at instance creation time, not the current config,
        // so config reloads/changes don't break stopping existing instances.
        val runtimeKey = instance.runtime

        val runtimeProvider = runtimeRegistry.get(runtimeKey)
            ?: runtimeRegistry.getAll().values.firstOrNull()
            ?: return log("No runtime provider '$runtimeKey' available to stop instance ${task.instanceId}", LogLevel.ERROR)

        val configuration = clusterStateService.getConfiguration(instance.configurationName)

        // Try graceful shutdown first (e.g., send "stop" to a Minecraft server)
        try {
            if (runtimeProvider.isRunning(task.instanceId)) {
                runtimeProvider.executeCommand(task.instanceId, "stop")
                log("Sent graceful stop command to instance ${task.instanceId}, waiting 30s...")
                Thread.sleep(30000)
            }
        } catch (e: Exception) {
            log("Graceful stop failed for instance ${task.instanceId}: ${e.message}, forcing...", LogLevel.WARNING)
        }

        runtimeProvider.stop(task.instanceId)
        portAllocator.release(instance.allocatedPort)

        // Clean up working directory (skip for static instances)
        if (configuration?.static != true) {
            val workingDir = Paths.get("./running/${task.instanceId}").toAbsolutePath().normalize()
            deleteStateFile(workingDir)
            try {
                if (Files.exists(workingDir)) {
                    Files.walk(workingDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                    log("Cleaned up working directory for instance ${task.instanceId}")
                }
            } catch (cleanupEx: Exception) {
                log("Failed to clean up working directory for instance ${task.instanceId}: ${cleanupEx.message}", LogLevel.WARNING)
            }
        }

        // Release node resources
        clusterStateService.removeNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)

        clusterStateService.updateInstanceState(task.instanceId, InstanceState.STOPPED)
        log("Instance ${task.instanceId} stopped")
    }

    private fun handleExecute(task: ExecuteCommandTask) {
        log("Routing execute command for instance ${task.instanceId}: ${task.command}")

        val instance = clusterStateService.getInstance(task.instanceId)
            ?: return log("Instance ${task.instanceId} not found", LogLevel.WARNING)

        val configuration = clusterStateService.getConfiguration(instance.configurationName)
        val runtimeKey = configuration?.runtime ?: instance.configurationName

        val runtimeProvider = runtimeRegistry.get(runtimeKey)
            ?: runtimeRegistry.getAll().values.firstOrNull()
            ?: return log("No runtime provider available to execute command on instance ${task.instanceId}", LogLevel.ERROR)

        runtimeProvider.executeCommand(task.instanceId, task.command)
    }

    private fun handleShutdown(task: ShutdownNodeTask) {
        log("Routing shutdown task — stopping all local instances and exiting")

        // Stop all instances assigned to this node
        val localStableNodeId = hazelcastInstance.cluster.localMember.stableNodeId()
        val localInstances = clusterStateService.getAllInstances()
            .filter { it.wrapperNodeId == localStableNodeId }
            .filter { it.state == InstanceState.ONLINE || it.state == InstanceState.CREATING }

        localInstances.forEach { instance ->
            try {
                val runtimeProvider = runtimeRegistry.get(instance.runtime)
                    ?: runtimeRegistry.getAll().values.firstOrNull()
                runtimeProvider?.stop(instance.id)
                portAllocator.release(instance.allocatedPort)
                clusterStateService.updateInstanceState(instance.id, InstanceState.STOPPED)
                log("Stopped instance ${instance.id} during shutdown")
            } catch (e: Exception) {
                log("Failed to stop instance ${instance.id} during shutdown: ${e.message}", LogLevel.WARNING)
            }
        }

        // Give Hazelcast a moment to propagate state changes
        Thread.sleep(500)

        log("Node shutdown complete, exiting JVM")
        Runtime.getRuntime().exit(0)
    }

    private fun writeStateFile(workingDir: Path, instance: InstanceInfo) {
        try {
            val stateFile = workingDir.resolve(".universe-state.json")
            val json = Serializers.GSON.toJson(instance)
            Files.writeString(stateFile, json)
        } catch (e: Exception) {
            log("Failed to write state file for instance ${instance.id}: ${e.message}", LogLevel.WARNING)
        }
    }

    private fun deleteStateFile(workingDir: Path) {
        try {
            val stateFile = workingDir.resolve(".universe-state.json")
            Files.deleteIfExists(stateFile)
        } catch (_: Exception) {
            // ignored
        }
    }
}
