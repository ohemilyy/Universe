package gg.scala.universe.hz.task

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.schema.Configuration
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
            ?: return failQueuedDeployment(
                task,
                "Configuration ${task.configurationName} not found for instance ${task.instanceId}"
            )
        val runtimeProvider = runtimeRegistry.get(configuration.runtime)
            ?: return failQueuedDeployment(
                task,
                "Runtime '${configuration.runtime}' not registered for instance ${task.instanceId}"
            )

        var allocatedPort: Int? = null
        var workingDir: Path? = null
        var runtimeStartAttempted = false
        var resourcesAdded = false
        var resourceNodeId: String? = null

        try {
            allocatedPort = portAllocator.allocate(configuration.availablePorts)
                ?: error(
                    "No available ports for instance ${task.instanceId} in range " +
                        "${configuration.availablePorts.min}-${configuration.availablePorts.max}"
                )

            workingDir = if (configuration.static) {
                Paths.get("./static/${configuration.name}").toAbsolutePath().normalize()
            } else {
                Paths.get("./running/${task.instanceId}").toAbsolutePath().normalize()
            }
            Files.createDirectories(workingDir)

            if (!configuration.static) {
                templateManager.installTemplates(
                    configuration = configuration,
                    instanceId = task.instanceId,
                    allocatedPort = allocatedPort,
                    targetDir = workingDir
                )
            }

            val variables = variableRegistry.collectVariables(configuration, task.instanceId, allocatedPort)
            val envVars = configuration.environmentVariables.mapValues { (_, value) ->
                var replaced = value
                variables.forEach { (placeholder, replacement) ->
                    replaced = replaced.replace(placeholder, replacement)
                }
                replaced
            }
            val resolvedHostAddress = run {
                var address = configuration.hostAddress
                variables.forEach { (placeholder, replacement) ->
                    address = address.replace(placeholder, replacement)
                }
                address
            }
            val resolvedConfiguration = configuration.copy(hostAddress = resolvedHostAddress)

            runtimeStartAttempted = true
            val processHandle = runtimeProvider.start(
                instanceId = task.instanceId,
                workingDir = workingDir,
                port = allocatedPort,
                command = configuration.command,
                ramMB = configuration.ramMB,
                cpu = configuration.cpu,
                configuration = resolvedConfiguration,
                environmentVariables = envVars,
            )

            val finalHostAddress = runtimeProvider.getHostAddress(task.instanceId)
                .ifBlank { resolvedHostAddress }
            val queued = clusterStateService.getInstance(task.instanceId)
                ?: error("Queued instance ${task.instanceId} disappeared during deployment")
            val online = queued.copy(
                state = InstanceState.ONLINE,
                allocatedPort = allocatedPort,
                processPid = processHandle.pid(),
                hostAddress = finalHostAddress,
                runtime = configuration.runtime
            )
            clusterStateService.putInstance(online)

            resourceNodeId = online.wrapperNodeId
            clusterStateService.addNodeResources(resourceNodeId, configuration.ramMB, configuration.cpu)
            resourcesAdded = true

            writeStateFile(workingDir, online)
            log("Instance ${task.instanceId} deployed with PID ${processHandle.pid()}", LogLevel.SUCCESS)
        } catch (failure: Exception) {
            if (runtimeStartAttempted) {
                try {
                    runtimeProvider.stop(task.instanceId)
                } catch (stopFailure: Exception) {
                    log(
                        "Failed to stop runtime after deployment failure for ${task.instanceId}: ${stopFailure.message}",
                        LogLevel.WARNING
                    )
                }
            }
            allocatedPort?.let(portAllocator::release)
            if (resourcesAdded && resourceNodeId != null) {
                clusterStateService.removeNodeResources(resourceNodeId, configuration.ramMB, configuration.cpu)
            }
            if (!configuration.static && workingDir != null) {
                cleanupWorkingDirectory(task.instanceId, workingDir)
            }
            clusterStateService.removeInstance(task.instanceId)

            val cause = failure.cause ?: failure
            val reason = "${cause.javaClass.simpleName}: ${cause.message ?: "no details"}"
            log("Failed to deploy instance ${task.instanceId}: $reason; removed queued instance", LogLevel.ERROR)
        }
    }

    private fun failQueuedDeployment(task: DeployInstanceTask, reason: String) {
        clusterStateService.removeInstance(task.instanceId)
        log("$reason; removed queued instance", LogLevel.ERROR)
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

        if (!task.force) {
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
        }

        runtimeProvider.stop(task.instanceId)
        portAllocator.release(instance.allocatedPort)
        cleanupWorkingDirectoryAndResources(instance, configuration)
        clusterStateService.updateInstanceState(task.instanceId, InstanceState.STOPPED)
        log("Instance ${task.instanceId} stopped")

        if (task.restart) {
            val queued = instance.copy(
                state = InstanceState.CREATING,
                allocatedPort = 0,
                processPid = null,
                lastHeartbeat = System.currentTimeMillis()
            )
            clusterStateService.putInstance(queued)
            handleDeploy(DeployInstanceTask(queued.id, queued.configurationName))
        }
    }

    private fun cleanupWorkingDirectoryAndResources(
        instance: InstanceInfo,
        configuration: Configuration?
    ) {
        if (configuration?.static != true) {
            val workingDir = Paths.get("./running/${instance.id}").toAbsolutePath().normalize()
            cleanupWorkingDirectory(instance.id, workingDir)
        }

        clusterStateService.removeNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)
    }

    private fun cleanupWorkingDirectory(instanceId: String, workingDir: Path) {
        deleteStateFile(workingDir)
        try {
            if (Files.exists(workingDir)) {
                Files.walk(workingDir).use { paths ->
                    paths.sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                }
                log("Cleaned up working directory for instance $instanceId")
            }
        } catch (cleanupFailure: Exception) {
            log(
                "Failed to clean up working directory for instance $instanceId: ${cleanupFailure.message}",
                LogLevel.WARNING
            )
        }
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
        val localInstances = clusterStateService.getAllInstances()
            .filter { it.wrapperNodeId == hazelcastInstance.cluster.localMember.uuid.toString() }
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
