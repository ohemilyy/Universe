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
import gg.scala.universe.service.InstanceLifecycleCoordinator
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
import java.util.Comparator

@Singleton
class TaskRouter @Inject constructor(
    private val runtimeRegistry: RuntimeRegistry,
    private val clusterStateService: ClusterStateService,
    private val portAllocator: PortAllocator,
    private val templateManager: TemplateManager,
    private val variableRegistry: TemplateVariableRegistry,
    private val hazelcastInstance: HazelcastInstance,
    private val workspace: InstanceWorkspace = InstanceWorkspace(),
    private val lifecycleCoordinator: InstanceLifecycleCoordinator = InstanceLifecycleCoordinator()
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
        lifecycleCoordinator.withInstance(task.instanceId) {
            handleDeployLocked(task)
        }
    }

    private fun handleDeployLocked(task: DeployInstanceTask) {
        log("Routing deploy task for instance ${task.instanceId}")

        if (lifecycleCoordinator.isQuiescing()) {
            return log("Ignoring deploy task for ${task.instanceId} while node is quiescing", LogLevel.WARNING)
        }
        if (task.expectedGeneration <= 0L) {
            return log("Rejecting legacy deploy token for ${task.instanceId}; lifecycle ownership retained", LogLevel.WARNING)
        }

        val queuedInstance = clusterStateService.getInstance(task.instanceId)
            ?: return log("Queued instance ${task.instanceId} no longer exists", LogLevel.WARNING)
        if (!clusterStateService.isCurrentLifecycle(
                queuedInstance,
                task.expectedGeneration,
                InstanceState.CREATING,
                requireReservation = true
            )
        ) {
            return log("Ignoring stale deploy task for instance ${task.instanceId}", LogLevel.WARNING)
        }

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
        var startupSnapshot: InstanceInfo? = null

        try {
            allocatedPort = portAllocator.allocate(configuration.availablePorts)
                ?: error(
                    "No available ports for instance ${task.instanceId} in range " +
                        "${configuration.availablePorts.min}-${configuration.availablePorts.max}"
                )

            startupSnapshot = clusterStateService.claimDeploymentStartup(
                expected = queuedInstance,
                generation = task.expectedGeneration,
                allocatedPort = allocatedPort,
                runtime = configuration.runtime
            ) ?: error("Queued instance ${task.instanceId} changed before runtime startup claim")

            workingDir = if (configuration.static) {
                workspace.staticConfiguration(configuration.name)
            } else {
                workspace.dynamicInstance(task.instanceId)
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

            if (lifecycleCoordinator.isQuiescing()) {
                error("Node began quiescing before runtime startup")
            }
            if (!clusterStateService.isCurrentLifecycle(
                    startupSnapshot,
                    task.expectedGeneration,
                    InstanceState.CREATING,
                    requireReservation = true
                )
            ) {
                error("Queued instance ${task.instanceId} changed before runtime startup")
            }
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
            val online = startupSnapshot.copy(
                state = InstanceState.ONLINE,
                allocatedPort = allocatedPort,
                processPid = processHandle.pid(),
                hostAddress = finalHostAddress,
                runtime = configuration.runtime
            )
            if (!clusterStateService.promoteCreatingInstance(
                    startupSnapshot,
                    task.expectedGeneration,
                    online
                )
            ) {
                error("Instance ${task.instanceId} changed while the runtime was starting")
            }

            writeStateFile(workingDir, online)
            log("Instance ${task.instanceId} deployed with PID ${processHandle.pid()}", LogLevel.SUCCESS)
        } catch (failure: Exception) {
            var teardownConfirmed = !runtimeStartAttempted
            if (runtimeStartAttempted) {
                try {
                    runtimeProvider.stop(task.instanceId)
                    teardownConfirmed = true
                } catch (stopFailure: Exception) {
                    log(
                        "Failed to confirm runtime teardown after deployment failure for ${task.instanceId}: " +
                            "${stopFailure.message}; retaining lifecycle ownership",
                        LogLevel.ERROR
                    )
                }
            }
            if (!teardownConfirmed) {
                val current = clusterStateService.getInstance(task.instanceId)
                startupSnapshot?.takeIf { current == it }?.let { claimed ->
                    clusterStateService.markDeploymentCleanupRequired(
                        claimed,
                        task.expectedGeneration
                    )
                }
                return
            }
            allocatedPort?.let(portAllocator::release)
            if (!configuration.static && workingDir != null) {
                cleanupWorkingDirectory(task.instanceId, workingDir)
            }
            val current = clusterStateService.getInstance(task.instanceId)
            val currentGeneration = clusterStateService.getLifecycleGeneration(task.instanceId)
            when {
                startupSnapshot == null && current == queuedInstance ->
                    clusterStateService.cancelCreatingInstance(
                        queuedInstance,
                        task.expectedGeneration
                    )
                startupSnapshot != null && current == startupSnapshot ->
                    clusterStateService.cancelCreatingAfterConfirmedTeardown(
                        startupSnapshot,
                        task.expectedGeneration
                    )
                current != null && current.state == InstanceState.STOPPING ->
                    clusterStateService.clearDeploymentCleanup(current, currentGeneration)
                else -> Unit
            }

            val cause = failure.cause ?: failure
            val reason = "${cause.javaClass.simpleName}: ${cause.message ?: "no details"}"
            log("Failed to deploy instance ${task.instanceId}: $reason; removed queued instance", LogLevel.ERROR)
        }
    }

    private fun failQueuedDeployment(task: DeployInstanceTask, reason: String) {
        val queued = clusterStateService.getInstance(task.instanceId)
        if (queued != null) {
            clusterStateService.cancelCreatingInstance(queued, task.expectedGeneration)
        }
        log("$reason; removed queued instance", LogLevel.ERROR)
    }

    private fun handleStop(task: StopInstanceTask) {
        lifecycleCoordinator.withInstance(task.instanceId) {
            handleStopLocked(task)
        }
    }

    private fun handleStopLocked(task: StopInstanceTask) {
        log("Routing stop task for instance ${task.instanceId}")

        if (task.expectedGeneration <= 0L) {
            return log("Rejecting legacy stop token for ${task.instanceId}; lifecycle ownership retained", LogLevel.WARNING)
        }

        val instance = clusterStateService.getInstance(task.instanceId)
            ?: return log("Instance ${task.instanceId} not found", LogLevel.WARNING)
        if (!clusterStateService.isCurrentLifecycle(
                instance,
                task.expectedGeneration,
                InstanceState.STOPPING
            )
        ) {
            return log("Ignoring stale stop task for instance ${task.instanceId}", LogLevel.WARNING)
        }

        // Use the runtime that was stored at instance creation time, not the current config,
        // so config reloads/changes don't break stopping existing instances.
        val runtimeKey = instance.runtime

        val runtimeProvider = runtimeRegistry.get(runtimeKey)
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

        if (!clusterStateService.isCurrentLifecycle(
                instance,
                task.expectedGeneration,
                InstanceState.STOPPING
            )
        ) {
            return log("Stop token for instance ${task.instanceId} changed before teardown", LogLevel.WARNING)
        }
        runtimeProvider.stop(task.instanceId)
        portAllocator.release(instance.allocatedPort)
        cleanupWorkingDirectory(instance, configuration)
        val stopped = if (task.restart) {
            clusterStateService.completeInstanceTerminationForRestart(
                expectedInstance = instance,
                expectedGeneration = task.expectedGeneration,
                nextGeneration = task.expectedGeneration + 1
            )
        } else {
            clusterStateService.completeInstanceTermination(
                expectedInstance = instance,
                expectedGeneration = task.expectedGeneration,
                finalState = InstanceState.STOPPED
            ).takeIf { it }?.let { clusterStateService.getInstance(task.instanceId) }
        }
        if (stopped == null) {
            return log(
                "Stop completion for instance ${task.instanceId} was superseded by reconciliation",
                LogLevel.WARNING
            )
        }
        log("Instance ${task.instanceId} stopped")

        if (task.restart) {
            val nextGeneration = task.expectedGeneration + 1
            val queued = instance.copy(
                state = InstanceState.CREATING,
                allocatedPort = 0,
                processPid = null,
                lastHeartbeat = System.currentTimeMillis()
            )
            if (!clusterStateService.reserveRestartCreating(stopped, queued, nextGeneration)) {
                clusterStateService.releaseTerminalRestartReservation(stopped, nextGeneration)
                return log("Could not reserve resources to restart instance ${task.instanceId}", LogLevel.WARNING)
            }
            handleDeploy(DeployInstanceTask(queued.id, queued.configurationName, nextGeneration))
        }
    }

    private fun cleanupWorkingDirectory(
        instance: InstanceInfo,
        configuration: Configuration?
    ) {
        if (configuration?.static != true) {
            val workingDir = workspace.dynamicInstance(instance.id)
            cleanupWorkingDirectory(instance.id, workingDir)
        }
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
        lifecycleCoordinator.beginShutdown()

        // Stop all instances assigned to this node
        val localInstances = clusterStateService.getAllInstances()
            .filter { it.wrapperNodeId == hazelcastInstance.cluster.localMember.uuid.toString() }
            .filter {
                it.state == InstanceState.ONLINE ||
                    it.state == InstanceState.CREATING ||
                    it.state == InstanceState.STOPPING
            }
            .map { it to clusterStateService.getLifecycleGeneration(it.id) }

        localInstances.forEach { (instance, expectedGeneration) ->
            try {
                lifecycleCoordinator.withInstance(instance.id) {
                    val (claimed, claimedGeneration) = clusterStateService.claimForShutdown(
                        instance, expectedGeneration
                    ) ?: return@withInstance
                    val runtimeProvider = runtimeRegistry.get(claimed.runtime)
                        ?: error("No runtime provider '${claimed.runtime}' available for ${claimed.id}")
                    if (!clusterStateService.isCurrentLifecycle(
                            claimed, claimedGeneration, InstanceState.STOPPING
                        )
                    ) return@withInstance
                    runtimeProvider.stop(claimed.id)
                    portAllocator.release(claimed.allocatedPort)
                    val configuration = clusterStateService.getConfiguration(claimed.configurationName)
                    cleanupWorkingDirectory(claimed, configuration)
                    clusterStateService.completeInstanceTermination(
                        expectedInstance = claimed,
                        expectedGeneration = claimedGeneration,
                        finalState = InstanceState.STOPPED
                    )
                    log("Stopped instance ${claimed.id} during shutdown")
                }
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
