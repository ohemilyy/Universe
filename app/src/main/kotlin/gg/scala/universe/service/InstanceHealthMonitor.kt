package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.InstanceWorkspace
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.runtime.RuntimeRecoveryInspector
import gg.scala.universe.runtime.RuntimeResourceState
import gg.scala.universe.schema.InstanceState
import java.nio.file.Files
import java.util.Comparator
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Monitors the health of instances running on this node.
 *
 * Every 5 seconds it checks all ONLINE instances whose wrapperNodeId matches
 * the local Hazelcast member. If an instance is no longer running (process
 * exited, container stopped, tmux/screen session ended), it is marked OFFLINE
 * and resources (port, node RAM/CPU) are released. The working directory is
 * cleaned up for non-static instances.
 */
@Singleton
class InstanceHealthMonitor @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val runtimeRegistry: RuntimeRegistry,
    private val portAllocator: PortAllocator,
    private val workspace: InstanceWorkspace = InstanceWorkspace()
) {
    private val executor = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "universe-health-monitor").apply { isDaemon = true }
    }

    fun start() {
        executor.scheduleAtFixedRate(
            ::checkHealth,
            5,   // initial delay
            5,   // period
            TimeUnit.SECONDS
        )
        log("InstanceHealthMonitor started (interval=5s)", LogLevel.INFO)
    }

    fun stop() {
        executor.shutdown()
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        } catch (_: InterruptedException) {
            executor.shutdownNow()
        }
    }

    internal fun checkHealth() {
        try {
            val localNodeId = hazelcastInstance.cluster.localMember.uuid.toString()
            val instances = clusterStateService.getAllInstances()
                .filter { it.wrapperNodeId == localNodeId && it.state == InstanceState.ONLINE }

            if (instances.isEmpty()) return

            for (instance in instances) {
                try {
                    val config = clusterStateService.getConfiguration(instance.configurationName)
                    val runtimeKey = instance.runtime
                    val runtimeProvider = runtimeRegistry.get(runtimeKey)
                    if (runtimeProvider == null) {
                        log("No runtime provider '$runtimeKey' for ${instance.id}; retaining ownership", LogLevel.WARNING)
                        continue
                    }
                    val workingDirectory = if (config?.static == true) {
                        workspace.staticConfiguration(config.name)
                    } else workspace.dynamicInstance(instance.id)
                    val runtimeState = if (runtimeProvider is RuntimeRecoveryInspector) {
                        runtimeProvider.inspectRecovered(
                            instance.id,
                            instance.processPid,
                            workingDirectory
                        )
                    } else if (runtimeProvider.isRunning(instance.id)) {
                        RuntimeResourceState.RUNNING
                    } else RuntimeResourceState.ABSENT
                    when (runtimeState) {
                        RuntimeResourceState.RUNNING -> Unit
                        RuntimeResourceState.ABSENT -> markOffline(instance, config)
                        RuntimeResourceState.TERMINAL -> {
                            runtimeProvider.stop(instance.id)
                            markOffline(instance, config)
                        }
                        RuntimeResourceState.PRESENT_TRANSITIONAL,
                        RuntimeResourceState.UNKNOWN -> log(
                            "Runtime state for ${instance.id} is $runtimeState; retaining ownership",
                            LogLevel.WARNING
                        )
                    }
                } catch (failure: Exception) {
                    log(
                        "Health discovery failed for ${instance.id}: ${failure.message}; retaining ownership",
                        LogLevel.WARNING
                    )
                }
            }
        } catch (e: Exception) {
            log("InstanceHealthMonitor encountered an error: ${e.message}", LogLevel.ERROR)
        }
    }

    private fun markOffline(instance: gg.scala.universe.schema.InstanceInfo, config: gg.scala.universe.schema.Configuration?) {
        val generation = clusterStateService.getLifecycleGeneration(instance.id)
        if (!clusterStateService.isCurrentLifecycle(
                instance, generation, InstanceState.ONLINE
            )
        ) return
        // Release port
        portAllocator.release(instance.allocatedPort)

        // Clean up working directory for non-static instances
        if (config?.static != true) {
            val workingDir = workspace.dynamicInstance(instance.id)
            try {
                if (Files.exists(workingDir)) {
                    Files.walk(workingDir).use { paths ->
                        paths.sorted(Comparator.reverseOrder())
                            .forEach { Files.deleteIfExists(it) }
                    }
                    log("Cleaned up working directory for dead instance ${instance.id}", LogLevel.INFO)
                }
            } catch (cleanupEx: Exception) {
                log("Failed to clean up working directory for dead instance ${instance.id}: ${cleanupEx.message}", LogLevel.WARNING)
            }
        }

        val completed = clusterStateService.completeInstanceTermination(
            expectedInstance = instance,
            expectedGeneration = generation,
            finalState = InstanceState.OFFLINE
        )
        if (completed) {
            log("Instance ${instance.id} marked OFFLINE and resources released", LogLevel.INFO)
        } else {
            log("Instance ${instance.id} termination was superseded", LogLevel.INFO)
        }
    }
}
