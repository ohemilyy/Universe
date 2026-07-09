package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.stableNodeId
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.schema.InstanceState
import java.nio.file.Files
import java.nio.file.Paths
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
    private val portAllocator: PortAllocator
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

    private fun checkHealth() {
        try {
            val localNodeId = hazelcastInstance.cluster.localMember.stableNodeId()
            val instances = clusterStateService.getAllInstances()
                .filter { it.wrapperNodeId == localNodeId && it.state == InstanceState.ONLINE }

            if (instances.isEmpty()) return

            for (instance in instances) {
                val config = clusterStateService.getConfiguration(instance.configurationName)
                // Use the runtime stored at instance creation time so config reloads
                // don't cause us to check the wrong runtime provider.
                val runtimeKey = instance.runtime
                val runtimeProvider = runtimeRegistry.get(runtimeKey)

                if (runtimeProvider == null) {
                    log("No runtime provider '$runtimeKey' for instance ${instance.id}, marking OFFLINE", LogLevel.WARNING)
                    markOffline(instance, config)
                    continue
                }

                if (!runtimeProvider.isRunning(instance.id)) {
                    log("Instance ${instance.id} is no longer running (runtime=$runtimeKey), marking OFFLINE", LogLevel.WARNING)
                    markOffline(instance, config)
                }
            }
        } catch (e: Exception) {
            log("InstanceHealthMonitor encountered an error: ${e.message}", LogLevel.ERROR)
        }
    }

    private fun markOffline(instance: gg.scala.universe.schema.InstanceInfo, config: gg.scala.universe.schema.Configuration?) {
        // Release port
        portAllocator.release(instance.allocatedPort)

        // Release node resources
        clusterStateService.removeNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)

        // Clean up working directory for non-static instances
        if (config?.static != true) {
            val workingDir = Paths.get("./running/${instance.id}").toAbsolutePath().normalize()
            try {
                if (Files.exists(workingDir)) {
                    Files.walk(workingDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                    log("Cleaned up working directory for dead instance ${instance.id}", LogLevel.INFO)
                }
            } catch (cleanupEx: Exception) {
                log("Failed to clean up working directory for dead instance ${instance.id}: ${cleanupEx.message}", LogLevel.WARNING)
            }
        }

        // Mark OFFLINE with updated heartbeat so we know when it went offline
        val updated = instance.copy(
            state = InstanceState.OFFLINE,
            lastHeartbeat = System.currentTimeMillis()
        )
        clusterStateService.putInstance(updated)
        log("Instance ${instance.id} marked OFFLINE and resources released", LogLevel.INFO)
    }
}
