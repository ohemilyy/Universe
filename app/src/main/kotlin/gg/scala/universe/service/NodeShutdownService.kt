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
import java.util.concurrent.CompletableFuture

/**
 * Gracefully stops all instances running on the local node.
 *
 * Called during shutdown (both console `stop` command and JVM shutdown hook)
 * to ensure no orphaned processes, containers, or resource leaks remain.
 */
@Singleton
class NodeShutdownService @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val runtimeRegistry: RuntimeRegistry,
    private val portAllocator: PortAllocator
) {

    /**
     * Stops all instances whose wrapperNodeId matches the local Hazelcast member.
     * Releases ports, node resources, and cleans up working directories.
     */
    fun stopAllLocalInstances() {
        val localNodeId = try {
            hazelcastInstance.cluster.localMember.stableNodeId()
        } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
            log("Hazelcast already shut down, skipping local instance cleanup")
            return
        } catch (_: IllegalStateException) {
            log("Hazelcast not available, skipping local instance cleanup")
            return
        }

        val localInstances = try {
            clusterStateService.getAllInstances()
                .filter { it.wrapperNodeId == localNodeId }
        } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
            log("Hazelcast already shut down, skipping local instance cleanup")
            return
        }

        if (localInstances.isEmpty()) {
            log("No local instances to stop")
            return
        }

        log("Stopping ${localInstances.size} local instance(s) in parallel...")

        val futures = localInstances.map { instance ->
            CompletableFuture.runAsync {
                val config = try {
                    clusterStateService.getConfiguration(instance.configurationName)
                } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                    null
                }
                // Use the runtime stored at instance creation time so config reloads
                // don't cause us to stop instances with the wrong provider.
                val runtimeKey = instance.runtime
                val runtimeProvider = runtimeRegistry.get(runtimeKey)
                    ?: runtimeRegistry.getAll().values.firstOrNull()

                if (runtimeProvider != null) {
                    try {
                        runtimeProvider.stop(instance.id)
                        log("Stopped instance ${instance.id} (runtime=$runtimeKey)")
                    } catch (e: Exception) {
                        log("Failed to stop instance ${instance.id}: ${e.message}", LogLevel.WARNING)
                    }
                }

                // Release port
                portAllocator.release(instance.allocatedPort)

                // Release node resources
                try {
                    clusterStateService.removeNodeResources(localNodeId, instance.allocatedRamMB, instance.allocatedCpu)
                } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                    // Hazelcast down — resources will be cleaned up on next startup
                }

                // Clean up working directory for non-static instances
                if (config?.static != true) {
                    val workingDir = Paths.get("./running/${instance.id}").toAbsolutePath().normalize()
                    try {
                        if (Files.exists(workingDir)) {
                            Files.walk(workingDir)
                                .sorted(Comparator.reverseOrder())
                                .forEach { Files.deleteIfExists(it) }
                            log("Cleaned up working directory for instance ${instance.id}")
                        }
                    } catch (cleanupEx: Exception) {
                        log("Failed to clean up working directory for instance ${instance.id}: ${cleanupEx.message}", LogLevel.WARNING)
                    }
                }

                // Mark as STOPPED
                try {
                    clusterStateService.updateInstanceState(instance.id, InstanceState.STOPPED)
                } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                    // Hazelcast down — state will be reconciled on next startup
                }
            }
        }

        CompletableFuture.allOf(*futures.toTypedArray()).join()
        log("All local instances stopped", LogLevel.SUCCESS)
    }
}
