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
import gg.scala.universe.schema.InstanceState
import java.nio.file.Files
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
    private val portAllocator: PortAllocator,
    private val workspace: InstanceWorkspace = InstanceWorkspace(),
    private val lifecycleCoordinator: InstanceLifecycleCoordinator = InstanceLifecycleCoordinator()
) {

    /**
     * Stops all instances whose wrapperNodeId matches the local Hazelcast member.
     * Releases ports, node resources, and cleans up working directories.
     */
    fun stopAllLocalInstances() {
        lifecycleCoordinator.beginShutdown()
        val localNodeId = try {
            hazelcastInstance.cluster.localMember.uuid.toString()
        } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
            log("Hazelcast already shut down, skipping local instance cleanup")
            return
        } catch (_: IllegalStateException) {
            log("Hazelcast not available, skipping local instance cleanup")
            return
        }

        val localInstanceIds = try {
            clusterStateService.getAllInstances()
                .filter { it.wrapperNodeId == localNodeId }
                .filter {
                    it.state == InstanceState.CREATING ||
                        it.state == InstanceState.ONLINE ||
                        it.state == InstanceState.STOPPING
                }
                .map { it.id }
        } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
            log("Hazelcast already shut down, skipping local instance cleanup")
            return
        }

        if (localInstanceIds.isEmpty()) {
            log("No local instances to stop")
            return
        }

        log("Stopping ${localInstanceIds.size} local instance(s) in parallel...")

        val futures = localInstanceIds.map { instanceId ->
            CompletableFuture.runAsync {
                lifecycleCoordinator.withInstance(instanceId) {
                    val latest = clusterStateService.getInstance(instanceId)
                        ?: return@withInstance
                    if (
                        latest.wrapperNodeId != localNodeId ||
                        latest.state !in setOf(
                            InstanceState.CREATING,
                            InstanceState.ONLINE,
                            InstanceState.STOPPING
                        )
                    ) return@withInstance
                    val expectedGeneration = clusterStateService.getLifecycleGeneration(instanceId)
                    val (claimed, claimedGeneration) = clusterStateService.claimForShutdown(
                        latest, expectedGeneration
                    ) ?: return@withInstance
                    val config = try {
                        clusterStateService.getConfiguration(claimed.configurationName)
                    } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                        null
                    }
                    val runtimeProvider = runtimeRegistry.get(claimed.runtime)
                    if (runtimeProvider == null) {
                        log("No runtime available to stop instance ${claimed.id}", LogLevel.WARNING)
                        return@withInstance
                    }
                    try {
                        if (!clusterStateService.isCurrentLifecycle(
                                claimed, claimedGeneration, InstanceState.STOPPING
                            )
                        ) return@withInstance
                        runtimeProvider.stop(claimed.id)
                        log("Stopped instance ${claimed.id} (runtime=${claimed.runtime})")
                    } catch (e: Exception) {
                        log(
                            "Failed to confirm teardown for instance ${claimed.id}: ${e.message}; " +
                                "retaining lifecycle ownership",
                            LogLevel.WARNING
                        )
                        return@withInstance
                    }

                    portAllocator.release(claimed.allocatedPort)
                    if (config?.static != true) {
                        val workingDir = workspace.dynamicInstance(claimed.id)
                        try {
                            if (Files.exists(workingDir)) {
                                Files.walk(workingDir).use { paths ->
                                    paths.sorted(Comparator.reverseOrder())
                                        .forEach { Files.deleteIfExists(it) }
                                }
                                log("Cleaned up working directory for instance ${claimed.id}")
                            }
                        } catch (cleanupEx: Exception) {
                            log("Failed to clean up working directory for instance ${claimed.id}: ${cleanupEx.message}", LogLevel.WARNING)
                        }
                    }

                    try {
                        clusterStateService.completeInstanceTermination(
                            expectedInstance = claimed,
                            expectedGeneration = claimedGeneration,
                            finalState = InstanceState.STOPPED
                        )
                    } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                        // Hazelcast down — state will be reconciled on next startup
                    }
                }
            }
        }

        CompletableFuture.allOf(*futures.toTypedArray()).join()
        log("All local instances stopped", LogLevel.SUCCESS)
    }
}
