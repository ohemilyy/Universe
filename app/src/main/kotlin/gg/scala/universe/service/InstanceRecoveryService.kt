package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.util.json.Serializers
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Comparator

/**
 * Recovers instances that were running before a node restart.
 *
 * On startup, this service:
 * 1. Checks Hazelcast for instances assigned to this node and verifies they are still running.
 * 2. Scans the filesystem for [./running/] and [./static/] state files and verifies they are still running.
 * 3. Registers recovered instances in [ClusterStateService] and tracks their resources.
 *
 * Runs before [InstanceCountEnforcer] to prevent duplicate instance creation.
 */
@Singleton
class InstanceRecoveryService @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val runtimeRegistry: RuntimeRegistry,
    private val portAllocator: PortAllocator
) {

    fun recover() {
        val localNodeId = hazelcastInstance.cluster.localMember.uuid.toString()
        log("Recovering instances for node $localNodeId...")

        val recovered = mutableSetOf<String>()

        // 1. Recover from Hazelcast (instances we already knew about)
        val hazelcastInstances = clusterStateService.getAllInstances()
            .filter { it.wrapperNodeId == localNodeId && it.state == InstanceState.ONLINE }

        for (instance in hazelcastInstances) {
            if (verifyAndRegister(instance)) {
                recovered.add(instance.id)
            }
        }

        // 2. Recover from filesystem state files (for full cluster restarts)
        val runningDir = Paths.get("./running").toAbsolutePath().normalize()
        if (Files.exists(runningDir)) {
            Files.list(runningDir).use { stream ->
                stream.filter { Files.isDirectory(it) }
                    .filter { !recovered.contains(it.fileName.toString()) }
                    .forEach { dir ->
                        val stateFile = dir.resolve(".universe-state.json")
                        if (Files.exists(stateFile)) {
                            try {
                                val json = Files.readString(stateFile)
                                val instance = Serializers.GSON.fromJson(json, InstanceInfo::class.java)
                                if (instance != null && verifyAndRegister(instance)) {
                                    recovered.add(instance.id)
                                }
                            } catch (e: Exception) {
                                log("Failed to parse state file in $dir: ${e.message}", LogLevel.WARNING)
                            }
                        }
                    }
            }
        }

        // 3. Check runtime providers for any instances not yet recovered
        for ((runtimeKey, provider) in runtimeRegistry.getAll()) {
            val instanceIds = provider.listRunningInstances()
            for (id in instanceIds) {
                if (recovered.contains(id)) continue

                // Try to find state file for this instance
                val stateFile = runningDir.resolve(id).resolve(".universe-state.json")
                val instance = if (Files.exists(stateFile)) {
                    try {
                        val json = Files.readString(stateFile)
                        Serializers.GSON.fromJson(json, InstanceInfo::class.java)
                    } catch (_: Exception) {
                        null
                    }
                } else {
                    null
                }

                if (instance != null && verifyAndRegister(instance)) {
                    recovered.add(instance.id)
                } else {
                    // Unknown running instance — can't recover without metadata, log and skip
                    log("Found unknown running instance '$id' via runtime '$runtimeKey', skipping recovery", LogLevel.WARNING)
                }
            }
        }

        if (recovered.isEmpty()) {
            log("No instances to recover")
        } else {
            log("Recovered ${recovered.size} instance(s): ${recovered.joinToString(", ")}", LogLevel.SUCCESS)
        }
    }

    /**
     * Verifies that the instance is actually running and registers it.
     * Returns true if successfully recovered.
     */
    private fun verifyAndRegister(instance: InstanceInfo): Boolean {
        val config = clusterStateService.getConfiguration(instance.configurationName)
        // Use the runtime stored at instance creation time so config reloads
        // don't cause us to check the wrong runtime provider.
        val runtimeKey = instance.runtime
        val provider = runtimeRegistry.get(runtimeKey)
            ?: runtimeRegistry.getAll().values.firstOrNull()

        if (provider == null) {
            log("No runtime provider for recovered instance ${instance.id}, marking OFFLINE", LogLevel.WARNING)
            clusterStateService.updateInstanceState(instance.id, InstanceState.OFFLINE)
            return false
        }

        if (!provider.isRunning(instance.id)) {
            log("Instance ${instance.id} is no longer running, cleaning up", LogLevel.WARNING)
            cleanupDeadInstance(instance, config)
            return false
        }

        // Instance is running — register it
        val updated = instance.copy(
            state = InstanceState.ONLINE,
            lastHeartbeat = System.currentTimeMillis()
        )
        if (!clusterStateService.putInstance(updated)) {
            log("Recovery of instance ${instance.id} was superseded by cleanup", LogLevel.WARNING)
            return false
        }

        // Re-allocate port to mark it as used
        portAllocator.reserve(instance.allocatedPort)

        // Track node resources
        clusterStateService.addNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)

        log("Recovered instance ${instance.id} (config=${instance.configurationName}, port=${instance.allocatedPort})", LogLevel.SUCCESS)
        return true
    }

    private fun cleanupDeadInstance(instance: InstanceInfo, config: gg.scala.universe.schema.Configuration?) {
        portAllocator.release(instance.allocatedPort)
        if (config?.static != true) {
            val workingDir = Paths.get("./running/${instance.id}").toAbsolutePath().normalize()
            try {
                if (Files.exists(workingDir)) {
                    Files.walk(workingDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                }
            } catch (_: Exception) {
                // ignored
            }
        }

        clusterStateService.completeInstanceTermination(
            expectedInstance = instance,
            finalState = InstanceState.OFFLINE
        )
    }
}
