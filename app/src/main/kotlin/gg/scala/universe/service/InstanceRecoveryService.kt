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
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.util.json.Serializers
import java.nio.file.Files
import java.nio.file.Path
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
    private val portAllocator: PortAllocator,
    private val workspace: InstanceWorkspace = InstanceWorkspace()
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
        val runningDir = workspace.runningRoot()
        val staticDir = workspace.staticRoot()
        for (stateRoot in listOf(runningDir, staticDir)) {
            if (!Files.exists(stateRoot)) continue
            Files.list(stateRoot).use { stream ->
                stream.filter { Files.isDirectory(it) }
                    .forEach { dir ->
                        val stateFile = dir.resolve(".universe-state.json")
                        if (Files.exists(stateFile)) {
                            try {
                                val json = Files.readString(stateFile)
                                val instance = Serializers.GSON.fromJson(json, InstanceInfo::class.java)
                                if (
                                    instance != null && instance.id !in recovered &&
                                    verifyAndRegister(instance)
                                ) {
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
            val instanceIds = try {
                provider.listRunningInstances()
            } catch (failure: Exception) {
                log(
                    "Could not discover instances from runtime '$runtimeKey': ${failure.message}",
                    LogLevel.WARNING
                )
                continue
            }
            for (id in instanceIds) {
                if (recovered.contains(id)) continue

                // Try to find state file for this instance
                val stateFile = findStateFile(id, runningDir, staticDir)
                val instance = if (stateFile != null && Files.exists(stateFile)) {
                    try {
                        val json = Files.readString(stateFile)
                        Serializers.GSON.fromJson(json, InstanceInfo::class.java)
                    } catch (_: Exception) {
                        null
                    }
                } else {
                    // Kubernetes workloads can survive a wrapper restart even
                    // without a local state directory. The pod label supplies
                    // the id; reuse its durable master-side metadata.
                    clusterStateService.getInstance(id)
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

        if (provider == null) {
            log(
                "No runtime provider '$runtimeKey' for recovered instance ${instance.id}; " +
                    "leaving lifecycle ownership unchanged",
                LogLevel.WARNING
            )
            return false
        }

        val running = try {
            provider.isRunning(instance.id)
        } catch (failure: Exception) {
            log(
                "Could not confirm runtime state for recovered instance ${instance.id}: " +
                    "${failure.message}; leaving lifecycle ownership unchanged",
                LogLevel.WARNING
            )
            return false
        }
        if (!running) {
            log("Instance ${instance.id} is no longer running, cleaning up", LogLevel.WARNING)
            cleanupDeadInstance(instance, config)
            return false
        }

        // Instance is running — register it
        val updated = instance.copy(
            wrapperNodeId = hazelcastInstance.cluster.localMember.uuid.toString(),
            state = InstanceState.ONLINE,
            lastHeartbeat = System.currentTimeMillis()
        )
        val generation = maxOf(1L, clusterStateService.getLifecycleGeneration(instance.id))
        if (!clusterStateService.recoverInstance(updated, generation)) {
            log("Recovery of instance ${instance.id} was superseded by cleanup", LogLevel.WARNING)
            return false
        }

        // Re-allocate port to mark it as used
        portAllocator.reserve(instance.allocatedPort)

        log("Recovered instance ${instance.id} (config=${instance.configurationName}, port=${instance.allocatedPort})", LogLevel.SUCCESS)
        return true
    }

    private fun cleanupDeadInstance(instance: InstanceInfo, config: gg.scala.universe.schema.Configuration?) {
        portAllocator.release(instance.allocatedPort)
        if (config?.static != true) {
            val workingDir = workspace.dynamicInstance(instance.id)
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
            expectedGeneration = clusterStateService.getLifecycleGeneration(instance.id),
            finalState = InstanceState.OFFLINE
        )
    }

    private fun findStateFile(id: String, runningDir: Path, staticDir: Path): Path? {
        val dynamic = runningDir.resolve(id).resolve(".universe-state.json")
        if (Files.exists(dynamic)) return dynamic
        if (!Files.exists(staticDir)) return null
        return Files.list(staticDir).use { stream ->
            stream.filter { Files.isDirectory(it) }
                .map { it.resolve(".universe-state.json") }
                .filter { Files.exists(it) }
                .filter { path ->
                    runCatching {
                        Serializers.GSON.fromJson(
                            Files.readString(path), InstanceInfo::class.java
                        )?.id == id
                    }.getOrDefault(false)
                }
                .findFirst()
                .orElse(null)
        }
    }
}
