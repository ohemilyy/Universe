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
        val localNodeId = hazelcastInstance.cluster.localMember.stableNodeId()
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
        var deferredToReconcile = 0
        for ((_, provider) in runtimeRegistry.getAll()) {
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
                    // No local metadata to recover from. Don't emit a warning per resource —
                    // reconciliation below is the authority: it adopts the live/ready ones and
                    // deletes the dead/orphaned ones. Defer silently and summarize once.
                    deferredToReconcile++
                }
            }
        }

        if (deferredToReconcile > 0) {
            log("Deferring $deferredToReconcile running runtime resource(s) without local state to reconciliation", LogLevel.INFO)
        }

        if (recovered.isEmpty()) {
            log("No instances to recover")
        } else {
            log("Recovered ${recovered.size} instance(s): ${recovered.joinToString(", ")}", LogLevel.SUCCESS)
        }

        // Reconcile each runtime's actual resources against what Universe tracks. This deletes
        // post-restart orphaned resources that would otherwise hold ports and node capacity.
        // It runs before the enforcer starts spawning, so we never spawn into a dirty cluster.
        reconcile()
    }

    private val reconcileExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "universe-reconcile").apply { isDaemon = true }
    }

    @Volatile
    private var reconcileShuttingDown = false

    fun startPeriodicReconcile(intervalSeconds: Long) {
        reconcileExecutor.scheduleAtFixedRate({
            if (reconcileShuttingDown) return@scheduleAtFixedRate
            try {
                reconcile()
            } catch (e: Exception) {
                log("Periodic reconcile failed: ${e.message}", LogLevel.WARNING)
            }
        }, intervalSeconds, intervalSeconds, java.util.concurrent.TimeUnit.SECONDS)
        log("Periodic runtime reconciliation started (interval=${intervalSeconds}s)", LogLevel.INFO)
    }

    fun stopPeriodicReconcile() {
        reconcileShuttingDown = true
        reconcileExecutor.shutdown()
        try {
            if (!reconcileExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                reconcileExecutor.shutdownNow()
            }
        } catch (_: InterruptedException) {
            reconcileExecutor.shutdownNow()
        }
    }

    /**
     * Asks every runtime to reconcile its live resources against the instances Universe still
     * tracks, then reacts to the report: tracked instances whose resource was dead are marked
     * OFFLINE and have their port and node resources released.
     */
    fun reconcile() {
        val trackedIds = clusterStateService.getAllInstances()
            .filter { it.state == InstanceState.ONLINE || it.state == InstanceState.CREATING }
            .map { it.id }
            .toSet()

        for ((runtimeKey, provider) in runtimeRegistry.getAll()) {
            val report = try {
                provider.reconcile(trackedIds)
            } catch (e: Exception) {
                log("Reconcile failed for runtime '$runtimeKey': ${e.message}", LogLevel.WARNING)
                continue
            }

            if (report.deletedOrphanCount > 0 || report.deletedDeadCount > 0) {
                log(
                    "Runtime '$runtimeKey' reconciled: ${report.adopted.size} running, " +
                    "${report.deletedOrphanCount} orphan(s) and ${report.deletedDeadCount} dead resource(s) deleted",
                    LogLevel.WARNING
                )
            }

            // Re-register healthy resources Universe lost track of (e.g. after a restart) so the
            // enforcer treats them as active instead of spawning duplicates.
            val localNodeId = hazelcastInstance.cluster.localMember.stableNodeId()
            for (resource in report.adopted) {
                if (clusterStateService.getInstance(resource.instanceId) != null) continue
                val config = resource.configurationName?.let { clusterStateService.getConfiguration(it) }
                if (config == null) {
                    log(
                        "Reconcile: running '$runtimeKey' instance ${resource.instanceId} has unknown config " +
                        "'${resource.configurationName}'; leaving it running but untracked",
                        LogLevel.WARNING
                    )
                    continue
                }
                if (resource.port > 0) portAllocator.reserve(resource.port)
                clusterStateService.putInstance(
                    InstanceInfo(
                        id = resource.instanceId,
                        configurationName = config.name,
                        wrapperNodeId = localNodeId,
                        hostAddress = resource.hostAddress.ifBlank { config.hostAddress },
                        allocatedPort = resource.port,
                        state = InstanceState.ONLINE,
                        lastHeartbeat = System.currentTimeMillis(),
                        processPid = null,
                        allocatedRamMB = config.ramMB,
                        allocatedCpu = config.cpu,
                        runtime = config.runtime
                    )
                )
                clusterStateService.addNodeResources(localNodeId, config.ramMB, config.cpu)
                log(
                    "Reconcile: re-registered running instance ${resource.instanceId} " +
                    "(config=${config.name}, port=${resource.port})",
                    LogLevel.SUCCESS
                )
            }

            for (id in report.deadInstanceIds) {
                val instance = clusterStateService.getInstance(id) ?: continue
                portAllocator.release(instance.allocatedPort)
                if (instance.state == InstanceState.ONLINE) {
                    clusterStateService.removeNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)
                }
                clusterStateService.updateInstanceState(id, InstanceState.OFFLINE)
                log("Instance $id had a dead resource during reconcile; marked OFFLINE and released port ${instance.allocatedPort}", LogLevel.WARNING)
            }
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
        clusterStateService.putInstance(updated)

        // Re-allocate port to mark it as used
        portAllocator.reserve(instance.allocatedPort)

        // Track node resources
        clusterStateService.addNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)

        log("Recovered instance ${instance.id} (config=${instance.configurationName}, port=${instance.allocatedPort})", LogLevel.SUCCESS)
        return true
    }

    private fun cleanupDeadInstance(instance: InstanceInfo, config: gg.scala.universe.schema.Configuration?) {
        portAllocator.release(instance.allocatedPort)
        clusterStateService.removeNodeResources(instance.wrapperNodeId, instance.allocatedRamMB, instance.allocatedCpu)

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

        clusterStateService.updateInstanceState(instance.id, InstanceState.OFFLINE)
    }
}
