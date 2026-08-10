package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.InstanceState
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Automatically enforces [Configuration.minimumServiceCount] by spawning
 * new instances when the running count drops below the configured minimum.
 *
 * Runs on the master node every 5 seconds. Uses a single-threaded executor
 * to avoid race conditions. Static configurations are ignored.
 */
@Singleton
class InstanceCountEnforcer @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val instanceStopDispatcher: InstanceStopDispatcher,
    private val instanceSpawner: InstanceSpawner,
    private val configuration: UniverseMainConfiguration
) {
    private val executor = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "universe-instance-enforcer").apply { isDaemon = true }
    }

    @Volatile
    private var shuttingDown = false

    fun start() {
        if (!configuration.isMasterNode) {
            log("InstanceCountEnforcer disabled on non-master node", LogLevel.INFO)
            return
        }

        executor.scheduleAtFixedRate(
            ::enforce,
            15,   // initial delay
            5,   // period
            TimeUnit.SECONDS
        )
        log("InstanceCountEnforcer started (interval=5s)", LogLevel.INFO)
    }

    fun stop() {
        shuttingDown = true
        executor.shutdown()
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        } catch (_: InterruptedException) {
            executor.shutdownNow()
        }
    }

    private fun enforce() {
        if (shuttingDown) return
        try {
            enforceOnce()
        } catch (e: Exception) {
            log("InstanceCountEnforcer encountered an error: ${e.message}", LogLevel.ERROR)
        }
    }

    internal fun enforceOnce(now: Long = System.currentTimeMillis()) {
        if (!configuration.isMasterNode || shuttingDown) return

        val liveMembers = hazelcastInstance.cluster.members.associateBy { it.uuid.toString() }
        val liveWrapperIds = liveMembers.keys
        val allInstances = clusterStateService.getAllInstances()

        for (instanceConfiguration in clusterStateService.configurations.values) {
            val plan = InstanceReconciliationPolicy.plan(
                configuration = instanceConfiguration,
                instances = allInstances,
                liveWrapperIds = liveWrapperIds,
                now = now
            )

            plan.reapCreatingIds.forEach { instanceId ->
                reapStaleCreating(instanceId, now)
            }
            plan.abandonedStoppingIds.forEach { instanceId ->
                finalizeAbandonedStopping(instanceId, liveWrapperIds, now)
            }
            plan.forceStopIds.forEach { instanceId ->
                val stopping = refreshStoppingTransition(instanceId, now) ?: return@forEach
                val member = liveMembers[stopping.wrapperNodeId] ?: return@forEach
                instanceStopDispatcher.dispatchStop(
                    instanceId = instanceId,
                    targetMember = member,
                    force = true,
                    restart = false
                )
            }

            if (plan.spawnCount <= 0) continue
            log(
                "Config '${instanceConfiguration.name}' is below its minimum. " +
                    "Spawning ${plan.spawnCount} instance(s)...",
                LogLevel.WARNING
            )
            repeat(plan.spawnCount) { index ->
                val instanceInfo = instanceSpawner.createInstance(instanceConfiguration)
                if (instanceInfo == null) {
                    log(
                        "Failed to spawn instance #$index for config '${instanceConfiguration.name}': " +
                            "no node has enough resources.",
                        LogLevel.WARNING
                    )
                    return@repeat
                }
                log(
                    "Auto-spawned instance ${instanceInfo.id} for config '${instanceConfiguration.name}' " +
                        "on node ${instanceInfo.wrapperNodeId}",
                    LogLevel.SUCCESS
                )
            }
        }
    }

    private fun reapStaleCreating(instanceId: String, now: Long) {
        val instances = clusterStateService.instances
        instances.lock(instanceId)
        try {
            val instance = instances[instanceId] ?: return
            if (
                instance.state == InstanceState.CREATING &&
                isStale(instance.lastHeartbeat, now, CREATING_TIMEOUT_MS)
            ) {
                instances.remove(instanceId)
            }
        } finally {
            instances.unlock(instanceId)
        }
    }

    private fun finalizeAbandonedStopping(
        instanceId: String,
        liveWrapperIds: Set<String>,
        now: Long
    ) {
        val instances = clusterStateService.instances
        instances.lock(instanceId)
        try {
            val instance = instances[instanceId] ?: return
            if (
                instance.state != InstanceState.STOPPING ||
                !isStale(instance.lastHeartbeat, now, STOPPING_TIMEOUT_MS) ||
                instance.wrapperNodeId in liveWrapperIds
            ) {
                return
            }

            clusterStateService.removeNodeResources(
                instance.wrapperNodeId,
                instance.allocatedRamMB,
                instance.allocatedCpu
            )
            instances[instanceId] = instance.copy(
                state = InstanceState.STOPPED,
                lastHeartbeat = now
            )
            instances.remove(instanceId)
        } finally {
            instances.unlock(instanceId)
        }
    }

    private fun refreshStoppingTransition(instanceId: String, now: Long) =
        clusterStateService.instances.let { instances ->
            instances.lock(instanceId)
            try {
                val instance = instances[instanceId] ?: return@let null
                if (
                    instance.state != InstanceState.STOPPING ||
                    !isStale(instance.lastHeartbeat, now, STOPPING_TIMEOUT_MS)
                ) {
                    return@let null
                }
                instance.copy(lastHeartbeat = now).also { instances[instanceId] = it }
            } finally {
                instances.unlock(instanceId)
            }
        }

    private fun isStale(lastHeartbeat: Long, now: Long, timeoutMs: Long): Boolean {
        return lastHeartbeat < now && now - lastHeartbeat > timeoutMs
    }
}
