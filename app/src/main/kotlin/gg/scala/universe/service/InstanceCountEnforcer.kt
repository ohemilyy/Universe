package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceState
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Automatically enforces [Configuration.minimumServiceCount] by spawning
 * new instances when the running count drops below the configured minimum.
 *
 * Runs on the master node every 5 seconds. Uses a single-threaded executor
 * to avoid race conditions. Static configurations spawn at most one instance per pass.
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

        clusterStateService.completePendingAbandonedStoppingCleanups()
        val liveMembers = hazelcastInstance.cluster.members.associateBy { it.uuid.toString() }
        val liveWrapperIds = liveMembers.keys
        val allInstances = clusterStateService.getAllInstances()
        val plannedInstancesById = allInstances.associateBy { it.id }

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
                val plannedInstance = plannedInstancesById[instanceId] ?: return@forEach
                finalizeAbandonedStopping(
                    instanceId,
                    liveWrapperIds,
                    plannedInstance.lastHeartbeat,
                    now
                )
            }
            plan.forceStopIds.forEach { instanceId ->
                val plannedInstance = plannedInstancesById[instanceId] ?: return@forEach
                val member = liveMembers[plannedInstance.wrapperNodeId] ?: return@forEach
                val result = instanceStopDispatcher.dispatchStop(
                    instanceId = instanceId,
                    targetMember = member,
                    force = true,
                    restart = false,
                    expectedLastHeartbeat = plannedInstance.lastHeartbeat,
                    transitionAt = now
                )
                if (
                    result == StopDispatchResult.TARGET_UNAVAILABLE ||
                    result == StopDispatchResult.SUBMISSION_FAILED
                ) {
                    val currentLiveWrapperIds = hazelcastInstance.cluster.members
                        .mapTo(mutableSetOf()) { it.uuid.toString() }
                    finalizeAbandonedStopping(
                        instanceId,
                        currentLiveWrapperIds,
                        plannedInstance.lastHeartbeat,
                        now
                    )
                }
            }

            if (plan.spawnCount <= 0 || !stillHasPlannedDeficit(instanceConfiguration, plan.spawnCount)) {
                continue
            }
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
        expectedLastHeartbeat: Long,
        now: Long
    ) {
        val instance = clusterStateService.getInstance(instanceId) ?: return
        if (
            instance.state != InstanceState.STOPPING ||
            instance.lastHeartbeat != expectedLastHeartbeat ||
            !isStale(instance.lastHeartbeat, now, STOPPING_TIMEOUT_MS) ||
            instance.wrapperNodeId in liveWrapperIds
        ) {
            return
        }

        clusterStateService.finalizeAbandonedStopping(
            instanceId = instanceId,
            expectedLastHeartbeat = expectedLastHeartbeat,
            stoppedAt = now
        )
    }

    private fun isStale(lastHeartbeat: Long, now: Long, timeoutMs: Long): Boolean {
        return lastHeartbeat < now && now - lastHeartbeat > timeoutMs
    }

    private fun stillHasPlannedDeficit(
        instanceConfiguration: Configuration,
        plannedSpawnCount: Int
    ): Boolean {
        val currentMatchingInstances = clusterStateService.getAllInstances()
            .filter { it.configurationName == instanceConfiguration.name }
        val currentStatus = InstanceLifecyclePolicy.minimumStatus(
            instanceConfiguration.name,
            currentMatchingInstances
        )
        if (currentStatus.replacementBlocked) return false

        val currentDeficit = (
            instanceConfiguration.minimumServiceCount - currentStatus.countedInstances
        ).coerceAtLeast(0)
        return currentDeficit >= plannedSpawnCount
    }
}
