package gg.scala.universe.service

import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import kotlin.math.min

internal const val CREATING_TIMEOUT_MS = 60_000L
internal const val STOPPING_TIMEOUT_MS = 120_000L

internal data class ReconciliationPlan(
    val reapCreatingIds: List<String>,
    val forceStopIds: List<String>,
    val abandonedStoppingIds: List<String>,
    val spawnCount: Int
)

internal object InstanceReconciliationPolicy {
    fun plan(
        configuration: Configuration,
        instances: Collection<InstanceInfo>,
        liveWrapperIds: Set<String>,
        now: Long
    ): ReconciliationPlan {
        val matchingInstances = instances
            .filter { it.configurationName == configuration.name }
            .sortedBy(InstanceInfo::id)

        val reapCreatingIds = matchingInstances
            .filter {
                it.state == InstanceState.CREATING &&
                    isStale(it.lastHeartbeat, now, CREATING_TIMEOUT_MS)
            }
            .map(InstanceInfo::id)
        val staleStopping = matchingInstances.filter {
            it.state == InstanceState.STOPPING &&
                isStale(it.lastHeartbeat, now, STOPPING_TIMEOUT_MS)
        }
        val forceStopIds = staleStopping
            .filter { it.wrapperNodeId in liveWrapperIds }
            .map(InstanceInfo::id)
        val abandonedStoppingIds = staleStopping
            .filterNot { it.wrapperNodeId in liveWrapperIds }
            .map(InstanceInfo::id)

        val excludedIds = (reapCreatingIds + abandonedStoppingIds).toSet()
        val effectiveStatus = InstanceLifecyclePolicy.minimumStatus(
            configuration.name,
            matchingInstances.filterNot { it.id in excludedIds }
        )
        val deficit = (configuration.minimumServiceCount - effectiveStatus.countedInstances)
            .coerceAtLeast(0)
        val spawnCount = when {
            effectiveStatus.replacementBlocked -> 0
            configuration.static -> min(deficit, 1)
            else -> deficit
        }

        return ReconciliationPlan(
            reapCreatingIds = reapCreatingIds,
            forceStopIds = forceStopIds,
            abandonedStoppingIds = abandonedStoppingIds,
            spawnCount = spawnCount
        )
    }

    private fun isStale(lastHeartbeat: Long, now: Long, timeoutMs: Long): Boolean {
        return lastHeartbeat < now && now - lastHeartbeat > timeoutMs
    }
}
