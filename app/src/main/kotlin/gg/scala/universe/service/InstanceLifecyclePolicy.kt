package gg.scala.universe.service

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState

internal val InstanceState.occupiesPort: Boolean
    get() = this == InstanceState.CREATING || this == InstanceState.ONLINE || this == InstanceState.STOPPING

internal data class MinimumStatus(
    val countedInstances: Int,
    val replacementBlocked: Boolean
)

internal enum class LifecycleTarget {
    START,
    STOP,
    RESTART
}

internal enum class LifecycleRequestDecision {
    DISPATCH,
    ACCEPTED_NOOP,
    CONFLICT
}

internal object InstanceLifecyclePolicy {
    fun minimumStatus(configurationName: String, instances: Collection<InstanceInfo>): MinimumStatus {
        val matchingInstances = instances.filter { it.configurationName == configurationName }
        return MinimumStatus(
            countedInstances = matchingInstances.count {
                it.state == InstanceState.ONLINE || it.state == InstanceState.CREATING
            },
            replacementBlocked = matchingInstances.any { it.state == InstanceState.STOPPING }
        )
    }

    fun evaluateRequest(state: InstanceState, target: LifecycleTarget): LifecycleRequestDecision = when {
        state != InstanceState.STOPPING -> LifecycleRequestDecision.DISPATCH
        target == LifecycleTarget.STOP -> LifecycleRequestDecision.ACCEPTED_NOOP
        else -> LifecycleRequestDecision.CONFLICT
    }
}
