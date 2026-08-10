package gg.scala.universe.service

import com.hazelcast.cluster.Member

enum class StopDispatchResult {
    DISPATCHED,
    ALREADY_STOPPING,
    NOT_FOUND
}

internal enum class StopDispatchOutcome {
    ACCEPTED,
    IDEMPOTENT,
    CONFLICT,
    NOT_FOUND
}

internal fun StopDispatchResult.toRequestOutcome(restart: Boolean): StopDispatchOutcome = when (this) {
    StopDispatchResult.DISPATCHED -> StopDispatchOutcome.ACCEPTED
    StopDispatchResult.ALREADY_STOPPING -> {
        if (restart) StopDispatchOutcome.CONFLICT else StopDispatchOutcome.IDEMPOTENT
    }
    StopDispatchResult.NOT_FOUND -> StopDispatchOutcome.NOT_FOUND
}

interface InstanceStopDispatcher {
    fun dispatchStop(
        instanceId: String,
        targetMember: Member,
        force: Boolean = false,
        restart: Boolean = false
    ): StopDispatchResult
}
