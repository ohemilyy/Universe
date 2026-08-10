package gg.scala.universe.service

import com.hazelcast.cluster.Member

enum class StopDispatchResult {
    DISPATCHED,
    ALREADY_STOPPING,
    STALE_TRANSITION,
    TARGET_UNAVAILABLE,
    SUBMISSION_FAILED,
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
    StopDispatchResult.STALE_TRANSITION -> StopDispatchOutcome.CONFLICT
    StopDispatchResult.TARGET_UNAVAILABLE -> StopDispatchOutcome.NOT_FOUND
    StopDispatchResult.SUBMISSION_FAILED -> StopDispatchOutcome.CONFLICT
    StopDispatchResult.NOT_FOUND -> StopDispatchOutcome.NOT_FOUND
}

interface InstanceStopDispatcher {
    fun dispatchStop(
        instanceId: String,
        targetMember: Member,
        force: Boolean = false,
        restart: Boolean = false,
        expectedLastHeartbeat: Long? = null,
        transitionAt: Long = System.currentTimeMillis()
    ): StopDispatchResult
}
