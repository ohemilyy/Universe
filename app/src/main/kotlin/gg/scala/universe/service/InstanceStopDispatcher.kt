package gg.scala.universe.service

import com.hazelcast.cluster.Member

enum class StopDispatchResult {
    DISPATCHED,
    ALREADY_STOPPING,
    NOT_FOUND
}

interface InstanceStopDispatcher {
    fun dispatchStop(
        instanceId: String,
        targetMember: Member,
        force: Boolean = false,
        restart: Boolean = false
    ): StopDispatchResult
}
