package gg.scala.universe.hz

import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.cluster.MembershipListener
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.schema.InstanceState

class ResilienceMembershipListener(
    private val clusterStateService: ClusterStateService
) : MembershipListener {

    override fun memberAdded(membershipEvent: MembershipEvent) {
        log("Wrapper joined: ${membershipEvent.member.uuid}")
    }

    override fun memberRemoved(membershipEvent: MembershipEvent) {
        val memberUuid = membershipEvent.member.uuid.toString()
        log("Wrapper disconnected: $memberUuid", LogLevel.WARNING)

        val affectedInstances = clusterStateService.getInstancesByWrapper(memberUuid)
        if (affectedInstances.isEmpty()) {
            log("No instances were running on disconnected wrapper $memberUuid")
            return
        }

        var offlineCount = 0
        affectedInstances.forEach { instance ->
            val updated = clusterStateService.markInstanceOfflineAfterWrapperDeparture(
                id = instance.id,
                wrapperNodeId = memberUuid
            ) ?: return@forEach
            if (updated.state == InstanceState.STOPPING) {
                log(
                    "Preserving STOPPING instance ${instance.id} for reconciliation",
                    LogLevel.WARNING
                )
            } else if (updated.state == InstanceState.OFFLINE) {
                offlineCount++
                log(
                    "Marked instance ${instance.id} as OFFLINE (was on wrapper $memberUuid)",
                    LogLevel.WARNING
                )
            }
        }

        log("Marked $offlineCount instance(s) as OFFLINE", LogLevel.WARNING)

        // Clear node resource tracking for the disconnected wrapper
        clusterStateService.clearNodeResources(memberUuid)
        log("Cleared resource tracking for node $memberUuid")
    }

}
