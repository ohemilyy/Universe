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
        // Use the stable nodeId so instances are matched even though the member's UUID is
        // gone — ownership is keyed on nodeId, not the volatile UUID.
        val nodeId = membershipEvent.member.stableNodeId()
        log("Wrapper disconnected: $nodeId", LogLevel.WARNING)

        val affectedInstances = clusterStateService.getInstancesByWrapper(nodeId)
        if (affectedInstances.isEmpty()) {
            log("No instances were running on disconnected wrapper $nodeId")
            return
        }

        affectedInstances.forEach { instance ->
            log("Marking instance ${instance.id} as OFFLINE (was on wrapper $nodeId)", LogLevel.WARNING)
            clusterStateService.updateInstanceState(instance.id, InstanceState.OFFLINE)
        }

        log("Marked ${affectedInstances.size} instance(s) as OFFLINE", LogLevel.WARNING)

        // Clear node resource tracking for the disconnected wrapper
        clusterStateService.clearNodeResources(nodeId)
        log("Cleared resource tracking for node $nodeId")
    }

}
