package gg.scala.universe.service

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InstanceLifecyclePolicyTest {

    @Test
    fun `stopping occupies its port but blocks minimum replacement`() {
        assertTrue(InstanceState.STOPPING.occupiesPort)

        val status = InstanceLifecyclePolicy.minimumStatus(
            "site",
            listOf(instance("site", InstanceState.STOPPING))
        )

        assertTrue(status.replacementBlocked)
        assertEquals(0, status.countedInstances)
    }

    @Test
    fun `repeated stop is idempotent and conflicting restart is rejected`() {
        assertEquals(
            LifecycleRequestDecision.ACCEPTED_NOOP,
            InstanceLifecyclePolicy.evaluateRequest(InstanceState.STOPPING, LifecycleTarget.STOP)
        )
        assertEquals(
            LifecycleRequestDecision.CONFLICT,
            InstanceLifecyclePolicy.evaluateRequest(InstanceState.STOPPING, LifecycleTarget.RESTART)
        )
    }

    private fun instance(configurationName: String, state: InstanceState) = InstanceInfo(
        id = "abc123",
        configurationName = configurationName,
        wrapperNodeId = "node-1",
        hostAddress = "127.0.0.1",
        allocatedPort = 25565,
        state = state,
        lastHeartbeat = 0,
        processPid = null
    )
}
