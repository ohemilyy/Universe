package gg.scala.universe.service

import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InstanceReconciliationPolicyTest {
    private val policy = InstanceReconciliationPolicy

    @Test
    fun `non-stale stopping instance blocks replacement`() {
        val plan = policy.plan(
            config(minimum = 1),
            listOf(instance(InstanceState.STOPPING, updatedAt = 9_000)),
            setOf("node-a"),
            now = 10_000
        )

        assertEquals(0, plan.spawnCount)
        assertTrue(plan.forceStopIds.isEmpty())
    }

    @Test
    fun `stale creating is reaped and deficit retried`() {
        val plan = policy.plan(
            config(minimum = 1),
            listOf(instance(InstanceState.CREATING, updatedAt = 0)),
            setOf("node-a"),
            now = 60_001
        )

        assertEquals(listOf("abc123"), plan.reapCreatingIds)
        assertEquals(1, plan.spawnCount)
    }

    @Test
    fun `stale stopping on dead wrapper is reaped locally`() {
        val plan = policy.plan(
            config(minimum = 1),
            listOf(instance(InstanceState.STOPPING, wrapper = "dead", updatedAt = 0)),
            emptySet(),
            now = 120_001
        )

        assertEquals(listOf("abc123"), plan.abandonedStoppingIds)
        assertEquals(1, plan.spawnCount)
    }

    @Test
    fun `stale stopping on live wrapper is force stopped and remains a barrier`() {
        val plan = policy.plan(
            config(minimum = 1),
            listOf(instance(InstanceState.STOPPING, updatedAt = 0)),
            setOf("node-a"),
            now = 120_001
        )

        assertEquals(listOf("abc123"), plan.forceStopIds)
        assertEquals(0, plan.spawnCount)
    }

    @Test
    fun `static configuration creates at most one instance per pass`() {
        val plan = policy.plan(
            config(minimum = 3, static = true),
            emptyList(),
            setOf("node-a"),
            now = 10_000
        )

        assertEquals(1, plan.spawnCount)
    }

    private fun config(minimum: Int, static: Boolean = false) = Configuration(
        name = "site",
        minimumServiceCount = minimum,
        static = static
    )

    private fun instance(
        state: InstanceState,
        wrapper: String = "node-a",
        updatedAt: Long
    ) = InstanceInfo(
        id = "abc123",
        configurationName = "site",
        wrapperNodeId = wrapper,
        hostAddress = "127.0.0.1",
        allocatedPort = 25565,
        state = state,
        lastHeartbeat = updatedAt,
        processPid = null
    )
}
