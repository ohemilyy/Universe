package gg.scala.universe.service

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class InstanceLifecyclePolicyTest {

    private fun instance(id: String, state: InstanceState, lastHeartbeat: Long) = InstanceInfo(
        id = id,
        configurationName = "cfg",
        wrapperNodeId = "node-1",
        hostAddress = "127.0.0.1",
        allocatedPort = 0,
        state = state,
        lastHeartbeat = lastHeartbeat,
        processPid = null
    )

    @Test
    fun `creating instance older than timeout is stale`() {
        val now = 1_000_000L
        val stale = instance("a", InstanceState.CREATING, now - 200_000L)
        val result = InstanceLifecyclePolicy.staleCreating(listOf(stale), now, timeoutMs = 120_000L)
        assertEquals(listOf("a"), result.map { it.id })
    }

    @Test
    fun `fresh creating instance is not stale`() {
        val now = 1_000_000L
        val fresh = instance("a", InstanceState.CREATING, now - 5_000L)
        val result = InstanceLifecyclePolicy.staleCreating(listOf(fresh), now, timeoutMs = 120_000L)
        assertEquals(emptyList<String>(), result.map { it.id })
    }

    @Test
    fun `online instance is never stale-creating`() {
        val now = 1_000_000L
        val online = instance("a", InstanceState.ONLINE, now - 999_999L)
        val result = InstanceLifecyclePolicy.staleCreating(listOf(online), now, timeoutMs = 120_000L)
        assertEquals(emptyList<String>(), result.map { it.id })
    }

    @Test
    fun `only stale creating instances are returned from a mixed set`() {
        val now = 1_000_000L
        val instances = listOf(
            instance("stale", InstanceState.CREATING, now - 200_000L),
            instance("fresh", InstanceState.CREATING, now - 1_000L),
            instance("online", InstanceState.ONLINE, now - 500_000L),
            instance("offline", InstanceState.OFFLINE, now - 500_000L)
        )
        val result = InstanceLifecyclePolicy.staleCreating(instances, now, timeoutMs = 120_000L)
        assertEquals(listOf("stale"), result.map { it.id })
    }
}
