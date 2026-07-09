package gg.scala.universe.runtime

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class InstancePortReservationsTest {

    private fun instance(
        id: String,
        port: Int,
        state: InstanceState,
        lastHeartbeat: Long
    ) = InstanceInfo(
        id = id,
        configurationName = "cfg",
        wrapperNodeId = "node-1",
        hostAddress = "127.0.0.1",
        allocatedPort = port,
        state = state,
        lastHeartbeat = lastHeartbeat,
        processPid = null
    )

    @Test
    fun `online instance reserves its port`() {
        val now = 1_000_000L
        val reserved = InstancePortReservations.reservedPorts(
            listOf(instance("a", 25565, InstanceState.ONLINE, now)),
            nowMs = now,
            creatingTimeoutMs = 120_000L
        )
        assertEquals(setOf(25565), reserved)
    }

    @Test
    fun `port zero is never reserved`() {
        val now = 1_000_000L
        val reserved = InstancePortReservations.reservedPorts(
            listOf(instance("a", 0, InstanceState.CREATING, now)),
            nowMs = now,
            creatingTimeoutMs = 120_000L
        )
        assertTrue(reserved.isEmpty())
    }

    @Test
    fun `stale CREATING instance does not reserve its port forever`() {
        val now = 1_000_000L
        val stale = instance("a", 25513, InstanceState.CREATING, now - 200_000L)
        val reserved = InstancePortReservations.reservedPorts(
            listOf(stale),
            nowMs = now,
            creatingTimeoutMs = 120_000L
        )
        assertFalse(reserved.contains(25513))
    }

    @Test
    fun `fresh CREATING instance reserves its port`() {
        val now = 1_000_000L
        val fresh = instance("a", 25513, InstanceState.CREATING, now - 5_000L)
        val reserved = InstancePortReservations.reservedPorts(
            listOf(fresh),
            nowMs = now,
            creatingTimeoutMs = 120_000L
        )
        assertTrue(reserved.contains(25513))
    }
}
