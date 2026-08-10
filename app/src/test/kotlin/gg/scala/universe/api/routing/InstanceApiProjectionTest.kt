package gg.scala.universe.api.routing

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import kotlin.test.Test
import kotlin.test.assertEquals

class InstanceApiProjectionTest {

    @Test
    fun `stopping projects as online without mutating the stored instance`() {
        val stored = InstanceInfo(
            id = "abc123",
            configurationName = "site",
            wrapperNodeId = "node-1",
            hostAddress = "127.0.0.1",
            allocatedPort = 25565,
            state = InstanceState.STOPPING,
            lastHeartbeat = 1,
            processPid = 42
        )

        val external = stored.toExternalApiView()

        assertEquals(InstanceState.ONLINE, external.state)
        assertEquals(InstanceState.STOPPING, stored.state)
        assertEquals(stored.id, external.id)
    }

    @Test
    fun `plugin state reports preserve master stopping state and transition timestamp`() {
        val stopping = InstanceInfo(
            id = "abc123",
            configurationName = "site",
            wrapperNodeId = "node-1",
            hostAddress = "127.0.0.1",
            allocatedPort = 25565,
            state = InstanceState.STOPPING,
            lastHeartbeat = 100,
            processPid = 42
        )

        listOf(InstanceState.ONLINE, InstanceState.OFFLINE).forEach { reportedState ->
            assertEquals(stopping, stopping.withPluginStateReport(reportedState, 200))
        }
    }
}
