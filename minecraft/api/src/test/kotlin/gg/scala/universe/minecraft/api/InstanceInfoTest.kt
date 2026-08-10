package gg.scala.universe.minecraft.api

import kotlin.test.Test
import kotlin.test.assertEquals

class InstanceInfoTest {

    @Test
    fun `unknown server state defaults to stopped`() {
        val instance = InstanceInfo(
            id = "abc123",
            configurationName = "site",
            wrapperNodeId = "node-1",
            hostAddress = "127.0.0.1",
            allocatedPort = 25565,
            state = "FUTURE_STATE",
            lastHeartbeat = 1
        )

        assertEquals(InstanceState.STOPPED, instance.getStateEnum())
    }
}
