package gg.scala.universe.service

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.InstanceWorkspace
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistryImpl
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import java.net.ServerSocket
import java.nio.file.Path
import java.util.UUID
import org.junit.jupiter.api.io.TempDir
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InstanceHealthMonitorTest {
    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `missing runtime provider cannot publish offline or release ownership`() {
        val hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "health-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
        try {
            val state = ClusterStateService(hazelcast)
            val port = ServerSocket(0).use { it.localPort }
            val creating = InstanceInfo(
                "old001", "site", hazelcast.cluster.localMember.uuid.toString(), "127.0.0.1",
                port, InstanceState.CREATING, 1, null, 64, 1, "missing"
            )
            assertTrue(state.reserveCreatingInstance(creating, 1, 1024, 100))
            state.updateInstanceState(creating.id, InstanceState.ONLINE, 2)
            val allocator = PortAllocator(state).also { it.reserve(port) }
            val monitor = InstanceHealthMonitor(
                state,
                hazelcast,
                RuntimeRegistryImpl(),
                allocator,
                InstanceWorkspace(tempDir)
            )

            monitor.checkHealth()

            assertEquals(InstanceState.ONLINE, state.getInstance(creating.id)?.state)
            assertTrue(port in allocator.getLocalAllocations())
            assertEquals(64, state.getNodeResources(creating.wrapperNodeId).usedRamMB)
        } finally {
            hazelcast.shutdown()
        }
    }
}
