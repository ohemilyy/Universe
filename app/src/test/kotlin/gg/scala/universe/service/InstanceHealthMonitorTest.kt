package gg.scala.universe.service

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.InstanceWorkspace
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRegistryImpl
import gg.scala.universe.runtime.RuntimeProvider
import gg.scala.universe.schema.Configuration
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

    @Test
    fun `one runtime discovery failure does not prevent checking another instance`() {
        val hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "health-isolation-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
        try {
            val state = ClusterStateService(hazelcast)
            val local = hazelcast.cluster.localMember.uuid.toString()
            val failed = InstanceInfo(
                "old001", "bad", local, "127.0.0.1", 25001,
                InstanceState.CREATING, 1, null, 64, 1, "bad"
            )
            val absent = failed.copy(id = "old002", configurationName = "gone", runtime = "gone", allocatedPort = 25002)
            assertTrue(state.reserveCreatingInstance(failed, 1, 1024, 100))
            assertTrue(state.reserveCreatingInstance(absent, 1, 1024, 100))
            state.updateInstanceState(failed.id, InstanceState.ONLINE, 2)
            state.updateInstanceState(absent.id, InstanceState.ONLINE, 2)
            val registry = RuntimeRegistryImpl().apply {
                register("bad", healthRuntime { error("discovery unavailable") })
                register("gone", healthRuntime { false })
            }

            InstanceHealthMonitor(
                state,
                hazelcast,
                registry,
                PortAllocator(state),
                InstanceWorkspace(tempDir)
            ).checkHealth()

            assertEquals(InstanceState.ONLINE, state.getInstance(failed.id)?.state)
            assertEquals(InstanceState.OFFLINE, state.getInstance(absent.id)?.state)
        } finally {
            hazelcast.shutdown()
        }
    }

    private fun healthRuntime(running: () -> Boolean) = object : RuntimeProvider {
        override fun start(
            instanceId: String, workingDir: Path, port: Int, command: String,
            ramMB: Int, cpu: Int, configuration: Configuration,
            environmentVariables: Map<String, String>?
        ): ProcessHandle = error("unused")
        override fun stop(instanceId: String) = Unit
        override fun executeCommand(instanceId: String, command: String) = Unit
        override fun isRunning(instanceId: String): Boolean = running()
    }
}
