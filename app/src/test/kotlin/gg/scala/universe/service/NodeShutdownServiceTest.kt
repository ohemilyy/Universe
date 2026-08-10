package gg.scala.universe.service

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.InstanceWorkspace
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeProvider
import gg.scala.universe.runtime.RuntimeRegistryImpl
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

class NodeShutdownServiceTest {
    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `failed teardown preserves lifecycle port and owned resources`() {
        val hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "node-shutdown-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
        try {
            val state = ClusterStateService(hazelcast)
            val port = ServerSocket(0).use { it.localPort }
            val instance = InstanceInfo(
                "old001", "site", hazelcast.cluster.localMember.uuid.toString(), "127.0.0.1",
                port, InstanceState.CREATING, 1, null, 64, 1, "failing"
            )
            assertTrue(state.reserveCreatingInstance(instance, 1, 1024, 100))
            state.updateInstanceState(instance.id, InstanceState.STOPPING, 2)
            val allocator = PortAllocator(state).also { it.reserve(port) }
            val registry = RuntimeRegistryImpl().apply {
                register("failing", object : RuntimeProvider {
                    override fun start(
                        instanceId: String, workingDir: Path, port: Int, command: String,
                        ramMB: Int, cpu: Int, configuration: Configuration,
                        environmentVariables: Map<String, String>?
                    ): ProcessHandle = error("unused")
                    override fun stop(instanceId: String) = error("delete failed")
                    override fun executeCommand(instanceId: String, command: String) = Unit
                    override fun isRunning(instanceId: String) = true
                })
            }

            NodeShutdownService(
                state, hazelcast, registry, allocator, InstanceWorkspace(tempDir)
            ).stopAllLocalInstances()

            assertEquals(InstanceState.STOPPING, state.getInstance(instance.id)?.state)
            assertTrue(port in allocator.getLocalAllocations())
            assertEquals(64, state.getNodeResources(instance.wrapperNodeId).usedRamMB)
        } finally {
            hazelcast.shutdown()
        }
    }
}
