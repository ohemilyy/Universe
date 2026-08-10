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
import gg.scala.universe.util.json.Serializers
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.junit.jupiter.api.io.TempDir
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InstanceRecoveryServiceTest {
    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `static state is scanned and rebound to current wrapper with owned resources`() {
        val hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "recovery-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
        try {
            val state = ClusterStateService(hazelcast)
            val config = Configuration(name = "site", runtime = "fake", static = true, ramMB = 64, cpu = 1)
            state.putConfiguration(config)
            val port = ServerSocket(0).use { it.localPort }
            val persisted = InstanceInfo(
                "old001", config.name, "departed-wrapper", "127.0.0.1", port,
                InstanceState.ONLINE, 1, 42, config.ramMB, config.cpu, config.runtime
            )
            val stateDir = tempDir.resolve("static/site")
            Files.createDirectories(stateDir)
            Files.writeString(stateDir.resolve(".universe-state.json"), Serializers.GSON.toJson(persisted))
            val registry = RuntimeRegistryImpl().apply { register("fake", AlwaysRunningRuntime) }
            val allocator = PortAllocator(state)

            InstanceRecoveryService(
                state, hazelcast, registry, allocator, InstanceWorkspace(tempDir)
            ).recover()

            val recovered = state.getInstance(persisted.id)!!
            assertEquals(hazelcast.cluster.localMember.uuid.toString(), recovered.wrapperNodeId)
            assertEquals(InstanceState.ONLINE, recovered.state)
            assertEquals(64, state.getNodeResources(recovered.wrapperNodeId).usedRamMB)
            assertTrue(port in allocator.getLocalAllocations())
        } finally {
            hazelcast.shutdown()
        }
    }

    @Test
    fun `runtime discovery rebinds durable metadata without a local state file`() {
        val hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "recovery-runtime-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
        try {
            val state = ClusterStateService(hazelcast)
            val config = Configuration(name = "site", runtime = "fake", ramMB = 64, cpu = 1)
            state.putConfiguration(config)
            val port = ServerSocket(0).use { it.localPort }
            val persisted = InstanceInfo(
                "old002", config.name, "departed-wrapper", "127.0.0.1", port,
                InstanceState.ONLINE, 1, 42, config.ramMB, config.cpu, config.runtime
            )
            state.putInstance(persisted)
            val runtime = object : RuntimeProvider by AlwaysRunningRuntime {
                override fun listRunningInstances(): List<String> = listOf(persisted.id)
            }
            val registry = RuntimeRegistryImpl().apply { register("fake", runtime) }
            val allocator = PortAllocator(state)

            InstanceRecoveryService(
                state, hazelcast, registry, allocator, InstanceWorkspace(tempDir)
            ).recover()

            val recovered = state.getInstance(persisted.id)!!
            assertEquals(hazelcast.cluster.localMember.uuid.toString(), recovered.wrapperNodeId)
            assertEquals(InstanceState.ONLINE, recovered.state)
            assertEquals(64, state.getNodeResources(recovered.wrapperNodeId).usedRamMB)
            assertTrue(port in allocator.getLocalAllocations())
        } finally {
            hazelcast.shutdown()
        }
    }

    @Test
    fun `recovery port conflict cannot publish or account a filesystem candidate`() {
        val hazelcast = newHazelcast("recovery-conflict")
        try {
            val state = ClusterStateService(hazelcast)
            val config = Configuration(name = "site", runtime = "fake", static = true, ramMB = 64, cpu = 1)
            state.putConfiguration(config)
            val port = ServerSocket(0).use { it.localPort }
            val persisted = InstanceInfo(
                "old003", config.name, "departed", "127.0.0.1", port,
                InstanceState.ONLINE, 1, 42, config.ramMB, config.cpu, config.runtime
            )
            val stateDir = tempDir.resolve("static/site")
            Files.createDirectories(stateDir)
            Files.writeString(stateDir.resolve(".universe-state.json"), Serializers.GSON.toJson(persisted))
            val allocator = PortAllocator(state)
            assertTrue(allocator.reserve(port))

            InstanceRecoveryService(
                state,
                hazelcast,
                RuntimeRegistryImpl().apply { register("fake", AlwaysRunningRuntime) },
                allocator,
                InstanceWorkspace(tempDir)
            ).recover()

            assertNull(state.getInstance(persisted.id))
            assertEquals(0, state.getNodeResources(hazelcast.cluster.localMember.uuid.toString()).usedRamMB)
        } finally {
            hazelcast.shutdown()
        }
    }

    @Test
    fun `recovery never overwrites an exact stopping barrier`() {
        val hazelcast = newHazelcast("recovery-stopping")
        try {
            val state = ClusterStateService(hazelcast)
            val config = Configuration(name = "site", runtime = "fake", static = true, ramMB = 64, cpu = 1)
            state.putConfiguration(config)
            val port = ServerSocket(0).use { it.localPort }
            val creating = InstanceInfo(
                "old004", config.name, hazelcast.cluster.localMember.uuid.toString(), "127.0.0.1", port,
                InstanceState.CREATING, 1, 42, config.ramMB, config.cpu, config.runtime
            )
            assertTrue(state.reserveCreatingInstance(creating, 1, 1024, 100))
            val stopping = creating.copy(state = InstanceState.STOPPING, lastHeartbeat = 2)
            assertTrue(state.transitionLifecycle(creating, 1, stopping, 2))

            InstanceRecoveryService(
                state,
                hazelcast,
                RuntimeRegistryImpl().apply { register("fake", AlwaysRunningRuntime) },
                PortAllocator(state),
                InstanceWorkspace(tempDir)
            ).recover()

            assertEquals(stopping, state.getInstance(stopping.id))
            assertEquals(2, state.getLifecycleGeneration(stopping.id))
        } finally {
            hazelcast.shutdown()
        }
    }

    private fun newHazelcast(prefix: String) = Hazelcast.newHazelcastInstance(Config().apply {
        clusterName = "$prefix-${UUID.randomUUID()}"
        setProperty("hazelcast.logging.type", "none")
        networkConfig.port = 0
        networkConfig.join.autoDetectionConfig.isEnabled = false
        networkConfig.join.multicastConfig.isEnabled = false
        networkConfig.join.tcpIpConfig.isEnabled = false
    })

    private object AlwaysRunningRuntime : RuntimeProvider {
        override fun start(
            instanceId: String, workingDir: Path, port: Int, command: String,
            ramMB: Int, cpu: Int, configuration: Configuration,
            environmentVariables: Map<String, String>?
        ): ProcessHandle = error("unused")
        override fun stop(instanceId: String) = Unit
        override fun executeCommand(instanceId: String, command: String) = Unit
        override fun isRunning(instanceId: String) = true
    }
}
