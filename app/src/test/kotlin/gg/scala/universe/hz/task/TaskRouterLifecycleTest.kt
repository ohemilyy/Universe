package gg.scala.universe.hz.task

import com.hazelcast.config.Config
import com.hazelcast.core.EntryEvent
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.listener.EntryUpdatedListener
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeProvider
import gg.scala.universe.runtime.RuntimeRegistryImpl
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.schema.PortRange
import gg.scala.universe.task.DeployInstanceTask
import gg.scala.universe.task.StopInstanceTask
import gg.scala.universe.template.TemplateManager
import gg.scala.universe.template.TemplateStorageRegistryImpl
import gg.scala.universe.template.TemplateVariableRegistryImpl
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Comparator
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Stream
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TaskRouterLifecycleTest {
    private lateinit var hazelcastInstance: HazelcastInstance
    private lateinit var state: ClusterStateService
    private lateinit var portAllocator: PortAllocator
    private lateinit var runtime: RecordingRuntimeProvider
    private lateinit var router: TaskRouter
    private lateinit var configuration: Configuration
    private var singlePort: Int = 0

    @BeforeTest
    fun setUp() {
        val hazelcastConfig = Config().apply {
            clusterName = "task-router-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        }
        hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig)
        state = ClusterStateService(hazelcastInstance)
        portAllocator = PortAllocator(state)
        runtime = RecordingRuntimeProvider()

        val runtimeRegistry = RuntimeRegistryImpl().apply { register("fake", runtime) }
        val variableRegistry = TemplateVariableRegistryImpl()
        router = TaskRouter(
            runtimeRegistry,
            state,
            portAllocator,
            TemplateManager(variableRegistry, TemplateStorageRegistryImpl()),
            variableRegistry,
            hazelcastInstance
        )

        singlePort = ServerSocket(0).use { it.localPort }
        configuration = Configuration(
            name = "site-$singlePort",
            runtime = "fake",
            command = "run",
            static = true,
            ramMB = 64,
            cpu = 1,
            availablePorts = PortRange(singlePort, singlePort, "sequential")
        )
        state.putConfiguration(configuration)
    }

    @AfterTest
    fun tearDown() {
        hazelcastInstance.shutdown()
        deleteRecursively(Path.of("./static/${configuration.name}"))
    }

    @Test
    fun `port allocation failure removes creating record`() {
        portAllocator.reserve(singlePort)
        state.putInstance(creatingInstance("new001", allocatedPort = 0))

        router.route(DeployInstanceTask("new001", configuration.name))

        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `stop publishes stopped only after releasing port`() {
        portAllocator.reserve(singlePort)
        state.putInstance(onlineInstance("old001"))
        val stoppedSawReleased = AtomicBoolean(false)
        val stoppedObserved = CountDownLatch(1)
        state.instances.addEntryListener(
            EntryUpdatedListener<String, InstanceInfo> { event: EntryEvent<String, InstanceInfo> ->
                if (event.value.state == InstanceState.STOPPED) {
                    stoppedSawReleased.set(singlePort !in portAllocator.getLocalAllocations())
                    stoppedObserved.countDown()
                }
            },
            true
        )

        router.route(StopInstanceTask("old001"))

        assertTrue(stoppedObserved.await(2, TimeUnit.SECONDS))
        assertTrue(stoppedSawReleased.get())
    }

    @Test
    fun `single port restart converges after release`() {
        portAllocator.reserve(singlePort)
        state.putInstance(onlineInstance("old001"))

        router.route(StopInstanceTask("old001", restart = true))

        val restarted = assertNotNull(state.getInstance("old001"))
        assertEquals(InstanceState.ONLINE, restarted.state)
        assertEquals(singlePort, restarted.allocatedPort)
        assertEquals(listOf("stop:old001", "start:old001"), runtime.operations)
    }

    @Test
    fun `forced stop skips graceful runtime probing`() {
        portAllocator.reserve(singlePort)
        state.putInstance(onlineInstance("old001"))

        router.route(StopInstanceTask("old001", force = true))

        assertEquals(0, runtime.isRunningChecks)
        assertEquals(listOf("stop:old001"), runtime.operations)
    }

    private fun creatingInstance(id: String, allocatedPort: Int) = InstanceInfo(
        id = id,
        configurationName = configuration.name,
        wrapperNodeId = hazelcastInstance.cluster.localMember.uuid.toString(),
        hostAddress = "127.0.0.1",
        allocatedPort = allocatedPort,
        state = InstanceState.CREATING,
        lastHeartbeat = 1,
        processPid = null,
        allocatedRamMB = configuration.ramMB,
        allocatedCpu = configuration.cpu,
        runtime = configuration.runtime
    )

    private fun onlineInstance(id: String) = creatingInstance(id, singlePort).copy(
        state = InstanceState.ONLINE,
        processPid = RecordingRuntimeProvider.PROCESS_PID
    )

    private fun deleteRecursively(path: Path) {
        if (!Files.exists(path)) return
        Files.walk(path).use { stream ->
            stream.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
        }
    }

    private class RecordingRuntimeProvider : RuntimeProvider {
        val operations = mutableListOf<String>()
        var isRunningChecks = 0

        override fun start(
            instanceId: String,
            workingDir: Path,
            port: Int,
            command: String,
            ramMB: Int,
            cpu: Int,
            configuration: Configuration,
            environmentVariables: Map<String, String>?
        ): ProcessHandle {
            operations += "start:$instanceId"
            return FixedProcessHandle
        }

        override fun stop(instanceId: String) {
            operations += "stop:$instanceId"
        }

        override fun executeCommand(instanceId: String, command: String) = Unit

        override fun isRunning(instanceId: String): Boolean {
            isRunningChecks++
            return false
        }

        companion object {
            const val PROCESS_PID = 4242L
        }
    }

    private object FixedProcessHandle : ProcessHandle {
        override fun pid(): Long = RecordingRuntimeProvider.PROCESS_PID
        override fun parent(): Optional<ProcessHandle> = Optional.empty()
        override fun children(): Stream<ProcessHandle> = Stream.empty()
        override fun descendants(): Stream<ProcessHandle> = Stream.empty()
        override fun info(): ProcessHandle.Info = FixedProcessInfo
        override fun onExit(): CompletableFuture<ProcessHandle> = CompletableFuture.completedFuture(this)
        override fun supportsNormalTermination(): Boolean = true
        override fun destroy(): Boolean = true
        override fun destroyForcibly(): Boolean = true
        override fun isAlive(): Boolean = true
        override fun compareTo(other: ProcessHandle): Int = pid().compareTo(other.pid())
    }

    private object FixedProcessInfo : ProcessHandle.Info {
        override fun command(): Optional<String> = Optional.empty()
        override fun commandLine(): Optional<String> = Optional.empty()
        override fun arguments(): Optional<Array<String>> = Optional.empty()
        override fun startInstant(): Optional<Instant> = Optional.empty()
        override fun totalCpuDuration(): Optional<Duration> = Optional.empty()
        override fun user(): Optional<String> = Optional.empty()
    }
}
