package gg.scala.universe.service

import com.hazelcast.cluster.Member
import com.hazelcast.config.Config
import com.hazelcast.core.EntryEvent
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.listener.EntryUpdatedListener
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.TaskRouter
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
import java.util.stream.Stream
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InstanceCountEnforcerTest {
    private lateinit var hazelcastInstance: HazelcastInstance
    private lateinit var state: ClusterStateService
    private lateinit var configuration: Configuration
    private lateinit var stopDispatcher: RecordingStopDispatcher

    @BeforeTest
    fun setUp() {
        val hazelcastConfig = Config().apply {
            clusterName = "instance-enforcer-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        }
        hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig)
        state = ClusterStateService(hazelcastInstance)
        configuration = Configuration(name = "site", minimumServiceCount = 1)
        state.putConfiguration(configuration)
        stopDispatcher = RecordingStopDispatcher(state)
    }

    @AfterTest
    fun tearDown() {
        hazelcastInstance.shutdown()
        deleteRecursively(Path.of("./static/${configuration.name}"))
    }

    @Test
    fun `stale creating is removed before replacement is spawned`() {
        state.putInstance(instance("old001", InstanceState.CREATING, lastHeartbeat = 0))
        val spawner = StateWritingSpawner(state, localMemberId())
        val enforcer = enforcer(spawner)

        enforcer.enforceOnce(now = 60_001)

        assertNull(state.getInstance("old001"))
        assertEquals(InstanceState.ONLINE, state.getInstance("new001")?.state)
    }

    @Test
    fun `stale stopping on dead wrapper is finalized before replacement`() {
        val stoppedPublished = CountDownLatch(1)
        state.instances.addEntryListener(
            EntryUpdatedListener<String, InstanceInfo> { event: EntryEvent<String, InstanceInfo> ->
                if (event.key == "old001" && event.value.state == InstanceState.STOPPED) {
                    stoppedPublished.countDown()
                }
            },
            true
        )
        state.addNodeResources("departed", configuration.ramMB, configuration.cpu)
        state.putInstance(
            instance(
                id = "old001",
                state = InstanceState.STOPPING,
                wrapper = "departed",
                lastHeartbeat = 0
            )
        )
        val spawner = StateWritingSpawner(state, localMemberId())
        val enforcer = enforcer(spawner)

        enforcer.enforceOnce(now = 120_001)

        assertTrue(stoppedPublished.await(2, TimeUnit.SECONDS))
        assertNull(state.getInstance("old001"))
        assertEquals(0, state.getNodeResources("departed").usedRamMB)
        assertEquals(0, state.getNodeResources("departed").usedCpu)
        assertEquals(InstanceState.ONLINE, state.getInstance("new001")?.state)
        assertTrue(stopDispatcher.invocations.isEmpty())
    }

    @Test
    fun `stale stopping on live wrapper refreshes throttle and forces stop`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance(
                id = "old001",
                state = InstanceState.STOPPING,
                wrapper = wrapperId,
                lastHeartbeat = 0
            )
        )
        val enforcer = enforcer(StateWritingSpawner(state, wrapperId))

        enforcer.enforceOnce(now = 120_001)

        assertEquals(120_001, state.getInstance("old001")?.lastHeartbeat)
        assertEquals(
            listOf(StopInvocation("old001", wrapperId, force = true, restart = false)),
            stopDispatcher.invocations
        )
        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `single port stop and replacement converges without deleting stopped record`() {
        val singlePort = ServerSocket(0).use { it.localPort }
        configuration = configuration.copy(
            name = "single-$singlePort",
            runtime = "fake",
            command = "run",
            static = true,
            ramMB = 64,
            cpu = 1,
            availablePorts = PortRange(singlePort, singlePort, "sequential")
        )
        state.configurations.clear()
        state.putConfiguration(configuration)

        val portAllocator = PortAllocator(state)
        val runtime = RecordingRuntimeProvider()
        val runtimeRegistry = RuntimeRegistryImpl().apply { register("fake", runtime) }
        val variableRegistry = TemplateVariableRegistryImpl()
        val router = TaskRouter(
            runtimeRegistry,
            state,
            portAllocator,
            TemplateManager(variableRegistry, TemplateStorageRegistryImpl()),
            variableRegistry,
            hazelcastInstance
        )
        val wrapperId = localMemberId()
        val oldInstance = instance(
            id = "old001",
            state = InstanceState.ONLINE,
            wrapper = wrapperId,
            lastHeartbeat = 10_000,
            allocatedPort = singlePort
        ).copy(
            allocatedRamMB = configuration.ramMB,
            allocatedCpu = configuration.cpu,
            runtime = configuration.runtime
        )
        portAllocator.reserve(singlePort)
        state.addNodeResources(wrapperId, configuration.ramMB, configuration.cpu)
        state.putInstance(oldInstance)

        stopDispatcher.transitionAt = 10_001
        assertEquals(
            StopDispatchResult.DISPATCHED,
            stopDispatcher.dispatchStop("old001", hazelcastInstance.cluster.localMember)
        )
        val spawner = RoutingSpawner(state, wrapperId, router)
        val enforcer = enforcer(spawner)

        enforcer.enforceOnce(now = 10_002)

        assertEquals(setOf("old001"), state.getAllInstances().mapTo(mutableSetOf()) { it.id })
        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)

        router.route(StopInstanceTask("old001"))
        assertEquals(InstanceState.STOPPED, state.getInstance("old001")?.state)
        assertTrue(singlePort !in portAllocator.getLocalAllocations())

        enforcer.enforceOnce(now = 10_003)

        val replacement = assertNotNull(state.getInstance("new001"))
        assertEquals(InstanceState.ONLINE, replacement.state)
        assertEquals(singlePort, replacement.allocatedPort)
        assertEquals(InstanceState.STOPPED, state.getInstance("old001")?.state)
        assertEquals(1, state.getAllInstances().count { it.state == InstanceState.ONLINE })
        assertEquals(listOf("stop:old001", "start:new001"), runtime.operations)
    }

    private fun enforcer(spawner: InstanceSpawner) = InstanceCountEnforcer(
        clusterStateService = state,
        hazelcastInstance = hazelcastInstance,
        instanceStopDispatcher = stopDispatcher,
        instanceSpawner = spawner,
        configuration = UniverseMainConfiguration(isMasterNode = true)
    )

    private fun localMemberId(): String = hazelcastInstance.cluster.localMember.uuid.toString()

    private fun instance(
        id: String,
        state: InstanceState,
        wrapper: String = localMemberId(),
        lastHeartbeat: Long,
        allocatedPort: Int = 25565
    ) = InstanceInfo(
        id = id,
        configurationName = configuration.name,
        wrapperNodeId = wrapper,
        hostAddress = "127.0.0.1",
        allocatedPort = allocatedPort,
        state = state,
        lastHeartbeat = lastHeartbeat,
        processPid = null,
        allocatedRamMB = configuration.ramMB,
        allocatedCpu = configuration.cpu,
        runtime = configuration.runtime
    )

    private fun deleteRecursively(path: Path) {
        if (!Files.exists(path)) return
        Files.walk(path).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
        }
    }

    private data class StopInvocation(
        val instanceId: String,
        val memberId: String,
        val force: Boolean,
        val restart: Boolean
    )

    private class RecordingStopDispatcher(
        private val state: ClusterStateService
    ) : InstanceStopDispatcher {
        val invocations = mutableListOf<StopInvocation>()
        var transitionAt: Long = 1

        override fun dispatchStop(
            instanceId: String,
            targetMember: Member,
            force: Boolean,
            restart: Boolean
        ): StopDispatchResult {
            val instance = state.getInstance(instanceId) ?: return StopDispatchResult.NOT_FOUND
            if (instance.state == InstanceState.STOPPING && !force) {
                return StopDispatchResult.ALREADY_STOPPING
            }
            if (!force) {
                state.updateInstanceState(instanceId, InstanceState.STOPPING, transitionAt)
            }
            invocations += StopInvocation(instanceId, targetMember.uuid.toString(), force, restart)
            return StopDispatchResult.DISPATCHED
        }
    }

    private class StateWritingSpawner(
        private val state: ClusterStateService,
        private val wrapperId: String
    ) : InstanceSpawner {
        override fun createInstance(configuration: Configuration, instanceId: String?): InstanceInfo {
            return InstanceInfo(
                id = instanceId ?: "new001",
                configurationName = configuration.name,
                wrapperNodeId = wrapperId,
                hostAddress = configuration.hostAddress,
                allocatedPort = configuration.availablePorts.min,
                state = InstanceState.ONLINE,
                lastHeartbeat = 1,
                processPid = null,
                allocatedRamMB = configuration.ramMB,
                allocatedCpu = configuration.cpu,
                runtime = configuration.runtime
            ).also(state::putInstance)
        }
    }

    private class RoutingSpawner(
        private val state: ClusterStateService,
        private val wrapperId: String,
        private val router: TaskRouter
    ) : InstanceSpawner {
        override fun createInstance(configuration: Configuration, instanceId: String?): InstanceInfo? {
            val id = instanceId ?: "new001"
            state.putInstance(
                InstanceInfo(
                    id = id,
                    configurationName = configuration.name,
                    wrapperNodeId = wrapperId,
                    hostAddress = configuration.hostAddress,
                    allocatedPort = 0,
                    state = InstanceState.CREATING,
                    lastHeartbeat = 10_003,
                    processPid = null,
                    allocatedRamMB = configuration.ramMB,
                    allocatedCpu = configuration.cpu,
                    runtime = configuration.runtime
                )
            )
            router.route(DeployInstanceTask(id, configuration.name))
            return state.getInstance(id)
        }
    }

    private class RecordingRuntimeProvider : RuntimeProvider {
        val operations = mutableListOf<String>()

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

        override fun isRunning(instanceId: String): Boolean = false

        override fun getHostAddress(instanceId: String): String = ""
    }

    private object FixedProcessHandle : ProcessHandle {
        override fun pid(): Long = 4242L
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
