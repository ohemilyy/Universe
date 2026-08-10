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
import gg.scala.universe.service.InstanceLifecycleCoordinator
import gg.scala.universe.task.DeployInstanceTask
import gg.scala.universe.task.StopInstanceTask
import gg.scala.universe.template.TemplateManager
import gg.scala.universe.template.TemplateStorageRegistryImpl
import gg.scala.universe.template.TemplateVariableRegistryImpl
import gg.scala.universe.util.json.Serializers
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
import org.junit.jupiter.api.io.TempDir
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TaskRouterLifecycleTest {
    @TempDir
    lateinit var tempDir: Path
    private lateinit var hazelcastInstance: HazelcastInstance
    private lateinit var state: ClusterStateService
    private lateinit var portAllocator: PortAllocator
    private lateinit var runtime: RecordingRuntimeProvider
    private lateinit var router: TaskRouter
    private lateinit var configuration: Configuration
    private lateinit var coordinator: InstanceLifecycleCoordinator
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
        coordinator = InstanceLifecycleCoordinator()

        val runtimeRegistry = RuntimeRegistryImpl().apply { register("fake", runtime) }
        val variableRegistry = TemplateVariableRegistryImpl()
        router = TaskRouter(
            runtimeRegistry,
            state,
            portAllocator,
            TemplateManager(variableRegistry, TemplateStorageRegistryImpl()),
            variableRegistry,
            hazelcastInstance,
            InstanceWorkspace(tempDir),
            coordinator
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
    }

    @Test
    fun `port allocation failure removes creating record`() {
        portAllocator.reserve(singlePort)
        queueCreating("new001")

        router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))

        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `missing runtime removes creating record`() {
        configuration = configuration.copy(runtime = "missing")
        state.putConfiguration(configuration)
        queueCreating("new001")

        router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))

        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `post start host lookup failure cleans runtime port and working directory`() {
        configuration = configuration.copy(static = false)
        state.putConfiguration(configuration)
        queueCreating("new001")
        runtime.failHostLookup = true

        val routingFailure = runCatching {
            router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))
        }.exceptionOrNull()

        assertNull(routingFailure)
        assertNull(state.getInstance("new001"))
        assertTrue(singlePort !in portAllocator.getLocalAllocations())
        assertEquals(listOf("start:new001", "stop:new001"), runtime.operations)
        assertTrue(Files.notExists(tempDir.resolve("running/new001")))
    }

    @Test
    fun `stop publishes stopped only after releasing port`() {
        portAllocator.reserve(singlePort)
        queueStopping("old001")
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

        router.route(StopInstanceTask("old001", expectedGeneration = 1))

        assertTrue(stoppedObserved.await(2, TimeUnit.SECONDS))
        assertTrue(stoppedSawReleased.get())
    }

    @Test
    fun `single port restart converges after release`() {
        portAllocator.reserve(singlePort)
        queueStopping("old001")

        router.route(StopInstanceTask("old001", restart = true, expectedGeneration = 1))

        val restarted = assertNotNull(state.getInstance("old001"))
        assertEquals(InstanceState.ONLINE, restarted.state)
        assertEquals(singlePort, restarted.allocatedPort)
        assertEquals(listOf("stop:old001", "start:old001"), runtime.operations)
        val saved = Serializers.GSON.fromJson(
            Files.readString(tempDir.resolve("static/${configuration.name}/.universe-state.json")),
            InstanceInfo::class.java
        )
        assertEquals(restarted, saved)
    }

    @Test
    fun `forced stop skips graceful runtime probing`() {
        portAllocator.reserve(singlePort)
        queueStopping("old001")

        router.route(StopInstanceTask("old001", force = true, expectedGeneration = 1))

        assertEquals(0, runtime.isRunningChecks)
        assertEquals(listOf("stop:old001"), runtime.operations)
    }

    @Test
    fun `stale deploy generation cannot start after stopping wins`() {
        val creating = creatingInstance("new001", allocatedPort = 0)
        assertTrue(
            state.reserveCreatingInstance(
                creating,
                generation = 7,
                maxRamMB = 1024,
                maxCpu = 100
            )
        )
        state.updateInstanceState("new001", InstanceState.STOPPING, lastHeartbeat = 2)

        router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 7))

        assertEquals(InstanceState.STOPPING, state.getInstance("new001")?.state)
        assertTrue(runtime.operations.isEmpty())
        assertTrue(singlePort !in portAllocator.getLocalAllocations())
    }

    @Test
    fun `delayed stop token cannot tear down newer incarnation`() {
        val creating = creatingInstance("old001", allocatedPort = singlePort)
        assertTrue(state.reserveCreatingInstance(creating, 8, maxRamMB = 1024, maxCpu = 100))
        state.updateInstanceState("old001", InstanceState.STOPPING, lastHeartbeat = 2)
        portAllocator.reserve(singlePort)

        router.route(StopInstanceTask("old001", force = true, expectedGeneration = 7))

        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)
        assertTrue(runtime.operations.isEmpty())
        assertTrue(singlePort in portAllocator.getLocalAllocations())
    }

    @Test
    fun `stopping that wins during deploy success rolls runtime back without deleting barrier`() {
        queueCreating("new001")
        val started = CountDownLatch(1)
        val finishStart = CountDownLatch(1)
        runtime.startBlocker = {
            started.countDown()
            finishStart.await(2, TimeUnit.SECONDS)
        }
        val deployment = CompletableFuture.runAsync {
            router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))
        }
        assertTrue(started.await(2, TimeUnit.SECONDS))
        state.updateInstanceState("new001", InstanceState.STOPPING, lastHeartbeat = 3)
        finishStart.countDown()
        deployment.get(2, TimeUnit.SECONDS)

        assertEquals(InstanceState.STOPPING, state.getInstance("new001")?.state)
        assertEquals(listOf("start:new001", "stop:new001"), runtime.operations)
        assertTrue(singlePort !in portAllocator.getLocalAllocations())
        assertEquals(configuration.ramMB, state.getNodeResources(creatingInstance("x", 0).wrapperNodeId).usedRamMB)
    }

    @Test
    fun `stopping that wins during deploy failure preserves barrier and owned reservation`() {
        queueCreating("new001")
        val started = CountDownLatch(1)
        val finishStart = CountDownLatch(1)
        runtime.startBlocker = {
            started.countDown()
            finishStart.await(2, TimeUnit.SECONDS)
        }
        runtime.failStart = true
        val deployment = CompletableFuture.runAsync {
            router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))
        }
        assertTrue(started.await(2, TimeUnit.SECONDS))
        state.updateInstanceState("new001", InstanceState.STOPPING, lastHeartbeat = 3)
        finishStart.countDown()
        deployment.get(2, TimeUnit.SECONDS)

        assertEquals(InstanceState.STOPPING, state.getInstance("new001")?.state)
        assertEquals(listOf("start:new001", "stop:new001"), runtime.operations)
        assertTrue(singlePort !in portAllocator.getLocalAllocations())
        assertEquals(
            configuration.ramMB,
            state.getNodeResources(creatingInstance("x", 0).wrapperNodeId).usedRamMB
        )
    }

    @Test
    fun `teardown failure retains stopping ownership port and resources`() {
        queueStopping("old001")
        portAllocator.reserve(singlePort)
        runtime.failStop = true

        val failure = runCatching {
            router.route(StopInstanceTask("old001", force = true, expectedGeneration = 1))
        }.exceptionOrNull()

        assertNotNull(failure)
        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)
        assertTrue(singlePort in portAllocator.getLocalAllocations())
        assertEquals(configuration.ramMB, state.getNodeResources(creatingInstance("x", 0).wrapperNodeId).usedRamMB)
    }

    @Test
    fun `failed deployment teardown becomes cleanup required stopping ownership`() {
        queueCreating("new001")
        runtime.failHostLookup = true
        runtime.failStop = true

        router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))

        val retained = assertNotNull(state.getInstance("new001"))
        assertEquals(InstanceState.STOPPING, retained.state)
        assertEquals(singlePort, retained.allocatedPort)
        assertTrue(singlePort in portAllocator.getLocalAllocations())
        assertTrue(state.hasDeploymentCleanup("new001", 2))
        assertEquals(configuration.ramMB, state.getNodeResources(retained.wrapperNodeId).usedRamMB)
    }

    @Test
    fun `shutdown quiescing wins a deploy waiting on the shared lifecycle lock`() {
        queueCreating("new001")
        val lockEntered = CountDownLatch(1)
        val releaseLock = CountDownLatch(1)
        val deployStarted = CountDownLatch(1)
        val lockHolder = CompletableFuture.runAsync {
            coordinator.withInstance("new001") {
                lockEntered.countDown()
                check(releaseLock.await(5, TimeUnit.SECONDS))
            }
        }
        assertTrue(lockEntered.await(5, TimeUnit.SECONDS))
        val deploy = CompletableFuture.runAsync {
            deployStarted.countDown()
            router.route(DeployInstanceTask("new001", configuration.name, expectedGeneration = 1))
        }
        assertTrue(deployStarted.await(5, TimeUnit.SECONDS))

        coordinator.beginShutdown()
        releaseLock.countDown()
        lockHolder.get(5, TimeUnit.SECONDS)
        deploy.get(5, TimeUnit.SECONDS)

        assertEquals(InstanceState.CREATING, state.getInstance("new001")?.state)
        assertTrue(runtime.operations.isEmpty())
        assertEquals(0, coordinator.trackedLockCount())
    }

    @Test
    fun `claimed abandoned cleanup owns delayed stop completion and blocks restart`() {
        val instance = onlineInstance("old001").copy(
            state = InstanceState.STOPPING,
            lastHeartbeat = 0
        )
        portAllocator.reserve(singlePort)
        state.addNodeResources(instance.wrapperNodeId, ramMB = 32, cpu = 2)
        assertTrue(
            state.reserveCreatingInstance(
                instance.copy(state = InstanceState.CREATING), 1, Int.MAX_VALUE, Int.MAX_VALUE
            )
        )
        state.updateInstanceState(instance.id, InstanceState.STOPPING, 0)
        val stopStarted = CountDownLatch(1)
        val allowStopCompletion = CountDownLatch(1)
        runtime.stopBlocker = {
            stopStarted.countDown()
            allowStopCompletion.await(2, TimeUnit.SECONDS)
        }
        val routing = CompletableFuture.runAsync {
            router.route(
                StopInstanceTask("old001", force = true, restart = true, expectedGeneration = 1)
            )
        }
        assertTrue(stopStarted.await(2, TimeUnit.SECONDS))
        try {
            assertTrue(
                state.claimAbandonedStopping(
                    instanceId = "old001",
                    expectedLastHeartbeat = 0,
                    stoppedAt = 120_001
                )
            )
        } finally {
            allowStopCompletion.countDown()
        }

        routing.get(2, TimeUnit.SECONDS)

        assertEquals(32, state.getNodeResources(instance.wrapperNodeId).usedRamMB)
        assertEquals(2, state.getNodeResources(instance.wrapperNodeId).usedCpu)
        assertEquals(InstanceState.STOPPED, state.getInstance("old001")?.state)
        assertEquals(120_001, state.getInstance("old001")?.lastHeartbeat)
        assertEquals(listOf("stop:old001"), runtime.operations)
        assertTrue(singlePort !in portAllocator.getLocalAllocations())
        assertEquals(1, state.completePendingAbandonedStoppingCleanups())
        assertNull(state.getInstance("old001"))
        assertEquals(32, state.getNodeResources(instance.wrapperNodeId).usedRamMB)
        assertEquals(2, state.getNodeResources(instance.wrapperNodeId).usedCpu)
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

    private fun queueCreating(id: String): InstanceInfo {
        val instance = creatingInstance(id, allocatedPort = 0)
        assertTrue(state.reserveCreatingInstance(instance, 1, maxRamMB = 1024, maxCpu = 100))
        return instance
    }

    private fun queueStopping(id: String): InstanceInfo {
        val creating = creatingInstance(id, allocatedPort = singlePort)
        assertTrue(state.reserveCreatingInstance(creating, 1, maxRamMB = 1024, maxCpu = 100))
        state.updateInstanceState(id, InstanceState.STOPPING, lastHeartbeat = 2)
        return state.getInstance(id)!!
    }

    private class RecordingRuntimeProvider : RuntimeProvider {
        val operations = mutableListOf<String>()
        var isRunningChecks = 0
        var failHostLookup = false
        var failStart = false
        var failStop = false
        var startBlocker: (() -> Unit)? = null
        var stopBlocker: (() -> Unit)? = null

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
            startBlocker?.invoke()
            if (failStart) error("startup failed")
            return FixedProcessHandle
        }

        override fun stop(instanceId: String) {
            operations += "stop:$instanceId"
            stopBlocker?.invoke()
            if (failStop) error("teardown failed")
        }

        override fun executeCommand(instanceId: String, command: String) = Unit

        override fun isRunning(instanceId: String): Boolean {
            isRunningChecks++
            return false
        }

        override fun getHostAddress(instanceId: String): String {
            if (failHostLookup) error("host lookup failed")
            return ""
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
