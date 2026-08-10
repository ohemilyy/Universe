package gg.scala.universe.service

import com.hazelcast.cluster.Member
import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.config.Config
import com.hazelcast.core.MemberLeftException
import com.hazelcast.core.EntryEvent
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.listener.EntryRemovedListener
import com.hazelcast.map.listener.EntryUpdatedListener
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.ResilienceMembershipListener
import gg.scala.universe.hz.task.TaskDispatcher
import gg.scala.universe.hz.task.TaskRouter
import gg.scala.universe.hz.task.StopTaskSubmissionGateway
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
import java.lang.reflect.Proxy
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
import java.util.concurrent.atomic.AtomicReference
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
    private val testOwnedStaticPaths = mutableSetOf<Path>()

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
        configuration = Configuration(
            name = "enforcer-${UUID.randomUUID()}",
            minimumServiceCount = 1
        )
        state.putConfiguration(configuration)
        stopDispatcher = RecordingStopDispatcher(state)
    }

    @AfterTest
    fun tearDown() {
        hazelcastInstance.shutdown()
        testOwnedStaticPaths.forEach(::deleteRecursively)
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
    fun `creating that becomes online before reap cancels stale planned spawn`() {
        configuration = configuration.copy(minimumServiceCount = 2)
        state.putConfiguration(configuration)
        val firstRemoved = removalLatch("aaa000")
        state.putInstance(instance("aaa000", InstanceState.CREATING, lastHeartbeat = 0))
        state.putInstance(instance("zzz999", InstanceState.CREATING, lastHeartbeat = 0))
        val enforcer = enforcer(StateWritingSpawner(state, localMemberId()))

        enforceWhileCleanupBlocked(enforcer, "zzz999", firstRemoved, now = 60_001) {
            val current = assertNotNull(state.getInstance("zzz999"))
            state.putInstance(current.copy(state = InstanceState.ONLINE, lastHeartbeat = 60_001))
        }

        assertEquals(InstanceState.ONLINE, state.getInstance("zzz999")?.state)
        assertNull(state.getInstance("new001"))
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
    fun `member removal preserves stopping until enforcement reaps it`() {
        val departedId = UUID.randomUUID()
        state.addNodeResources(departedId.toString(), configuration.ramMB, configuration.cpu)
        state.putInstance(
            instance(
                id = "old001",
                state = InstanceState.STOPPING,
                wrapper = departedId.toString(),
                lastHeartbeat = 0
            )
        )
        val departedMember = absentMember(departedId)
        val event = MembershipEvent(
            hazelcastInstance.cluster,
            departedMember,
            MembershipEvent.MEMBER_REMOVED,
            hazelcastInstance.cluster.members
        )

        ResilienceMembershipListener(state).memberRemoved(event)

        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)
        assertEquals(0, state.getInstance("old001")?.lastHeartbeat)

        enforcer(StateWritingSpawner(state, localMemberId())).enforceOnce(now = 120_001)

        assertNull(state.getInstance("old001"))
        assertEquals(InstanceState.ONLINE, state.getInstance("new001")?.state)
        assertEquals(0, state.getNodeResources(departedId.toString()).usedRamMB)
        assertEquals(0, state.getNodeResources(departedId.toString()).usedCpu)
    }

    @Test
    fun `pending abandoned cleanup is reaped before replacement planning`() {
        state.addNodeResources("departed", configuration.ramMB + 32, configuration.cpu + 2)
        state.putInstance(
            instance("old001", InstanceState.STOPPING, "departed", lastHeartbeat = 0)
        )
        assertTrue(
            state.claimAbandonedStopping(
                instanceId = "old001",
                expectedLastHeartbeat = 0,
                stoppedAt = 120_001
            )
        )
        val enforcer = enforcer(StateWritingSpawner(state, localMemberId()))

        enforcer.enforceOnce(now = 120_002)

        assertNull(state.getInstance("old001"))
        assertEquals(32, state.getNodeResources("departed").usedRamMB)
        assertEquals(2, state.getNodeResources("departed").usedCpu)
        assertEquals(InstanceState.ONLINE, state.getInstance("new001")?.state)
    }

    @Test
    fun `abandoned stopping refreshed before cleanup cancels stale planned spawn`() {
        configuration = configuration.copy(minimumServiceCount = 2)
        state.putConfiguration(configuration)
        val firstRemoved = removalLatch("aaa000")
        state.addNodeResources(
            "departed",
            configuration.ramMB * 2,
            configuration.cpu * 2
        )
        state.putInstance(
            instance("aaa000", InstanceState.STOPPING, "departed", lastHeartbeat = 0)
        )
        state.putInstance(
            instance("zzz999", InstanceState.STOPPING, "departed", lastHeartbeat = 0)
        )
        val enforcer = enforcer(StateWritingSpawner(state, localMemberId()))

        enforceWhileCleanupBlocked(enforcer, "zzz999", firstRemoved, now = 120_001) {
            val current = assertNotNull(state.getInstance("zzz999"))
            state.putInstance(current.copy(lastHeartbeat = 120_001))
        }

        assertEquals(InstanceState.STOPPING, state.getInstance("zzz999")?.state)
        assertEquals(120_001, state.getInstance("zzz999")?.lastHeartbeat)
        assertNull(state.getInstance("new001"))
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
            listOf(
                StopInvocation(
                    "old001",
                    wrapperId,
                    force = true,
                    restart = false,
                    expectedLastHeartbeat = 0,
                    transitionAt = 120_001
                )
            ),
            stopDispatcher.invocations
        )
        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `forced redispatch does not rewrite instance that became online after planning`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        stopDispatcher.beforeDispatch = {
            val current = assertNotNull(state.getInstance("old001"))
            state.putInstance(current.copy(state = InstanceState.ONLINE, lastHeartbeat = 99))
        }
        val enforcer = enforcer(StateWritingSpawner(state, wrapperId))

        enforcer.enforceOnce(now = 120_001)

        assertEquals(InstanceState.ONLINE, state.getInstance("old001")?.state)
        assertEquals(99, state.getInstance("old001")?.lastHeartbeat)
        assertTrue(stopDispatcher.invocations.isEmpty())
    }

    @Test
    fun `task dispatcher rejects forced redispatch when stopping timestamp changed`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 99)
        )
        val dispatcher = TaskDispatcher(hazelcastInstance, state)

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.STALE_TRANSITION, result)
        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)
        assertEquals(99, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `task dispatcher rejects forced redispatch when state changed`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.ONLINE, wrapperId, lastHeartbeat = 99)
        )
        val dispatcher = TaskDispatcher(hazelcastInstance, state)

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.STALE_TRANSITION, result)
        assertEquals(InstanceState.ONLINE, state.getInstance("old001")?.state)
        assertEquals(99, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `task dispatcher rejects target that left before forced redispatch`() {
        val departedId = UUID.randomUUID()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, departedId.toString(), lastHeartbeat = 0)
        )
        val dispatcher = TaskDispatcher(hazelcastInstance, state)

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = absentMember(departedId),
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.TARGET_UNAVAILABLE, result)
        assertEquals(0, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `task dispatcher rejects live fallback that is not the recorded wrapper`() {
        state.putInstance(
            instance("old001", InstanceState.STOPPING, "departed", lastHeartbeat = 0)
        )
        val dispatcher = TaskDispatcher(hazelcastInstance, state)

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.TARGET_UNAVAILABLE, result)
        assertEquals(0, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `member departure during submission restores stale transition`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        val dispatcher = TaskDispatcher(
            hazelcastInstance,
            state,
            StopTaskSubmissionGateway { _, _ ->
                CompletableFuture.failedFuture(MemberLeftException("member left"))
            }
        )

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.TARGET_UNAVAILABLE, result)
        assertEquals(InstanceState.STOPPING, state.getInstance("old001")?.state)
        assertEquals(0, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `submission failure cannot roll a newer online transition back to stopping`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        val dispatcher = TaskDispatcher(
            hazelcastInstance,
            state,
            StopTaskSubmissionGateway { _, _ ->
                val current = assertNotNull(state.getInstance("old001"))
                state.putInstance(current.copy(state = InstanceState.ONLINE, lastHeartbeat = 77))
                CompletableFuture.failedFuture(MemberLeftException("member left"))
            }
        )

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )

        assertEquals(StopDispatchResult.TARGET_UNAVAILABLE, result)
        assertEquals(InstanceState.ONLINE, state.getInstance("old001")?.state)
        assertEquals(77, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `target unavailable overrides stale membership but preserves newer transition`() {
        val wrapperId = localMemberId()
        state.addNodeResources(
            wrapperId,
            ramMB = configuration.ramMB * 2 + 32,
            cpu = configuration.cpu * 2 + 2
        )
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        state.putInstance(
            instance("old002", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        val dispatcher = TaskDispatcher(
            hazelcastInstance,
            state,
            StopTaskSubmissionGateway { task, _ ->
                if (task.instanceId == "old002") {
                    val current = assertNotNull(state.getInstance(task.instanceId))
                    state.putInstance(
                        current.copy(state = InstanceState.ONLINE, lastHeartbeat = 77)
                    )
                }
                CompletableFuture.failedFuture(MemberLeftException("member left"))
            }
        )
        val enforcer = InstanceCountEnforcer(
            clusterStateService = state,
            hazelcastInstance = hazelcastInstance,
            instanceStopDispatcher = dispatcher,
            instanceSpawner = StateWritingSpawner(state, wrapperId),
            configuration = UniverseMainConfiguration(isMasterNode = true)
        )

        enforcer.enforceOnce(now = 120_001)

        assertNull(state.getInstance("old001"))
        assertEquals(InstanceState.ONLINE, state.getInstance("old002")?.state)
        assertEquals(77, state.getInstance("old002")?.lastHeartbeat)
        assertEquals(configuration.ramMB + 32, state.getNodeResources(wrapperId).usedRamMB)
        assertEquals(configuration.cpu + 2, state.getNodeResources(wrapperId).usedCpu)
        assertNull(state.getInstance("new001"))
    }

    @Test
    fun `accepted stop submission wait is bounded`() {
        val wrapperId = localMemberId()
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        val pending = CompletableFuture<String>()
        val dispatcher = TaskDispatcher(
            hazelcastInstance,
            state,
            StopTaskSubmissionGateway { _, _ -> pending }
        )

        val startedAt = System.nanoTime()
        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            expectedLastHeartbeat = 0,
            transitionAt = 120_001
        )
        val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
        pending.cancel(true)

        assertEquals(StopDispatchResult.DISPATCHED, result)
        assertTrue(elapsedMs < 2_000, "dispatch waited ${elapsedMs}ms")
        assertEquals(120_001, state.getInstance("old001")?.lastHeartbeat)
    }

    @Test
    fun `task dispatcher cannot overwrite claimed stopped snapshot`() {
        val wrapperId = localMemberId()
        state.addNodeResources(wrapperId, configuration.ramMB, configuration.cpu)
        state.putInstance(
            instance("old001", InstanceState.STOPPING, wrapperId, lastHeartbeat = 0)
        )
        assertTrue(state.claimAbandonedStopping("old001", 0, stoppedAt = 120_001))
        val dispatcher = TaskDispatcher(
            hazelcastInstance,
            state,
            StopTaskSubmissionGateway { _, _ -> CompletableFuture.completedFuture("ignored") }
        )

        val result = dispatcher.dispatchStop(
            instanceId = "old001",
            targetMember = hazelcastInstance.cluster.localMember,
            force = true,
            transitionAt = 120_002
        )

        assertEquals(StopDispatchResult.NOT_FOUND, result)
        assertEquals(InstanceState.STOPPED, state.getInstance("old001")?.state)
        assertEquals(120_001, state.getInstance("old001")?.lastHeartbeat)
        assertEquals(1, state.completePendingAbandonedStoppingCleanups())
        assertNull(state.getInstance("old001"))
    }

    @Test
    fun `single port stop and replacement converges without deleting stopped record`() {
        val singlePort = ServerSocket(0).use { it.localPort }
        configuration = configuration.copy(
            name = "single-$singlePort-${UUID.randomUUID()}",
            runtime = "fake",
            command = "run",
            static = true,
            ramMB = 64,
            cpu = 1,
            availablePorts = PortRange(singlePort, singlePort, "sequential")
        )
        state.configurations.clear()
        state.putConfiguration(configuration)
        val testStaticPath = Path.of("./static/${configuration.name}")
            .toAbsolutePath()
            .normalize()
        assertTrue(Files.notExists(testStaticPath))
        testOwnedStaticPaths.add(testStaticPath)

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

        assertEquals(
            StopDispatchResult.DISPATCHED,
            stopDispatcher.dispatchStop(
                "old001",
                hazelcastInstance.cluster.localMember,
                transitionAt = 10_001
            )
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

    @Suppress("UNCHECKED_CAST")
    private fun absentMember(uuid: UUID): Member = Proxy.newProxyInstance(
        Member::class.java.classLoader,
        arrayOf(Member::class.java)
    ) { _, method, arguments ->
        when (method.name) {
            "getUuid" -> uuid
            "hashCode" -> uuid.hashCode()
            "equals" -> arguments?.firstOrNull() === this
            "toString" -> "absent-member-$uuid"
            else -> error("Unavailable member must not be queried through ${method.name}")
        }
    } as Member

    private fun removalLatch(instanceId: String): CountDownLatch {
        val removed = CountDownLatch(1)
        state.instances.addEntryListener(
            EntryRemovedListener<String, InstanceInfo> { event ->
                if (event.key == instanceId) removed.countDown()
            },
            true
        )
        return removed
    }

    private fun enforceWhileCleanupBlocked(
        enforcer: InstanceCountEnforcer,
        blockedInstanceId: String,
        firstRemoved: CountDownLatch,
        now: Long,
        transition: () -> Unit
    ) {
        val instances = state.instances
        val failure = AtomicReference<Throwable?>()
        val completed = CountDownLatch(1)
        instances.lock(blockedInstanceId)
        val worker = Thread({
            try {
                enforcer.enforceOnce(now)
            } catch (throwable: Throwable) {
                failure.set(throwable)
            } finally {
                completed.countDown()
            }
        }, "enforcer-race-test")
        try {
            worker.start()
            assertTrue(firstRemoved.await(2, TimeUnit.SECONDS))
            transition()
        } finally {
            instances.unlock(blockedInstanceId)
        }
        assertTrue(completed.await(2, TimeUnit.SECONDS))
        failure.get()?.let { throw AssertionError("Enforcement failed", it) }
    }

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
        val restart: Boolean,
        val expectedLastHeartbeat: Long?,
        val transitionAt: Long
    )

    private class RecordingStopDispatcher(
        private val state: ClusterStateService
    ) : InstanceStopDispatcher {
        val invocations = mutableListOf<StopInvocation>()
        var beforeDispatch: () -> Unit = {}

        override fun dispatchStop(
            instanceId: String,
            targetMember: Member,
            force: Boolean,
            restart: Boolean,
            expectedLastHeartbeat: Long?,
            transitionAt: Long
        ): StopDispatchResult {
            beforeDispatch()
            val instance = state.getInstance(instanceId) ?: return StopDispatchResult.NOT_FOUND
            if (
                expectedLastHeartbeat != null &&
                (instance.state != InstanceState.STOPPING || instance.lastHeartbeat != expectedLastHeartbeat)
            ) {
                return StopDispatchResult.STALE_TRANSITION
            }
            if (instance.state == InstanceState.STOPPING && !force) {
                return StopDispatchResult.ALREADY_STOPPING
            }
            state.updateInstanceState(instanceId, InstanceState.STOPPING, transitionAt)
            invocations += StopInvocation(
                instanceId,
                targetMember.uuid.toString(),
                force,
                restart,
                expectedLastHeartbeat,
                transitionAt
            )
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
