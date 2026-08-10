package gg.scala.universe.hz

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ClusterStateServiceAtomicityTest {
    private lateinit var hazelcastInstance: HazelcastInstance
    private lateinit var state: ClusterStateService

    @BeforeTest
    fun setUp() {
        val config = Config().apply {
            clusterName = "cluster-state-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        }
        hazelcastInstance = Hazelcast.newHazelcastInstance(config)
        state = ClusterStateService(hazelcastInstance)
    }

    @AfterTest
    fun tearDown() {
        hazelcastInstance.shutdown()
    }

    @Test
    fun `concurrent resource additions retain every update`() {
        val workers = 8
        val additionsPerWorker = 100
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(workers)
        try {
            val futures = (1..workers).map {
                executor.submit {
                    start.await()
                    repeat(additionsPerWorker) {
                        state.addNodeResources("node-1", 1, 1)
                    }
                }
            }

            start.countDown()
            futures.forEach { it.get(10, TimeUnit.SECONDS) }

            assertEquals(workers * additionsPerWorker, state.getNodeResources("node-1").usedRamMB)
            assertEquals(workers * additionsPerWorker, state.getNodeResources("node-1").usedCpu)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `creating persistence and capacity reservation are one atomic claim`() {
        val start = CountDownLatch(1)
        val accepted = AtomicInteger()
        val executor = Executors.newFixedThreadPool(2)
        try {
            val futures = listOf("new001", "new002").mapIndexed { index, id ->
                executor.submit {
                    start.await()
                    if (
                        state.reserveCreatingInstance(
                            instance = instance(id, InstanceState.CREATING, lastHeartbeat = index.toLong()),
                            generation = 1L,
                            maxRamMB = 64,
                            maxCpu = 1
                        )
                    ) {
                        accepted.incrementAndGet()
                    }
                }
            }
            start.countDown()
            futures.forEach { it.get(5, TimeUnit.SECONDS) }

            assertEquals(1, accepted.get())
            assertEquals(1, state.getAllInstances().size)
            assertEquals(64, state.getNodeResources("node-1").usedRamMB)
            assertEquals(1, state.getNodeResources("node-1").usedCpu)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `unowned creating termination cannot release another reservation`() {
        val owned = instance("new001", InstanceState.CREATING, lastHeartbeat = 1)
        assertTrue(state.reserveCreatingInstance(owned, 1L, maxRamMB = 128, maxCpu = 2))
        val unowned = instance("new002", InstanceState.CREATING, lastHeartbeat = 2)
        state.putInstance(unowned)

        assertTrue(
            state.completeInstanceTermination(
                expectedInstance = unowned,
                expectedGeneration = 0L,
                finalState = InstanceState.STOPPED
            )
        )

        assertEquals(64, state.getNodeResources("node-1").usedRamMB)
        assertEquals(1, state.getNodeResources("node-1").usedCpu)
    }

    @Test
    fun `late plugin report cannot revive stopped instance or move heartbeat backward`() {
        val online = instance("old001", InstanceState.ONLINE, lastHeartbeat = 100)
        state.putInstance(online)
        state.updateInstanceState("old001", InstanceState.STOPPED, lastHeartbeat = 200)

        val stopped = state.updateInstanceFromPlugin("old001", InstanceState.ONLINE, 300)
        assertEquals(InstanceState.STOPPED, stopped?.state)
        assertEquals(200, stopped?.lastHeartbeat)

        state.putInstance(online)
        val stale = state.updateInstanceFromPlugin("old001", InstanceState.ONLINE, 99)
        assertEquals(100, stale?.lastHeartbeat)
    }

    @Test
    fun `plugin report cannot overwrite stopping transition that wins the key lock`() {
        val instanceId = "old001"
        state.putInstance(instance(instanceId, InstanceState.ONLINE, lastHeartbeat = 1))
        val pluginStarted = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        val instances = state.instances
        instances.lock(instanceId)
        var locked = true
        try {
            val pluginUpdate = executor.submit {
                pluginStarted.countDown()
                state.updateInstanceFromPlugin(instanceId, InstanceState.ONLINE, lastHeartbeat = 999)
            }
            assertTrue(pluginStarted.await(2, TimeUnit.SECONDS))

            state.updateInstanceState(instanceId, InstanceState.STOPPING, lastHeartbeat = 42)
            instances.unlock(instanceId)
            locked = false
            pluginUpdate.get(2, TimeUnit.SECONDS)

            assertEquals(
                instance(instanceId, InstanceState.STOPPING, lastHeartbeat = 42),
                state.getInstance(instanceId)
            )
        } finally {
            if (locked) instances.unlock(instanceId)
            executor.shutdownNow()
        }
    }

    @Test
    fun `abandoned cleanup is idempotent and preserves unrelated resources`() {
        val instanceId = "old001"
        state.addNodeResources("node-1", ramMB = 32, cpu = 2)
        putOwnedStopping(instance(instanceId, InstanceState.STOPPING, lastHeartbeat = 0))

        assertTrue(
            state.finalizeAbandonedStopping(
                instanceId = instanceId,
                expectedLastHeartbeat = 0,
                stoppedAt = 120_001
            )
        )
        assertFalse(
            state.finalizeAbandonedStopping(
                instanceId = instanceId,
                expectedLastHeartbeat = 0,
                stoppedAt = 120_001
            )
        )

        assertNull(state.getInstance(instanceId))
        assertEquals(32, state.getNodeResources("node-1").usedRamMB)
        assertEquals(2, state.getNodeResources("node-1").usedCpu)
    }

    @Test
    fun `claimed abandoned cleanup survives interruption and rejects late revival`() {
        val instanceId = "old001"
        state.addNodeResources("node-1", ramMB = 32, cpu = 2)
        putOwnedStopping(instance(instanceId, InstanceState.STOPPING, lastHeartbeat = 0))

        assertTrue(
            state.claimAbandonedStopping(
                instanceId = instanceId,
                expectedLastHeartbeat = 0,
                stoppedAt = 120_001
            )
        )
        assertEquals(InstanceState.STOPPED, state.getInstance(instanceId)?.state)
        assertEquals(32, state.getNodeResources("node-1").usedRamMB)
        assertEquals(2, state.getNodeResources("node-1").usedCpu)

        val lateReport = state.updateInstanceFromPlugin(
            instanceId,
            InstanceState.ONLINE,
            lastHeartbeat = 120_002
        )
        assertEquals(InstanceState.STOPPED, lateReport?.state)
        assertEquals(InstanceState.STOPPED, state.getInstance(instanceId)?.state)

        assertTrue(
            state.claimAbandonedStopping(
                instanceId = instanceId,
                expectedLastHeartbeat = 0,
                stoppedAt = 120_003
            )
        )
        assertEquals(32, state.getNodeResources("node-1").usedRamMB)
        assertEquals(2, state.getNodeResources("node-1").usedCpu)

        assertEquals(1, state.completePendingAbandonedStoppingCleanups())
        assertEquals(0, state.completePendingAbandonedStoppingCleanups())
        assertNull(state.getInstance(instanceId))
        assertEquals(32, state.getNodeResources("node-1").usedRamMB)
        assertEquals(2, state.getNodeResources("node-1").usedCpu)
    }

    @Test
    fun `cleanup claims reject generic master and wrapper state publication`() {
        state.addNodeResources("node-1", ramMB = 32, cpu = 2)
        val first = instance("old001", InstanceState.STOPPING, lastHeartbeat = 0)
        val second = instance("old002", InstanceState.STOPPING, lastHeartbeat = 0)
        putOwnedStopping(first)
        putOwnedStopping(second)
        assertTrue(state.claimAbandonedStopping("old001", 0, 120_001))
        assertTrue(state.claimAbandonedStopping("old002", 0, 120_001))

        state.updateInstanceState("old001", InstanceState.ONLINE, lastHeartbeat = 120_002)
        state.putInstance(
            second.copy(state = InstanceState.ONLINE, lastHeartbeat = 120_002)
        )

        assertEquals(InstanceState.STOPPED, state.getInstance("old001")?.state)
        assertEquals(120_001, state.getInstance("old001")?.lastHeartbeat)
        assertEquals(InstanceState.STOPPED, state.getInstance("old002")?.state)
        assertEquals(120_001, state.getInstance("old002")?.lastHeartbeat)
        assertEquals(32, state.getNodeResources("node-1").usedRamMB)
        assertEquals(2, state.getNodeResources("node-1").usedCpu)
        assertEquals(2, state.completePendingAbandonedStoppingCleanups())
        assertNull(state.getInstance("old001"))
        assertNull(state.getInstance("old002"))
    }

    @Test
    fun `terminal completion rejects already terminal snapshots without resource changes`() {
        state.addNodeResources("node-1", ramMB = 96, cpu = 3)
        val online = instance("old001", InstanceState.ONLINE, lastHeartbeat = 1)
        state.putInstance(online)

        assertTrue(
            state.completeInstanceTermination(
                expectedInstance = online,
                finalState = InstanceState.STOPPED,
                completedAt = 2
            )
        )
        val stopped = state.getInstance("old001")!!
        assertFalse(
            state.completeInstanceTermination(
                expectedInstance = stopped,
                finalState = InstanceState.STOPPED,
                completedAt = 3
            )
        )
        val offline = instance("old002", InstanceState.OFFLINE, lastHeartbeat = 1)
        state.putInstance(offline)
        assertFalse(
            state.completeInstanceTermination(
                expectedInstance = offline,
                finalState = InstanceState.STOPPED,
                completedAt = 3
            )
        )

        assertEquals(96, state.getNodeResources("node-1").usedRamMB)
        assertEquals(3, state.getNodeResources("node-1").usedCpu)
        assertEquals(2, state.getInstance("old001")?.lastHeartbeat)
        assertEquals(InstanceState.OFFLINE, state.getInstance("old002")?.state)
    }

    @Test
    fun `restart transfers exact reservation without opening a capacity race`() {
        val creating = instance("old001", InstanceState.CREATING, lastHeartbeat = 1)
        assertTrue(state.reserveCreatingInstance(creating, 1, maxRamMB = 64, maxCpu = 1))
        val stopping = creating.copy(state = InstanceState.STOPPING, lastHeartbeat = 2)
        assertTrue(state.transitionLifecycle(creating, 1, stopping, 2))

        val stopped = state.completeInstanceTerminationForRestart(stopping, 2, 3, 3)!!
        assertEquals(64, state.getNodeResources("node-1").usedRamMB)
        assertFalse(
            state.reserveCreatingInstance(
                instance("new002", InstanceState.CREATING, lastHeartbeat = 3),
                1,
                maxRamMB = 64,
                maxCpu = 1
            )
        )

        val restarted = stopped.copy(state = InstanceState.CREATING, lastHeartbeat = 4)
        assertTrue(state.reserveRestartCreating(stopped, restarted, 3))
        assertEquals(64, state.getNodeResources("node-1").usedRamMB)
    }

    @Test
    fun `shutdown claim cannot act on a newer lifecycle incarnation`() {
        val creating = instance("old001", InstanceState.CREATING, lastHeartbeat = 1)
        assertTrue(state.reserveCreatingInstance(creating, 1, 1024, 100))
        val online = creating.copy(state = InstanceState.ONLINE, lastHeartbeat = 2)
        assertTrue(state.transitionLifecycle(creating, 1, online, 2))

        assertNull(state.claimForShutdown(creating, 1, now = 3))
        assertEquals(online, state.getInstance(online.id))
        assertEquals(2, state.getLifecycleGeneration(online.id))
    }

    private fun instance(id: String, state: InstanceState, lastHeartbeat: Long) = InstanceInfo(
        id = id,
        configurationName = "site",
        wrapperNodeId = "node-1",
        hostAddress = "127.0.0.1",
        allocatedPort = 25565,
        state = state,
        lastHeartbeat = lastHeartbeat,
        processPid = 42,
        allocatedRamMB = 64,
        allocatedCpu = 1,
        runtime = "fake"
    )

    private fun putOwnedStopping(instance: InstanceInfo) {
        val creating = instance.copy(state = InstanceState.CREATING)
        assertTrue(state.reserveCreatingInstance(creating, 1, Int.MAX_VALUE, Int.MAX_VALUE))
        state.updateInstanceState(instance.id, InstanceState.STOPPING, instance.lastHeartbeat)
    }
}
