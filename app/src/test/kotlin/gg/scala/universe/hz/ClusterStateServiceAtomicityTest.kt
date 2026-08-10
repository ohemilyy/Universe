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
        state.addNodeResources("node-1", ramMB = 96, cpu = 3)
        state.putInstance(instance(instanceId, InstanceState.STOPPING, lastHeartbeat = 0))

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
        state.addNodeResources("node-1", ramMB = 96, cpu = 3)
        state.putInstance(instance(instanceId, InstanceState.STOPPING, lastHeartbeat = 0))

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
}
