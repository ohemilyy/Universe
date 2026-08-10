package gg.scala.universe.runtime

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.PortRange
import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class PortAllocatorConcurrencyTest {
    private lateinit var hazelcast: HazelcastInstance

    @BeforeTest
    fun setUp() {
        hazelcast = Hazelcast.newHazelcastInstance(Config().apply {
            clusterName = "port-allocator-${UUID.randomUUID()}"
            setProperty("hazelcast.logging.type", "none")
            networkConfig.port = 0
            networkConfig.join.autoDetectionConfig.isEnabled = false
            networkConfig.join.multicastConfig.isEnabled = false
            networkConfig.join.tcpIpConfig.isEnabled = false
        })
    }

    @AfterTest
    fun tearDown() = hazelcast.shutdown()

    @Test
    fun `concurrent callers cannot both receive the same port`() {
        val port = ServerSocket(0).use { it.localPort }
        val allocator = PortAllocator(ClusterStateService(hazelcast))
        val workers = 32
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(workers)
        try {
            val futures = (1..workers).map {
                executor.submit<Int?> {
                    start.await()
                    allocator.allocate(PortRange(port, port, "sequential"))
                }
            }
            start.countDown()
            val allocations = futures.map { it.get(5, TimeUnit.SECONDS) }.filterNotNull()

            assertEquals(listOf(port), allocations)
        } finally {
            executor.shutdownNow()
        }
    }
}
