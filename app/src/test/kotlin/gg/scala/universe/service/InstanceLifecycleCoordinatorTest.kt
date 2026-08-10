package gg.scala.universe.service

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InstanceLifecycleCoordinatorTest {
    @Test
    fun `one durable lock entry is retained for each historical instance id`() {
        val coordinator = InstanceLifecycleCoordinator()

        coordinator.withInstance("old001") { Unit }
        coordinator.withInstance("old001") { Unit }

        assertEquals(1, coordinator.trackedLockCount())
    }

    @Test
    fun `concurrent entrants for one id never execute lifecycle effects together`() {
        val coordinator = InstanceLifecycleCoordinator()
        val workers = 16
        val iterations = 100
        val start = CountDownLatch(1)
        val active = AtomicInteger()
        val maximumActive = AtomicInteger()
        val executor = Executors.newFixedThreadPool(workers)
        try {
            val futures = (1..workers).map {
                executor.submit {
                    check(start.await(5, TimeUnit.SECONDS))
                    repeat(iterations) {
                        coordinator.withInstance("old001") {
                            val current = active.incrementAndGet()
                            maximumActive.accumulateAndGet(current, ::maxOf)
                            Thread.yield()
                            active.decrementAndGet()
                        }
                    }
                }
            }
            start.countDown()
            futures.forEach { it.get(10, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
        }

        assertEquals(1, maximumActive.get())
        assertEquals(1, coordinator.trackedLockCount())
    }
}
