package gg.scala.universe.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SpawnBackoffTest {

    @Test
    fun `a fresh key is ready`() {
        val backoff = SpawnBackoff(baseMs = 5_000L)
        assertTrue(backoff.ready("cfg", nowMs = 0L))
    }

    @Test
    fun `a failure blocks retries until the delay elapses`() {
        val backoff = SpawnBackoff(baseMs = 5_000L)
        backoff.recordFailure("cfg", nowMs = 1_000L)
        assertFalse(backoff.ready("cfg", nowMs = 1_000L))
        assertFalse(backoff.ready("cfg", nowMs = 5_999L))
        assertTrue(backoff.ready("cfg", nowMs = 6_000L))
    }

    @Test
    fun `delay grows exponentially with consecutive failures`() {
        val backoff = SpawnBackoff(baseMs = 5_000L)
        backoff.recordFailure("cfg", nowMs = 0L)
        assertEquals(5_000L, backoff.nextAttemptMs("cfg"))
        backoff.recordFailure("cfg", nowMs = 0L)
        assertEquals(10_000L, backoff.nextAttemptMs("cfg"))
        backoff.recordFailure("cfg", nowMs = 0L)
        assertEquals(20_000L, backoff.nextAttemptMs("cfg"))
    }

    @Test
    fun `delay is capped`() {
        val backoff = SpawnBackoff(baseMs = 5_000L, maxBackoffMs = 30_000L)
        repeat(20) { backoff.recordFailure("cfg", nowMs = 0L) }
        assertEquals(30_000L, backoff.nextAttemptMs("cfg"))
    }

    @Test
    fun `success clears the backoff`() {
        val backoff = SpawnBackoff(baseMs = 5_000L)
        backoff.recordFailure("cfg", nowMs = 0L)
        assertFalse(backoff.ready("cfg", nowMs = 0L))
        backoff.recordSuccess("cfg")
        assertTrue(backoff.ready("cfg", nowMs = 0L))
        backoff.recordFailure("cfg", nowMs = 0L)
        assertEquals(5_000L, backoff.nextAttemptMs("cfg"))
    }

    @Test
    fun `keys back off independently`() {
        val backoff = SpawnBackoff(baseMs = 5_000L)
        backoff.recordFailure("a", nowMs = 0L)
        assertFalse(backoff.ready("a", nowMs = 0L))
        assertTrue(backoff.ready("b", nowMs = 0L))
    }
}
