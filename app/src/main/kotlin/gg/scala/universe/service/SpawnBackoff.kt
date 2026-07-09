package gg.scala.universe.service

class SpawnBackoff(
    private val baseMs: Long = 5_000L,
    private val maxBackoffMs: Long = 300_000L
) {
    private data class State(val failures: Int, val nextAttemptMs: Long)

    private val states = HashMap<String, State>()

    fun ready(key: String, nowMs: Long): Boolean {
        val state = states[key] ?: return true
        return nowMs >= state.nextAttemptMs
    }

    fun recordFailure(key: String, nowMs: Long) {
        val failures = (states[key]?.failures ?: 0) + 1
        val delay = minOf(maxBackoffMs, baseMs shl (failures - 1).coerceIn(0, 20))
        states[key] = State(failures, nowMs + delay)
    }

    fun recordSuccess(key: String) {
        states.remove(key)
    }

    fun nextAttemptMs(key: String): Long = states[key]?.nextAttemptMs ?: 0L
}
