package gg.scala.universe.runtime

import java.nio.file.Path

/** Runtime state used by wrapper recovery without changing the shared instance schema. */
enum class RuntimeResourceState {
    ABSENT,
    RUNNING,
    PRESENT_TRANSITIONAL,
    CLEANUP_REQUIRED,
    TERMINAL,
    UNKNOWN
}

/**
 * Optional recovery capability for runtimes whose durable resources survive a
 * wrapper process. Implementations must return [RuntimeResourceState.UNKNOWN]
 * when discovery cannot prove a safe state.
 */
interface RuntimeRecoveryInspector {
    fun inspectRecovered(
        instanceId: String,
        processPid: Long?,
        workingDirectory: Path
    ): RuntimeResourceState
}
