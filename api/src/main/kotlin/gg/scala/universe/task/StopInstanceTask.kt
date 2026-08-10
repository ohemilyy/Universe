package gg.scala.universe.task

/**
 * Task sent to a Wrapper to stop a running instance.
 */
data class StopInstanceTask(
    val instanceId: String,
    val force: Boolean = false,
    val restart: Boolean = false,
    /** Master-owned STOPPING transition token. Zero preserves legacy Gson payloads. */
    val expectedGeneration: Long = 0,
    override val type: String = "stop"
) : UniverseTask
