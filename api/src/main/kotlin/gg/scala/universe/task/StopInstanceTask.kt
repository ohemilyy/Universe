package gg.scala.universe.task

/**
 * Task sent to a Wrapper to stop a running instance.
 */
data class StopInstanceTask(
    val instanceId: String,
    val force: Boolean = false,
    val restart: Boolean = false,
    override val type: String = "stop"
) : UniverseTask
