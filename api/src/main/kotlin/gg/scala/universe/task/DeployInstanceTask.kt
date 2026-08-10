package gg.scala.universe.task

/**
 * Task sent to a Wrapper to deploy (start) a new instance.
 */
data class DeployInstanceTask(
    val instanceId: String,
    val configurationName: String,
    /** Master-owned incarnation token. Zero preserves compatibility with legacy payloads. */
    val expectedGeneration: Long = 0,
    override val type: String = "deploy"
) : UniverseTask
