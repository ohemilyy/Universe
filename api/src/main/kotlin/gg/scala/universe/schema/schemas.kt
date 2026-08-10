package gg.scala.universe.schema

data class PortRange(val min: Int, val max: Int, val strategy: String = "random")

data class AdditionalPort(
    val port: Int,
    val protocol: String = "TCP",
    val name: String = ""
)

enum class InstanceState {
    CREATING,
    ONLINE,
    STOPPING,
    OFFLINE,
    STOPPED
}

data class InstanceInfo(
    val id: String,
    val configurationName: String,
    val wrapperNodeId: String,
    val hostAddress: String,
    val allocatedPort: Int,
    val state: InstanceState,
    val lastHeartbeat: Long,
    val processPid: Long?,
    val allocatedRamMB: Int = 0,
    val allocatedCpu: Int = 0,
    /** Runtime provider key used when this instance was created (e.g., "docker", "k8s", "screen"). */
    val runtime: String = "screen"
)

data class Template(
    val name: String,
    val group: String,
    val storage: String,
    val priority: Int,
)

data class Configuration(
    val name: String = "default",
    val runtime: String = "screen",
    val command: String = "",
    val static: Boolean = false,
    val ramMB: Int = 2048,
    val cpu: Int = 100,
    val instanceGroups: List<String> = emptyList(),
    val nodes: List<String> = listOf("node-1"),
    val hostAddress: String = "127.0.0.1",
    val availablePorts: PortRange = PortRange(25565, 25570),
    val minimumServiceCount: Int = 1,
    val environmentVariables: Map<String, String> = mapOf("UNIVERSE_INSTANCE_ID" to "%INSTANCE_ID%"),
    val templateInstallationConfig: TemplateInstallationConfig = TemplateInstallationConfig(),
    val fileModifications: List<String> = emptyList(),
    val properties: Map<String, String> = emptyMap(),
    val additionalPorts: List<AdditionalPort> = emptyList(),
)

data class NodeResources(
    val usedRamMB: Int = 0,
    val usedCpu: Int = 0
)

data class TemplateInstallationConfig(
    val allOf: List<Template> = emptyList(),
    val allInGroups: List<String> = emptyList(),
    val oneOf: List<Template> = emptyList(),
    val oneInGroups: List<String> = emptyList(),
    val onTemplatePasteOverridePresentFiles: Boolean = false,
)
