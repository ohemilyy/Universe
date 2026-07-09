package gg.scala.universe.command.commands

import com.google.inject.Inject
import com.google.inject.Singleton
import gg.scala.universe.app.UniverseApplication
import gg.scala.universe.console.Ansi
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.command.CommandSource
import gg.scala.universe.config.ConfigurationLoader
import gg.scala.universe.extension.ExtensionService
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.TaskDispatcher
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.service.InstanceCreationService
import gg.scala.universe.service.NodeShutdownService
import gg.scala.universe.template.TemplateManager
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.http.HttpStatusCode
import org.incendo.cloud.annotations.Argument
import org.incendo.cloud.annotations.Command
import org.incendo.cloud.annotation.specifier.FlagYielding
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.hz.nodeName
import gg.scala.universe.hz.ownsInstance
import kotlinx.coroutines.delay
import org.incendo.cloud.annotations.Default
import java.nio.file.Path
import java.nio.file.Files
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.name

/**
 * Management commands for the Universe orchestrator.
 *
 * Provides cluster, instance, configuration, template, and extension management
 * via the console command system.
 */
@Singleton
class ManagementCommands @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val taskDispatcher: TaskDispatcher,
    private val extensionService: ExtensionService,
    private val templateManager: TemplateManager,
    private val instanceCreationService: InstanceCreationService,
    private val nodeShutdownService: NodeShutdownService
) {

    // ─── Cluster Commands ───

    @Command("cluster status")
    fun clusterStatus(source: CommandSource) {
        val localMember = hazelcastInstance.cluster.localMember
        val members = hazelcastInstance.cluster.members

        source.sendMessage("=== Cluster Status ===")
        source.sendMessage("Cluster name: ${hazelcastInstance.config.clusterName}")
        source.sendMessage("Local node: ${localMember.uuid} (master=${localMember.localMember()})")
        source.sendMessage("Members: ${members.size}")
        members.forEach { member ->
            val isLocal = if (member.localMember()) " [LOCAL]" else ""
            source.sendMessage("  - ${member.uuid}$isLocal")
        }
    }

    @Command("cluster nodes")
    fun clusterNodes(source: CommandSource) {
        val members = hazelcastInstance.cluster.members
        source.sendMessage("=== Cluster Nodes ===")
        members.forEach { member ->
            val nodeId = member.getAttribute("nodeId") ?: "unknown"
            val isLocal = if (member.localMember()) " (local)" else ""
            source.sendMessage("  $nodeId - ${member.uuid}$isLocal")
        }
    }

    @Command("cluster shutdown")
    fun clusterShutdown(source: CommandSource) {
        val members = hazelcastInstance.cluster.members
        source.sendMessage("Shutting down ${members.size} cluster node(s)...")

        members.forEach { member ->
            val nodeId = member.getAttribute("nodeId") ?: member.uuid.toString()
            source.sendMessage("  → Sending shutdown to $nodeId")
            taskDispatcher.dispatchShutdown(member)
        }

        source.sendMessage("Shutdown dispatched to all nodes.")
    }

    @Command("node info")
    fun nodeInfo(source: CommandSource) {
        val localMember = hazelcastInstance.cluster.localMember
        val nodeId = localMember.nodeName()
        val resources = clusterStateService.getNodeResources(nodeId)
        val maxRam = localMember.getAttribute("maxRamMB")?.toIntOrNull() ?: 0
        val maxCpu = localMember.getAttribute("maxCpu")?.toIntOrNull() ?: 0

        source.sendMessage("=== Local Node: $nodeId ===")
        source.sendMessage("  RAM: ${resources.usedRamMB}MB / ${maxRam}MB used")
        source.sendMessage("  CPU: ${resources.usedCpu} / ${maxCpu} units used")
        source.sendMessage("  Instances: ${clusterStateService.getInstancesByWrapper(nodeId).size}")
    }

    @Command("node resources")
    fun nodeResources(source: CommandSource) {
        val members = hazelcastInstance.cluster.members
        source.sendMessage("=== Node Resources ===")
        source.sendMessage(String.format("%-12s %-10s %-10s %-10s %-10s", "Node", "RAM Used", "RAM Max", "CPU Used", "CPU Max"))
        members.forEach { member ->
            val nodeId = member.nodeName()
            val resources = clusterStateService.getNodeResources(nodeId)
            val maxRam = member.getAttribute("maxRamMB")?.toIntOrNull() ?: 0
            val maxCpu = member.getAttribute("maxCpu")?.toIntOrNull() ?: 0
            source.sendMessage(
                String.format("%-12s %-10d %-10d %-10d %-10d", nodeId, resources.usedRamMB, maxRam, resources.usedCpu, maxCpu)
            )
        }
    }

    // ─── Instance Commands ───

    @Command("instance|instances list")
    fun instanceList(source: CommandSource) {
        val instances = clusterStateService.getActiveInstances()
        if (instances.isEmpty()) {
            source.sendMessage("No instances found.")
            return
        }

        source.sendMessage("=== Instances (${instances.size}) ===")
        source.sendMessage(String.format("%-10s %-12s %-10s %-15s %-8s %-10s", "ID", "Config", "Runtime", "Host", "Port", "State"))
        instances.forEach { instance ->
            val config = clusterStateService.getConfiguration(instance.configurationName)
            val runtime = instance.runtime
            val staticMarker = if (config?.static == true) " [STATIC]" else ""
            source.sendMessage(
                String.format(
                    "%-10s %-12s %-10s %-15s %-8d %-10s%s",
                    instance.id,
                    instance.configurationName,
                    runtime,
                    instance.hostAddress,
                    instance.allocatedPort,
                    instance.state,
                    staticMarker
                )
            )
        }
    }

    @Command("instance|instances create <config> <amount>")
    fun instanceCreate(
        source: CommandSource,
        @Argument("config") configName: String,
        @Argument("amount") @Default("1") amount: Int = 1
    ) {
        val configuration = clusterStateService.getConfiguration(configName)
        if (configuration == null) {
            source.sendMessage("Configuration '$configName' not found.")
            return
        }

        // Static configs: only one instance, deterministic ID, no templates
        if (configuration.static) {
            val existing = clusterStateService.getInstance(configName)
            if (existing != null && (existing.state == InstanceState.ONLINE || existing.state == InstanceState.CREATING)) {
                source.sendMessage("Static instance '$configName' is already running (state=${existing.state}).")
                return
            }

            val instanceInfo = instanceCreationService.createInstance(configuration, instanceId = configName)
            if (instanceInfo == null) {
                source.sendMessage(
                    "No node has enough resources (RAM=${configuration.ramMB}MB, CPU=${configuration.cpu}) " +
                    "for static instance '$configName'."
                )
                return
            }

            val member = hazelcastInstance.cluster.members.firstOrNull {
                it.ownsInstance(instanceInfo.wrapperNodeId)
            } ?: hazelcastInstance.cluster.localMember

            source.sendMessage("Created static instance '$configName' on node ${member.nodeName()}")
            return
        }

        // Non-static: resource-aware node selection
        for (i in 1..amount) {
            val instanceInfo = instanceCreationService.createInstance(configuration)
            if (instanceInfo == null) {
                source.sendMessage(
                    "No node has enough resources (RAM=${configuration.ramMB}MB, CPU=${configuration.cpu}) " +
                    "for instance #$i of '$configName'."
                )
                continue
            }

            val member = hazelcastInstance.cluster.members.firstOrNull {
                it.ownsInstance(instanceInfo.wrapperNodeId)
            } ?: hazelcastInstance.cluster.localMember

            source.sendMessage(
                "Created instance ${instanceInfo.id} from configuration '$configName' on node ${member.nodeName()}"
            )
        }
    }

    @Command("instance|instances stop <id>")
    fun instanceStop(
        source: CommandSource,
        @Argument("id") instanceId: String
    ) {
        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val member = hazelcastInstance.cluster.members.firstOrNull {
            it.ownsInstance(instance.wrapperNodeId)
        } ?: hazelcastInstance.cluster.localMember

        taskDispatcher.dispatchStop(instanceId, member)
        source.sendMessage("Stopping instance $instanceId...")
    }

    @Command("instance|instances info <id>")
    fun instanceInfo(
        source: CommandSource,
        @Argument("id") instanceId: String
    ) {
        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val config = clusterStateService.getConfiguration(instance.configurationName)
        val isStatic = config?.static == true

        source.sendMessage("=== Instance $instanceId ===")
        source.sendMessage("  Configuration: ${instance.configurationName}")
        source.sendMessage("  Runtime: ${config?.runtime ?: "default"}")
        source.sendMessage("  Static: $isStatic")
        source.sendMessage("  State: ${instance.state}")
        source.sendMessage("  Host: ${instance.hostAddress}:${instance.allocatedPort}")
        source.sendMessage("  Wrapper: ${instance.wrapperNodeId}")
        source.sendMessage("  PID: ${instance.processPid ?: "N/A"}")
        source.sendMessage("  Last heartbeat: ${instance.lastHeartbeat}")
        if (isStatic) {
            source.sendMessage("  Working dir: ./static/${instance.configurationName}")
        } else {
            source.sendMessage("  Working dir: ./running/${instance.id}")
        }
    }

    @Command("instance|instances execute|exec|run|cmd|command <id> <command>")
    fun instanceExecute(
        source: CommandSource,
        @Argument("id") instanceId: String,
        @FlagYielding @Argument("command") command: String
    ) {

        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val member = hazelcastInstance.cluster.members.firstOrNull {
            it.ownsInstance(instance.wrapperNodeId)
        } ?: hazelcastInstance.cluster.localMember

        taskDispatcher.dispatchExecute(instanceId, command, member)
        source.sendMessage("Executed command on instance $instanceId: $command")
    }

    @Command("instance|instances restart <id>")
    fun instanceRestart(
        source: CommandSource,
        @Argument("id") instanceId: String
    ) {
        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val config = clusterStateService.getConfiguration(instance.configurationName)
        if (config == null) {
            source.sendMessage("Configuration '${instance.configurationName}' not found.")
            return
        }

        // Stop existing
        val member = hazelcastInstance.cluster.members.firstOrNull {
            it.ownsInstance(instance.wrapperNodeId)
        } ?: hazelcastInstance.cluster.localMember
        taskDispatcher.dispatchStop(instanceId, member)
        source.sendMessage("Stopping instance $instanceId for restart...")

        // Wait a moment for stop to process
        Thread.sleep(500)

        // Create new
        val newInstance = if (config.static) {
            instanceCreationService.createInstance(config, instanceId)
        } else {
            instanceCreationService.createInstance(config)
        }

        if (newInstance == null) {
            source.sendMessage("Failed to restart instance: no node has enough resources.")
            return
        }

        source.sendMessage("Restarted instance as ${newInstance.id} on node ${newInstance.wrapperNodeId}")
    }

    @Command("instance|instances kill <id>")
    fun instanceKill(
        source: CommandSource,
        @Argument("id") instanceId: String
    ) {
        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val member = hazelcastInstance.cluster.members.firstOrNull {
            it.ownsInstance(instance.wrapperNodeId)
        } ?: hazelcastInstance.cluster.localMember

        taskDispatcher.dispatchStop(instanceId, member)
        clusterStateService.updateInstanceState(instanceId, InstanceState.STOPPED)
        source.sendMessage("Force-killed instance $instanceId.")
    }

    @Command("instance|instances logs <id>")
    fun instanceLogs(
        source: CommandSource,
        @Argument("id") instanceId: String
    ) {
        val instance = clusterStateService.getInstance(instanceId)
        if (instance == null) {
            source.sendMessage("Instance '$instanceId' not found.")
            return
        }

        val config = clusterStateService.getConfiguration(instance.configurationName)
        val workingDir = if (config?.static == true) {
            java.nio.file.Paths.get("./static/${instance.configurationName}")
        } else {
            java.nio.file.Paths.get("./running/$instanceId")
        }

        val logFile = workingDir.resolve("stdout.log").toFile()
        if (!logFile.exists()) {
            source.sendMessage("No logs found for instance $instanceId.")
            return
        }

        val lines = logFile.readLines()
        val tail = lines.takeLast(25)
        source.sendMessage("=== Last ${tail.size} lines of $instanceId ===")
        tail.forEach { source.sendMessage(it) }
    }

    // ─── Configuration Commands ───

    @Command("config|configs list")
    fun configList(source: CommandSource) {
        val configs = clusterStateService.configurations.values
        if (configs.isEmpty()) {
            source.sendMessage("No configurations found.")
            return
        }

        source.sendMessage("=== Configurations (${configs.size}) ===")
        configs.forEach { config ->
            source.sendMessage("  ${config.name} (runtime=${config.runtime}, ports=${config.availablePorts.min}-${config.availablePorts.max})")
        }
    }

    @Command("config|configs show <name>")
    fun configShow(
        source: CommandSource,
        @Argument("name") configName: String
    ) {
        val config = clusterStateService.getConfiguration(configName)
        if (config == null) {
            source.sendMessage("Configuration '$configName' not found.")
            return
        }

        source.sendMessage("=== Configuration: $configName ===")
        source.sendMessage("  Runtime: ${config.runtime}")
        source.sendMessage("  Command: ${config.command}")
        source.sendMessage("  Static: ${config.static}")
        source.sendMessage("  RAM: ${config.ramMB}MB")
        source.sendMessage("  CPU: ${config.cpu}")
        source.sendMessage("  Min Count: ${config.minimumServiceCount}")
        source.sendMessage("  Ports: ${config.availablePorts.min}-${config.availablePorts.max}")
        source.sendMessage("  Groups: ${config.instanceGroups}")
        source.sendMessage("  Nodes: ${config.nodes}")
        source.sendMessage("  Host: ${config.hostAddress}")
        source.sendMessage("  Env: ${config.environmentVariables}")
        source.sendMessage("  Properties: ${config.properties}")
        source.sendMessage("  File Mods: ${config.fileModifications}")
    }

    @Command("config|configs create <name> <runtime> <command>")
    fun configCreate(
        source: CommandSource,
        @Argument("name") configName: String,
        @Argument("runtime") runtime: String,
        @Argument("command") command: String
    ) {
        if (clusterStateService.getConfiguration(configName) != null) {
            source.sendMessage("Configuration '$configName' already exists.")
            return
        }

        val config = gg.scala.universe.schema.Configuration(
            name = configName,
            runtime = runtime,
            command = command
        )
        clusterStateService.putConfiguration(config)
        source.sendMessage("Created configuration '$configName' (runtime=$runtime).")
    }

    @Command("config|configs reload")
    fun configReload(source: CommandSource) {
        ConfigurationLoader.load(clusterStateService)
        source.sendMessage("Configurations reloaded from disk.")
    }

    // ─── Template Commands ───

    @Command("template|templates list")
    fun templateList(source: CommandSource) {
        val templatesDir = Path.of("./templates")
        if (!templatesDir.exists() || !templatesDir.isDirectory()) {
            source.sendMessage("No templates directory found.")
            return
        }

        source.sendMessage("=== Templates ===")
        Files.list(templatesDir).use { groupStream ->
            groupStream.filter { it.isDirectory() }.forEach { groupDir ->
                val groupName = groupDir.fileName.toString()
                Files.list(groupDir).use { templateStream ->
                    templateStream.filter { it.isDirectory() }.forEach { templateDir ->
                        source.sendMessage("  $groupName/${templateDir.fileName}")
                    }
                }
            }
        }
    }

    // ─── Extension Commands ───

    @Command("extension|extensions list")
    fun extensionList(source: CommandSource) {
        val installed = extensionService.getInstalledExtensions()
        val loaded = extensionService.getLoadedExtensions()

        source.sendMessage("=== Extensions (${installed.size} installed, ${loaded.size} loaded) ===")
        installed.forEach { (id, ext) ->
            val status = if (loaded.containsKey(id)) "[LOADED]" else "[INSTALLED]"
            val flags = buildList {
                if (ext.masterOnly()) add("master-only")
                if (!ext.reloadable()) add("no-reload")
            }.joinToString(prefix = " ", separator = ", ").ifBlank { "" }
            source.sendMessage("  $status $id v${ext.version()}$flags")
        }
    }

    @Command("extension|extensions reload")
    fun extensionReload(source: CommandSource) {
        val loaded = extensionService.getLoadedExtensions()
        val skipped = loaded.filter { !it.value.reloadable() }.keys
        if (skipped.isNotEmpty()) {
            source.sendMessage("  ${Ansi.YELLOW}⚠${Ansi.RESET} Skipped non-reloadable extension(s): ${skipped.joinToString()}")
        }
        extensionService.reloadExtensions()
        source.sendMessage("Extensions reloaded.")
    }

    // ─── System Commands ───

    @Command("stop|exit")
    suspend fun stop(source: CommandSource) {
        source.sendMessage("  ${Ansi.BLUE}→${Ansi.RESET} Stopping Universe...")
        UniverseApplication.instance.shutdown()
        Runtime.getRuntime().exit(0)
    }

    @Command("help")
    fun help(source: CommandSource) {
        source.sendMessage("=== Universe Commands ===")
        source.sendMessage("")
        source.sendMessage("Cluster:")
        source.sendMessage("  cluster status          - Show cluster status")
        source.sendMessage("  cluster nodes           - List cluster nodes")
        source.sendMessage("  cluster shutdown        - Shutdown all cluster nodes")
        source.sendMessage("")
        source.sendMessage("Node:")
        source.sendMessage("  node info               - Show local node resources")
        source.sendMessage("  node resources          - Show all nodes resource usage")
        source.sendMessage("")
        source.sendMessage("Instances:")
        source.sendMessage("  instances list          - List all instances")
        source.sendMessage("  instances create <cfg> [amount]  - Create new instances")
        source.sendMessage("  instances stop <id>     - Stop an instance")
        source.sendMessage("  instances kill <id>     - Force-stop an instance")
        source.sendMessage("  instances restart <id>  - Restart an instance")
        source.sendMessage("  instances info <id>     - Show instance details")
        source.sendMessage("  instances logs <id>     - Show last 25 log lines")
        source.sendMessage("  instances execute <id> <cmd>  - Execute command on instance")
        source.sendMessage("")
        source.sendMessage("Configuration:")
        source.sendMessage("  configs list            - List configurations")
        source.sendMessage("  configs show <name>     - Show configuration details")
        source.sendMessage("  configs create <n> <rt> <cmd>  - Create a configuration")
        source.sendMessage("  configs reload          - Reload configurations from disk")
        source.sendMessage("")
        source.sendMessage("Templates:")
        source.sendMessage("  templates list          - List local templates")
        source.sendMessage("  templates sync <p> <n>  - Sync template to node")
        source.sendMessage("")
        source.sendMessage("Extensions:")
        source.sendMessage("  extensions list         - List extensions")
        source.sendMessage("  extensions reload       - Reload extensions")
        source.sendMessage("")
        source.sendMessage("System:")
        source.sendMessage("  help                    - Show this help")
        source.sendMessage("  stop                    - Shutdown Universe")
    }

}
