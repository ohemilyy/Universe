package gg.scala.universe.config

import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.PortRange
import gg.scala.universe.schema.Template
import gg.scala.universe.schema.TemplateInstallationConfig
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.streams.asSequence

object ConfigurationLoader {
    private const val CONFIGURATION_DIR = "./configuration"
    private val gson = Gson()

    fun load(clusterStateService: ClusterStateService) {
        val dir = Path.of(CONFIGURATION_DIR)
        if (!dir.exists()) {
            log("Configuration directory not found at $CONFIGURATION_DIR, creating it...", LogLevel.WARNING)
            Files.createDirectories(dir)
        }

        val configFiles = Files.list(dir)
            .asSequence()
            .filter { it.extension.equals("json", ignoreCase = true) }
            .toList()

        if (configFiles.isEmpty()) {
            log("No configuration files found in $CONFIGURATION_DIR, creating default configuration...", LogLevel.WARNING)
            val defaultConfig = createDefaultConfiguration()
            val defaultPath = dir.resolve("lobby.json")
            val prettyGson = gson.newBuilder().setPrettyPrinting().create()
            defaultPath.writeText(prettyGson.toJson(defaultConfig))
            clusterStateService.putConfiguration(defaultConfig)
            log("Created and loaded default configuration '${defaultConfig.name}' from $defaultPath")
            return
        }

        var loadedCount = 0
        configFiles.forEach { path ->
            try {
                val content = path.readText()
                val configuration = gson.fromJson(content, Configuration::class.java)
                gg.scala.universe.service.ConfigValidation.warnings(configuration)
                    .forEach { log(it, LogLevel.WARNING) }
                clusterStateService.putConfiguration(configuration)
                loadedCount++
                log("Loaded configuration '${configuration.name}' from $path")
            } catch (e: JsonSyntaxException) {
                log("Failed to parse configuration file $path: ${e.message}", LogLevel.ERROR)
            } catch (e: Exception) {
                log("Failed to load configuration file $path: ${e.message}", LogLevel.ERROR)
            }
        }

        if (loadedCount == 0) {
            log("No valid configuration files loaded, creating default configuration...", LogLevel.WARNING)
            val defaultConfig = createDefaultConfiguration()
            val defaultPath = dir.resolve("lobby.json")
            val prettyGson = gson.newBuilder().setPrettyPrinting().create()
            defaultPath.writeText(prettyGson.toJson(defaultConfig))
            clusterStateService.putConfiguration(defaultConfig)
            log("Created and loaded default configuration '${defaultConfig.name}' from $defaultPath")
        }
    }

    private fun createDefaultConfiguration(): Configuration {
        return Configuration(
            name = "lobby",
            runtime = "screen",
            command = "java -XX:MaxRAMPercentage=90.0 -XX:InitialRAMPercentage=75.0 -jar server.jar",
            static = false,
            instanceGroups = listOf("lobby"),
            nodes = listOf("node-1"),
            hostAddress = "127.0.0.1",
            availablePorts = PortRange(25600, 25700),
            minimumServiceCount = 1,
            environmentVariables = emptyMap(),
            templateInstallationConfig = TemplateInstallationConfig(
                allOf = listOf(Template(
                    name = "server",
                    group = "default",
                    storage = "local",
                    priority = 1
                )),
                allInGroups = listOf("default"),
                oneOf = emptyList(),
                oneInGroups = emptyList(),
                onTemplatePasteOverridePresentFiles = false
            ),
            fileModifications = emptyList()
        )
    }
}
