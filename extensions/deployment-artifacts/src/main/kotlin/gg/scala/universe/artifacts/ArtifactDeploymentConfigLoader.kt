package gg.scala.universe.artifacts

import gg.scala.universe.util.json.Serializers
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path

/**
 * @author Luna
 * @date August 2, 2026
 */
object ArtifactDeploymentConfigLoader {
    private val configPath = Path.of("./extensions/deployment-artifacts/config.json")

    suspend fun load(): ArtifactDeploymentConfig = withContext(Dispatchers.IO) {
        if (!Files.exists(configPath)) {
            val defaultConfig = ArtifactDeploymentConfig()
            Files.createDirectories(configPath.parent)
            Files.newBufferedWriter(configPath).use { Serializers.GSON.toJson(defaultConfig, it) }
            return@withContext defaultConfig
        }

        Files.newBufferedReader(configPath).use {
            Serializers.GSON.fromJson(it, ArtifactDeploymentConfig::class.java)
        }
    }
}
