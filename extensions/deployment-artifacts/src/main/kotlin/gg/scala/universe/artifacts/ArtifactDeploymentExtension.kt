package gg.scala.universe.artifacts

import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.extension.Extension
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration

/**
 * @author Luna
 * @date August 2, 2026
 */
class ArtifactDeploymentExtension : Extension {
    private var scope: CoroutineScope? = null
    private var pollingJob: Job? = null

    override fun id(): String = "deployment-artifacts"

    override fun version(): String = "1.0.0"

    override fun onLoad() {
        val extensionScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        scope = extensionScope
        pollingJob = extensionScope.launch {
            try {
                val config = ArtifactDeploymentConfigLoader.load()
                if (!config.enabled) {
                    log("Artifact deployment: Disabled", LogLevel.DEBUG)
                    return@launch
                }

                require(config.pollIntervalSeconds > 0) { "pollIntervalSeconds must be greater than zero" }
                require(config.requestTimeoutSeconds > 0) { "requestTimeoutSeconds must be greater than zero" }
                val httpClient = ArtifactHttpClient(Duration.ofSeconds(config.requestTimeoutSeconds))
                val service = ArtifactDeploymentService(httpClient, ArtifactSourceResolver(httpClient))
                val sourceCount = config.forgejo.count { it.enabled } +
                    config.artifactory.count { it.enabled } + config.teamCity.count { it.enabled }
                log("Artifact deployment: Watching $sourceCount source(s)")

                while (isActive) {
                    service.update(config)
                    delay(Duration.ofSeconds(config.pollIntervalSeconds).toMillis())
                }
            } catch (_: CancellationException) {
                return@launch
            } catch (exception: Exception) {
                log("Artifact deployment failed to start: ${exception.message}", LogLevel.ERROR)
            }
        }
    }

    override fun onUnload() {
        scope?.cancel()
        runBlocking { pollingJob?.join() }
        pollingJob = null
        scope = null
        log("Artifact deployment: Stopped")
    }

    override fun onReload() {
        onUnload()
        onLoad()
    }
}
