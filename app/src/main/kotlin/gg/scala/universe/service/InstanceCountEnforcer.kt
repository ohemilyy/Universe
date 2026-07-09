package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.schema.InstanceState
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Automatically enforces [Configuration.minimumServiceCount] by spawning
 * new instances when the running count drops below the configured minimum.
 *
 * Runs on the master node every 5 seconds. Uses a single-threaded executor
 * to avoid race conditions. Static configurations are ignored.
 */
@Singleton
class InstanceCountEnforcer @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val instanceCreationService: InstanceCreationService,
    private val configuration: UniverseMainConfiguration,
    private val portAllocator: PortAllocator
) {
    private val executor = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "universe-instance-enforcer").apply { isDaemon = true }
    }

    @Volatile
    private var shuttingDown = false

    private val spawnBackoff = SpawnBackoff()

    fun start() {
        if (!configuration.isMasterNode) {
            log("InstanceCountEnforcer disabled on non-master node", LogLevel.INFO)
            return
        }

        executor.scheduleAtFixedRate(
            ::enforce,
            15,   // initial delay
            5,   // period
            TimeUnit.SECONDS
        )
        log("InstanceCountEnforcer started (interval=5s)", LogLevel.INFO)
    }

    fun stop() {
        shuttingDown = true
        executor.shutdown()
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        } catch (_: InterruptedException) {
            executor.shutdownNow()
        }
    }

    private fun enforce() {
        if (shuttingDown) return
        try {
            val now = System.currentTimeMillis()
            val configs = clusterStateService.configurations.values
            val initialInstances = clusterStateService.getAllInstances()

            // Watchdog: reap instances stuck in CREATING (lost deploy task, port that never
            // freed, resource that never became ready) so they stop counting as active and can be
            // retried on this very tick.
            val stuck = InstanceLifecyclePolicy.staleCreating(
                initialInstances, now, InstanceLifecyclePolicy.CREATING_TIMEOUT_MS
            )
            for (instance in stuck) {
                log(
                    "Instance ${instance.id} (config=${instance.configurationName}) stuck in CREATING for over " +
                    "${InstanceLifecyclePolicy.CREATING_TIMEOUT_MS / 1000}s; marking OFFLINE for retry",
                    LogLevel.WARNING
                )
                // Release the port the wedged deploy allocated but never released, otherwise the
                // in-memory allocation outlives the instance and a fixed range stays exhausted.
                if (instance.allocatedPort > 0) portAllocator.release(instance.allocatedPort)
                clusterStateService.updateInstanceState(instance.id, InstanceState.OFFLINE)
            }
            stuck.mapTo(HashSet()) { it.configurationName }.forEach { spawnBackoff.recordFailure(it, now) }

            // Re-read after reaping so the deficit reflects the freed-up slots.
            val allInstances = if (stuck.isEmpty()) initialInstances else clusterStateService.getAllInstances()

            for (config in configs) {
                if (config.minimumServiceCount <= 0) continue

                val activeCount = allInstances.count {
                    it.configurationName == config.name &&
                    (it.state == InstanceState.ONLINE || it.state == InstanceState.CREATING)
                }
                val onlineCount = allInstances.count {
                    it.configurationName == config.name && it.state == InstanceState.ONLINE
                }
                if (onlineCount >= config.minimumServiceCount) spawnBackoff.recordSuccess(config.name)

                val deficit = if (config.static) min(config.minimumServiceCount - activeCount, 1) else config.minimumServiceCount - activeCount
                if (deficit <= 0) continue

                if (!spawnBackoff.ready(config.name, now)) {
                    val retryInSec = ((spawnBackoff.nextAttemptMs(config.name) - now).coerceAtLeast(0)) / 1000
                    log(
                        "Config '${config.name}' is below minimum ($activeCount/${config.minimumServiceCount}) " +
                        "but backing off after repeated spawn failures; next attempt in ${retryInSec}s",
                        LogLevel.DEBUG
                    )
                    continue
                }

                log(
                    "Config '${config.name}' has $activeCount active instance(s), " +
                    "minimum=${config.minimumServiceCount}. Spawning $deficit...",
                    LogLevel.WARNING
                )

                repeat(deficit) { i ->
                    val instanceInfo = instanceCreationService.createInstance(config)
                    if (instanceInfo == null) {
                        log(
                            "Failed to spawn instance #$i for config '${config.name}': no node has enough resources.",
                            LogLevel.WARNING
                        )
                        return@repeat
                    }
                    log(
                        "Auto-spawned instance ${instanceInfo.id} for config '${config.name}' on node ${instanceInfo.wrapperNodeId}",
                        LogLevel.SUCCESS
                    )
                }
                spawnBackoff.recordFailure(config.name, now)
            }
        } catch (e: Exception) {
            log("InstanceCountEnforcer encountered an error: ${e.message}", LogLevel.ERROR)
        }
    }
}
