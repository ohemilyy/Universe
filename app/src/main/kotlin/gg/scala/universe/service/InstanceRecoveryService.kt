package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.task.InstanceWorkspace
import gg.scala.universe.runtime.PortAllocator
import gg.scala.universe.runtime.RuntimeRecoveryInspector
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.runtime.RuntimeResourceState
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.util.json.Serializers
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

/** Recovers wrapper-owned runtime resources before count reconciliation starts. */
@Singleton
class InstanceRecoveryService @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val runtimeRegistry: RuntimeRegistry,
    private val portAllocator: PortAllocator,
    private val workspace: InstanceWorkspace = InstanceWorkspace()
) {
    fun recover() {
        val localNodeId = hazelcastInstance.cluster.localMember.uuid.toString()
        log("Recovering instances for node $localNodeId...")
        val recovered = mutableSetOf<String>()

        clusterStateService.getAllInstances()
            .filter { it.wrapperNodeId == localNodeId && it.state == InstanceState.ONLINE }
            .forEach { if (verifyAndRegister(it)) recovered += it.id }

        val roots = listOf(workspace.runningRoot(), workspace.staticRoot())
        roots.forEach { root ->
            if (!Files.exists(root)) return@forEach
            Files.list(root).use { stream ->
                stream.filter(Files::isDirectory).forEach { directory ->
                    readState(directory)?.takeIf { it.id !in recovered }?.let {
                        if (verifyAndRegister(it)) recovered += it.id
                    }
                }
            }
        }

        runtimeRegistry.getAll().forEach { (runtimeKey, provider) ->
            val ids = try {
                provider.listRunningInstances()
            } catch (failure: Exception) {
                log("Could not discover instances from runtime '$runtimeKey': ${failure.message}", LogLevel.WARNING)
                return@forEach
            }
            ids.filterNot(recovered::contains).forEach { id ->
                val persisted = findStateFile(id, roots)?.let(::readStateFile)
                    ?: clusterStateService.getInstance(id)
                if (persisted != null && verifyAndRegister(persisted)) {
                    recovered += id
                } else if (persisted == null) {
                    log("Found unknown running instance '$id' via runtime '$runtimeKey', skipping recovery", LogLevel.WARNING)
                }
            }
        }

        if (recovered.isEmpty()) log("No instances to recover") else {
            log("Recovered ${recovered.size} instance(s): ${recovered.joinToString(", ")}", LogLevel.SUCCESS)
        }
    }

    private fun verifyAndRegister(candidate: InstanceInfo): Boolean {
        val expected = clusterStateService.getInstance(candidate.id)
        val durable = expected ?: candidate
        if (durable.state == InstanceState.STOPPING || durable.state == InstanceState.STOPPED) {
            log("Recovery rejected terminal/barrier snapshot for ${durable.id}", LogLevel.WARNING)
            return false
        }
        val expectedGeneration = expected?.let { clusterStateService.getLifecycleGeneration(it.id) }
        val config = clusterStateService.getConfiguration(durable.configurationName)
        val provider = runtimeRegistry.get(durable.runtime)
        if (provider == null) {
            log("No runtime provider '${durable.runtime}' for recovered instance ${durable.id}; leaving ownership unchanged", LogLevel.WARNING)
            return false
        }
        val workingDirectory = if (config?.static == true) {
            workspace.staticConfiguration(config.name)
        } else workspace.dynamicInstance(durable.id)
        val state = try {
            if (provider is RuntimeRecoveryInspector) {
                provider.inspectRecovered(durable.id, durable.processPid, workingDirectory)
            } else if (provider.isRunning(durable.id)) RuntimeResourceState.RUNNING
            else RuntimeResourceState.ABSENT
        } catch (failure: Exception) {
            log("Could not confirm runtime state for ${durable.id}: ${failure.message}; retaining ownership", LogLevel.WARNING)
            return false
        }

        when (state) {
            RuntimeResourceState.PRESENT_TRANSITIONAL, RuntimeResourceState.UNKNOWN -> {
                log("Runtime state for ${durable.id} is $state; retaining lifecycle ownership", LogLevel.WARNING)
                return false
            }
            RuntimeResourceState.CLEANUP_REQUIRED, RuntimeResourceState.TERMINAL -> {
                try {
                    provider.stop(durable.id)
                    val confirmed = if (provider is RuntimeRecoveryInspector) {
                        provider.inspectRecovered(durable.id, durable.processPid, workingDirectory)
                    } else if (provider.isRunning(durable.id)) RuntimeResourceState.RUNNING
                    else RuntimeResourceState.ABSENT
                    if (confirmed != RuntimeResourceState.ABSENT) return false
                } catch (failure: Exception) {
                    log("Failed to delete terminal runtime ${durable.id}: ${failure.message}", LogLevel.WARNING)
                    return false
                }
                cleanupDeadInstance(durable, config, expectedGeneration)
                return false
            }
            RuntimeResourceState.ABSENT -> {
                cleanupDeadInstance(durable, config, expectedGeneration)
                return false
            }
            RuntimeResourceState.RUNNING -> Unit
        }

        if (!portAllocator.reserve(durable.allocatedPort)) {
            log("Recovery port ${durable.allocatedPort} is already locally owned; retaining ${durable.id}", LogLevel.WARNING)
            return false
        }
        val generation = maxOf(1L, expectedGeneration ?: 0L)
        val updated = durable.copy(
            wrapperNodeId = hazelcastInstance.cluster.localMember.uuid.toString(),
            state = InstanceState.ONLINE,
            lastHeartbeat = System.currentTimeMillis()
        )
        if (!clusterStateService.recoverInstance(expected, expectedGeneration, updated, generation)) {
            portAllocator.release(durable.allocatedPort)
            log("Recovery of ${durable.id} lost its exact lifecycle claim", LogLevel.WARNING)
            return false
        }
        log("Recovered ${durable.id} (config=${durable.configurationName}, port=${durable.allocatedPort})", LogLevel.SUCCESS)
        return true
    }

    private fun cleanupDeadInstance(
        instance: InstanceInfo,
        config: Configuration?,
        expectedGeneration: Long?
    ) {
        if (expectedGeneration == null || expectedGeneration <= 0L) {
            log("Legacy/unowned instance ${instance.id} is absent; retaining metadata", LogLevel.WARNING)
            return
        }
        val (claimed, generation) = clusterStateService.claimForShutdown(
            instance, expectedGeneration
        ) ?: return
        portAllocator.release(claimed.allocatedPort)
        if (config?.static != true) deleteDirectory(workspace.dynamicInstance(claimed.id))
        clusterStateService.completeInstanceTermination(
            expectedInstance = claimed,
            expectedGeneration = generation,
            finalState = InstanceState.OFFLINE
        )
    }

    private fun deleteDirectory(directory: Path) {
        try {
            if (Files.exists(directory)) Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
            }
        } catch (failure: Exception) {
            log("Failed to clean recovered directory $directory: ${failure.message}", LogLevel.WARNING)
        }
    }

    private fun readState(directory: Path): InstanceInfo? =
        readStateFile(directory.resolve(".universe-state.json"))

    private fun readStateFile(path: Path): InstanceInfo? {
        if (!Files.exists(path)) return null
        return try {
            Serializers.GSON.fromJson(Files.readString(path), InstanceInfo::class.java)
        } catch (failure: Exception) {
            log("Failed to parse state file $path: ${failure.message}", LogLevel.WARNING)
            null
        }
    }

    private fun findStateFile(id: String, roots: List<Path>): Path? {
        roots.forEach { root ->
            if (!Files.exists(root)) return@forEach
            Files.list(root).use { stream ->
                val found = stream.filter(Files::isDirectory)
                    .map { it.resolve(".universe-state.json") }
                    .filter(Files::exists)
                    .filter { readStateFile(it)?.id == id }
                    .findFirst().orElse(null)
                if (found != null) return found
            }
        }
        return null
    }
}
