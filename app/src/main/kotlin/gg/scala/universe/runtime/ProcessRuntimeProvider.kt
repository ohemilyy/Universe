package gg.scala.universe.runtime

import com.google.inject.Singleton
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.schema.Configuration
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

internal interface ProcessIdentityLookup {
    fun find(pid: Long): ProcessHandle?
    fun matchesWorkingDirectory(pid: Long, expected: Path): Boolean
}

private object SystemProcessIdentityLookup : ProcessIdentityLookup {
    override fun find(pid: Long): ProcessHandle? = ProcessHandle.of(pid).orElse(null)

    override fun matchesWorkingDirectory(pid: Long, expected: Path): Boolean {
        val cwd = Path.of("/proc", pid.toString(), "cwd")
        if (!Files.exists(cwd)) return false
        return try {
            Files.isSameFile(cwd, expected.toAbsolutePath().normalize())
        } catch (_: Exception) {
            false
        }
    }
}

/** Direct subprocess runtime with durable PID recovery after wrapper restart. */
@Singleton
class ProcessRuntimeProvider internal constructor(
    private val processLookup: ProcessIdentityLookup = SystemProcessIdentityLookup
) : RuntimeProvider, RuntimeRecoveryInspector {
    private val processes = ConcurrentHashMap<String, Process>()
    private val recoveredHandles = ConcurrentHashMap<String, ProcessHandle>()
    private val confirmedAbsentPids = ConcurrentHashMap<String, Long>()

    override fun start(
        instanceId: String,
        workingDir: Path,
        port: Int,
        command: String,
        ramMB: Int,
        cpu: Int,
        configuration: Configuration,
        environmentVariables: Map<String, String>?
    ): ProcessHandle {
        require(command.isNotBlank()) { "Command is blank for instance $instanceId" }
        val builder = ProcessBuilder(
            "bash", "-c", CgroupResourceEnforcer.buildFallbackPrefix(ramMB, cpu) + command
        ).directory(workingDir.toFile())
            .redirectOutput(ProcessBuilder.Redirect.to(workingDir.resolve("stdout.log").toFile()))
            .redirectError(ProcessBuilder.Redirect.to(workingDir.resolve("stderr.log").toFile()))
            .redirectInput(ProcessBuilder.Redirect.PIPE)
        if (!environmentVariables.isNullOrEmpty()) builder.environment().putAll(environmentVariables)
        val process = builder.start()
        confirmedAbsentPids.remove(instanceId)
        processes[instanceId] = process
        recoveredHandles[instanceId] = process.toHandle()
        CgroupResourceEnforcer.createCgroup(instanceId, ramMB, cpu)?.let {
            CgroupResourceEnforcer.movePidToCgroup(process.pid(), it)
        }
        log("Started process for instance $instanceId (PID ${process.pid()})", LogLevel.SUCCESS)
        return process.toHandle()
    }

    override fun inspectRecovered(
        instanceId: String,
        processPid: Long?,
        workingDirectory: Path
    ): RuntimeResourceState {
        val tracked = processes[instanceId]?.toHandle() ?: recoveredHandles[instanceId]
        if (tracked != null) {
            return if (tracked.isAlive) {
                confirmedAbsentPids.remove(instanceId)
                RuntimeResourceState.RUNNING
            } else {
                confirmedAbsentPids[instanceId] = tracked.pid()
                RuntimeResourceState.ABSENT
            }
        }
        processPid ?: return RuntimeResourceState.UNKNOWN
        val handle = processLookup.find(processPid)
        if (handle == null || !handle.isAlive) {
            confirmedAbsentPids[instanceId] = processPid
            recoveredHandles.remove(instanceId)
            return RuntimeResourceState.ABSENT
        }
        if (!processLookup.matchesWorkingDirectory(processPid, workingDirectory)) {
            confirmedAbsentPids.remove(instanceId)
            return RuntimeResourceState.UNKNOWN
        }
        confirmedAbsentPids.remove(instanceId)
        recoveredHandles[instanceId] = handle
        return RuntimeResourceState.RUNNING
    }

    override fun stop(instanceId: String) {
        val process = processes[instanceId]
        val handle = process?.toHandle() ?: recoveredHandles[instanceId]
        if (handle == null) {
            check(confirmedAbsentPids[instanceId] != null) {
                "Process identity for instance $instanceId is unknown; durable PID recovery is required"
            }
            CgroupResourceEnforcer.cleanupCgroup(instanceId)
            log("Process for instance $instanceId was already confirmed absent")
            return
        }
        if (handle.isAlive) {
            handle.destroy()
            awaitExit(instanceId, handle, force = false)
            if (handle.isAlive) {
                handle.destroyForcibly()
                awaitExit(instanceId, handle, force = true)
            }
        }
        check(!handle.isAlive) { "Process for instance $instanceId is still running" }
        CgroupResourceEnforcer.cleanupCgroup(instanceId)
        process?.let { processes.remove(instanceId, it) }
        recoveredHandles.remove(instanceId, handle)
        confirmedAbsentPids[instanceId] = handle.pid()
        log("Stopped process for instance $instanceId")
    }

    private fun awaitExit(instanceId: String, handle: ProcessHandle, force: Boolean) {
        try {
            handle.onExit().get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (failure: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IllegalStateException("Interrupted while confirming process teardown for $instanceId", failure)
        } catch (_: TimeoutException) {
            if (force) throw IllegalStateException("Failed to confirm forced process teardown for $instanceId")
        } catch (failure: Exception) {
            throw IllegalStateException("Failed to confirm process teardown for $instanceId", failure)
        }
    }

    override fun executeCommand(instanceId: String, command: String) {
        val process = processes[instanceId]
            ?: return log("Recovered process $instanceId has no attachable stdin", LogLevel.WARNING)
        process.outputStream.bufferedWriter().use {
            it.write(command)
            it.newLine()
            it.flush()
        }
    }

    override fun isRunning(instanceId: String): Boolean =
        processes[instanceId]?.isAlive ?: recoveredHandles[instanceId]?.isAlive ?: false

    override fun listRunningInstances(): List<String> =
        (processes.keys + recoveredHandles.keys).distinct()

    override fun getLogs(instanceId: String, lines: Int): List<String> = emptyList()

    private companion object {
        const val STOP_TIMEOUT_SECONDS = 10L
    }
}
