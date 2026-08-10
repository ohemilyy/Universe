package gg.scala.universe.runtime

import com.google.inject.Singleton
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * [RuntimeProvider] implementation using [GNU Screen](https://www.gnu.org/software/screen/).
 *
 * Each instance runs in a dedicated screen session named after the instance ID.
 * Commands are piped via `screen -X stuff`.
 */
@Singleton
class ScreenRuntimeProvider : RuntimeProvider {

    internal data class CommandResult(val exitCode: Int, val output: String, val error: String)

    private val sessions = ConcurrentHashMap<String, ProcessHandle>()
    private var statusCommand: () -> CommandResult = ::runStatusCommand

    internal fun useStatusCommandForTest(command: () -> CommandResult) {
        statusCommand = command
    }

    override fun start(
        instanceId: String,
        workingDir: Path,
        port: Int,
        command: String,
        ramMB: Int,
        cpu: Int,
        configuration: gg.scala.universe.schema.Configuration,
        environmentVariables: Map<String, String>?,
    ): ProcessHandle {
        val sessionName = sessionName(instanceId)

        // Ensure any stale session with this name is cleaned up first
        runCommand("screen", "-S", sessionName, "-X", "quit")

        // Build command with resource limit fallback prefix
        val prefix = CgroupResourceEnforcer.buildFallbackPrefix(ramMB, cpu)
        val shellCommand = "cd ${workingDir.toAbsolutePath()} && $prefix$command"
        val processBuilder = ProcessBuilder("screen", "-dmS", sessionName, "bash", "-c", shellCommand)
            .inheritIO()

        if (!environmentVariables.isNullOrEmpty()) {
            processBuilder.environment().putAll(environmentVariables)
        }

        val process = processBuilder.start()

        val handle = process.toHandle()
        sessions[instanceId] = handle

        // Attempt cgroup v2 enforcement
        val cgroupPath = CgroupResourceEnforcer.createCgroup(instanceId, ramMB, cpu)
        if (cgroupPath != null) {
            CgroupResourceEnforcer.movePidToCgroup(handle.pid(), cgroupPath)
        }

        log("Started screen session '$sessionName' for instance $instanceId (PID ${handle.pid()})", LogLevel.SUCCESS)
        return handle
    }

    override fun stop(instanceId: String) {
        val sessionName = sessionName(instanceId)
        runCommand("screen", "-S", sessionName, "-X", "quit")
        check(!hasSession(sessionName)) { "Screen session '$sessionName' is still running" }
        sessions.remove(instanceId)
        CgroupResourceEnforcer.cleanupCgroup(instanceId)
        log("Stopped screen session '$sessionName' for instance $instanceId")
    }

    override fun executeCommand(instanceId: String, command: String) {
        val sessionName = sessionName(instanceId)
        val process = ProcessBuilder("screen", "-S", sessionName, "-X", "stuff", "$command\n")
            .inheritIO()
            .start()
        process.waitFor()
        log("Executed command on screen session '$sessionName': $command")
    }

    override fun isRunning(instanceId: String): Boolean {
        return hasSession(sessionName(instanceId))
    }

    override fun listRunningInstances(): List<String> {
        return try {
            val process = ProcessBuilder("screen", "-ls")
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start()
            val output = process.inputStream.bufferedReader().readText()
            process.waitFor()
            output.lines()
                .filter { it.contains("universe-") }
                .mapNotNull { line ->
                    Regex("universe-([a-zA-Z0-9]+)").find(line)?.groupValues?.get(1)
                }
        } catch (failure: Exception) {
            throw IllegalStateException("Failed to discover screen sessions", failure)
        }
    }

    override fun getLogs(instanceId: String, lines: Int): List<String> {
        val sessionName = sessionName(instanceId)
        val tempFile = java.nio.file.Files.createTempFile("screen-log-", ".txt")
        return try {
            val process = ProcessBuilder("screen", "-S", sessionName, "-X", "hardcopy", tempFile.toString())
                .inheritIO()
                .start()
            process.waitFor()
            val lines = tempFile.toFile().readLines().takeLast(lines)
            java.nio.file.Files.deleteIfExists(tempFile)
            lines.filter { it.isNotBlank() }
        } catch (_: Exception) {
            java.nio.file.Files.deleteIfExists(tempFile)
            emptyList()
        }
    }

    private fun sessionName(instanceId: String): String = "universe-$instanceId"

    private fun runCommand(vararg command: String): Int {
        return ProcessBuilder(*command)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
            .waitFor()
    }

    private fun hasSession(sessionName: String): Boolean {
        val result = statusCommand()
        val output = result.output
        val error = result.error
        val exitCode = result.exitCode
        val diagnostic = "$output\n$error"
        return when {
            exitCode == 0 -> output.contains(sessionName)
            exitCode == 1 && (
                diagnostic.contains("No Sockets found", ignoreCase = true) ||
                    diagnostic.contains("No screen session found", ignoreCase = true)
                ) -> false
            else -> error(
                "Unable to confirm screen session '$sessionName' state: ${diagnostic.trim()}"
            )
        }
    }

    private fun runStatusCommand(): CommandResult {
        val process = ProcessBuilder("screen", "-ls")
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.PIPE)
            .start()
        val output = process.inputStream.bufferedReader().readText()
        val error = process.errorStream.bufferedReader().readText()
        return CommandResult(process.waitFor(), output, error)
    }
}
