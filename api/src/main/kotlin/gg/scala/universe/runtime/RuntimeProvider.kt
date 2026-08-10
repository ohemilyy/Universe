package gg.scala.universe.runtime

import gg.scala.universe.schema.Configuration
import java.nio.file.Path

/**
 * Abstraction for starting, stopping, and piping commands to an instance runtime.
 *
 * Concrete implementations (e.g., Tmux, Screen, Docker) are registered
 * via [RuntimeRegistry] under a technology key.
 */
interface RuntimeProvider {
    /**
     * Starts a new process for the given instance.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     * @param workingDir Directory in which the process should run.
     * @param port The allocated local port for the instance.
     * @param command The command string to execute.
     * @param ramMB Maximum RAM the instance may use (in megabytes). Zero means unlimited.
     * @param cpu Maximum CPU units the instance may use. Zero means unlimited.
     * @param configuration The full instance configuration. Runtimes can access
     *        fileModifications, properties, template configs, etc.
     * @param environmentVariables Optional environment variables to set for the process.
     *        Values may contain template placeholders that have already been replaced.
     * @param additionalPorts Extra ports to expose/forward alongside the main allocated port.
     *        Used by Docker and K8s runtimes for multi-port applications (e.g. voice chat, metrics).
     * @return A [ProcessHandle] representing the started process.
     */
    fun start(
        instanceId: String,
        workingDir: Path,
        port: Int,
        command: String,
        ramMB: Int,
        cpu: Int,
        configuration: Configuration,
        environmentVariables: Map<String, String>? = null,
    ): ProcessHandle

    /**
     * Stops the process associated with the given instance.
     *
     * Returning normally confirms that no runtime resource for [instanceId]
     * remains. Implementations must also treat an already-absent resource as a
     * successful idempotent stop. If absence cannot be confirmed (including
     * client, transport, permission, or deletion failures), this method must
     * throw. Callers rely on that contract before releasing ports, filesystem
     * state, resource reservations, or publishing a terminal lifecycle state.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     */
    fun stop(instanceId: String)

    /**
     * Pipes a command string into the stdin of the running process
     * for the given instance.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     * @param command The command to send.
     */
    fun executeCommand(instanceId: String, command: String)

    /**
     * Returns true if the instance is currently running and false only when
     * the runtime can confirm that it is absent or not running. Discovery or
     * transport failures must throw so callers do not release lifecycle
     * ownership based on an unverified absence.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     */
    fun isRunning(instanceId: String): Boolean

    /**
     * Returns a list of instance IDs currently managed by this runtime.
     * Used for instance recovery after node restart.
     */
    fun listRunningInstances(): List<String> = emptyList()
    /**
     * Returns the reachable host address for the given instance.
     * Used by the proxy (or other services) to connect to this instance.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     * @return The host address (IP or DNS name), or empty string if not applicable.
     */
    fun getHostAddress(instanceId: String): String = ""

    /**
     * Retrieves the latest log lines for the given instance.
     *
     * @param instanceId Unique 6-character identifier for the instance.
     * @param lines Maximum number of lines to return (default 100).
     * @return List of log lines, or empty list if not available.
     */
    fun getLogs(instanceId: String, lines: Int = 100): List<String> = emptyList()
}
