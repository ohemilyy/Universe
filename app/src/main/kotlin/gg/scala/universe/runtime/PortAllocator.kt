package gg.scala.universe.runtime

import com.google.inject.Inject
import com.google.inject.Singleton
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.PortRange
import gg.scala.universe.service.occupiesPort
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap

/**
 * Scans local port ranges and allocates an available port, in random order
 * by default or lowest-first when the range's strategy is "sequential".
 *
 * Ports are tracked in-memory to avoid allocating the same port twice
 * within the same JVM instance. The actual availability test is performed
 * by attempting to bind a [ServerSocket] and, as a secondary check, verifying
 * no existing service is listening on the port.
 *
 * Additionally, the allocator checks against all active instances across the
 * cluster (via Hazelcast) to avoid conflicts with ports already allocated
 * to other configurations.
 */
@Singleton
class PortAllocator @Inject constructor(
    private val clusterStateService: ClusterStateService
) {

    private val allocatedPorts = ConcurrentHashMap.newKeySet<Int>()

    /**
     * Finds and locks an available port in the given [range], scanning in
     * random or sequential order per the range's strategy.
     *
     * Checks, in order:
     * 1. Local in-memory allocations (this JVM)
     * 2. Active instances cluster-wide (Hazelcast — all configurations)
     * 3. OS-level port availability (ServerSocket bind + connect probe)
     *
     * @param range The inclusive min/max port range to scan.
     * @return The allocated port number, or `null` if none are available.
     */
    fun allocate(range: PortRange): Int? {
        // Build a snapshot of ports held by instances across the cluster.
        val clusterUsedPorts = clusterStateService.getAllInstances()
            .filter { it.state.occupiesPort }
            .map { it.allocatedPort }
            .toSet()

        val candidates = if ("sequential".equals(range.strategy, ignoreCase = true)) {
            (range.min..range.max).toList()
        } else {
            (range.min..range.max).shuffled()
        }

        for (port in candidates) {
            // Claim locally before any slow cluster/OS probes. contains()+add() is not
            // atomic and allowed concurrent callers to both return the same port.
            if (!allocatedPorts.add(port)) {
                log("Port $port skipped — already allocated locally", LogLevel.DEBUG)
                continue
            }

            var accepted = false
            try {
                // 2. Check cluster-wide active instances (all configurations)
                if (port in clusterUsedPorts) {
                    log("Port $port skipped — in use by another instance in the cluster", LogLevel.DEBUG)
                    continue
                }

                // 3. OS-level availability check
                if (!isPortAvailable(port)) {
                    log("Port $port skipped — bound by another process on this machine", LogLevel.DEBUG)
                    continue
                }

                accepted = true
                log("Allocated port $port (range ${range.min}-${range.max})")
                return port
            } finally {
                if (!accepted) allocatedPorts.remove(port)
            }
        }

        log("No available ports in range ${range.min}-${range.max}", LogLevel.ERROR)
        return null
    }

    /**
     * Releases a previously allocated port so it can be reused.
     */
    fun release(port: Int) {
        if (allocatedPorts.remove(port)) {
            log("Released port $port")
        }
    }

    /**
     * Marks a port as used without checking availability.
     * Used during instance recovery to prevent duplicate allocation.
     */
    fun reserve(port: Int): Boolean {
        val reserved = allocatedPorts.add(port)
        if (reserved) {
            log("Reserved port $port (recovered)")
        }
        return reserved
    }

    /**
     * Returns the set of ports currently allocated in this JVM.
     */
    fun getLocalAllocations(): Set<Int> = allocatedPorts.toSet()

    /**
     * Checks OS-level port availability using two strategies:
     *
     * 1. Attempt to bind a [ServerSocket] — catches most listeners.
     * 2. Attempt a TCP connect to `localhost:port` with a short timeout —
     *    catches services that may be listening but where bind might succeed
     *    due to socket reuse options.
     *
     * @return `true` if the port appears to be free on this machine.
     */
    private fun isPortAvailable(port: Int): Boolean {
        // Strategy 1: Try to bind a server socket
        val bindable = try {
            ServerSocket(port).use { true }
        } catch (_: Exception) {
            false
        }

        if (!bindable) return false

        // Strategy 2: Try to connect to localhost:port with a very short timeout.
        // If the connection succeeds, something is already listening.
        val connectable = try {
            Socket().use { socket ->
                socket.connect(InetSocketAddress("localhost", port), 100)
                true  // Something answered — port is in use
            }
        } catch (_: Exception) {
            false // Nothing answered — port is likely free
        }

        return !connectable
    }
}
