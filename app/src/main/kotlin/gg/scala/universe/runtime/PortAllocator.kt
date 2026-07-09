package gg.scala.universe.runtime

import com.google.inject.Inject
import com.google.inject.Singleton
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.PortRange
import gg.scala.universe.service.InstanceLifecyclePolicy
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap

/**
 * Scans local port ranges and allocates the lowest available port.
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
     * Finds and locks the lowest available port in the given [range].
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
        val now = System.currentTimeMillis()
        val instances = clusterStateService.getAllInstances()

        // Ports genuinely held by live instances. Stale CREATING instances and the port-0
        // sentinel are excluded, so a wedged deploy can never reserve a port forever.
        val reserved = InstancePortReservations.reservedPorts(
            instances, now, InstanceLifecyclePolicy.CREATING_TIMEOUT_MS
        )

        // Pass 1 — hand out a port that is free by every measure. The local set is honoured
        // here, so a port that was just allocated for an in-flight deploy (allocated, but not
        // yet written onto its CREATING record) is never handed to a concurrent caller.
        for (port in range.min..range.max) {
            // 1. Check local in-memory allocations
            if (allocatedPorts.contains(port)) {
                log("Port $port skipped — already allocated locally", LogLevel.DEBUG)
                continue
            }

            // 2. Check cluster-wide live instances (all configurations)
            if (port in reserved) {
                val owner = instances.firstOrNull { it.allocatedPort == port }
                log("Port $port skipped — held by live instance ${owner?.id ?: "?"} (state=${owner?.state})", LogLevel.DEBUG)
                continue
            }

            // 3. OS-level availability check
            if (!isPortAvailable(port)) {
                log("Port $port skipped — bound by another process on this machine", LogLevel.DEBUG)
                continue
            }

            allocatedPorts.add(port)
            log("Allocated port $port (range ${range.min}-${range.max})")
            return port
        }

        // Pass 2 — the range looks exhausted. Before giving up, reclaim any port that is held
        // only by the in-JVM set but owned by no live instance: a stale reservation left by a
        // deploy that died without releasing it (or that released the port-0 sentinel). Without
        // this, a single leaked fixed port wedges its whole range for the lifetime of the JVM,
        // so a static instance can never start again until Universe itself is restarted — which
        // is exactly the "No available ports … reserved locally in this JVM" failure on restart.
        for (port in range.min..range.max) {
            if (port in reserved) continue                 // genuinely held by a live instance
            if (!allocatedPorts.contains(port)) continue   // not a local reservation; pass 1 already judged it
            if (!isPortAvailable(port)) continue           // really bound on this host — don't steal it
            log(
                "Reclaiming stale local reservation for port $port — no live instance owns it " +
                "(left by a deploy that never released it)",
                LogLevel.WARNING
            )
            // The port is already in allocatedPorts, so it stays marked as ours — just hand it out.
            log("Allocated port $port (range ${range.min}-${range.max})")
            return port
        }

        logRangeExhausted(range, instances)
        return null
    }

    /**
     * Explains why a range could not be satisfied — naming the owner of each port and
     * whether it is a live Universe instance or an unowned binding (a likely leftover resource).
     * Especially important for fixed single-port ranges, which otherwise fail opaquely.
     */
    private fun logRangeExhausted(range: PortRange, instances: Collection<InstanceInfo>) {
        log("No available ports in range ${range.min}-${range.max}", LogLevel.ERROR)
        for (port in range.min..range.max) {
            val owner = instances.firstOrNull { it.allocatedPort == port }
            when {
                allocatedPorts.contains(port) ->
                    log("  port $port: reserved locally in this JVM", LogLevel.WARNING)
                owner != null ->
                    log("  port $port: owned by instance ${owner.id} (config=${owner.configurationName}, state=${owner.state})", LogLevel.WARNING)
                else ->
                    log("  port $port: bound on this host with no Universe owner — likely a leftover resource/process", LogLevel.WARNING)
            }
        }
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
    fun reserve(port: Int) {
        allocatedPorts.add(port)
        log("Reserved port $port (recovered)")
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
