package gg.scala.universe.runtime

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState

/**
 * Pure helpers for deciding which ports are genuinely reserved by live instances.
 *
 * Kept free of any runtime/Hazelcast dependency so the logic is unit-testable in
 * isolation. The allocator wires this against live cluster state.
 *
 * @author Luna
 * @date 2026-06-25
 */
object InstancePortReservations {

    /**
     * Returns the set of ports that are actually held by a live instance and must
     * not be handed out again.
     *
     * A port counts as reserved only when:
     * - it is a real allocated port (port `0` is the "not yet allocated" sentinel), and
     * - the owning instance is [InstanceState.ONLINE], or [InstanceState.CREATING] but
     *   still within [creatingTimeoutMs] of its last heartbeat (a fresh, in-flight deploy).
     *
     * Stale CREATING instances (deploys that never completed) are intentionally excluded
     * so a wedged deploy can never reserve a port forever.
     */
    fun reservedPorts(
        instances: Collection<InstanceInfo>,
        nowMs: Long,
        creatingTimeoutMs: Long
    ): Set<Int> {
        return instances.asSequence()
            // port 0 means "not yet allocated" — never reserve it.
            .filter { it.allocatedPort > 0 }
            .filter { instance ->
                when (instance.state) {
                    InstanceState.ONLINE -> true
                    // A CREATING instance only holds its port while the deploy is still fresh.
                    InstanceState.CREATING -> nowMs - instance.lastHeartbeat <= creatingTimeoutMs
                    else -> false
                }
            }
            .map { it.allocatedPort }
            .toSet()
    }
}
