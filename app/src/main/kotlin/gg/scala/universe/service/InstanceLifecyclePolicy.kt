package gg.scala.universe.service

import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState

/**
 * Pure lifecycle policy decisions, kept free of Hazelcast so they can be unit-tested.
 *
 * @author Luna
 * @date 2026-06-25
 */
object InstanceLifecyclePolicy {

    /**
     * How long an instance may stay in [InstanceState.CREATING] before it is treated as
     * stuck. Shared by the port allocator (so a stale CREATING instance stops reserving its
     * port) and the watchdog (so it is reaped) — keeping both in step.
     */
    const val CREATING_TIMEOUT_MS = 120_000L

    /**
     * Returns the instances that have been stuck in [InstanceState.CREATING] for longer
     * than [timeoutMs] (measured from their last heartbeat). These are deploys that never
     * completed — a lost task, a port that never freed, or a resource that never became ready —
     * and must be transitioned out of CREATING so they stop counting as "active" and can
     * be retried.
     */
    fun staleCreating(
        instances: Collection<InstanceInfo>,
        nowMs: Long,
        timeoutMs: Long
    ): List<InstanceInfo> {
        return instances.filter { instance ->
            instance.state == InstanceState.CREATING && nowMs - instance.lastHeartbeat > timeoutMs
        }
    }
}
