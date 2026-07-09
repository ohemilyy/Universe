package gg.scala.universe.runtime

/**
 * Pure, runtime-agnostic model for reconciling a runtime's managed resources against
 * Universe's desired state.
 *
 * The decision logic lives here, free of any runtime/Hazelcast dependency, so it can be
 * unit-tested in isolation. Runtime providers build a [ResourceSnapshot] from their
 * native objects and act on the returned [ReconcileAction].
 *
 * @author Luna
 * @date 2026-06-25
 */
enum class ReconcileAction {
    /** Tracked, running and ready — adopt it back as a live instance. */
    ADOPT,

    /** Universe no longer tracks this resource — delete it (post-restart zombie). */
    DELETE_ORPHAN,

    /** Tracked but failed/succeeded/unknown or stuck pending — delete and fail the instance. */
    DELETE_DEAD,

    /** Transient state (terminating, or pending within grace) — leave for the next tick. */
    WAIT
}

/**
 * A minimal, native-type-free view of a managed resource needed to classify it.
 *
 * @param instanceId The Universe instance id this resource is labelled with, or null if it carries none.
 * @param phase The runtime lifecycle phase (e.g. Running / Pending / Failed / Succeeded / Unknown), or null if unknown.
 * @param ready Whether the resource reports itself ready.
 * @param terminating Whether the resource is already being deleted.
 * @param ageMs How long the resource has existed, in milliseconds.
 */
data class ResourceSnapshot(
    val instanceId: String?,
    val phase: String?,
    val ready: Boolean,
    val terminating: Boolean,
    val ageMs: Long
)

/**
 * A live, healthy resource that was adopted during reconciliation. Carries enough detail for
 * the caller to re-register the instance if Universe lost track of it (e.g. across a restart).
 */
data class AdoptedResource(
    val instanceId: String,
    val configurationName: String?,
    val hostAddress: String,
    val port: Int
)

/**
 * Outcome of a runtime reconciliation pass.
 *
 * @param adopted Healthy resources confirmed running and ready (tracked or recovered).
 * @param deadInstanceIds Tracked instances whose resource was failed/stuck and has been deleted —
 *        the caller should mark these OFFLINE and release their ports.
 * @param deletedOrphanCount Number of untracked (zombie) dead resources deleted.
 * @param deletedDeadCount Number of tracked-but-dead resources deleted.
 */
data class RuntimeReconcileReport(
    val adopted: List<AdoptedResource> = emptyList(),
    val deadInstanceIds: Set<String> = emptySet(),
    val deletedOrphanCount: Int = 0,
    val deletedDeadCount: Int = 0
)

object RuntimeReconcile {

    /**
     * Decides what to do with a single managed resource.
     *
     * @param snapshot The resource view.
     * @param isTracked Whether Universe currently has an instance record for [ResourceSnapshot.instanceId].
     * @param pendingGraceMs How long a resource may stay un-ready/pending before it is considered dead.
     */
    fun classify(
        snapshot: ResourceSnapshot,
        isTracked: Boolean,
        pendingGraceMs: Long
    ): ReconcileAction {
        // A resource without an instance id isn't ours to touch.
        if (snapshot.instanceId == null) return ReconcileAction.WAIT

        // Already being deleted — let Kubernetes finish the job.
        if (snapshot.terminating) return ReconcileAction.WAIT

        // A healthy (Running + Ready) resource is always adopted — even if Universe lost track
        // of it across a restart. Deletion is driven by deadness, never by "untracked", so a
        // state wipe can never delete a healthy instance.
        if (snapshot.phase == "Running" && snapshot.ready) return ReconcileAction.ADOPT

        val overGrace = snapshot.ageMs > pendingGraceMs
        val dead = when (snapshot.phase) {
            "Failed", "Succeeded", "Unknown" -> true
            "Running" -> overGrace            // Running but never became Ready
            "Pending", null -> overGrace      // can't schedule, or no status yet
            else -> false
        }
        if (!dead) return ReconcileAction.WAIT

        // Dead: delete either way; the distinction only drives reporting (a tracked instance
        // gets marked OFFLINE, an untracked one is just a zombie to remove).
        return if (isTracked) ReconcileAction.DELETE_DEAD else ReconcileAction.DELETE_ORPHAN
    }
}
