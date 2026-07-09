package gg.scala.universe.runtime

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RuntimeReconcileTest {

    private val grace = 90_000L

    private fun snap(
        instanceId: String? = "abc123",
        phase: String? = "Running",
        ready: Boolean = true,
        terminating: Boolean = false,
        ageMs: Long = 10_000L
    ) = ResourceSnapshot(instanceId, phase, ready, terminating, ageMs)

    @Test
    fun `tracked running ready resource is adopted`() {
        val action = RuntimeReconcile.classify(snap(), isTracked = true, pendingGraceMs = grace)
        assertEquals(ReconcileAction.ADOPT, action)
    }

    @Test
    fun `untracked running ready resource is adopted not deleted`() {
        // After a state wipe every resource looks "untracked". A healthy resource must be adopted, never
        // killed — otherwise a fresh restart would delete healthy static/min instances.
        val action = RuntimeReconcile.classify(snap(), isTracked = false, pendingGraceMs = grace)
        assertEquals(ReconcileAction.ADOPT, action)
    }

    @Test
    fun `tracked failed resource is deleted as dead`() {
        val action = RuntimeReconcile.classify(snap(phase = "Failed", ready = false), isTracked = true, pendingGraceMs = grace)
        assertEquals(ReconcileAction.DELETE_DEAD, action)
    }

    @Test
    fun `untracked failed resource is deleted as orphan`() {
        // The genuine post-restart zombie: a dead/stuck resource Universe no longer tracks.
        val action = RuntimeReconcile.classify(snap(phase = "Failed", ready = false), isTracked = false, pendingGraceMs = grace)
        assertEquals(ReconcileAction.DELETE_ORPHAN, action)
    }

    @Test
    fun `untracked resource pending past grace is an orphan`() {
        val action = RuntimeReconcile.classify(
            snap(phase = "Pending", ready = false, ageMs = grace + 1_000L),
            isTracked = false,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.DELETE_ORPHAN, action)
    }

    @Test
    fun `tracked resource pending past grace is dead`() {
        val action = RuntimeReconcile.classify(
            snap(phase = "Pending", ready = false, ageMs = grace + 1_000L),
            isTracked = true,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.DELETE_DEAD, action)
    }

    @Test
    fun `tracked resource pending within grace waits`() {
        val action = RuntimeReconcile.classify(
            snap(phase = "Pending", ready = false, ageMs = 5_000L),
            isTracked = true,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.WAIT, action)
    }

    @Test
    fun `terminating resource waits even when untracked`() {
        val action = RuntimeReconcile.classify(
            snap(terminating = true),
            isTracked = false,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.WAIT, action)
    }

    @Test
    fun `tracked running-but-not-ready resource past grace is dead`() {
        val action = RuntimeReconcile.classify(
            snap(phase = "Running", ready = false, ageMs = grace + 1_000L),
            isTracked = true,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.DELETE_DEAD, action)
    }

    @Test
    fun `resource without instance id is left alone`() {
        val action = RuntimeReconcile.classify(
            snap(instanceId = null),
            isTracked = false,
            pendingGraceMs = grace
        )
        assertEquals(ReconcileAction.WAIT, action)
    }
}
