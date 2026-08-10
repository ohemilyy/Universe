package gg.scala.universe.service

import kotlin.test.Test
import kotlin.test.assertEquals

class StopDispatchSemanticsTest {
    @Test
    fun `repeated normal stop is idempotent`() {
        assertEquals(
            StopDispatchOutcome.IDEMPOTENT,
            StopDispatchResult.ALREADY_STOPPING.toRequestOutcome(restart = false)
        )
    }

    @Test
    fun `restart cannot be queued after stopping wins race`() {
        assertEquals(
            StopDispatchOutcome.CONFLICT,
            StopDispatchResult.ALREADY_STOPPING.toRequestOutcome(restart = true)
        )
    }

    @Test
    fun `missing instance remains not found after precheck race`() {
        assertEquals(
            StopDispatchOutcome.NOT_FOUND,
            StopDispatchResult.NOT_FOUND.toRequestOutcome(restart = true)
        )
    }

    @Test
    fun `successful dispatch is accepted`() {
        assertEquals(
            StopDispatchOutcome.ACCEPTED,
            StopDispatchResult.DISPATCHED.toRequestOutcome(restart = true)
        )
    }

    @Test
    fun `target and submission failures are service unavailable`() {
        assertEquals(
            StopDispatchOutcome.SERVICE_UNAVAILABLE,
            StopDispatchResult.TARGET_UNAVAILABLE.toRequestOutcome(restart = false)
        )
        assertEquals(
            StopDispatchOutcome.SERVICE_UNAVAILABLE,
            StopDispatchResult.SUBMISSION_FAILED.toRequestOutcome(restart = true)
        )
    }
}
