package gg.scala.universe.runtime

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith

class ScreenRuntimeProviderTest {
    @Test
    fun `exit one with permission failure is unknown rather than absent`() {
        val provider = ScreenRuntimeProvider().apply {
            useStatusCommandForTest {
                ScreenRuntimeProvider.CommandResult(1, "", "Cannot open your terminal: Permission denied")
            }
        }

        assertFailsWith<IllegalStateException> { provider.isRunning("old001") }
    }

    @Test
    fun `exit one with explicit no sockets is confirmed absent`() {
        val provider = ScreenRuntimeProvider().apply {
            useStatusCommandForTest {
                ScreenRuntimeProvider.CommandResult(1, "No Sockets found in /run/screen/S-user.", "")
            }
        }

        assertFalse(provider.isRunning("old001"))
    }
}
