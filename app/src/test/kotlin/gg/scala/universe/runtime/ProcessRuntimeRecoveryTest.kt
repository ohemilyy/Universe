package gg.scala.universe.runtime

import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class ProcessRuntimeRecoveryTest {
    @Test
    fun `recovered pid identity is rebound and stopped`() {
        val handle = FakeHandle(42)
        val provider = ProcessRuntimeProvider(object : ProcessIdentityLookup {
            override fun find(pid: Long): ProcessHandle? = handle.takeIf { pid == 42L }
            override fun matchesWorkingDirectory(pid: Long, expected: Path) = true
        })

        assertEquals(
            RuntimeResourceState.RUNNING,
            provider.inspectRecovered("old001", 42, Path.of("work"))
        )
        provider.stop("old001")

        assertFalse(handle.isAlive)
    }

    @Test
    fun `pid reuse with wrong working directory remains unknown`() {
        val provider = ProcessRuntimeProvider(object : ProcessIdentityLookup {
            override fun find(pid: Long): ProcessHandle = FakeHandle(pid)
            override fun matchesWorkingDirectory(pid: Long, expected: Path) = false
        })
        assertEquals(
            RuntimeResourceState.UNKNOWN,
            provider.inspectRecovered("old001", 42, Path.of("work"))
        )
    }

    @Test
    fun `stop before durable pid recovery cannot claim confirmed absence`() {
        val provider = ProcessRuntimeProvider(object : ProcessIdentityLookup {
            override fun find(pid: Long): ProcessHandle = FakeHandle(pid)
            override fun matchesWorkingDirectory(pid: Long, expected: Path) = true
        })

        assertFailsWith<IllegalStateException> { provider.stop("old001") }
    }

    @Test
    fun `crash before pid publication remains unknown and retains teardown ownership`() {
        val provider = ProcessRuntimeProvider(object : ProcessIdentityLookup {
            override fun find(pid: Long): ProcessHandle? = null
            override fun matchesWorkingDirectory(pid: Long, expected: Path) = false
        })

        assertEquals(
            RuntimeResourceState.UNKNOWN,
            provider.inspectRecovered("old001", null, Path.of("work"))
        )
        assertFailsWith<IllegalStateException> { provider.stop("old001") }
    }

    @Test
    fun `durable dead pid inspection permits idempotent confirmed stop`() {
        val provider = ProcessRuntimeProvider(object : ProcessIdentityLookup {
            override fun find(pid: Long): ProcessHandle? = null
            override fun matchesWorkingDirectory(pid: Long, expected: Path) = false
        })

        assertEquals(
            RuntimeResourceState.ABSENT,
            provider.inspectRecovered("old001", 42, Path.of("work"))
        )
        provider.stop("old001")
        provider.stop("old001")
    }

    private class FakeHandle(private val value: Long) : ProcessHandle {
        private var alive = true
        override fun pid() = value
        override fun parent(): Optional<ProcessHandle> = Optional.empty()
        override fun children(): Stream<ProcessHandle> = Stream.empty()
        override fun descendants(): Stream<ProcessHandle> = Stream.empty()
        override fun info(): ProcessHandle.Info = FakeInfo
        override fun onExit(): CompletableFuture<ProcessHandle> = CompletableFuture.completedFuture(this)
        override fun supportsNormalTermination() = true
        override fun destroy(): Boolean { alive = false; return true }
        override fun destroyForcibly(): Boolean { alive = false; return true }
        override fun isAlive() = alive
        override fun compareTo(other: ProcessHandle) = pid().compareTo(other.pid())
    }

    private object FakeInfo : ProcessHandle.Info {
        override fun command(): Optional<String> = Optional.empty()
        override fun commandLine(): Optional<String> = Optional.empty()
        override fun arguments(): Optional<Array<String>> = Optional.empty()
        override fun startInstant(): Optional<Instant> = Optional.empty()
        override fun totalCpuDuration(): Optional<Duration> = Optional.empty()
        override fun user(): Optional<String> = Optional.empty()
    }
}
