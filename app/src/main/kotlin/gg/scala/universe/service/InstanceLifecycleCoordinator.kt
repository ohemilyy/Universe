package gg.scala.universe.service

import com.google.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Serializes every runtime side effect for one instance and gates node shutdown. */
@Singleton
class InstanceLifecycleCoordinator {
    private data class LockEntry(
        val lock: ReentrantLock = ReentrantLock(),
        val users: AtomicInteger = AtomicInteger()
    )

    private val entries = ConcurrentHashMap<String, LockEntry>()
    private val quiescing = AtomicBoolean()

    fun beginShutdown() {
        quiescing.set(true)
    }

    fun isQuiescing(): Boolean = quiescing.get()

    fun <T> withInstance(instanceId: String, block: () -> T): T {
        val entry = entries.compute(instanceId) { _, current ->
            (current ?: LockEntry()).also { it.users.incrementAndGet() }
        }!!
        return try {
            entry.lock.withLock(block)
        } finally {
            if (entry.users.decrementAndGet() == 0) {
                entries.remove(instanceId, entry)
            }
        }
    }

    internal fun trackedLockCount(): Int = entries.size
}
