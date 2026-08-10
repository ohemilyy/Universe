package gg.scala.universe.service

import com.google.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Serializes every runtime side effect for one instance and gates node shutdown. */
@Singleton
class InstanceLifecycleCoordinator {
    private val entries = ConcurrentHashMap<String, ReentrantLock>()
    private val quiescing = AtomicBoolean()

    fun beginShutdown() {
        quiescing.set(true)
    }

    fun isQuiescing(): Boolean = quiescing.get()

    fun <T> withInstance(instanceId: String, block: () -> T): T {
        val lock = entries.computeIfAbsent(instanceId) { ReentrantLock() }
        return lock.withLock(block)
    }

    internal fun trackedLockCount(): Int = entries.size
}
