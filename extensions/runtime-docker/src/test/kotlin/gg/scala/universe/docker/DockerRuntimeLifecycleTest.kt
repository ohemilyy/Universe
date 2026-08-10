package gg.scala.universe.docker

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.StopContainerCmd
import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DockerRuntimeLifecycleTest {
    @Test
    fun `delete failure throws and retains the container mapping for retry`() {
        val stopAttempts = AtomicInteger()
        lateinit var stopCommand: StopContainerCmd
        stopCommand = proxy { method ->
            when (method) {
                "withTimeout" -> stopCommand
                "exec" -> {
                    stopAttempts.incrementAndGet()
                    error("docker daemon unavailable")
                }
                else -> null
            }
        }
        val client = proxy<DockerClient> { method ->
            when (method) {
                "stopContainerCmd" -> stopCommand
                else -> error("Unexpected Docker call: $method")
            }
        }
        val provider = DockerRuntimeProvider(DockerConfig(), client)
        provider.rememberContainerForTest("old001", "container-1")

        assertFailsWith<IllegalStateException> { provider.stop("old001") }
        assertFailsWith<IllegalStateException> { provider.stop("old001") }

        assertEquals(2, stopAttempts.get())
    }

    private inline fun <reified T> proxy(
        crossinline result: (methodName: String) -> Any?
    ): T = Proxy.newProxyInstance(
        T::class.java.classLoader,
        arrayOf(T::class.java)
    ) { _, method, _ -> result(method.name) } as T
}
