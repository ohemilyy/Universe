package gg.scala.universe.docker

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.StopContainerCmd
import com.github.dockerjava.api.command.InspectContainerCmd
import com.github.dockerjava.api.command.InspectContainerResponse
import com.github.dockerjava.api.command.ListContainersCmd
import com.github.dockerjava.api.command.RemoveContainerCmd
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model.Container
import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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

    @Test
    fun `unknown docker running state is not treated as stopped`() {
        val response = InspectContainerResponse()
        val state = response.ContainerState()
        InspectContainerResponse::class.java.getDeclaredField("state").apply {
            isAccessible = true
            set(response, state)
        }
        lateinit var inspectCommand: InspectContainerCmd
        inspectCommand = proxy { method ->
            when (method) {
                "exec" -> response
                else -> inspectCommand
            }
        }
        val client = proxy<DockerClient> { method ->
            when (method) {
                "inspectContainerCmd" -> inspectCommand
                else -> error("Unexpected Docker call: $method")
            }
        }
        val provider = DockerRuntimeProvider(DockerConfig(), client)
        provider.rememberContainerForTest("old001", "container-1")

        assertFailsWith<IllegalStateException> { provider.isRunning("old001") }
    }

    @Test
    fun `not found mapped id is rediscovered by canonical name before stop succeeds`() {
        val canonical = Container()
        setField(canonical, "id", "canonical-2")
        setField(canonical, "names", arrayOf("/universe-old001"))
        val listCalls = AtomicInteger()
        lateinit var listCommand: ListContainersCmd
        listCommand = proxy { method ->
            when (method) {
                "withShowAll", "withNameFilter" -> listCommand
                "exec" -> if (listCalls.getAndIncrement() == 0) listOf(canonical) else emptyList<Container>()
                else -> null
            }
        }
        fun stopCommand(missing: Boolean): StopContainerCmd {
            lateinit var command: StopContainerCmd
            command = proxy { method ->
                when (method) {
                    "withTimeout" -> command
                    "exec" -> if (missing) throw NotFoundException("gone") else null
                    else -> null
                }
            }
            return command
        }
        fun removeCommand(missing: Boolean): RemoveContainerCmd {
            lateinit var command: RemoveContainerCmd
            command = proxy { method ->
                when (method) {
                    "withForce" -> command
                    "exec" -> if (missing) throw NotFoundException("gone") else null
                    else -> null
                }
            }
            return command
        }
        val client = Proxy.newProxyInstance(
            DockerClient::class.java.classLoader,
            arrayOf(DockerClient::class.java)
        ) { _, method, args ->
            when (method.name) {
                "stopContainerCmd" -> stopCommand(args[0] == "stale-1")
                "removeContainerCmd" -> removeCommand(args[0] == "stale-1")
                "listContainersCmd" -> listCommand
                else -> error("Unexpected Docker call: ${method.name}")
            }
        } as DockerClient
        val provider = DockerRuntimeProvider(DockerConfig(), client)
        provider.rememberContainerForTest("old001", "stale-1")

        provider.stop("old001")

        assertTrue(listCalls.get() >= 2)
    }

    private fun setField(target: Any, name: String, value: Any) {
        target.javaClass.getDeclaredField(name).apply {
            isAccessible = true
            set(target, value)
        }
    }

    private inline fun <reified T> proxy(
        crossinline result: (methodName: String) -> Any?
    ): T = Proxy.newProxyInstance(
        T::class.java.classLoader,
        arrayOf(T::class.java)
    ) { _, method, _ -> result(method.name) } as T
}
