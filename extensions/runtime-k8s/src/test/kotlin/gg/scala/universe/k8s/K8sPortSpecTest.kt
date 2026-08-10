package gg.scala.universe.k8s

import gg.scala.universe.schema.AdditionalPort
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodSpecBuilder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class K8sPortSpecTest {

    @Test
    fun `bind address is applied to primary and additional ports`() {
        val ports = K8sPortSpec.build(
            primaryPort = 25565,
            additionalPorts = listOf(AdditionalPort(8080), AdditionalPort(40004, "UDP", "voice")),
            configuredBindAddress = " 127.0.0.1 "
        )
        val pod = PodBuilder()
            .withSpec(
                PodSpecBuilder()
                    .withContainers(ContainerBuilder().withName("main").withPorts(ports).build())
                    .build()
            )
            .build()

        assertEquals(
            listOf("127.0.0.1", "127.0.0.1", "127.0.0.1"),
            pod.spec.containers.single().ports.map { it.hostIP }
        )
    }

    @Test
    fun `wildcard-equivalent settings omit hostIP`() {
        listOf<String?>(null, "", "   ", "0.0.0.0").forEach { configured ->
            val ports = K8sPortSpec.build(25565, listOf(AdditionalPort(8080)), configured)

            assertTrue(ports.all { it.hostIP == null })
        }
    }
}
