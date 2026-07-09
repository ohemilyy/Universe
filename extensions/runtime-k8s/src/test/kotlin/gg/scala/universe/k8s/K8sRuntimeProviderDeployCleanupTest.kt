package gg.scala.universe.k8s

import gg.scala.universe.schema.Configuration
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.file.Files

@EnableKubernetesMockClient(https = false, crud = true)
class K8sRuntimeProviderDeployCleanupTest {

    private lateinit var server: KubernetesMockServer
    private lateinit var client: KubernetesClient

    private fun provider(): K8sRuntimeProvider {
        val config = K8sConfig(
            namespace = "universe",
            masterUrl = server.url("/"),
            timeoutSeconds = 1,
            service = K8sServiceConfig(enabled = false)
        )
        return K8sRuntimeProvider(config, clusterName = "test", nodeId = "node-1")
    }

    @Test
    fun `repeated failed deploys leave no pods behind`() {
        val provider = provider()
        val config = Configuration(name = "survival", runtime = "kube", ramMB = 1024, cpu = 100)
        val workDir = Files.createTempDirectory("universe-k8s-test")

        val attempts = 3
        repeat(attempts) { i ->
            assertThrows(RuntimeException::class.java) {
                provider.start(
                    instanceId = "inst-$i",
                    workingDir = workDir,
                    port = 30000 + i,
                    command = "java -jar server.jar",
                    ramMB = 1024,
                    cpu = 100,
                    configuration = config,
                    environmentVariables = null
                )
            }
        }

        val pods = client.pods().inNamespace("universe").list().items
        assertEquals(0, pods.size, "failed deploys leaked pods: ${pods.map { it.metadata.name }}")
    }

    @Test
    fun `a single failed deploy deletes the pod it created`() {
        val provider = provider()
        val config = Configuration(name = "lobby", runtime = "kube", ramMB = 512, cpu = 50)
        val workDir = Files.createTempDirectory("universe-k8s-test")

        assertThrows(RuntimeException::class.java) {
            provider.start(
                instanceId = "solo-1",
                workingDir = workDir,
                port = 31000,
                command = "java -jar server.jar",
                ramMB = 512,
                cpu = 50,
                configuration = config,
                environmentVariables = null
            )
        }

        assertEquals(null, client.pods().inNamespace("universe").withName("universe-solo-1").get())
    }
}
