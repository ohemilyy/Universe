package gg.scala.universe.k8s

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.StatusBuilder
import io.fabric8.kubernetes.client.NamespacedKubernetesClient
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class K8sRuntimeLifecycleTest {
    private lateinit var server: KubernetesMockServer
    private lateinit var client: NamespacedKubernetesClient
    private var destroyed = false

    @BeforeTest
    fun setUp() {
        server = KubernetesMockServer(false)
        server.init()
        client = server.createClient()
        destroyed = false
    }

    @AfterTest
    fun tearDown() {
        client.close()
        if (!destroyed) server.destroy()
    }

    @Test
    fun `provider restart discovers surviving labeled pod for running and stop`() {
        val instanceId = "abc123"
        val pod = PodBuilder()
                .withNewMetadata()
                    .withName("universe-$instanceId")
                    .withNamespace("test")
                    .addToLabels("app", "universe")
                    .addToLabels("universe-instance-id", instanceId)
                .endMetadata()
                .withNewStatus().withPhase("Running").endStatus()
                .build()
        val podPath = "/api/v1/namespaces/test/pods/universe-$instanceId"
        server.expect().get().withPath(podPath).andReturn(200, pod).times(3)
        server.expect().delete().withPath(podPath).andReturn(
            200, StatusBuilder().withStatus("Success").build()
        ).once()
        server.expect().get().withPath(podPath).andReturn(404, "").always()

        val restartedProvider = K8sRuntimeProvider(
            K8sConfig(namespace = "test"), client
        )

        assertTrue(restartedProvider.isRunning(instanceId))
        restartedProvider.stop(instanceId)
    }

    @Test
    fun `deletion failure is surfaced to lifecycle caller`() {
        val instanceId = "abc123"
        val pod = PodBuilder()
                .withNewMetadata()
                    .withName("universe-$instanceId")
                    .withNamespace("test")
                    .addToLabels("app", "universe")
                    .addToLabels("universe-instance-id", instanceId)
                .endMetadata()
                .withNewStatus().withPhase("Running").endStatus()
                .build()
        val podPath = "/api/v1/namespaces/test/pods/universe-$instanceId"
        server.expect().get().withPath(podPath).andReturn(200, pod).once()
        server.expect().delete().withPath(podPath).andReturn(
            500, StatusBuilder().withStatus("Failure").withMessage("delete failed").build()
        ).always()
        val provider = K8sRuntimeProvider(K8sConfig(namespace = "test"), client)

        assertFailsWith<IllegalStateException> { provider.stop(instanceId) }
    }
}
