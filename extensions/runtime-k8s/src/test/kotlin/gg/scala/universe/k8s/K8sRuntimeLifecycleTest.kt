package gg.scala.universe.k8s

import gg.scala.universe.runtime.RuntimeResourceState
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodListBuilder
import io.fabric8.kubernetes.api.model.StatusBuilder
import io.fabric8.kubernetes.api.model.ServiceBuilder
import io.fabric8.kubernetes.client.NamespacedKubernetesClient
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import java.nio.file.Path
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertEquals
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
        val provider = K8sRuntimeProvider(K8sConfig(namespace = "test", timeoutSeconds = 1), client)

        assertFailsWith<IllegalStateException> { provider.stop(instanceId) }
    }

    @Test
    fun `recovery distinguishes pending and terminal pods from running`() {
        val pendingId = "pending1"
        val pending = PodBuilder()
            .withNewMetadata().withName("universe-$pendingId").withNamespace("test").endMetadata()
            .withNewStatus().withPhase("Pending").endStatus()
            .build()
        val path = "/api/v1/namespaces/test/pods/universe-$pendingId"
        server.expect().get().withPath(path).andReturn(200, pending).times(2)
        val provider = K8sRuntimeProvider(K8sConfig(namespace = "test"), client)

        assertEquals(
            RuntimeResourceState.PRESENT_TRANSITIONAL,
            provider.inspectRecovered(pendingId, null, Path.of("work"))
        )

        val terminalId = "failed1"
        val terminal = PodBuilder()
            .withNewMetadata().withName("universe-$terminalId").withNamespace("test").endMetadata()
            .withNewStatus().withPhase("Failed").endStatus()
            .build()
        val terminalPath = "/api/v1/namespaces/test/pods/universe-$terminalId"
        server.expect().get().withPath(terminalPath).andReturn(200, terminal).times(2)
        assertEquals(
            RuntimeResourceState.TERMINAL,
            provider.inspectRecovered(terminalId, null, Path.of("work"))
        )
    }

    @Test
    fun `service deletion failure is surfaced after pod absence is confirmed`() {
        val instanceId = "svc123"
        val pod = PodBuilder()
            .withNewMetadata().withName("universe-$instanceId").withNamespace("test").endMetadata()
            .withNewStatus().withPhase("Running").endStatus()
            .build()
        val podPath = "/api/v1/namespaces/test/pods/universe-$instanceId"
        val servicePath = "/api/v1/namespaces/test/services/universe-$instanceId"
        server.expect().get().withPath(podPath).andReturn(200, pod).once()
        server.expect().delete().withPath(podPath).andReturn(200, StatusBuilder().withStatus("Success").build()).once()
        server.expect().get().withPath(podPath).andReturn(404, "").always()
        val service = ServiceBuilder()
            .withNewMetadata().withName("universe-$instanceId").withNamespace("test").endMetadata()
            .build()
        server.expect().delete().withPath(servicePath).andReturn(
            200,
            StatusBuilder().withStatus("Success").build()
        ).once()
        server.expect().get().withPath(servicePath).andReturn(200, service).always()
        val provider = K8sRuntimeProvider(K8sConfig(namespace = "test", timeoutSeconds = 1), client)

        assertFailsWith<IllegalStateException> { provider.stop(instanceId) }
    }

    @Test
    fun `orphan service is cleanup required until confirmed deletion`() {
        val instanceId = "orphan1"
        val podPath = "/api/v1/namespaces/test/pods/universe-$instanceId"
        val podListPath = "/api/v1/namespaces/test/pods?labelSelector=app%3Duniverse%2Cuniverse-instance-id%3D$instanceId"
        val servicePath = "/api/v1/namespaces/test/services/universe-$instanceId"
        val service = ServiceBuilder()
                .withNewMetadata()
                    .withName("universe-$instanceId")
                    .withNamespace("test")
                .endMetadata()
                .build()
        server.expect().get().withPath(podPath).andReturn(404, "").always()
        server.expect().get().withPath(podListPath).andReturn(200, PodListBuilder().build()).always()
        server.expect().get().withPath(servicePath).andReturn(200, service).once()
        server.expect().delete().withPath(servicePath).andReturn(
            200, StatusBuilder().withStatus("Success").build()
        ).once()
        server.expect().get().withPath(servicePath).andReturn(404, "").always()
        val provider = K8sRuntimeProvider(K8sConfig(namespace = "test", timeoutSeconds = 1), client)

        assertEquals(
            RuntimeResourceState.CLEANUP_REQUIRED,
            provider.inspectRecovered(instanceId, null, Path.of("work"))
        )
        provider.stop(instanceId)
        assertEquals(
            RuntimeResourceState.ABSENT,
            provider.inspectRecovered(instanceId, null, Path.of("work"))
        )
    }
}
