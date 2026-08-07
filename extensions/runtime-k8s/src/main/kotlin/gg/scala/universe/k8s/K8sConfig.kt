package gg.scala.universe.k8s

/**
 * Configuration for the Kubernetes runtime extension.
 */
data class K8sConfig(
    val factoryName: String = "kube",
    val namespace: String = "default",
    val image: String = "azul-zulu:21-jdk-alpine",
    val imagePullPolicy: String = "IfNotPresent",
    val workingDir: String = "/app",
    val restartPolicy: String = "Never",
    val serviceAccount: String? = null,
    val nodeSelector: Map<String, String> = emptyMap(),
    val tolerations: List<K8sTolerationConfig> = emptyList(),
    val env: Map<String, String> = emptyMap(),
    val labels: Map<String, String> = emptyMap(),
    val annotations: Map<String, String> = emptyMap(),
    val volumes: List<K8sVolumeConfig> = emptyList(),
    val volumeMounts: List<K8sVolumeMountConfig> = emptyList(),
    val kubeConfigPath: String? = null,
    val masterUrl: String? = null,
    val timeoutSeconds: Int = 30,
    /**
     * Host filesystem path that corresponds to the Universe data directory.
     * Required when Universe itself runs inside a Docker container, because
     * K8s hostPath volumes are resolved on the host filesystem.
     *
     * Example: Universe container mounts `./data:/data` via docker-compose,
     * and compose.yaml lives at `/opt/universe`. Then set:
     * hostDataPath = "/opt/universe/data"
     */
    val hostDataPath: String? = null,

    /**
     * When true and hostDataPath is null (cloud mode), the K8s extension
     * auto-generates an init container that downloads templates from S3
     * before the main container starts.
     *
     * Requires the S3 storage extension to be enabled so credentials can
     * be read from `./extensions/s3/config.json`.
     */
    val s3TemplateInit: Boolean = true,

    /**
     * Container image used for the S3 template init container.
     * Must provide `aws` CLI and `unzip`.
     */
    val s3InitImage: String = "amazon/aws-cli:latest",

    /**
     * S3 bucket override. If null, the extension reads the bucket from
     * the S3 extension's config (`./extensions/s3/config.json`).
     */
    val s3Bucket: String? = null,

    /**
     * S3 prefix override for template keys. If null, uses the S3
     * extension's configured prefix (defaults to `templates/`).
     */
    val s3Prefix: String? = null,

    /**
     * Service configuration for per-instance Kubernetes Services.
     * When enabled, a Service is created alongside each Pod to provide
     * stable DNS resolution and in-cluster connectivity.
     */
    val service: K8sServiceConfig = K8sServiceConfig()
)

/**
 * Configuration for per-instance Kubernetes Services created by the K8s runtime.
 */
data class K8sServiceConfig(
    /** Whether to create a Service for each instance. */
    val enabled: Boolean = true,

    /** Service type. Defaults to ClusterIP; use NodePort or LoadBalancer for external access. */
    val type: String = "ClusterIP",

    /** When null, K8s assigns an IP. Set to "None" for headless (DNS-only) services. */
    val clusterIP: String? = "None",

    /** Extra labels merged into the Service metadata. */
    val labels: Map<String, String> = emptyMap(),

    /** Extra annotations merged into the Service metadata. */
    val annotations: Map<String, String> = emptyMap(),

    /**
     * Whether to set the Pod as the Service's ownerReference so K8s
     * garbage-collects the Service when the Pod is deleted.
     */
    val ownerReference: Boolean = true,

    /**
     * Whether to clean up orphaned services (services without a matching pod)
     * when the K8s runtime provider initializes.
     */
    val cleanupOrphans: Boolean = true
)

data class K8sTolerationConfig(
    val key: String,
    val operator: String = "Equal",
    val value: String? = null,
    val effect: String = "NoSchedule"
)

data class K8sVolumeConfig(
    val name: String,
    val hostPath: String? = null,
    val emptyDir: Boolean = false,
    val configMapName: String? = null,
    val secretName: String? = null,
    val claimName: String? = null
)

data class K8sVolumeMountConfig(
    val name: String,
    val mountPath: String,
    val readOnly: Boolean = false
)
