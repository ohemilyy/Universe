package gg.scala.universe.docker

data class DockerImageConfig(
    val repository: String = "azul-zulu",
    val tag: String = "21-jdk-alpine",
    val registry: String? = null,
    val platform: String? = null
)

data class DockerVolumeConfig(
    val hostPath: String,
    val containerPath: String,
    val readOnly: Boolean = false
)

data class DockerPortConfig(
    val containerPort: Int,
    val protocol: String = "tcp"
)

data class DockerConfig(
    val factoryName: String = "docker",
    val network: String? = "host",
    val javaImage: DockerImageConfig = DockerImageConfig(),
    val volumes: List<DockerVolumeConfig> = emptyList(),
    val binds: List<String> = emptyList(),
    val exposedPorts: List<DockerPortConfig> = emptyList(),
    val dockerHost: String? = "unix:///var/run/docker.sock",
    val dockerCertPath: String? = null,
    val registryUsername: String? = null,
    val registryEmail: String? = null,
    val registryPassword: String? = null,
    val registryUrl: String? = null,
    val user: String? = null,
    val containerWorkDir: String = "/app",
    val autoRemove: Boolean = true,
    val stopTimeout: Int = 30,
    /**
     * Host filesystem path that corresponds to the Universe data directory.
     * Required when Universe itself runs inside a Docker container, because
     * bind mounts in child containers are resolved by the Docker daemon on
     * the host filesystem.
     *
     * Example: Universe container mounts `./data:/data` via docker-compose,
     * and compose.yaml lives at `/opt/universe`. Then set:
     * hostDataPath = "/opt/universe/data"
     */
    val hostDataPath: String? = null
)
