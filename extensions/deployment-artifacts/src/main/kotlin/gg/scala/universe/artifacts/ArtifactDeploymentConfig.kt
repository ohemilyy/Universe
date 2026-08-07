package gg.scala.universe.artifacts

/**
 * @author Luna
 * @date August 2, 2026
 */
data class ArtifactDeploymentConfig(
    val enabled: Boolean = false,
    val pollIntervalSeconds: Long = 60,
    val requestTimeoutSeconds: Long = 30,
    val forgejo: List<ForgejoArtifactSource> = emptyList(),
    val artifactory: List<ArtifactoryArtifactSource> = emptyList(),
    val teamCity: List<TeamCityArtifactSource> = emptyList()
)

data class ForgejoArtifactSource(
    val id: String = "forgejo-release",
    val enabled: Boolean = true,
    val serverUrl: String = "https://codeberg.org",
    val owner: String = "",
    val repository: String = "",
    val assetPattern: String = "*.jar",
    val targetPath: String = "",
    val branch: String? = null,
    val releaseTag: String? = null,
    val includePrereleases: Boolean = false,
    val token: String? = null
)

data class ArtifactoryArtifactSource(
    val id: String = "artifactory-artifact",
    val enabled: Boolean = true,
    val serverUrl: String = "",
    val repository: String = "",
    val artifactPath: String = "",
    val targetPath: String = "",
    val branch: String? = null,
    val accessToken: String? = null,
    val username: String? = null,
    val password: String? = null
)

data class TeamCityArtifactSource(
    val id: String = "teamcity-artifact",
    val enabled: Boolean = true,
    val serverUrl: String = "",
    val buildTypeId: String = "",
    val artifactPath: String = "",
    val targetPath: String = "",
    val branch: String? = null,
    val accessToken: String? = null,
    val username: String? = null,
    val password: String? = null
)
