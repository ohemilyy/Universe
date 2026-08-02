package gg.scala.universe.artifacts

import com.google.gson.JsonObject
import gg.scala.universe.util.json.Serializers
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.nio.file.Path
import java.util.Base64

/**
 * @author Luna
 * @date August 2, 2026
 */
class ArtifactSourceResolver(
    private val httpClient: ArtifactHttpClient
) {
    data class ArtifactRequest(
        val id: String,
        val uri: URI,
        val headers: Map<String, String>,
        val targetPath: String
    )

    suspend fun resolve(source: ForgejoArtifactSource): ArtifactRequest {
        require(source.owner.isNotBlank() && source.repository.isNotBlank()) {
            "Forgejo source '${source.id}' requires owner and repository"
        }

        val headers = source.token?.takeIf(String::isNotBlank)
            ?.let { mapOf("Authorization" to "token $it") }
            ?: emptyMap()
        val apiRoot = "${source.serverUrl.trimEnd('/')}/api/v1/repos/${encode(source.owner)}/${encode(source.repository)}/releases"
        val releases = if (source.releaseTag.isNullOrBlank()) {
            Serializers.GSON.fromJson(
                httpClient.getText(URI.create("$apiRoot?limit=50"), headers),
                Array<JsonObject>::class.java
            ).asList()
        } else {
            listOf(
                Serializers.GSON.fromJson(
                    httpClient.getText(URI.create("$apiRoot/tags/${encode(source.releaseTag)}"), headers),
                    JsonObject::class.java
                )
            )
        }

        val release = releases.firstOrNull {
            !it.boolean("draft") &&
                (source.includePrereleases || !it.boolean("prerelease")) &&
                (source.branch.isNullOrBlank() || it.string("target_commitish") == source.branch)
        } ?: throw IllegalStateException("Forgejo source '${source.id}' has no matching published release")

        val matcher = FileSystems.getDefault().getPathMatcher("glob:${source.assetPattern}")
        val asset = release.getAsJsonArray("assets")
            ?.map { it.asJsonObject }
            ?.firstOrNull { matcher.matches(Path.of(it.string("name"))) }
            ?: throw IllegalStateException("Forgejo source '${source.id}' has no asset matching '${source.assetPattern}'")
        val downloadUrl = asset.string("browser_download_url")
            ?: throw IllegalStateException("Forgejo source '${source.id}' returned an asset without a download URL")

        return ArtifactRequest(source.id, URI.create(downloadUrl), headers, source.targetPath)
    }

    fun resolve(source: ArtifactoryArtifactSource): ArtifactRequest {
        require(source.serverUrl.isNotBlank() && source.repository.isNotBlank() && source.artifactPath.isNotBlank()) {
            "Artifactory source '${source.id}' requires serverUrl, repository, and artifactPath"
        }
        val artifactPath = substituteBranch(source.artifactPath, source.branch, source.id)
        val serverUrl = source.serverUrl.trimEnd('/').let {
            if (it.endsWith("/artifactory")) it else "$it/artifactory"
        }
        val uri = URI.create("$serverUrl/${encode(source.repository)}/${encodePath(artifactPath)}")
        return ArtifactRequest(source.id, uri, authentication(source.accessToken, source.username, source.password), source.targetPath)
    }

    fun resolve(source: TeamCityArtifactSource): ArtifactRequest {
        require(source.serverUrl.isNotBlank() && source.buildTypeId.isNotBlank() && source.artifactPath.isNotBlank()) {
            "TeamCity source '${source.id}' requires serverUrl, buildTypeId, and artifactPath"
        }
        val branchQuery = source.branch?.takeIf(String::isNotBlank)?.let { "?branch=${encode(it)}" }.orEmpty()
        val uri = URI.create(
            "${source.serverUrl.trimEnd('/')}/repository/download/${encode(source.buildTypeId)}/.lastSuccessful/${encodePath(source.artifactPath)}$branchQuery"
        )
        return ArtifactRequest(source.id, uri, authentication(source.accessToken, source.username, source.password), source.targetPath)
    }

    private fun authentication(token: String?, username: String?, password: String?): Map<String, String> {
        if (!token.isNullOrBlank()) return mapOf("Authorization" to "Bearer $token")
        if (!username.isNullOrBlank() && password != null) {
            val credentials = Base64.getEncoder().encodeToString("$username:$password".toByteArray(StandardCharsets.UTF_8))
            return mapOf("Authorization" to "Basic $credentials")
        }
        return emptyMap()
    }

    private fun substituteBranch(path: String, branch: String?, sourceId: String): String {
        if ("{branch}" !in path) return path
        require(!branch.isNullOrBlank()) { "Artifactory source '$sourceId' uses {branch} without configuring branch" }
        return path.replace("{branch}", branch)
    }

    private fun encodePath(path: String): String = path.split('/').joinToString("/") { encode(it) }

    private fun encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")

    private fun JsonObject.string(name: String): String? = get(name)?.takeUnless { it.isJsonNull }?.asString

    private fun JsonObject.boolean(name: String): Boolean = get(name)?.takeUnless { it.isJsonNull }?.asBoolean ?: false
}
