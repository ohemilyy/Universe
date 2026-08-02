package gg.scala.universe.artifacts

import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermission
import java.security.MessageDigest

/**
 * @author Luna
 * @date August 2, 2026
 */
class ArtifactDeploymentService(
    private val httpClient: ArtifactHttpClient,
    private val resolver: ArtifactSourceResolver,
    private val root: Path = Path.of(".").toAbsolutePath().normalize()
) {
    suspend fun update(config: ArtifactDeploymentConfig) {
        config.forgejo.filter { it.enabled }.forEach { source -> deploySafely(source.id) { resolver.resolve(source) } }
        config.artifactory.filter { it.enabled }.forEach { source -> deploySafely(source.id) { resolver.resolve(source) } }
        config.teamCity.filter { it.enabled }.forEach { source -> deploySafely(source.id) { resolver.resolve(source) } }
    }

    private suspend fun deploySafely(id: String, request: suspend () -> ArtifactSourceResolver.ArtifactRequest) {
        try {
            deploy(request())
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            log("Artifact deployment '$id' failed: ${exception.message}", LogLevel.WARNING)
        }
    }

    private suspend fun deploy(request: ArtifactSourceResolver.ArtifactRequest) = withContext(Dispatchers.IO) {
        val target = resolveTarget(request.targetPath)
        Files.createDirectories(target.parent)
        val temporary = Files.createTempFile(target.parent, ".${target.fileName}.", ".download")

        try {
            httpClient.download(request.uri, request.headers, temporary)
            if (Files.exists(target) && sha256(target).contentEquals(sha256(temporary))) {
                log("Artifact deployment '${request.id}' is unchanged", LogLevel.DEBUG)
                return@withContext
            }

            val permissions = existingPermissions(target)
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (_: AtomicMoveNotSupportedException) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
            }
            permissions?.let { Files.setPosixFilePermissions(target, it) }
            log("Artifact deployment '${request.id}' replaced $target", LogLevel.SUCCESS)
        } finally {
            Files.deleteIfExists(temporary)
        }
    }

    private fun resolveTarget(configuredPath: String): Path {
        require(configuredPath.isNotBlank()) { "targetPath must not be blank" }
        val relative = Path.of(configuredPath)
        require(!relative.isAbsolute) { "targetPath must be relative to the Universe directory" }
        val target = root.resolve(relative).normalize()
        require(target.startsWith(root) && target != root) { "targetPath must stay inside the Universe directory" }
        return target
    }

    private fun sha256(path: Path): ByteArray {
        val digest = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest()
    }

    private fun existingPermissions(path: Path): Set<PosixFilePermission>? {
        if (!Files.exists(path)) return null
        return runCatching { Files.getPosixFilePermissions(path) }.getOrNull()
    }
}
