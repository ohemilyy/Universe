package gg.scala.universe.artifacts

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * @author Luna
 * @date August 2, 2026
 */
class ArtifactHttpClient(
    private val requestTimeout: Duration
) {
    private val client = HttpClient.newBuilder()
        .connectTimeout(requestTimeout)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build()

    suspend fun getText(uri: URI, headers: Map<String, String>): String {
        val response = client.sendAsync(request(uri, headers), HttpResponse.BodyHandlers.ofString()).await()
        requireSuccessful(uri, response.statusCode())
        return response.body()
    }

    suspend fun download(uri: URI, headers: Map<String, String>, destination: Path) {
        val response = client.sendAsync(request(uri, headers), HttpResponse.BodyHandlers.ofInputStream()).await()
        requireSuccessful(uri, response.statusCode())
        withContext(Dispatchers.IO) {
            response.body().use { input -> Files.newOutputStream(destination).use(input::copyTo) }
        }
    }

    private fun request(uri: URI, headers: Map<String, String>): HttpRequest {
        val builder = HttpRequest.newBuilder(uri).GET().timeout(requestTimeout)
        headers.forEach(builder::header)
        return builder.build()
    }

    private fun requireSuccessful(uri: URI, statusCode: Int) {
        if (statusCode !in 200..299) {
            throw IllegalStateException("GET $uri returned HTTP $statusCode")
        }
    }
}
