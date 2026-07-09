package gg.scala.universe.api.routing

import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.service.ConfigValidation
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.auth.authenticate
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

fun Application.configureConfigurationRoutes(clusterStateService: ClusterStateService) {
    routing {
        authenticate("protected") {
            route("/api/configurations") {
                get {
                    val configs = clusterStateService.configurations.values
                    call.respond(HttpStatusCode.OK, configs)
                }

                get("/{name}") {
                    val name = call.parameters["name"]
                        ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing configuration name"))

                    val config = clusterStateService.getConfiguration(name)
                        ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Configuration not found"))

                    call.respond(HttpStatusCode.OK, config)
                }

                put("/{name}") {
                    val name = call.parameters["name"]
                        ?: return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing configuration name"))

                    val config = call.receive<gg.scala.universe.schema.Configuration>().copy(name = name)
                    ConfigValidation.warnings(config).forEach { log(it, LogLevel.WARNING) }
                    clusterStateService.putConfiguration(config)
                    call.respond(HttpStatusCode.NoContent)
                }

                delete("/{name}") {
                    val name = call.parameters["name"]
                        ?: return@delete call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing configuration name"))

                    clusterStateService.configurations.remove(name)
                    call.respond(HttpStatusCode.NoContent)
                }
            }
        }
    }
}
