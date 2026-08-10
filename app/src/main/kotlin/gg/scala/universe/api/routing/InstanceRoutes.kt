package gg.scala.universe.api.routing

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cluster.Member
import gg.scala.universe.api.plugins.ApiKeyCache
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.nodeName
import gg.scala.universe.hz.task.TaskDispatcher
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.service.InstanceCreationService
import gg.scala.universe.service.InstanceLifecyclePolicy
import gg.scala.universe.service.LifecycleRequestDecision
import gg.scala.universe.service.LifecycleTarget
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.auth.authenticate
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.patch
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSocketServerSession
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.delay
import java.nio.file.Paths
import kotlin.time.Duration.Companion.seconds

fun Application.configureInstanceRoutes(
    clusterStateService: ClusterStateService,
    hazelcastInstance: HazelcastInstance,
    taskDispatcher: TaskDispatcher,
    instanceCreationService: InstanceCreationService,
    apiKeyCache: ApiKeyCache,
    runtimeRegistry: RuntimeRegistry
) {
    routing {
        route("/api/instances") {
            // Protected: list all and create
            authenticate("protected") {
                get {
                    val instances = clusterStateService.getVisibleInstances().map { it.toExternalApiView() }
                    call.respond(HttpStatusCode.OK, instances)
                }

                post {
                    val request = call.receive<CreateInstanceRequest>()
                    val configuration = clusterStateService.getConfiguration(request.configurationName)
                        ?: return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Configuration not found"))

                    val instanceInfo = instanceCreationService.createInstance(configuration)
                        ?: return@post call.respond(
                            HttpStatusCode.ServiceUnavailable,
                            mapOf("error" to "No node has enough resources for this configuration")
                        )

                    call.respond(HttpStatusCode.Created, instanceInfo.toExternalApiView())
                }
            }

            // All instance endpoints require ALL permission
            authenticate("protected") {
                get("/{id}") {
                    val id = call.parameters["id"]
                        ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val instance = clusterStateService.getInstance(id)
                        ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    call.respond(HttpStatusCode.OK, instance.toExternalApiView())
                }

                get("/{id}/logs") {
                    val id = call.parameters["id"]
                        ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val instance = clusterStateService.getInstance(id)
                        ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    val lines = call.request.queryParameters["lines"]?.toIntOrNull() ?: 100
                    val runtimeProvider = runtimeRegistry.get(instance.runtime)
                        ?: runtimeRegistry.getAll().values.firstOrNull()

                    if (runtimeProvider == null) {
                        return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Runtime provider not available"))
                    }

                    val logLines = runtimeProvider.getLogs(id, lines)
                    if (logLines.isEmpty()) {
                        return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "No logs available for this instance"))
                    }

                    call.respond(HttpStatusCode.OK, mapOf(
                        "instanceId" to id,
                        "lines" to logLines,
                        "totalLines" to logLines.size,
                        "requestedLines" to lines
                    ))
                }

                webSocket("/{id}/live-log") {
                    val id = call.parameters["id"]
                    if (id == null) {
                        close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, "Missing instance ID"))
                        return@webSocket
                    }

                    val instance = clusterStateService.getInstance(id)
                    if (instance == null) {
                        close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, "Instance not found"))
                        return@webSocket
                    }

                    val runtimeProvider = runtimeRegistry.get(instance.runtime)
                        ?: runtimeRegistry.getAll().values.firstOrNull()

                    if (runtimeProvider == null) {
                        outgoing.send(Frame.Text("Runtime provider not available"))
                        close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, "Runtime provider not available"))
                        return@webSocket
                    }

                    // Poll for new log lines every 500ms
                    var lastLineCount = 0
                    try {
                        while (true) {
                            val logLines = runtimeProvider.getLogs(id, 1000)
                            if (logLines.size > lastLineCount) {
                                val newLines = logLines.drop(lastLineCount)
                                newLines.forEach { line ->
                                    outgoing.send(Frame.Text(line))
                                }
                                lastLineCount = logLines.size
                            }
                            delay(500)
                        }
                    } catch (_: Exception) {
                        // Client disconnected
                    }
                }

                put("/{id}/state") {
                    val id = call.parameters["id"]
                        ?: return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val request = call.receive<UpdateStateRequest>()
                    val instance = clusterStateService.getInstance(id)
                        ?: return@put call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    val newState = try {
                        InstanceState.valueOf(request.state)
                    } catch (e: IllegalArgumentException) {
                        return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid state"))
                    }

                    if (newState == InstanceState.STOPPING) {
                        return@put call.respond(
                            HttpStatusCode.BadRequest,
                            mapOf("error" to "STOPPING is reserved for master-side orchestration")
                        )
                    }

                    val updated = instance.copy(
                        state = newState,
                        lastHeartbeat = request.lastHeartbeat ?: System.currentTimeMillis()
                    )
                    clusterStateService.putInstance(updated)

                    call.respond(HttpStatusCode.OK, updated.toExternalApiView())
                }

                delete("/{id}") {
                    val id = call.parameters["id"]
                        ?: return@delete call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val instance = clusterStateService.getInstance(id)
                        ?: return@delete call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    if (InstanceLifecyclePolicy.evaluateRequest(instance.state, LifecycleTarget.STOP) == LifecycleRequestDecision.ACCEPTED_NOOP) {
                        return@delete call.respond(HttpStatusCode.Accepted, mapOf("message" to "Instance $id is already stopping"))
                    }

                    val member = hazelcastInstance.cluster.members.firstOrNull {
                        it.uuid.toString() == instance.wrapperNodeId
                    } ?: hazelcastInstance.cluster.localMember

                    taskDispatcher.dispatchStop(id, member)
                    clusterStateService.updateInstanceState(id, InstanceState.STOPPED)

                    call.respond(HttpStatusCode.OK, mapOf("message" to "Instance $id stopped"))
                }

                patch("/{id}/lifecycle") {
                    val id = call.parameters["id"]
                        ?: return@patch call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val instance = clusterStateService.getInstance(id)
                        ?: return@patch call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    val target = call.request.queryParameters["target"]
                        ?: return@patch call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing 'target' query parameter"))

                    val lifecycleTarget = when (target.lowercase()) {
                        "start" -> LifecycleTarget.START
                        "stop" -> LifecycleTarget.STOP
                        "restart" -> LifecycleTarget.RESTART
                        else -> return@patch call.respond(
                            HttpStatusCode.BadRequest,
                            mapOf("error" to "Invalid lifecycle target. Supported: start, stop, restart")
                        )
                    }

                    when (InstanceLifecyclePolicy.evaluateRequest(instance.state, lifecycleTarget)) {
                        LifecycleRequestDecision.ACCEPTED_NOOP -> {
                            return@patch call.respond(
                                HttpStatusCode.Accepted,
                                mapOf("message" to "Instance $id is already stopping")
                            )
                        }
                        LifecycleRequestDecision.CONFLICT -> {
                            return@patch call.respond(
                                HttpStatusCode.Conflict,
                                mapOf("error" to "Instance $id is stopping and cannot be ${target.lowercase()}ed")
                            )
                        }
                        LifecycleRequestDecision.DISPATCH -> Unit
                    }

                    val member = hazelcastInstance.cluster.members.firstOrNull {
                        it.uuid.toString() == instance.wrapperNodeId
                    } ?: hazelcastInstance.cluster.localMember

                    when (lifecycleTarget) {
                        LifecycleTarget.START -> {
                            val config = clusterStateService.getConfiguration(instance.configurationName)
                                ?: return@patch call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Configuration not found"))
                            val newInstance = instanceCreationService.createInstance(config)
                                ?: return@patch call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "No node has enough resources"))
                            call.respond(HttpStatusCode.OK, mapOf("message" to "Instance started", "instance" to newInstance.toExternalApiView()))
                        }
                        LifecycleTarget.STOP -> {
                            taskDispatcher.dispatchStop(id, member)
                            clusterStateService.updateInstanceState(id, InstanceState.STOPPED)
                            call.respond(HttpStatusCode.OK, mapOf("message" to "Instance $id stopped"))
                        }
                        LifecycleTarget.RESTART -> {
                            taskDispatcher.dispatchStop(id, member)
                            clusterStateService.updateInstanceState(id, InstanceState.STOPPED)
                            val config = clusterStateService.getConfiguration(instance.configurationName)
                                ?: return@patch call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Configuration not found"))
                            val newInstance = instanceCreationService.createInstance(config)
                                ?: return@patch call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "No node has enough resources"))
                            call.respond(HttpStatusCode.OK, mapOf("message" to "Instance restarted", "instance" to newInstance.toExternalApiView()))
                        }
                    }
                }

                post("/{id}/execute") {
                    val id = call.parameters["id"]
                        ?: return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing instance ID"))

                    val instance = clusterStateService.getInstance(id)
                        ?: return@post call.respond(HttpStatusCode.NotFound, mapOf("error" to "Instance not found"))

                    val request = call.receive<ExecuteOnInstanceRequest>()
                    val member = hazelcastInstance.cluster.members.firstOrNull {
                        it.uuid.toString() == instance.wrapperNodeId
                    } ?: hazelcastInstance.cluster.localMember

                    taskDispatcher.dispatchExecute(id, request.command, member)
                    call.respond(HttpStatusCode.OK, mapOf("message" to "Command sent to $id"))
                }
            }
        }
    }
}

data class CreateInstanceRequest(val configurationName: String)
data class UpdateStateRequest(val state: String, val lastHeartbeat: Long? = null)
data class ExecuteOnInstanceRequest(val command: String)

internal fun InstanceInfo.toExternalApiView(): InstanceInfo =
    if (state == InstanceState.STOPPING) copy(state = InstanceState.ONLINE) else this
