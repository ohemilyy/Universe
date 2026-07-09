package gg.scala.universe.api.routing

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cluster.Member
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.nodeName
import gg.scala.universe.hz.ownsInstance
import gg.scala.universe.hz.stableNodeId
import gg.scala.universe.hz.task.TaskDispatcher
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.auth.authenticate
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

fun Application.configureClusterRoutes(
    clusterStateService: ClusterStateService,
    hazelcastInstance: HazelcastInstance,
    taskDispatcher: TaskDispatcher
) {
    routing {
        authenticate("protected") {
        route("/api/cluster") {
            get("/nodes") {
                val nodes = hazelcastInstance.cluster.members.map { member ->
                    val nodeId = member.stableNodeId()
                    val resources = clusterStateService.getNodeResources(nodeId)
                    mapOf(
                        "id" to nodeId,
                        "name" to member.nodeName(),
                        "address" to member.address.host,
                        "port" to member.address.port,
                        "local" to (member == hazelcastInstance.cluster.localMember),
                        "resources" to mapOf(
                            "usedRamMB" to resources.usedRamMB,
                            "usedCpu" to resources.usedCpu
                        )
                    )
                }
                call.respond(HttpStatusCode.OK, nodes)
            }

            get("/nodes/{id}") {
                val nodeId = call.parameters["id"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing node ID"))

                val member = hazelcastInstance.cluster.members.firstOrNull {
                    it.ownsInstance(nodeId)
                } ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Node not found"))

                val resources = clusterStateService.getNodeResources(nodeId)
                val instances = clusterStateService.getInstancesByWrapper(nodeId)

                call.respond(HttpStatusCode.OK, mapOf(
                    "id" to nodeId,
                    "name" to member.nodeName(),
                    "address" to member.address.host,
                    "port" to member.address.port,
                    "local" to (member == hazelcastInstance.cluster.localMember),
                    "resources" to mapOf(
                        "usedRamMB" to resources.usedRamMB,
                        "usedCpu" to resources.usedCpu
                    ),
                    "instances" to instances.map { it.id }
                ))
            }

            post("/nodes/{id}/command") {
                val nodeId = call.parameters["id"]
                    ?: return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Missing node ID"))

                val member = hazelcastInstance.cluster.members.firstOrNull {
                    it.ownsInstance(nodeId)
                } ?: return@post call.respond(HttpStatusCode.NotFound, mapOf("error" to "Node not found"))

                val request = call.receive<ClusterCommandRequest>()

                // Dispatch as a no-op task for now; remote command execution via executor would need a custom callable
                // For now, respond with a note that remote console execution is local-only.
                if (member == hazelcastInstance.cluster.localMember) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Use /api/commands/execute for local node"))
                    return@post
                }

                // TODO: Implement remote command dispatch via Hazelcast executor
                call.respond(HttpStatusCode.NotImplemented, mapOf("error" to "Remote console command execution is not yet implemented"))
            }
        }
        }
    }
}

data class ClusterCommandRequest(val command: String)
