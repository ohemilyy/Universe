package gg.scala.universe.service

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.cluster.Member
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.LogLevel
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.stableNodeId
import gg.scala.universe.hz.task.TaskDispatcher
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.runtime.RuntimeResources
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.util.InstanceIdGenerator

/**
 * Centralized service for creating and dispatching instances.
 * Handles resource-aware node selection, InstanceInfo construction,
 * state persistence, and deployment dispatching.
 */
@Singleton
class InstanceCreationService @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val hazelcastInstance: HazelcastInstance,
    private val taskDispatcher: TaskDispatcher,
    private val runtimeRegistry: RuntimeRegistry
) {

    /**
     * Creates a single instance from the given configuration.
     * Performs resource-aware node selection and dispatches the deploy task.
     *
     * @param configuration The configuration to use
     * @param instanceId Optional explicit ID (for static instances). If null, a random ID is generated.
     * @return The created [InstanceInfo] if a suitable node was found, null otherwise.
     */
    fun createInstance(configuration: Configuration, instanceId: String? = null): InstanceInfo? {
        val id = instanceId ?: InstanceIdGenerator.generate()

        val wrapperMember = findBestNode(configuration)
        if (wrapperMember == null) {
            log(
                "No node has enough resources (RAM=${configuration.ramMB}MB, CPU=${configuration.cpu}) " +
                "for instance '$id'.",
                LogLevel.WARNING
            )
            return null
        }

        val instanceInfo = InstanceInfo(
            id = id,
            configurationName = configuration.name,
            wrapperNodeId = wrapperMember.stableNodeId(),
            hostAddress = configuration.hostAddress,
            allocatedPort = 0,
            state = InstanceState.CREATING,
            lastHeartbeat = System.currentTimeMillis(),
            processPid = null,
            allocatedRamMB = configuration.ramMB,
            allocatedCpu = configuration.cpu,
            runtime = configuration.runtime
        )

        clusterStateService.putInstance(instanceInfo)
        taskDispatcher.dispatchDeploy(instanceInfo, wrapperMember)

        return instanceInfo
    }

    /**
     * Finds the cluster node with the most available RAM that can fit the given configuration.
     * Returns null if no node has enough resources.
     */
    fun findBestNode(configuration: Configuration): Member? {
        val allowedNodes = configuration.nodes
        val localMember = hazelcastInstance.cluster.localMember

        // Query the runtime's real node Allocatable once per call (local node only — we can't
        // reach a remote node's runtime synchronously). Lazy so we only pay for it if the
        // local member is actually a candidate.
        val localAllocatable by lazy {
            runtimeRegistry.get(configuration.runtime)?.queryNodeAllocatable()
        }

        val candidates = hazelcastInstance.cluster.members.filter { member ->
            val nodeId = member.getAttribute("nodeId") ?: return@filter false

            // Only consider nodes explicitly listed in the configuration
            if (nodeId !in allowedNodes) {
                return@filter false
            }

            // Prefer the runtime's real Allocatable for the local node so a powerful box isn't
            // falsely reported "full" by drift in Universe's own counter.
            if (member.uuid == localMember.uuid && localAllocatable != null) {
                val alloc = localAllocatable!!
                val requestCpuMillis = RuntimeResources.cpuUnitsToMillicores(configuration.cpu).toLong()
                val fits = RuntimeResources.fits(
                    allocatableCpuMillis = alloc.cpuMillis,
                    allocatableMemMB = alloc.memMB,
                    usedCpuMillis = alloc.usedCpuMillis,
                    usedMemMB = alloc.usedMemMB,
                    requestCpuMillis = requestCpuMillis,
                    requestMemMB = configuration.ramMB.toLong()
                )
                if (!fits) {
                    log(
                        "Node $nodeId cannot fit '${configuration.name}': request ${requestCpuMillis}m/${configuration.ramMB}MB, " +
                        "allocatable ${alloc.cpuMillis}m/${alloc.memMB}MB, already requested ${alloc.usedCpuMillis}m/${alloc.usedMemMB}MB",
                        LogLevel.WARNING
                    )
                }
                return@filter fits
            }

            // Fallback: Universe's per-node tracked limits (static maxRamMB/maxCpu attributes).
            val resources = clusterStateService.getNodeResources(member.stableNodeId())
            val maxRam = member.getAttribute("maxRamMB")?.toIntOrNull() ?: Int.MAX_VALUE
            val maxCpu = member.getAttribute("maxCpu")?.toIntOrNull() ?: Int.MAX_VALUE

            resources.usedRamMB + configuration.ramMB <= maxRam &&
            resources.usedCpu + configuration.cpu <= maxCpu
        }

        if (candidates.isEmpty()) {
            return null
        }

        // Pick the node with the most available RAM
        return candidates.maxByOrNull { member ->
            val resources = clusterStateService.getNodeResources(member.stableNodeId())
            val maxRam = member.getAttribute("maxRamMB")?.toIntOrNull() ?: Int.MAX_VALUE
            maxRam - resources.usedRamMB
        }
    }
}
