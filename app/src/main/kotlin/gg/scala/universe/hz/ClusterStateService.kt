package gg.scala.universe.hz

import com.google.inject.Inject
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.schema.NodeResources

class ClusterStateService @Inject constructor(
    private val hazelcastInstance: HazelcastInstance
) {
    val configurations: IMap<String, Configuration>
        get() = hazelcastInstance.getMap("configurations")

    val instances: IMap<String, InstanceInfo>
        get() = hazelcastInstance.getMap("instances")

    val nodeResources: IMap<String, NodeResources>
        get() = hazelcastInstance.getMap("nodeResources")

    fun getConfiguration(name: String): Configuration? {
        return configurations[name]
    }

    fun putConfiguration(configuration: Configuration) {
        configurations[configuration.name] = configuration
    }

    fun getInstance(id: String): InstanceInfo? {
        return instances[id]
    }

    fun getAllInstances(): Collection<InstanceInfo> {
        return instances.values
    }

    /**
     * Returns all visible instances, filtering out those that have been OFFLINE or STOPPED
     * for more than [staleThresholdMs] (default 15s for OFFLINE, 10s for STOPPED).
     */
    fun getVisibleInstances(staleThresholdMs: Long = 15000): Collection<InstanceInfo> {
        val now = System.currentTimeMillis()
        return instances.values.filter { instance ->
            when (instance.state) {
                InstanceState.OFFLINE -> now - instance.lastHeartbeat <= staleThresholdMs
                InstanceState.STOPPED -> now - instance.lastHeartbeat <= 10_000L
                else -> true
            }
        }
    }

    fun putInstance(info: InstanceInfo) {
        instances[info.id] = info
    }

    fun removeInstance(id: String) {
        instances.remove(id)
    }

    fun getInstancesByWrapper(nodeId: String): List<InstanceInfo> {
        return instances.values.filter { it.wrapperNodeId == nodeId }
    }

    fun updateInstanceState(id: String, state: InstanceState) {
        val existing = instances[id] ?: return
        instances[id] = existing.copy(
            state = state,
            lastHeartbeat = System.currentTimeMillis()
        )
    }

    fun getNodeResources(nodeId: String): NodeResources {
        return nodeResources[nodeId] ?: NodeResources(0, 0)
    }

    fun addNodeResources(nodeId: String, ramMB: Int, cpu: Int) {
        val current = getNodeResources(nodeId)
        nodeResources[nodeId] = NodeResources(
            usedRamMB = current.usedRamMB + ramMB,
            usedCpu = current.usedCpu + cpu
        )
    }

    fun removeNodeResources(nodeId: String, ramMB: Int, cpu: Int) {
        val current = getNodeResources(nodeId)
        nodeResources[nodeId] = NodeResources(
            usedRamMB = maxOf(0, current.usedRamMB - ramMB),
            usedCpu = maxOf(0, current.usedCpu - cpu)
        )
    }

    fun clearNodeResources(nodeId: String) {
        nodeResources.remove(nodeId)
    }
}
