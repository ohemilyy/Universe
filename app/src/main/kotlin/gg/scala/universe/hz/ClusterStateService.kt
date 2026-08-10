package gg.scala.universe.hz

import com.google.inject.Inject
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.map.EntryProcessor
import com.hazelcast.transaction.TransactionOptions
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

    fun updateInstanceState(
        id: String,
        state: InstanceState,
        lastHeartbeat: Long = System.currentTimeMillis()
    ) {
        val existing = instances[id] ?: return
        instances[id] = existing.copy(
            state = state,
            lastHeartbeat = lastHeartbeat
        )
    }

    fun updateInstanceFromPlugin(
        id: String,
        state: InstanceState,
        lastHeartbeat: Long
    ): InstanceInfo? {
        instances.lock(id)
        return try {
            val existing = instances[id] ?: return null
            if (existing.state == InstanceState.STOPPING) {
                existing
            } else {
                existing.copy(state = state, lastHeartbeat = lastHeartbeat).also {
                    instances[id] = it
                }
            }
        } finally {
            instances.unlock(id)
        }
    }

    internal fun finalizeAbandonedStopping(
        instanceId: String,
        expectedLastHeartbeat: Long,
        stoppedAt: Long
    ): Boolean {
        val transactionOptions = TransactionOptions().setTransactionType(
            TransactionOptions.TransactionType.TWO_PHASE
        )
        val stoppedInstance = hazelcastInstance.executeTransaction(transactionOptions) { context ->
            val transactionalInstances = context.getMap<String, InstanceInfo>("instances")
            val instance = transactionalInstances.getForUpdate(instanceId)
                ?: return@executeTransaction null
            if (instance.state == InstanceState.STOPPED && instance.lastHeartbeat == stoppedAt) {
                return@executeTransaction instance
            }
            if (
                instance.state != InstanceState.STOPPING ||
                instance.lastHeartbeat != expectedLastHeartbeat
            ) {
                return@executeTransaction null
            }

            val transactionalResources = context.getMap<String, NodeResources>("nodeResources")
            val resources = transactionalResources.getForUpdate(instance.wrapperNodeId)
                ?: NodeResources()
            transactionalResources.put(
                instance.wrapperNodeId,
                NodeResources(
                    usedRamMB = maxOf(0, resources.usedRamMB - instance.allocatedRamMB),
                    usedCpu = maxOf(0, resources.usedCpu - instance.allocatedCpu)
                )
            )
            instance.copy(state = InstanceState.STOPPED, lastHeartbeat = stoppedAt).also {
                transactionalInstances.put(instanceId, it)
            }
        }
        stoppedInstance ?: return false
        instances.remove(instanceId, stoppedInstance)
        return true
    }

    fun getNodeResources(nodeId: String): NodeResources {
        return nodeResources[nodeId] ?: NodeResources(0, 0)
    }

    fun addNodeResources(nodeId: String, ramMB: Int, cpu: Int) {
        nodeResources.executeOnKey(nodeId, AdjustNodeResourcesProcessor(ramMB, cpu))
    }

    fun removeNodeResources(nodeId: String, ramMB: Int, cpu: Int) {
        nodeResources.executeOnKey(nodeId, AdjustNodeResourcesProcessor(-ramMB, -cpu))
    }

    fun clearNodeResources(nodeId: String) {
        nodeResources.remove(nodeId)
    }

    private data class AdjustNodeResourcesProcessor(
        val ramDelta: Int,
        val cpuDelta: Int
    ) : EntryProcessor<String, NodeResources, Unit> {
        override fun process(entry: MutableMap.MutableEntry<String, NodeResources>) {
            @Suppress("USELESS_ELVIS")
            val current = entry.value ?: NodeResources()
            entry.setValue(
                NodeResources(
                    usedRamMB = maxOf(0, current.usedRamMB + ramDelta),
                    usedCpu = maxOf(0, current.usedCpu + cpuDelta)
                )
            )
        }
    }
}
