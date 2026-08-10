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

    private val abandonedStoppingCleanups: IMap<String, InstanceInfo>
        get() = hazelcastInstance.getMap("abandonedStoppingCleanups")

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

    fun putInstance(info: InstanceInfo): Boolean {
        instances.lock(info.id)
        return try {
            if (abandonedStoppingCleanups.containsKey(info.id)) return false
            instances[info.id] = info
            true
        } finally {
            instances.unlock(info.id)
        }
    }

    fun removeInstance(id: String): Boolean {
        instances.lock(id)
        return try {
            if (abandonedStoppingCleanups.containsKey(id)) return false
            instances.remove(id) != null
        } finally {
            instances.unlock(id)
        }
    }

    fun getInstancesByWrapper(nodeId: String): List<InstanceInfo> {
        return instances.values.filter { it.wrapperNodeId == nodeId }
    }

    internal fun isAbandonedStoppingCleanupClaimed(id: String): Boolean {
        return abandonedStoppingCleanups.containsKey(id)
    }

    fun updateInstanceState(
        id: String,
        state: InstanceState,
        lastHeartbeat: Long = System.currentTimeMillis()
    ) {
        instances.lock(id)
        try {
            val existing = instances[id] ?: return
            if (abandonedStoppingCleanups.containsKey(id)) return
            instances[id] = existing.copy(
                state = state,
                lastHeartbeat = lastHeartbeat
            )
        } finally {
            instances.unlock(id)
        }
    }

    fun updateInstanceFromPlugin(
        id: String,
        state: InstanceState,
        lastHeartbeat: Long
    ): InstanceInfo? {
        instances.lock(id)
        return try {
            val existing = instances[id] ?: return null
            if (
                existing.state == InstanceState.STOPPING ||
                abandonedStoppingCleanups.containsKey(id)
            ) {
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

    internal fun completeInstanceTermination(
        expectedInstance: InstanceInfo,
        finalState: InstanceState,
        completedAt: Long = System.currentTimeMillis()
    ): Boolean {
        require(finalState == InstanceState.STOPPED || finalState == InstanceState.OFFLINE) {
            "Terminal completion requires STOPPED or OFFLINE, got $finalState"
        }
        val transactionOptions = TransactionOptions().setTransactionType(
            TransactionOptions.TransactionType.TWO_PHASE
        )
        return hazelcastInstance.executeTransaction(transactionOptions) { context ->
            val transactionalInstances = context.getMap<String, InstanceInfo>("instances")
            val currentInstance = transactionalInstances.getForUpdate(expectedInstance.id)
                ?: return@executeTransaction false
            val transactionalCleanups = context.getMap<String, InstanceInfo>(
                "abandonedStoppingCleanups"
            )
            if (transactionalCleanups.getForUpdate(expectedInstance.id) != null) {
                return@executeTransaction false
            }
            if (
                currentInstance != expectedInstance ||
                currentInstance.state !in setOf(
                    InstanceState.CREATING,
                    InstanceState.ONLINE,
                    InstanceState.STOPPING
                )
            ) {
                return@executeTransaction false
            }

            val transactionalResources = context.getMap<String, NodeResources>("nodeResources")
            val resources = transactionalResources.getForUpdate(expectedInstance.wrapperNodeId)
                ?: NodeResources()
            transactionalResources.put(
                expectedInstance.wrapperNodeId,
                NodeResources(
                    usedRamMB = maxOf(0, resources.usedRamMB - expectedInstance.allocatedRamMB),
                    usedCpu = maxOf(0, resources.usedCpu - expectedInstance.allocatedCpu)
                )
            )
            transactionalInstances.put(
                expectedInstance.id,
                expectedInstance.copy(state = finalState, lastHeartbeat = completedAt)
            )
            true
        }
    }

    fun markInstanceOfflineAfterWrapperDeparture(
        id: String,
        wrapperNodeId: String,
        lastHeartbeat: Long = System.currentTimeMillis()
    ): InstanceInfo? {
        instances.lock(id)
        return try {
            val existing = instances[id] ?: return null
            if (
                existing.wrapperNodeId != wrapperNodeId ||
                existing.state == InstanceState.STOPPING ||
                abandonedStoppingCleanups.containsKey(id)
            ) {
                existing
            } else {
                existing.copy(
                    state = InstanceState.OFFLINE,
                    lastHeartbeat = lastHeartbeat
                ).also { instances[id] = it }
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
        if (!claimAbandonedStopping(instanceId, expectedLastHeartbeat, stoppedAt)) {
            return false
        }
        return completeAbandonedStoppingCleanup(instanceId)
    }

    internal fun claimAbandonedStopping(
        instanceId: String,
        expectedLastHeartbeat: Long,
        stoppedAt: Long
    ): Boolean {
        val transactionOptions = TransactionOptions().setTransactionType(
            TransactionOptions.TransactionType.TWO_PHASE
        )
        return hazelcastInstance.executeTransaction(transactionOptions) { context ->
            val transactionalInstances = context.getMap<String, InstanceInfo>("instances")
            val instance = transactionalInstances.getForUpdate(instanceId)
            val transactionalCleanups = context.getMap<String, InstanceInfo>(
                "abandonedStoppingCleanups"
            )
            if (transactionalCleanups.getForUpdate(instanceId) != null) {
                return@executeTransaction true
            }
            instance ?: return@executeTransaction false
            if (
                instance.state != InstanceState.STOPPING ||
                instance.lastHeartbeat != expectedLastHeartbeat
            ) {
                return@executeTransaction false
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
            val stoppedInstance = instance.copy(
                state = InstanceState.STOPPED,
                lastHeartbeat = stoppedAt
            )
            transactionalInstances.put(instanceId, stoppedInstance)
            transactionalCleanups.put(instanceId, stoppedInstance)
            true
        }
    }

    internal fun completePendingAbandonedStoppingCleanups(): Int {
        return abandonedStoppingCleanups.keys.count { instanceId ->
            completeAbandonedStoppingCleanup(instanceId)
        }
    }

    private fun completeAbandonedStoppingCleanup(instanceId: String): Boolean {
        val transactionOptions = TransactionOptions().setTransactionType(
            TransactionOptions.TransactionType.TWO_PHASE
        )
        return hazelcastInstance.executeTransaction(transactionOptions) { context ->
            val transactionalInstances = context.getMap<String, InstanceInfo>("instances")
            val currentInstance = transactionalInstances.getForUpdate(instanceId)
            val transactionalCleanups = context.getMap<String, InstanceInfo>(
                "abandonedStoppingCleanups"
            )
            val claimedInstance = transactionalCleanups.getForUpdate(instanceId)
                ?: return@executeTransaction false

            when {
                currentInstance == null -> {
                    transactionalCleanups.remove(instanceId)
                    true
                }
                currentInstance == claimedInstance -> {
                    transactionalInstances.remove(instanceId)
                    transactionalCleanups.remove(instanceId)
                    true
                }
                else -> false
            }
        }
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
