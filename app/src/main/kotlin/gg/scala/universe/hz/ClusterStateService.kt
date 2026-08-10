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

internal data class InstanceResourceReservation(
    val generation: Long,
    val nodeId: String,
    val ramMB: Int,
    val cpu: Int
)

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

    private val lifecycleGenerations: IMap<String, Long>
        get() = hazelcastInstance.getMap("instanceLifecycleGenerations")

    private val resourceReservations: IMap<String, InstanceResourceReservation>
        get() = hazelcastInstance.getMap("instanceResourceReservations")

    fun getConfiguration(name: String): Configuration? {
        return configurations[name]
    }

    fun putConfiguration(configuration: Configuration) {
        configurations[configuration.name] = configuration
    }

    fun getInstance(id: String): InstanceInfo? {
        return instances[id]
    }

    fun getLifecycleGeneration(id: String): Long = lifecycleGenerations[id] ?: 0L

    /** Atomically transfers an exact lifecycle snapshot and its reservation token. */
    internal fun transitionLifecycle(
        expectedInstance: InstanceInfo,
        expectedGeneration: Long,
        updatedInstance: InstanceInfo,
        updatedGeneration: Long
    ): Boolean {
        require(expectedInstance.id == updatedInstance.id)
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expectedInstance.id
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(id) != expectedInstance) return@executeTransaction false
            val txCleanups = context.getMap<String, InstanceInfo>("abandonedStoppingCleanups")
            if (txCleanups.getForUpdate(id) != null) return@executeTransaction false
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(id) ?: 0L) != expectedGeneration) {
                return@executeTransaction false
            }
            val txReservations = context.getMap<String, InstanceResourceReservation>(
                "instanceResourceReservations"
            )
            val reservation = txReservations.getForUpdate(id)
            if (reservation != null) {
                if (reservation.generation != expectedGeneration) return@executeTransaction false
                txReservations.put(id, reservation.copy(generation = updatedGeneration))
            }
            txGenerations.put(id, updatedGeneration)
            txInstances.put(id, updatedInstance)
            true
        }
    }

    internal fun isCurrentLifecycle(
        expected: InstanceInfo,
        expectedGeneration: Long,
        requiredState: InstanceState,
        requireReservation: Boolean = false
    ): Boolean {
        val map = instances
        map.lock(expected.id)
        return try {
            map[expected.id] == expected &&
                expected.state == requiredState &&
                getLifecycleGeneration(expected.id) == expectedGeneration &&
                (!requireReservation || resourceReservations[expected.id]?.generation == expectedGeneration)
        } finally {
            map.unlock(expected.id)
        }
    }

    /**
     * Persists a CREATING incarnation and reserves its node capacity in the same
     * Hazelcast transaction. A stale capacity snapshot can therefore never admit
     * two instances beyond the node limit.
     */
    fun reserveCreatingInstance(
        instance: InstanceInfo,
        generation: Long,
        maxRamMB: Int,
        maxCpu: Int
    ): Boolean {
        require(instance.state == InstanceState.CREATING)
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(instance.id) != null) return@executeTransaction false
            val txCleanups = context.getMap<String, InstanceInfo>("abandonedStoppingCleanups")
            if (txCleanups.getForUpdate(instance.id) != null) return@executeTransaction false
            val txReservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            if (txReservations.getForUpdate(instance.id) != null) return@executeTransaction false
            val txResources = context.getMap<String, NodeResources>("nodeResources")
            val current = txResources.getForUpdate(instance.wrapperNodeId) ?: NodeResources()
            if (
                current.usedRamMB + instance.allocatedRamMB > maxRamMB ||
                current.usedCpu + instance.allocatedCpu > maxCpu
            ) return@executeTransaction false

            txResources.put(
                instance.wrapperNodeId,
                NodeResources(
                    current.usedRamMB + instance.allocatedRamMB,
                    current.usedCpu + instance.allocatedCpu
                )
            )
            txReservations.put(
                instance.id,
                InstanceResourceReservation(
                    generation,
                    instance.wrapperNodeId,
                    instance.allocatedRamMB,
                    instance.allocatedCpu
                )
            )
            context.getMap<String, Long>("instanceLifecycleGenerations").put(instance.id, generation)
            txInstances.put(instance.id, instance)
            true
        }
    }

    internal fun reserveRestartCreating(
        expectedTerminal: InstanceInfo,
        creating: InstanceInfo,
        generation: Long,
        maxRamMB: Int = Int.MAX_VALUE,
        maxCpu: Int = Int.MAX_VALUE
    ): Boolean {
        require(expectedTerminal.id == creating.id)
        require(expectedTerminal.state == InstanceState.STOPPED)
        require(creating.state == InstanceState.CREATING)
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(creating.id) != expectedTerminal) return@executeTransaction false
            val txReservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            if (txReservations.getForUpdate(creating.id) != null) return@executeTransaction false
            val txResources = context.getMap<String, NodeResources>("nodeResources")
            val current = txResources.getForUpdate(creating.wrapperNodeId) ?: NodeResources()
            if (
                current.usedRamMB + creating.allocatedRamMB > maxRamMB ||
                current.usedCpu + creating.allocatedCpu > maxCpu
            ) return@executeTransaction false
            txResources.put(
                creating.wrapperNodeId,
                NodeResources(
                    current.usedRamMB + creating.allocatedRamMB,
                    current.usedCpu + creating.allocatedCpu
                )
            )
            txReservations.put(
                creating.id,
                InstanceResourceReservation(
                    generation,
                    creating.wrapperNodeId,
                    creating.allocatedRamMB,
                    creating.allocatedCpu
                )
            )
            context.getMap<String, Long>("instanceLifecycleGenerations").put(creating.id, generation)
            txInstances.put(creating.id, creating)
            true
        }
    }

    /** Rebinds a surviving runtime to this wrapper and owns its accounting exactly once. */
    internal fun recoverInstance(instance: InstanceInfo, generation: Long): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            txInstances.getForUpdate(instance.id)
            val txCleanups = context.getMap<String, InstanceInfo>("abandonedStoppingCleanups")
            if (txCleanups.getForUpdate(instance.id) != null) return@executeTransaction false
            val txReservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            val previous = txReservations.getForUpdate(instance.id)
            val desired = InstanceResourceReservation(
                generation,
                instance.wrapperNodeId,
                instance.allocatedRamMB,
                instance.allocatedCpu
            )
            if (previous != desired) {
                val txResources = context.getMap<String, NodeResources>("nodeResources")
                if (previous != null) {
                    val old = txResources.getForUpdate(previous.nodeId) ?: NodeResources()
                    txResources.put(
                        previous.nodeId,
                        NodeResources(
                            maxOf(0, old.usedRamMB - previous.ramMB),
                            maxOf(0, old.usedCpu - previous.cpu)
                        )
                    )
                }
                val current = txResources.getForUpdate(desired.nodeId) ?: NodeResources()
                txResources.put(
                    desired.nodeId,
                    NodeResources(current.usedRamMB + desired.ramMB, current.usedCpu + desired.cpu)
                )
                txReservations.put(instance.id, desired)
            }
            context.getMap<String, Long>("instanceLifecycleGenerations").put(instance.id, generation)
            txInstances.put(instance.id, instance)
            true
        }
    }

    internal fun promoteCreatingInstance(
        expectedInstance: InstanceInfo,
        expectedGeneration: Long,
        onlineInstance: InstanceInfo
    ): Boolean {
        val map = instances
        map.lock(expectedInstance.id)
        return try {
            if (
                map[expectedInstance.id] != expectedInstance ||
                expectedInstance.state != InstanceState.CREATING ||
                getLifecycleGeneration(expectedInstance.id) != expectedGeneration ||
                resourceReservations[expectedInstance.id]?.generation != expectedGeneration
            ) false else {
                map[expectedInstance.id] = onlineInstance
                true
            }
        } finally {
            map.unlock(expectedInstance.id)
        }
    }

    internal fun cancelCreatingInstance(
        expectedInstance: InstanceInfo,
        expectedGeneration: Long
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            val current = txInstances.getForUpdate(expectedInstance.id) ?: return@executeTransaction false
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if (
                current != expectedInstance || current.state != InstanceState.CREATING ||
                (txGenerations.getForUpdate(expectedInstance.id) ?: 0L) != expectedGeneration
            ) return@executeTransaction false
            if (!reservationMatches(context, expectedInstance.id, expectedGeneration)) {
                return@executeTransaction false
            }
            releaseReservation(context, expectedInstance.id, expectedGeneration)
            txInstances.remove(expectedInstance.id)
            txGenerations.remove(expectedInstance.id)
            true
        }
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
                existing.state == InstanceState.STOPPED ||
                lastHeartbeat <= existing.lastHeartbeat ||
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
        expectedGeneration: Long? = null,
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
            val transactionalGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            val generation = transactionalGenerations.getForUpdate(expectedInstance.id) ?: 0L
            if (expectedGeneration != null && generation != expectedGeneration) {
                return@executeTransaction false
            }
            if (!reservationMatches(context, expectedInstance.id, generation)) {
                return@executeTransaction false
            }
            releaseReservation(context, expectedInstance.id, generation)
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

            val generation = context.getMap<String, Long>("instanceLifecycleGenerations")
                .getForUpdate(instanceId) ?: 0L
            if (!reservationMatches(context, instanceId, generation)) {
                return@executeTransaction false
            }
            releaseReservation(context, instanceId, generation)
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
                    context.getMap<String, Long>("instanceLifecycleGenerations").remove(instanceId)
                    true
                }
                currentInstance == claimedInstance -> {
                    transactionalInstances.remove(instanceId)
                    transactionalCleanups.remove(instanceId)
                    context.getMap<String, Long>("instanceLifecycleGenerations").remove(instanceId)
                    true
                }
                else -> false
            }
        }
    }

    private fun releaseReservation(
        context: com.hazelcast.transaction.TransactionalTaskContext,
        instanceId: String,
        expectedGeneration: Long
    ): Boolean {
        val reservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
        val reservation = reservations.getForUpdate(instanceId) ?: return false
        if (reservation.generation != expectedGeneration) return false
        val resources = context.getMap<String, NodeResources>("nodeResources")
        val current = resources.getForUpdate(reservation.nodeId) ?: NodeResources()
        resources.put(
            reservation.nodeId,
            NodeResources(
                maxOf(0, current.usedRamMB - reservation.ramMB),
                maxOf(0, current.usedCpu - reservation.cpu)
            )
        )
        reservations.remove(instanceId)
        return true
    }

    private fun reservationMatches(
        context: com.hazelcast.transaction.TransactionalTaskContext,
        instanceId: String,
        expectedGeneration: Long
    ): Boolean {
        val reservation = context
            .getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            .getForUpdate(instanceId)
        return reservation == null || reservation.generation == expectedGeneration
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
