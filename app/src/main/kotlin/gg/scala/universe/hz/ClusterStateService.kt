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

internal data class DeploymentCleanupOwnership(
    val generation: Long,
    val allocatedPort: Int,
    val runtime: String,
    val runtimeStartAttempted: Boolean
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

    private val deploymentCleanups: IMap<String, DeploymentCleanupOwnership>
        get() = hazelcastInstance.getMap("instanceDeploymentCleanups")

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
            val txDeployments = context.getMap<String, DeploymentCleanupOwnership>(
                "instanceDeploymentCleanups"
            )
            txDeployments.getForUpdate(id)?.let { cleanup ->
                if (cleanup.generation != expectedGeneration) return@executeTransaction false
                txDeployments.put(id, cleanup.copy(generation = updatedGeneration))
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

    internal fun claimForShutdown(
        expected: InstanceInfo,
        expectedGeneration: Long,
        now: Long = System.currentTimeMillis()
    ): Pair<InstanceInfo, Long>? {
        if (expectedGeneration <= 0L || expected.state == InstanceState.STOPPED) return null
        if (expected.state == InstanceState.STOPPING) {
            return if (isCurrentLifecycle(expected, expectedGeneration, InstanceState.STOPPING)) {
                expected to expectedGeneration
            } else null
        }
        val claimed = expected.copy(state = InstanceState.STOPPING, lastHeartbeat = now)
        val generation = expectedGeneration + 1
        return if (transitionLifecycle(expected, expectedGeneration, claimed, generation)) {
            claimed to generation
        } else null
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
        generation: Long
    ): Boolean {
        require(expectedTerminal.id == creating.id)
        require(expectedTerminal.state == InstanceState.STOPPED)
        require(creating.state == InstanceState.CREATING)
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(creating.id) != expectedTerminal) return@executeTransaction false
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(creating.id) ?: 0L) != generation) {
                return@executeTransaction false
            }
            val txReservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            val reservation = txReservations.getForUpdate(creating.id) ?: return@executeTransaction false
            if (
                reservation.generation != generation ||
                reservation.nodeId != creating.wrapperNodeId ||
                reservation.ramMB != creating.allocatedRamMB ||
                reservation.cpu != creating.allocatedCpu
            ) return@executeTransaction false
            txInstances.put(creating.id, creating)
            true
        }
    }

    /** Rebinds a surviving runtime only if the durable snapshot is still exact. */
    internal fun recoverInstance(
        expected: InstanceInfo?,
        expectedGeneration: Long?,
        instance: InstanceInfo,
        generation: Long
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            val current = txInstances.getForUpdate(instance.id)
            if (current != expected) return@executeTransaction false
            if (current?.state == InstanceState.STOPPING || current?.state == InstanceState.STOPPED) {
                return@executeTransaction false
            }
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            val currentGeneration = txGenerations.getForUpdate(instance.id) ?: 0L
            if (expectedGeneration != null && currentGeneration != expectedGeneration) {
                return@executeTransaction false
            }
            // Generation zero is migrated only through this exact, runtime-confirmed
            // recovery transaction; legacy tasks themselves remain rejected.
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
                listOfNotNull(previous?.nodeId, desired.nodeId).distinct().sorted()
                    .forEach { txResources.getForUpdate(it) }
                if (previous != null) {
                    val old = txResources.get(previous.nodeId) ?: NodeResources()
                    txResources.put(
                        previous.nodeId,
                        NodeResources(
                            maxOf(0, old.usedRamMB - previous.ramMB),
                            maxOf(0, old.usedCpu - previous.cpu)
                        )
                    )
                }
                val currentResources = txResources.get(desired.nodeId) ?: NodeResources()
                txResources.put(
                    desired.nodeId,
                    NodeResources(
                        currentResources.usedRamMB + desired.ramMB,
                        currentResources.usedCpu + desired.cpu
                    )
                )
                txReservations.put(instance.id, desired)
            }
            txGenerations.put(instance.id, generation)
            txInstances.put(instance.id, instance)
            true
        }
    }

    internal fun claimDeploymentStartup(
        expected: InstanceInfo,
        generation: Long,
        allocatedPort: Int,
        runtime: String
    ): InstanceInfo? {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expected.id
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(id) != expected || expected.state != InstanceState.CREATING) {
                return@executeTransaction null
            }
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(id) ?: 0L) != generation) return@executeTransaction null
            if (!reservationMatches(context, id, generation)) return@executeTransaction null
            val txDeployments = context.getMap<String, DeploymentCleanupOwnership>(
                "instanceDeploymentCleanups"
            )
            if (txDeployments.getForUpdate(id) != null) return@executeTransaction null
            val updated = expected.copy(allocatedPort = allocatedPort)
            txDeployments.put(id, DeploymentCleanupOwnership(generation, allocatedPort, runtime, true))
            txInstances.put(id, updated)
            updated
        }
    }

    internal fun hasDeploymentCleanup(instanceId: String, generation: Long): Boolean =
        deploymentCleanups[instanceId]?.generation == generation

    internal fun clearDeploymentCleanup(
        expected: InstanceInfo,
        generation: Long
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expected.id
            if (context.getMap<String, InstanceInfo>("instances").getForUpdate(id) != expected) {
                return@executeTransaction false
            }
            if (
                (context.getMap<String, Long>("instanceLifecycleGenerations").getForUpdate(id) ?: 0L) != generation
            ) return@executeTransaction false
            val cleanups = context.getMap<String, DeploymentCleanupOwnership>("instanceDeploymentCleanups")
            val cleanup = cleanups.getForUpdate(id) ?: return@executeTransaction true
            if (cleanup.generation != generation) return@executeTransaction false
            cleanups.remove(id)
            true
        }
    }

    internal fun cancelCreatingAfterConfirmedTeardown(
        expected: InstanceInfo,
        generation: Long
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expected.id
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(id) != expected || expected.state != InstanceState.CREATING) {
                return@executeTransaction false
            }
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(id) ?: 0L) != generation) return@executeTransaction false
            if (!reservationMatches(context, id, generation)) {
                return@executeTransaction false
            }
            val cleanups = context.getMap<String, DeploymentCleanupOwnership>("instanceDeploymentCleanups")
            val cleanup = cleanups.getForUpdate(id) ?: return@executeTransaction false
            if (cleanup.generation != generation) return@executeTransaction false
            releaseReservation(context, id, generation)
            cleanups.remove(id)
            txInstances.remove(id)
            txGenerations.remove(id)
            true
        }
    }

    internal fun markDeploymentCleanupRequired(
        expected: InstanceInfo,
        generation: Long,
        now: Long = System.currentTimeMillis()
    ): InstanceInfo? {
        val stopping = expected.copy(state = InstanceState.STOPPING, lastHeartbeat = now)
        val nextGeneration = generation + 1
        return if (transitionLifecycle(expected, generation, stopping, nextGeneration)) stopping else null
    }

    internal fun promoteCreatingInstance(
        expectedInstance: InstanceInfo,
        expectedGeneration: Long,
        onlineInstance: InstanceInfo
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expectedInstance.id
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (
                txInstances.getForUpdate(id) != expectedInstance ||
                expectedInstance.state != InstanceState.CREATING
            ) return@executeTransaction false
            if (
                (context.getMap<String, Long>("instanceLifecycleGenerations").getForUpdate(id) ?: 0L) !=
                expectedGeneration
            ) return@executeTransaction false
            val reservation = context.getMap<String, InstanceResourceReservation>(
                "instanceResourceReservations"
            ).getForUpdate(id)
            if (reservation?.generation != expectedGeneration) return@executeTransaction false
            val cleanups = context.getMap<String, DeploymentCleanupOwnership>("instanceDeploymentCleanups")
            val cleanup = cleanups.getForUpdate(id) ?: return@executeTransaction false
            if (cleanup.generation != expectedGeneration) return@executeTransaction false
            cleanups.remove(id)
            txInstances.put(id, onlineInstance)
            true
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
            val txDeployments = context.getMap<String, DeploymentCleanupOwnership>(
                "instanceDeploymentCleanups"
            )
            if (txDeployments.getForUpdate(expectedInstance.id) != null) {
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
            val txDeployments = context.getMap<String, DeploymentCleanupOwnership>(
                "instanceDeploymentCleanups"
            )
            val deploymentCleanup = txDeployments.getForUpdate(expectedInstance.id)
            if (currentInstance.state == InstanceState.CREATING && deploymentCleanup != null) {
                return@executeTransaction false
            }
            txDeployments.remove(expectedInstance.id)
            releaseReservation(context, expectedInstance.id, generation)
            transactionalInstances.put(
                expectedInstance.id,
                expectedInstance.copy(state = finalState, lastHeartbeat = completedAt)
            )
            true
        }
    }

    internal fun completeInstanceTerminationForRestart(
        expectedInstance: InstanceInfo,
        expectedGeneration: Long,
        nextGeneration: Long,
        completedAt: Long = System.currentTimeMillis()
    ): InstanceInfo? {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val id = expectedInstance.id
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(id) != expectedInstance || expectedInstance.state != InstanceState.STOPPING) {
                return@executeTransaction null
            }
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(id) ?: 0L) != expectedGeneration) return@executeTransaction null
            val txReservations = context.getMap<String, InstanceResourceReservation>("instanceResourceReservations")
            val reservation = txReservations.getForUpdate(id) ?: return@executeTransaction null
            if (reservation.generation != expectedGeneration) return@executeTransaction null
            val stopped = expectedInstance.copy(state = InstanceState.STOPPED, lastHeartbeat = completedAt)
            txReservations.put(id, reservation.copy(generation = nextGeneration))
            txGenerations.put(id, nextGeneration)
            context.getMap<String, DeploymentCleanupOwnership>("instanceDeploymentCleanups").remove(id)
            txInstances.put(id, stopped)
            stopped
        }
    }

    internal fun releaseTerminalRestartReservation(
        expected: InstanceInfo,
        generation: Long
    ): Boolean {
        val options = TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
        return hazelcastInstance.executeTransaction(options) { context ->
            val txInstances = context.getMap<String, InstanceInfo>("instances")
            if (txInstances.getForUpdate(expected.id) != expected || expected.state != InstanceState.STOPPED) {
                return@executeTransaction false
            }
            val txGenerations = context.getMap<String, Long>("instanceLifecycleGenerations")
            if ((txGenerations.getForUpdate(expected.id) ?: 0L) != generation) return@executeTransaction false
            releaseReservation(context, expected.id, generation)
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

            if (
                context.getMap<String, DeploymentCleanupOwnership>("instanceDeploymentCleanups")
                    .getForUpdate(instanceId) != null
            ) return@executeTransaction false

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
