package gg.scala.universe.hz.task

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cluster.Member
import com.hazelcast.core.MemberLeftException
import gg.scala.universe.console.LogLevel
import gg.scala.universe.hz.nodeName
import gg.scala.universe.console.log
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.InstanceInfo
import gg.scala.universe.schema.InstanceState
import gg.scala.universe.service.InstanceStopDispatcher
import gg.scala.universe.service.StopDispatchResult
import gg.scala.universe.task.DeployInstanceTask
import gg.scala.universe.task.ExecuteCommandTask
import gg.scala.universe.task.ShutdownNodeTask
import gg.scala.universe.task.StopInstanceTask
import gg.scala.universe.util.json.Serializers
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val STOP_SUBMISSION_ACK_TIMEOUT_MS = 250L

fun interface StopTaskSubmissionGateway {
    fun submit(task: StopInstanceTask, targetMember: Member): Future<String>
}

@Singleton
class HazelcastStopTaskSubmissionGateway @Inject constructor(
    hazelcastInstance: HazelcastInstance
) : StopTaskSubmissionGateway {
    private val executorService by lazy {
        hazelcastInstance.getExecutorService("universe-executor")
    }

    override fun submit(task: StopInstanceTask, targetMember: Member): Future<String> {
        val payload = Serializers.GSON.toJson(task)
        return executorService.submitToMember(UniverseCallableTask(payload), targetMember)
    }
}

@Singleton
class TaskDispatcher @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val clusterStateService: ClusterStateService,
    private val stopTaskSubmissionGateway: StopTaskSubmissionGateway =
        HazelcastStopTaskSubmissionGateway(hazelcastInstance)
) : InstanceStopDispatcher {
    private val executorService by lazy {
        hazelcastInstance.getExecutorService("universe-executor")
    }

    fun dispatchDeploy(instanceInfo: InstanceInfo, targetMember: Member) {
        log("Dispatching deploy task for instance ${instanceInfo.id} to node ${targetMember.nodeName()}")
        val task = DeployInstanceTask(
            instanceId = instanceInfo.id,
            configurationName = instanceInfo.configurationName,
            expectedGeneration = clusterStateService.getLifecycleGeneration(instanceInfo.id)
        )
        submit(task, targetMember)
    }

    override fun dispatchStop(
        instanceId: String,
        targetMember: Member,
        force: Boolean,
        restart: Boolean,
        expectedLastHeartbeat: Long?,
        transitionAt: Long
    ): StopDispatchResult {
        val instances = clusterStateService.instances
        val originalInstance: InstanceInfo
        val stoppingInstance: InstanceInfo
        val originalGeneration: Long
        val stoppingGeneration: Long
        instances.lock(instanceId)
        try {
            val instance = instances[instanceId]
                ?: return StopDispatchResult.NOT_FOUND
            if (clusterStateService.isAbandonedStoppingCleanupClaimed(instanceId)) {
                return StopDispatchResult.NOT_FOUND
            }
            if (
                expectedLastHeartbeat != null &&
                (instance.state != InstanceState.STOPPING ||
                    instance.lastHeartbeat != expectedLastHeartbeat)
            ) {
                return StopDispatchResult.STALE_TRANSITION
            }
            if (instance.state == InstanceState.STOPPING && !force) {
                return StopDispatchResult.ALREADY_STOPPING
            }
            if (
                targetMember.uuid.toString() != instance.wrapperNodeId ||
                hazelcastInstance.cluster.members.none { it.uuid == targetMember.uuid }
            ) {
                return StopDispatchResult.TARGET_UNAVAILABLE
            }

            originalInstance = instance
            originalGeneration = clusterStateService.getLifecycleGeneration(instanceId)
            stoppingGeneration = if (instance.state == InstanceState.STOPPING) {
                originalGeneration
            } else {
                originalGeneration + 1
            }
            stoppingInstance = instance.copy(
                state = InstanceState.STOPPING,
                lastHeartbeat = transitionAt
            )
        } finally {
            instances.unlock(instanceId)
        }

        if (!clusterStateService.transitionLifecycle(
                originalInstance,
                originalGeneration,
                stoppingInstance,
                stoppingGeneration
            )
        ) {
            return if (clusterStateService.getInstance(instanceId)?.state == InstanceState.STOPPING) {
                StopDispatchResult.ALREADY_STOPPING
            } else {
                StopDispatchResult.STALE_TRANSITION
            }
        }
        log("Dispatching stop task for instance $instanceId to node ${targetMember.nodeName()}")
        val submission = try {
            stopTaskSubmissionGateway.submit(
                StopInstanceTask(instanceId, force, restart, stoppingGeneration),
                targetMember
            )
        } catch (failure: RuntimeException) {
            return submissionFailureResult(targetMember, failure)
        }

        return awaitSubmission(
            targetMember = targetMember,
            submission = submission
        )
    }

    private fun awaitSubmission(
        targetMember: Member,
        submission: Future<String>
    ): StopDispatchResult {
        return try {
            submission.get(STOP_SUBMISSION_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            StopDispatchResult.DISPATCHED
        } catch (_: TimeoutException) {
            StopDispatchResult.DISPATCHED
        } catch (failure: InterruptedException) {
            Thread.currentThread().interrupt()
            StopDispatchResult.SUBMISSION_FAILED
        } catch (failure: CancellationException) {
            submissionFailureResult(targetMember, failure)
        } catch (failure: ExecutionException) {
            submissionFailureResult(targetMember, failure.cause ?: failure)
        }
    }

    private fun submissionFailureResult(
        targetMember: Member,
        failure: Throwable
    ): StopDispatchResult {
        val memberStillPresent = hazelcastInstance.cluster.members.any {
            it.uuid == targetMember.uuid
        }
        return if (!memberStillPresent || failure.hasCause<MemberLeftException>()) {
            StopDispatchResult.TARGET_UNAVAILABLE
        } else {
            StopDispatchResult.SUBMISSION_FAILED
        }
    }

    private inline fun <reified T : Throwable> Throwable.hasCause(): Boolean {
        var current: Throwable? = this
        while (current != null) {
            if (current is T) return true
            current = current.cause
        }
        return false
    }

    fun dispatchExecute(instanceId: String, command: String, targetMember: Member) {
        log("Dispatching execute task for instance $instanceId to node ${targetMember.nodeName()}: $command")
        val task = ExecuteCommandTask(
            instanceId = instanceId,
            command = command
        )
        submit(task, targetMember)
    }

    fun dispatchShutdown(targetMember: Member) {
        log("Dispatching shutdown task to node ${targetMember.nodeName()}")
        val task = ShutdownNodeTask()
        submit(task, targetMember)
    }

    private fun submit(task: Any, targetMember: Member) {
        val payload = Serializers.GSON.toJson(task)
        executorService.submitToMember(
            UniverseCallableTask(payload),
            targetMember
        )
    }
}
