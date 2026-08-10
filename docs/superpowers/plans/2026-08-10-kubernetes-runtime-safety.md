# Kubernetes Runtime Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scope Kubernetes host ports to an explicitly configured host address and make single-port stop/restart lifecycle reconciliation recover automatically.

**Architecture:** Kubernetes port construction moves into a deterministic builder shared by production and tests. Master lifecycle orchestration gains an internal `STOPPING` state, pure reconciliation/lifecycle policies, serialized wrapper-side restart, and stale-state recovery; REST projects internal state to a backward-compatible representation.

**Tech Stack:** Kotlin 2.4.0 using the repository's configured JVM toolchains, Google Guice, Gson, Hazelcast 5.7.0, Fabric8 Kubernetes Client 7.7.0, JUnit 6.0.3, Gradle version catalog.

## Global Constraints

- Work only on the existing `codex/fix-kubernetes` branch; do not create or switch branches.
- Use Google Guice for dependency injection and Gson for serialization.
- Keep Kubernetes implementation inside `extensions/runtime-k8s`; the extension must depend only on `:api` and `:extensions:extension-api`.
- `hostPortBindAddress: String? = null` is the only Kubernetes host-IP setting; do not derive it from `Configuration.hostAddress`.
- Null, blank, and `0.0.0.0` omit Kubernetes `hostIP` for backward compatibility.
- Hazelcast task payloads remain Gson-serialized JSON strings.
- Do not expose `STOPPING` through plugin-facing REST responses or add it to `:minecraft:api`'s public enum.
- Stop ordering is runtime teardown, port release, resource cleanup, `STOPPED`, then optional redeploy.
- All new dependencies must be declared in `gradle/libs.versions.toml`.

---

### Task 1: Test infrastructure and Kubernetes host-port binding

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `build.gradle.kts`
- Modify: `extensions/runtime-k8s/build.gradle.kts`
- Modify: `extensions/runtime-k8s/src/main/kotlin/gg/scala/universe/k8s/K8sConfig.kt`
- Create: `extensions/runtime-k8s/src/main/kotlin/gg/scala/universe/k8s/K8sPortSpec.kt`
- Modify: `extensions/runtime-k8s/src/main/kotlin/gg/scala/universe/k8s/K8sRuntimeProvider.kt`
- Modify: `extensions/runtime-k8s/SCHEMA.md`
- Test: `extensions/runtime-k8s/src/test/kotlin/gg/scala/universe/k8s/K8sPortSpecTest.kt`

**Interfaces:**
- Consumes: `K8sConfig`, `Configuration.additionalPorts`, Fabric8 `ContainerPort`.
- Produces: `K8sConfig.hostPortBindAddress: String?`, `K8sPortSpec.resolveHostIp(String?): String?`, and `K8sPortSpec.build(Int, List<AdditionalPort>, String?): List<ContainerPort>`.

- [ ] **Step 1: Add version-catalogued JUnit test dependencies**

Add `junit = "6.0.3"`, `kotlin-test-junit5`, and `junit-jupiter-engine` aliases to `gradle/libs.versions.toml`. Configure all `Test` tasks with `useJUnitPlatform()` in the root build, and add these test dependencies plus `testImplementation(project(":api"))` and `testImplementation(libs.k8s.client)` to `extensions/runtime-k8s/build.gradle.kts`.

```kotlin
testImplementation(libs.kotlin.test.junit5)
testRuntimeOnly(libs.junit.jupiter.engine)
testImplementation(project(":api"))
testImplementation(libs.k8s.client)
```

- [ ] **Step 2: Write failing Kubernetes port-spec tests**

Create tests that build a real Fabric8 `PodSpec` from `K8sPortSpec.build(...)` and assert literal port-to-hostIP mappings:

```kotlin
@Test
fun `bind address is applied to primary and additional ports`() {
    val ports = K8sPortSpec.build(
        primaryPort = 25565,
        additionalPorts = listOf(AdditionalPort(8080), AdditionalPort(40004, "UDP", "voice")),
        configuredBindAddress = " 127.0.0.1 "
    )
    val pod = PodBuilder().withNewSpec()
        .addNewContainer().withName("main").withPorts(ports).endContainer()
        .endSpec().build()

    assertEquals(listOf("127.0.0.1", "127.0.0.1", "127.0.0.1"), pod.spec.containers.single().ports.map { it.hostIP })
}

@Test
fun `wildcard-equivalent settings omit hostIP`() {
    listOf<String?>(null, "", "   ", "0.0.0.0").forEach { configured ->
        val ports = K8sPortSpec.build(25565, listOf(AdditionalPort(8080)), configured)
        assertTrue(ports.all { it.hostIP == null })
    }
}
```

- [ ] **Step 3: Run the focused tests and verify RED**

Run: `./gradlew :extensions:runtime-k8s:test --tests "gg.scala.universe.k8s.K8sPortSpecTest"`

Expected: compilation fails because `K8sPortSpec` and `hostPortBindAddress` do not exist.

- [ ] **Step 4: Implement minimal port construction and configuration**

Add `hostPortBindAddress: String? = null` to `K8sConfig`. Implement `K8sPortSpec` with the exact normalization rule and use `ContainerPortBuilder` for all ports:

```kotlin
internal object K8sPortSpec {
    fun resolveHostIp(configured: String?): String? = configured
        ?.trim()
        ?.takeIf { it.isNotEmpty() && it != "0.0.0.0" }

    fun build(primaryPort: Int, additionalPorts: List<AdditionalPort>, configuredBindAddress: String?): List<ContainerPort> {
        val hostIp = resolveHostIp(configuredBindAddress)
        return buildList {
            add(port(primaryPort, "TCP", null, hostIp))
            additionalPorts.forEach { additional ->
                val protocol = if (additional.protocol.equals("udp", true)) "UDP" else "TCP"
                add(port(additional.port, protocol, additional.name.ifBlank { "port-${additional.port}" }, hostIp))
            }
        }
    }
}
```

Its KDoc must state that a non-null `hostIP` makes CNI emit a destination-scoped DNAT rule using `-d <hostIP>/32`. Replace the inline `addNewPort()` calls in `K8sRuntimeProvider` with `.withPorts(K8sPortSpec.build(...))`, and log `hostPort bind address: <address>` or `hostPort bind address: wildcard (hostIP omitted)` next to resource-limit logging.

- [ ] **Step 5: Document the K8s extension option**

Add `hostPortBindAddress` to `SCHEMA.md` with `null` as the compatibility default and an example of `"127.0.0.1"` for loopback-only publication. Explicitly state that `Configuration.hostAddress` does not control Kubernetes binding.

- [ ] **Step 6: Run the focused tests and verify GREEN**

Run: `./gradlew :extensions:runtime-k8s:test --tests "gg.scala.universe.k8s.K8sPortSpecTest"`

Expected: all bind-address cases pass.

- [ ] **Step 7: Commit Task 1**

```bash
git add gradle/libs.versions.toml build.gradle.kts extensions/runtime-k8s
git commit -m "fix: scope Kubernetes host ports to configured address"
```

---

### Task 2: Internal lifecycle state, shared policies, and REST compatibility

**Files:**
- Modify: `api/src/main/kotlin/gg/scala/universe/schema/schemas.kt`
- Create: `app/src/main/kotlin/gg/scala/universe/service/InstanceLifecyclePolicy.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/hz/ClusterStateService.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/runtime/PortAllocator.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/api/routing/InstanceRoutes.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/cluster/ClusterDataProviderImpl.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/command/commands/ManagementCommands.kt`
- Modify: `app/build.gradle.kts`
- Modify: `minecraft/api/build.gradle.kts`
- Test: `app/src/test/kotlin/gg/scala/universe/service/InstanceLifecyclePolicyTest.kt`
- Test: `app/src/test/kotlin/gg/scala/universe/api/routing/InstanceApiProjectionTest.kt`
- Test: `minecraft/api/src/test/kotlin/gg/scala/universe/minecraft/api/InstanceInfoTest.kt`

**Interfaces:**
- Produces: `InstanceState.STOPPING`, `InstanceState.occupiesPort`, `InstanceLifecyclePolicy.evaluateRequest`, `InstanceInfo.toExternalApiView()`, and `ClusterStateService.getVisibleInstances()`.
- Consumes: Task 3 stop dispatch and Task 4 reconciliation planning use these lifecycle semantics.

- [ ] **Step 1: Write failing lifecycle policy tests**

First add the version-catalogued test dependencies to both modules. `app` also needs Hazelcast on the test runtime because lifecycle integration tests start an isolated embedded member:

```kotlin
// app/build.gradle.kts
testImplementation(libs.kotlin.test.junit5)
testImplementation(libs.hazelcast)
testRuntimeOnly(libs.junit.jupiter.engine)

// minecraft/api/build.gradle.kts
testImplementation(libs.kotlin.test.junit5)
testRuntimeOnly(libs.junit.jupiter.engine)
```

Test these hand-derived contracts:

```kotlin
@Test
fun `stopping occupies its port but blocks minimum replacement`() {
    assertTrue(InstanceState.STOPPING.occupiesPort)
    val status = InstanceLifecyclePolicy.minimumStatus("site", listOf(instance("site", InstanceState.STOPPING)))
    assertTrue(status.replacementBlocked)
    assertEquals(0, status.countedInstances)
}

@Test
fun `repeated stop is idempotent and conflicting restart is rejected`() {
    assertEquals(LifecycleRequestDecision.ACCEPTED_NOOP, InstanceLifecyclePolicy.evaluateRequest(InstanceState.STOPPING, LifecycleTarget.STOP))
    assertEquals(LifecycleRequestDecision.CONFLICT, InstanceLifecyclePolicy.evaluateRequest(InstanceState.STOPPING, LifecycleTarget.RESTART))
}
```

Add a REST projection test asserting `STOPPING` becomes `ONLINE` without changing the stored object, and a Minecraft API test asserting `InstanceInfo(..., state = "FUTURE_STATE").getStateEnum()` returns `STOPPED` rather than throwing.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `./gradlew :app:test --tests "*InstanceLifecyclePolicyTest" --tests "*InstanceApiProjectionTest" :minecraft:api:test --tests "*InstanceInfoTest"`

Expected: compilation fails because the internal state, policies, projection, and test dependencies are absent.

- [ ] **Step 3: Implement lifecycle and visibility policies**

Add `STOPPING` only to `gg.scala.universe.schema.InstanceState`. Implement explicit policy types:

```kotlin
internal val InstanceState.occupiesPort: Boolean
    get() = this == InstanceState.CREATING || this == InstanceState.ONLINE || this == InstanceState.STOPPING

internal data class MinimumStatus(val countedInstances: Int, val replacementBlocked: Boolean)
internal enum class LifecycleTarget { START, STOP, RESTART }
internal enum class LifecycleRequestDecision { DISPATCH, ACCEPTED_NOOP, CONFLICT }
```

`minimumStatus` counts only `ONLINE` and `CREATING`, but sets `replacementBlocked` when any matching instance is `STOPPING`. `evaluateRequest` returns `ACCEPTED_NOOP` for stop-on-STOPPING, `CONFLICT` for start/restart-on-STOPPING, and `DISPATCH` otherwise.

Rename the state-retention method to `ClusterStateService.getVisibleInstances()` and update its three callers. Change `PortAllocator` to use `occupiesPort` so a draining instance keeps ownership until explicit release.

- [ ] **Step 4: Implement REST compatibility projection and idempotency**

Add:

```kotlin
internal fun InstanceInfo.toExternalApiView(): InstanceInfo =
    if (state == InstanceState.STOPPING) copy(state = InstanceState.ONLINE) else this
```

Apply it to GET collection/single-instance responses and lifecycle responses. For stop/delete during `STOPPING`, return `202 Accepted` without dispatch. For start/restart during `STOPPING`, return `409 Conflict`. Do not modify the `:minecraft:api` enum.

Reject client attempts to write `STOPPING` through `PUT /api/instances/{id}/state`; that transition is reserved for master-side orchestration. An accepted initial restart returns `202 Accepted` after queuing one `restart = true` stop task instead of returning a prematurely created replacement.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: `./gradlew :app:test --tests "*InstanceLifecyclePolicyTest" --tests "*InstanceApiProjectionTest" :minecraft:api:test --tests "*InstanceInfoTest"`

Expected: lifecycle, REST projection, and future-state tolerance tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add api/src/main/kotlin/gg/scala/universe/schema/schemas.kt app minecraft/api
git commit -m "fix: define safe internal instance lifecycle states"
```

---

### Task 3: Deploy failure cleanup and serialized stop/restart ordering

**Files:**
- Modify: `api/src/main/kotlin/gg/scala/universe/task/StopInstanceTask.kt`
- Create: `app/src/main/kotlin/gg/scala/universe/service/InstanceStopDispatcher.kt`
- Create: `app/src/main/kotlin/gg/scala/universe/service/InstanceSpawner.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/app/MainGuiceModule.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/hz/task/TaskDispatcher.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/service/InstanceCreationService.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/hz/task/TaskRouter.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/api/routing/InstanceRoutes.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/command/commands/ManagementCommands.kt`
- Test: `app/src/test/kotlin/gg/scala/universe/hz/task/TaskRouterLifecycleTest.kt`

**Interfaces:**
- Produces: `StopInstanceTask(instanceId, force, restart)`, `InstanceStopDispatcher.dispatchStop(instanceId, member, force = false, restart = false): StopDispatchResult`, and `InstanceSpawner.createInstance(configuration, instanceId = null): InstanceInfo?`.
- Consumes: Task 2 lifecycle transitions and port-ownership policy; Task 4 uses forced dispatch.

- [ ] **Step 1: Write failing TaskRouter regression tests**

Use an embedded Hazelcast instance with a unique cluster name, real `ClusterStateService`, real `PortAllocator`, `RuntimeRegistryImpl`, and a fake `RuntimeProvider`. Cover three observable behaviors:

```kotlin
@Test
fun `port allocation failure removes creating record`() {
    portAllocator.reserve(singlePort)
    state.putInstance(creatingInstance(allocatedPort = 0))
    router.route(DeployInstanceTask("new001", "site"))
    assertNull(state.getInstance("new001"))
}

@Test
fun `stop publishes stopped only after releasing port`() {
    portAllocator.reserve(singlePort)
    state.instances.addEntryListener(updatedListener {
        stoppedSawReleased.set(singlePort !in portAllocator.getLocalAllocations())
        stoppedObserved.countDown()
    }, true)
    router.route(StopInstanceTask("old001"))
    assertTrue(stoppedObserved.await(2, TimeUnit.SECONDS))
    assertTrue(stoppedSawReleased.get())
}

@Test
fun `single port restart converges after release`() {
    portAllocator.reserve(singlePort)
    router.route(StopInstanceTask("old001", restart = true))
    val restarted = assertNotNull(state.getInstance("old001"))
    assertEquals(InstanceState.ONLINE, restarted.state)
    assertEquals(singlePort, restarted.allocatedPort)
}
```

Use a static configuration so template installation is not part of this lifecycle test. The fake runtime must return `isRunning = false`, record `stop` before `start`, and return a deterministic `ProcessHandle`.

- [ ] **Step 2: Run TaskRouter tests and verify RED**

Run: `./gradlew :app:test --tests "gg.scala.universe.hz.task.TaskRouterLifecycleTest"`

Expected: the allocation-failure record remains, `StopInstanceTask` lacks restart fields, and restart cannot converge.

- [ ] **Step 3: Clean failed pre-allocation deployments**

Replace the allocation early return with explicit cleanup:

```kotlin
val allocatedPort = portAllocator.allocate(configuration.availablePorts)
if (allocatedPort == null) {
    clusterStateService.removeInstance(task.instanceId)
    log("No available ports for instance ${task.instanceId} in range ${configuration.availablePorts.min}-${configuration.availablePorts.max}; removed queued instance so reconciliation can retry", LogLevel.ERROR)
    return
}
```

Keep runtime-start failure cleanup consistent with this rule.

- [ ] **Step 4: Make stop dispatch transition atomically**

Extend the task with Gson-compatible default booleans:

```kotlin
data class StopInstanceTask(
    val instanceId: String,
    val force: Boolean = false,
    val restart: Boolean = false,
    override val type: String = "stop"
) : UniverseTask
```

Add narrow Guice-bound interfaces so reconciliation tests can exercise real state transitions without mocking Hazelcast task execution:

```kotlin
enum class StopDispatchResult { DISPATCHED, ALREADY_STOPPING, NOT_FOUND }

interface InstanceStopDispatcher {
    fun dispatchStop(instanceId: String, targetMember: Member, force: Boolean = false, restart: Boolean = false): StopDispatchResult
}

interface InstanceSpawner {
    fun createInstance(configuration: Configuration, instanceId: String? = null): InstanceInfo?
}
```

`TaskDispatcher` implements `InstanceStopDispatcher`; `InstanceCreationService` implements `InstanceSpawner`; `MainGuiceModule` binds each interface to its implementation. Mark `TaskDispatcher` as a Guice singleton so concrete command callers and the interface binding share one dispatcher. Before normal submission, `TaskDispatcher.dispatchStop` copies the instance to `STOPPING` through `ClusterStateService.updateInstanceState(id, state, now)` so `lastHeartbeat` is reset at the same write. It returns `ALREADY_STOPPING` without submission for repeated normal stops, but `force = true` is allowed to refresh the timestamp and redispatch stale recovery. Route and command callers stop writing `STOPPED` optimistically.

- [ ] **Step 5: Serialize wrapper-side restart after release**

In `TaskRouter.handleStop`, skip graceful command/wait when `force` is true. Preserve this exact completion sequence:

```kotlin
runtimeProvider.stop(task.instanceId)
portAllocator.release(instance.allocatedPort)
cleanupWorkingDirectoryAndResources(instance, configuration)
clusterStateService.updateInstanceState(task.instanceId, InstanceState.STOPPED)
if (task.restart) {
    val queued = instance.copy(
        state = InstanceState.CREATING,
        allocatedPort = 0,
        processPid = null,
        lastHeartbeat = System.currentTimeMillis()
    )
    clusterStateService.putInstance(queued)
    handleDeploy(DeployInstanceTask(queued.id, queued.configurationName))
}
```

Update REST and console restart paths to dispatch one `restart = true` stop task and remove the 500 ms sleep/immediate create calls.

- [ ] **Step 6: Run TaskRouter tests and verify GREEN**

Run: `./gradlew :app:test --tests "gg.scala.universe.hz.task.TaskRouterLifecycleTest"`

Expected: failed queued records are removed, release precedes `STOPPED`, and the same single port is reused only after release.

- [ ] **Step 7: Commit Task 3**

```bash
git add api/src/main/kotlin/gg/scala/universe/task/StopInstanceTask.kt app/src/main/kotlin/gg/scala/universe
git commit -m "fix: serialize instance stop and restart lifecycle"
```

---

### Task 4: Reconciliation barrier and stale transitional-state recovery

**Files:**
- Create: `app/src/main/kotlin/gg/scala/universe/service/InstanceReconciliationPolicy.kt`
- Modify: `app/src/main/kotlin/gg/scala/universe/service/InstanceCountEnforcer.kt`
- Test: `app/src/test/kotlin/gg/scala/universe/service/InstanceReconciliationPolicyTest.kt`
- Test: `app/src/test/kotlin/gg/scala/universe/service/InstanceCountEnforcerTest.kt`

**Interfaces:**
- Produces: `InstanceReconciliationPolicy.plan(...)`, `ReconciliationPlan`, `CREATING_TIMEOUT_MS = 60_000L`, `STOPPING_TIMEOUT_MS = 120_000L`, and `InstanceCountEnforcer.enforceOnce(now: Long)`.
- Consumes: Task 2 lifecycle policy and Task 3 forced stop dispatch.

- [ ] **Step 1: Write failing pure reconciliation tests**

Cover literal plans for:

```kotlin
@Test
fun `non-stale stopping instance blocks replacement`() {
    val plan = policy.plan(config(minimum = 1), listOf(instance(STOPPING, updatedAt = 9_000)), setOf("node-a"), now = 10_000)
    assertEquals(0, plan.spawnCount)
    assertTrue(plan.forceStopIds.isEmpty())
}

@Test
fun `stale creating is reaped and deficit retried`() {
    val plan = policy.plan(config(minimum = 1), listOf(instance(CREATING, updatedAt = 0)), setOf("node-a"), now = 60_001)
    assertEquals(listOf("abc123"), plan.reapCreatingIds)
    assertEquals(1, plan.spawnCount)
}

@Test
fun `stale stopping on dead wrapper is reaped locally`() {
    val plan = policy.plan(config(minimum = 1), listOf(instance(STOPPING, wrapper = "dead", updatedAt = 0)), emptySet(), now = 120_001)
    assertEquals(listOf("abc123"), plan.abandonedStoppingIds)
    assertEquals(1, plan.spawnCount)
}
```

Also test that stale `STOPPING` on an online wrapper produces `forceStopIds`, no spawn, and a refreshed transition timestamp when applied.

- [ ] **Step 2: Run policy tests and verify RED**

Run: `./gradlew :app:test --tests "*InstanceReconciliationPolicyTest" --tests "*InstanceCountEnforcerTest"`

Expected: reconciliation plan types and `enforceOnce` do not exist.

- [ ] **Step 3: Implement the pure planner**

Create:

```kotlin
internal data class ReconciliationPlan(
    val reapCreatingIds: List<String>,
    val forceStopIds: List<String>,
    val abandonedStoppingIds: List<String>,
    val spawnCount: Int
)
```

The planner filters by configuration, classifies stale records from transition-reset `lastHeartbeat`, blocks spawn for every non-stale or online-wrapper `STOPPING`, excludes stale `CREATING` and abandoned `STOPPING` records from the effective count, and preserves the static-configuration maximum of one spawn per pass.

- [ ] **Step 4: Apply plans in `InstanceCountEnforcer`**

Extract `internal fun enforceOnce(now: Long = System.currentTimeMillis())`. For each plan, apply actions in this order:

1. Remove stale `CREATING` records.
2. For abandoned `STOPPING`, release node resource accounting, write `STOPPED`, then remove the tracking record without dispatch.
3. For stale `STOPPING` on live members, refresh `lastHeartbeat` to `now` and dispatch `force = true` without restart.
4. Spawn the planned deficit through `InstanceSpawner`.

Resolve live wrapper IDs from `hazelcastInstance.cluster.members`. Inject `HazelcastInstance`, `InstanceStopDispatcher`, and `InstanceSpawner` into the enforcer; continue running only on the master node.

- [ ] **Step 5: Add the end-to-end reconciliation-cycle test**

Using embedded Hazelcast, a synchronous test `InstanceSpawner` that routes deploys through the real `TaskRouter`, and deterministic `enforceOnce(now)`, simulate:

1. One `ONLINE` instance owns the only configured port.
2. Stop dispatch transitions it to `STOPPING`; enforcement produces no replacement.
3. Wrapper-side stop releases the port and publishes `STOPPED`.
4. Enforcement creates and dispatches one replacement.
5. The replacement deploys to `ONLINE` on the same single port.

Assert no manual record deletion occurs and there is exactly one `ONLINE` instance at convergence.

- [ ] **Step 6: Run reconciliation tests and verify GREEN**

Run: `./gradlew :app:test --tests "*InstanceReconciliationPolicyTest" --tests "*InstanceCountEnforcerTest" --tests "*TaskRouterLifecycleTest"`

Expected: barrier, stale cleanup, dead-wrapper fallback, forced-stop throttling, and single-port convergence all pass.

- [ ] **Step 7: Commit Task 4**

```bash
git add app/src/main/kotlin/gg/scala/universe/service app/src/test/kotlin/gg/scala/universe/service
git commit -m "fix: recover stalled instance reconciliation"
```

---

### Task 5: Full verification and publication readiness

**Files:**
- Verify all changed files from Tasks 1–4.
- Modify only files required to fix failures directly caused by this work.

**Interfaces:**
- Consumes: all prior task outputs.
- Produces: verified branch ready for push and draft pull request.

- [ ] **Step 1: Run focused regression suites fresh**

Run:

```bash
./gradlew :extensions:runtime-k8s:test :app:test :minecraft:api:test
```

Expected: zero failed tests.

- [ ] **Step 2: Run architecture and build verification**

Run:

```bash
./gradlew :api:check :extensions:extension-api:check :extensions:runtime-k8s:check :app:check :minecraft:api:check
./gradlew :extensions:runtime-k8s:build :app:build
```

Expected: both commands exit 0 with no compilation or test failures.

- [ ] **Step 3: Inspect repository boundaries and diff hygiene**

Run:

```bash
git diff --check
git status -sb
git diff --stat origin/HEAD...HEAD
```

Confirm the K8s extension has no `:app` dependency, Docker code is unchanged, `Configuration.hostAddress` is not used by `K8sPortSpec`, and no `STOPPING` value was added to `minecraft/api/.../InstanceState.kt`.

- [ ] **Step 4: Commit any verification-only corrections**

If verification required a scoped correction, stage only those files and commit with:

```bash
git commit -m "test: cover Kubernetes lifecycle regressions"
```

If no correction was required, do not create an empty commit.

- [ ] **Step 5: Publish through the requested GitHub workflow**

Confirm `git status -sb`, authenticate `gh`, push `codex/fix-kubernetes` with tracking, and create a draft PR through the GitHub app. The PR body must explain both root causes, backward-compatible bind behavior, reconciliation/recovery changes, plugin compatibility, and exact verification commands.
