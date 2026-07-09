# Hardening K8s orchestration against pod-leak / split-brain meltdown

Branch: `fix/k8s-runtime-reconciliation`

## Background

During a production incident, k3s crashed and later self-recovered. The outage itself was transient —
the damage was the aftermath: the master leaked **2,167 Pending pods**, went split-brain (old pods kept
running under containerd with stale instance-ids while the master wanted new ids it could never
schedule), and several configs got permanently stuck in `CREATING`. Recovery required manual
`kubectl delete`, `DELETE /api/instances/{id}`, and clearing the Pending pile by hand.

This branch makes the orchestrator self-converge from that state with **no manual intervention**. Each
change below names the failure mode it removes. Every change is behind clear logging.

---

## 1. `CREATING` no longer causes a respawn deadlock  ★

**Failure mode:** an instance stuck in `CREATING` (pod never reached Ready, or was deleted out from
under it) counted toward `minimumServiceCount` forever, so the config was never respawned — a
permanent outage of that service.

**Fix:** the enforcer counts an instance as "active" only while it is `ONLINE` or *freshly* `CREATING`
(a deploy genuinely in flight — this is deliberate, so a normal 30s deploy doesn't trigger a spawn
storm). A watchdog (`InstanceLifecyclePolicy.staleCreating`, 120s) transitions any instance stuck in
`CREATING` past the deadline to `OFFLINE`, releases its port, and lets the enforcer replace it on the
same tick.

**Tests:** `InstanceLifecyclePolicyTest`, `InstancePortReservationsTest`.

## 2. Deploy timeout/failure no longer leaks the pod  ★

**Failure mode:** `waitForPodPhase(...)` timed out and the instance was failed, but the pod it created
was never deleted. Under load/outage these accumulated into thousands of Pending pods.

**Fix:** `start()` now pairs every "create pod" with "delete pod on failure." On any timeout / failure
/ exception it calls `deletePodServiceAndWait(...)`, which deletes the pod **and** its service and
blocks until the pod (and its hostPort) is actually gone, force-deleting a stuck-Terminating pod.
Gating was also tightened from pod *phase* to the *Ready condition* so an instance is never marked
`ONLINE` before it can serve traffic.

**Test:** `K8sRuntimeProviderDeployCleanupTest` — drives the real provider against a fabric8
mock API server in CRUD mode where pods never become Ready; asserts that after N failed deploys the
namespace holds **zero** pods (not N).

## 3. Reconciliation against actual cluster state (anti split-brain)  ★

**Failure mode:** the master trusted its in-memory/DB registry and never re-discovered reality, so
after a restart it had no idea which pods were actually running — the split-brain.

**Fix:** a runtime-agnostic reconcile pass (`RuntimeReconcile` + `K8sRuntimeProvider.reconcile`) lists
pods by ownership label and, for each: **adopts** a running+ready pod whose id it still tracks (or
lost track of across a restart), **deletes** untracked dead/stuck pods (orphans) and tracked-but-dead
pods, and **waits** on transient/terminating ones. Deletion is driven by *deadness*, never by
"untracked," so a state wipe can never delete a healthy instance. Runs at startup **and now every 60s**
(`InstanceRecoveryService.startPeriodicReconcile`), so orphans created during normal operation are
reaped continuously — not only on restart.

**Tests:** `RuntimeReconcileTest` (adopt / delete-orphan / delete-dead / wait matrix),
`ResourceOwnershipTest`.

## 4. Retry storm bounded: backoff + cap + reaper

**Failure mode:** failed deploys retried immediately, every 5s, with no ceiling — flooding etcd.

**Fix:** three bounds.
- **Cap** — a fresh `CREATING` instance counts as active, so the enforcer never launches more than the
  real deficit (≈1 in-flight per config).
- **Backoff** — `SpawnBackoff` applies per-config exponential backoff (5s → 10s → 20s …, capped at
  5min). Because `createInstance` returns as soon as a deploy is *dispatched* (its real outcome —
  unschedulable, timeout — only surfaces later, after which `TaskRouter` removes the record too fast
  for the 120s `CREATING` watchdog to see), the enforcer treats **every** spawn round as a tentative
  failure; a config reaching its minimum in `ONLINE` instances clears it via `recordSuccess`. The
  delay is free for a healthy deploy (its `CREATING` instance drives the deficit to 0, so the backoff
  is never consulted) and turns a genuinely-unschedulable config into a slow trickle instead of a
  create/delete storm every 5s.
- **Reaper** — the periodic reconcile (item 3) deletes long-Pending / dead pods continuously.

**Tests:** `SpawnBackoffTest`, plus the reconcile tests above.

## 5. Distinguish "unschedulable" from "slow start"

**Failure mode:** a fixed 30s timeout treated an unschedulable pod (insufficient memory / no free
port) the same as a slow-starting container, so an oversized config produced a false-negative and
kept spawning replacements.

**Fix:** two layers.
- **Pre-flight** — `findBestNode` queries the runtime's real node Allocatable
  (`queryNodeAllocatable` / `RuntimeResources.fits`) and rejects a config that cannot fit **before a
  pod is ever created** — no pod, no storm.
- **In-flight** — `waitForPodReady` inspects pod conditions; on `PodScheduled=False / Unschedulable`
  it fails fast with a single clear "cannot schedule: <reason>" log instead of burning the whole
  timeout, and the enforcer then backs off hard (item 4). The wait-timeout is configurable
  (`K8sConfig.timeoutSeconds`, and `pendingGraceSeconds` for reconcile) so slow image pulls /
  `npm install` can be given more room.

**Tests:** `RuntimeResourcesTest` (unit conversions + fit checks). Pod-condition detection is exercised
by the mock-server test harness.

## 6. Auto-inject JVM heap from `ramMB`

**Failure mode:** a Java config set the container memory limit from `ramMB` but passed no `-Xmx`, so
the JVM defaulted to ~25% of the container (a 16 GB container ran a ~4 GB heap and wasted the rest);
shrinking the container without adjusting the heap instead invited the OOM killer.

**Fix:** `JvmHeapArgs.inject` sizes `-Xms`/`-Xmx` (pinned equal) to a configurable fraction of `ramMB`
(default 0.75, min 512 MB) and injects them right after the `java` launcher token when building a
`kube` instance's command. It leaves the command untouched when the operator already expressed a heap
intent — `-Xmx`/`-Xms`/`-Xmn`/`-XX:MaxHeapSize` **or** the container-aware `-XX:*RAMPercentage`
family (an explicit `-Xmx` would silently override those, and the shipped default command uses
`-XX:MaxRAMPercentage`) — and when the container is too small to give an explicit heap headroom
(`heap >= ramMB`), leaving the JVM's own container-aware default rather than pinning a heap at the
cgroup limit and OOM-killing the pod. Opt out globally (`K8sConfig.autoHeap`) or per config
(`properties["autoHeap"] = "false"`).

**Test:** `JvmHeapArgsTest` (sizing, floor/headroom skip, `-XX:*RAMPercentage` opt-out, non-java
commands, path/`JAVA_HOME` prefix edge cases).

## 7. Config validation warning (low priority)

**Failure mode:** `legacy-proxy`, the legacy network's always-on entry point, was left at
`minimumServiceCount = 0`. After its instance was cleared during recovery, nothing brought it back and
players couldn't join.

**Fix:** `ConfigValidation.warnings` flags a config that looks like an always-on entry point
(proxy/gateway, by name/command/`properties["entrypoint"]`) but has `minimumServiceCount = 0`. Logged
at load time and on `PUT /api/configurations/{name}` — the config is still accepted (no API change).

**Test:** `ConfigValidationTest`.

---

## API compatibility

No breaking changes to `PUT /api/configurations/{name}`, `DELETE /api/instances/{id}`, or any other
route. `K8sConfig` gains opt-in/opt-out fields with backward-compatible defaults.

## Definition of done

Simulate an outage — start several instances, kill/restart the k8s API (or the master), and the system
self-converges: one tracked instance per running pod, every config at or above its minimum, and the
Pending/Failed pod count stays near zero throughout. No manual `kubectl delete` is required to recover.

## Review notes

This change was put through an adversarial multi-agent review (correctness / concurrency /
heap / integration lenses, each finding independently verified). Fixes it drove:

- Auto-heap now treats `-XX:*RAMPercentage`/`-Xmn`/`-XX:MaxHeapSize` as an explicit-heap opt-out (was
  overriding the repo's own default command) and refuses to inject a heap without container headroom
  (was OOM-killing small containers).
- The spawn backoff now engages for *dispatched-but-failed* deploys (unschedulable / timeout), not
  only the "no node fits" case — closing the every-5s create/delete churn hole.
- Periodic reconcile no longer subtracts node resources for a dead `CREATING` instance (which never
  had them added), preventing capacity under-counting; and its grace window is
  `max(pendingGraceSeconds, timeoutSeconds)` so it can't reap a pod an in-flight `start()` is still
  legitimately waiting on.

Known, accepted test gaps (behaviour is exercised indirectly; the safety-critical "no leaked pods"
invariant is directly tested): the item-5 unschedulable fast-fail path and the `SpawnBackoff`↔enforcer
wiring have no dedicated integration test — both require standing up Guice/Hazelcast or scripting pod
conditions, disproportionate to the simple diagnostics/latency logic they cover.
