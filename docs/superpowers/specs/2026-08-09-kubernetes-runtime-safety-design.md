# Kubernetes Runtime Safety Design

## Goal

Prevent Kubernetes `hostPort` mappings from being unintentionally reachable on every host interface, and make single-port instance stop/restart cycles converge without operator intervention.

## Confirmed Root Causes

The Kubernetes runtime assigns `hostPort` to the primary and additional container ports but never assigns `hostIP`. CNI portmap therefore creates destination NAT rules that accept traffic addressed to any host interface.

Instance creation persists a `CREATING` record before dispatch. When wrapper-side port allocation fails, `TaskRouter.handleDeploy` returns without removing that record. `InstanceCountEnforcer` counts `CREATING` as satisfying the configured minimum, so no later reconciliation attempt occurs.

Stop requests are also exposed to a race. REST stop and restart paths publish `STOPPED` before wrapper-side graceful shutdown releases the port. The enforcer reads `getAllInstances()` and counts only `ONLINE` and `CREATING`, so it may dispatch a replacement during the 30-second drain window. The explicit restart paths similarly recreate before port release.

## Kubernetes Bind Address

`K8sConfig` gains `hostPortBindAddress: String? = null`. This extension setting is the only source for Kubernetes `hostIP`; `Configuration.hostAddress` is deliberately not consulted because its existing default and runtime mutation make it unsuitable as a backward-compatible bind setting.

The value is normalized by trimming whitespace. Null, blank, and `0.0.0.0` mean that `hostIP` is omitted, preserving the current wildcard behavior. Any other value is assigned to every generated Kubernetes `ContainerPort`, including the primary port and every `Configuration.additionalPorts` entry.

Port construction moves behind a focused internal builder so production and tests use the same path. Startup logging reports the resolved bind address next to the existing memory and CPU resource logs, using a clear wildcard/omitted label when no `hostIP` is emitted.

KDoc will explain that setting `hostIP` causes CNI portmap to emit a destination-scoped DNAT match such as `-d <hostIP>/32`, limiting which host address can reach the pod.

## Lifecycle and Reconciliation

The master-side `InstanceState` gains `STOPPING`. Stop dispatch changes state to `STOPPING` before task submission. Callers no longer publish `STOPPED` themselves; only wrapper-side stop completion may do that.

Minimum-count reconciliation uses one shared lifecycle policy rather than an inline `ONLINE || CREATING` expression. `ONLINE` and `CREATING` count toward the minimum. If any instance of a configuration is `STOPPING`, replacement for that configuration is deferred entirely until the stop completes and its port has been released. This is intentionally a barrier, not merely another state counted toward the minimum.

The existing API-oriented `getActiveInstances()` retention filter has a different purpose: it controls short-lived visibility of `OFFLINE` and `STOPPED` records. It will be renamed or expressed as a visibility policy so it is not mistaken for reconciliation activity. Both policies will have explicit names and tests instead of competing uses of the word “active.”

Port-allocation failure removes the just-created `CREATING` instance record before returning. This change lands as an independently tested behavior because it is the self-healing mechanism: the next enforcer cycle sees the real deficit and retries. Runtime startup failure continues to use the same cleanup rule.

Stop tasks support an explicit restart intent. Wrapper-side restart is serialized as stop, port release, `STOPPED`, then redeploy, so no fixed 500 ms delay races the old allocation. The graceful wait remains bounded; after its existing timeout the runtime is forcibly stopped before the port is released.

## Stale-State Recovery

Reconciliation reaps stale lifecycle records using bounded thresholds:

- A stale `CREATING` record with no successful deployment is removed so minimum-count reconciliation can retry.
- A stale `STOPPING` record causes a forced stop task to be redispatched to its recorded wrapper. The forced path skips graceful waiting, tears down the runtime, releases the port, and only then writes `STOPPED`.

The timeout values will be named constants and evaluated from `lastHeartbeat`, allowing deterministic tests without real sleeps. Repeated forced-stop dispatch remains safe because stop and port release operations are idempotent.

## Compatibility

`STOPPING` is internal orchestration state. Hazelcast members and in-process extensions use the real value, but REST instance representations project `STOPPING` as `ONLINE`. Existing Minecraft plugins therefore continue receiving only known states while a stop is draining.

The Minecraft API already stores remote state as a string and catches unknown `InstanceState.valueOf` values. A compatibility test will pin that tolerance, and REST projection tests will ensure `STOPPING` is not exposed to older deployed plugin JARs.

## Ordering Guarantees

Wrapper-side lifecycle ordering is contractual:

1. Stop or forcibly tear down the runtime.
2. Release the allocated port.
3. Release node resources and clean transient files.
4. Publish `STOPPED`.
5. For restart, create/deploy the replacement only after steps 1–4.

A regression test will record these effects and assert that port release occurs before `STOPPED` and before replacement deployment.

## Tests

Regression coverage will include:

- A Kubernetes container/pod spec with a configured bind address carries `hostIP` on the primary and every additional port.
- Null, blank, and `0.0.0.0` bind settings omit `hostIP` from every port.
- Port-allocation failure removes the failed `CREATING` record, allowing a later enforcer pass to retry.
- A single-port stop/drain/respawn cycle does not deploy while stopping and converges after release.
- Port release precedes the `STOPPED` state transition and restart deployment.
- Stale `CREATING` is reaped and stale `STOPPING` triggers forced teardown.
- REST responses never expose `STOPPING`, and plugin-side unknown-state parsing remains tolerant.

Tests will use deterministic lifecycle functions and fakes at external boundaries; they will not depend on a live Kubernetes or Hazelcast cluster.

## Scope

This change is limited to Kubernetes port publication, shared lifecycle state/policy, stop/restart dispatch, stale-state recovery, REST compatibility projection, and their tests. It does not alter Docker binding behavior, Kubernetes Services, unrelated runtime providers, or the public Minecraft API enum.
