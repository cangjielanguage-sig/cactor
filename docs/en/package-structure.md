# CActor Package & Module Structure

CActor is a cjpm workspace with **14 modules**, listed in the root `cjpm.toml`. A module contains its root package and subpackages. Dependencies must be acyclic; module manifests define the exact edges and `tools/check_module_deps.py` enforces layering.

## Modules and direct dependencies

Dependency names below omit the `cactor_` prefix except for the facade, `cactor`.

| Directory | Module | Direct dependencies | Purpose |
|---|---|---|---|
| foundation | `cactor_foundation` | none | Queues, network abstractions, configuration, logging, monitoring, circuit breakers |
| macros | `cactor_macros` | none | Actor, message and state machine macros |
| core | `cactor_core` | foundation | Actors, messages, supervision, typed behaviors and protocols |
| runtime | `cactor_runtime` | foundation, core | Actor cells, mailboxes, scheduling and lifecycle |
| patterns | `cactor_patterns` | foundation, core | Ask, routing, backpressure and SpawnProtocol |
| remote | `cactor_remote` | foundation, core | TCP, handshakes, quarantine, reliable delivery and TLS |
| persistence | `cactor_persistence` | foundation, core | Journals, snapshots, classic and typed persistence |
| cactor | `cactor` | core, runtime | System facade and management HTTP API |
| streams | `cactor_streams` | macros, foundation, core, cactor | Backpressured streams (E60): Reactive Streams interfaces, Source/Flow/Sink blueprints, fused islands + async boundaries, cancellation and in-stream supervision |
| cluster | `cactor_cluster` | foundation, core, remote, cactor | Membership, sharding, singleton, service discovery and distributed data |
| xstate | `cactor_xstate` | foundation, core, cactor | Experimental state machines; no cluster dependency |
| testkit | `cactor_testkit` | foundation, core, runtime, cactor | TestProbe, BehaviorTestKit, ManualTime, ProcessBarrier and other testing tools |
| tests | `cactor_tests` | foundation, core, runtime, patterns, remote, persistence, cactor, testkit, macros, cluster | Unit and integration tests |
| examples | `cactor_examples` | All library modules and macros (excluding tests) | Runnable examples and benchmarks |

## Dependency direction and package paths

`modules/<directory>/src/` contains the module's package tree. The main composition paths are:

```text
foundation → core → runtime → cactor
                  → patterns
                  → remote → cluster
                  → persistence
cactor → streams / xstate / testkit
these modules → tests / examples
```

Arrows show composition from lower to higher layers. Macros are independent. stdx is allowed only in remoting and higher layers; lower layers receive capabilities through interfaces such as `TransportSecurity`.

Common entry points:

- `cactor_core.typed`: behaviors, references, ask and protocol codecs. Plain enums cross the network through `TypedPayloadCodecs`.
- `cactor_cluster.membership`: real membership, `ClusterSharding`, `ClusterSingleton` and `ClusterReceptionist`.
- `cactor_cluster.ddata`: CRDTs and Replicator; `cactor_cluster.ops`: management and fault injection routes.
- `cactor_persistence.typed`: `EventSourcedBehavior` and `DurableStateBehavior`.
- `cactor_foundation.logging` / `cactor_foundation.monitoring`: logging and monitoring, including the migrated `Histogram` / `calculatePercentile`.
- `cactor_testkit`: runtime and synchronous behavior testing. The compatibility `TestActorContext` rejects watch, behavior switching and timers; use `ActorTestKit` or the relevant behavior testing tool for these operations.

## Removed modules

`cactor_distribution` and `cactor_integration` have been removed. Depend directly on remote / persistence / cluster. The umbrella streaming prototype and duplicate logging, configuration and monitoring implementations are gone. Histogram utilities moved to foundation, testing helpers to testkit, and tests to the tests module. See [CHANGELOG](../../CHANGELOG.md). The `modules/tests/src/integration/` test directory remains.

## Visibility

Public packages expose public declarations. A `protected package` is visible only inside its module. Modules can re-export public APIs through `public import`. Same-package `*_test.cj` files can access internal members and compile only during testing.

## Build and verification

Run from the workspace root to inherit optimization and stdx settings:

```bash
source tools/env.sh
cjpm build
cjpm test
python3 tools/check_module_deps.py
tools/ci.sh
```

Check layering before adding imports; avoid bringing runtime dependencies into core through testkit or the facade.
