# CActor Package & Module Structure

CActor is a cjpm **workspace** made of 12 Cangjie **modules**. In Cangjie, a module is the tree formed by a root package and all of its subpackages, and the root package name must equal the module name. Cyclic dependencies are forbidden between packages and between modules.

## Layout

```
cactor/
├── cjpm.toml                 # [workspace]: members + shared compile options
└── modules/
    ├── foundation/           # module cactor_foundation
    │   ├── cjpm.toml         # [package] name = "cactor_foundation"
    │   └── src/
    │       ├── pkg.cj        # root package cactor_foundation
    │       └── queue/ ...    # subpackage cactor_foundation.queue
    ├── core/                 # cactor_core
    ├── patterns/             # cactor_patterns
    ├── runtime/              # cactor_runtime
    ├── cactor/               # cactor (facade: cactor / cactor.api.* / cactor.config.*)
    ├── macros/               # cactor_macros (macro package)
    ├── streams/              # cactor_streams
    ├── distribution/         # cactor_distribution
    ├── xstate/               # cactor_xstate
    ├── integration/          # cactor_integration
    ├── tests/                # cactor_tests
    └── examples/             # cactor_examples
```

## Modules and dependencies

| Module | Files / lines | Direct deps | Subpackages |
|---|---|---|---|
| `cactor_foundation` | 35 / 9,350 | — | abstractions, circuit_breaker, config, contracts, di, memory, monitoring, network, queue, serialization, simple_config |
| `cactor_core` | 31 / 6,733 | foundation | actor, config, context, error, guardian, memory, message, supervision, system, typed |
| `cactor_patterns` | 11 / 2,990 | foundation, core | ask, backpressure, routing(.advanced/.factory) |
| `cactor_runtime` | 63 / 16,185 | foundation, core, patterns | actor, context, dispatcher.*, events, execution, guardian, lifecycle, mailbox.*, message, monitoring, registry, scheduler, supervision, system, typed, wrappers |
| `cactor` | 16 / 2,140 | core, runtime | api, api.config, api.extensions, api.public, config, config.file |
| `cactor_macros` | 1 / 695 | — | (the macro package itself) |
| `cactor_streams` | 6 / 2,551 | core, cactor | core, dsl, unified |
| `cactor_distribution` | 20 / 4,993 | foundation, core, cactor | cluster, persistence, remote, streaming |
| `cactor_xstate` | 17 / 4,867 | core, cactor, distribution | core.*, features.*, enterprise.*, lang.macros |
| `cactor_integration` | 28 / 8,138 | foundation, core, runtime, cactor | configuration, logging, monitoring, testing (executables) |
| `cactor_tests` | 37 / 4,812 | core, runtime, cactor | unit.*, integration |
| `cactor_examples` | 61 / 14,015 | all of the above | 38 runnable examples |

Dependency direction (bottom-up, acyclic):

```
foundation ─► core ─► patterns ─► runtime ─► cactor ─┬─► streams
                                                     ├─► distribution ─► xstate
                                                     └─► integration
tests / examples sit on top; macros is standalone and only used by examples
```

## Visibility conventions

- **Module public API** lives in `public` packages (the default) as `public` declarations.
- **Module internals** are declared `protected package`: visible only inside the module, and any `import` from another module is a compile error. Currently protected in `cactor_runtime`: actor, context, guardian, registry, message, monitoring, execution, scheduler, mailbox.lockfree, mailbox.ringbuffer, dispatcher.batch_processing / optimized / work_stealing.
- **Re-export**: a module's root package exposes a curated API with `public import x.y.{A, B}`. For example, `cactor_patterns` re-exports `cactor_foundation.circuit_breaker.*`, so `import cactor_patterns.*` is enough.

## Commands

```bash
cjpm build                                        # build the whole workspace
cjpm test                                         # run @Test in all modules
cjpm run --name cactor_examples.hello_world_demo  # run an example from the workspace root
```

## Actual package tree

Generated from the real `modules/*/src` directories; the number in parentheses is the count of `.cj` files in that package:

```
modules/foundation/src/            # module cactor_foundation
├── abstractions/  (2)
├── circuit_breaker/  (2)
├── config/  (3)
├── contracts/  (2)
├── di/  (2)
├── memory/  (5)
├── monitoring/  (4)
├── network/  (2)
├── queue/  (6)
├── serialization/  (5)
└── simple_config/  (1)

modules/core/src/            # module cactor_core
├── actor/  (3)
├── config/  (2)
├── context/  (2)
├── error/  (2)
├── guardian/  (2)
├── memory/  (1)
├── message/  (6)
├── supervision/  (5)
├── system/  (3)
└── typed/  (4)

modules/patterns/src/            # module cactor_patterns
├── ask/  (4)
├── backpressure/  (2)
└── routing/  (2)
    ├── advanced/  (1)
    └── factory/  (1)

modules/runtime/src/            # module cactor_runtime
├── actor/  (2)
├── context/  (2)
├── dispatcher/  (3)
│   ├── adaptive/  (1)
│   ├── advanced/  (1)
│   ├── batch_processing/  (2)
│   ├── dynamic/  (1)
│   ├── factory/  (2)
│   ├── monitoring/  (3)
│   ├── optimized/  (2)
│   └── work_stealing/  (2)
├── events/  (2)
├── execution/  (1)
├── guardian/  (4)
├── lifecycle/  (2)
├── mailbox/  (5)
│   ├── advanced/  (1)
│   ├── batching/  (2)
│   ├── factory/  (1)
│   ├── lockfree/  (1)
│   ├── queue/  (2)
│   └── ringbuffer/  (2)
├── message/  (2)
├── monitoring/  (1)
├── registry/  (2)
├── scheduler/  (1)
├── supervision/  (3)
├── system/  (3)
├── typed/  (4)
└── wrappers/  (1)

modules/cactor/src/            # module cactor
├── api/  (5)
│   ├── config/  (2)
│   ├── extensions/  (4)
│   └── public/  (2)
└── config/  (1)
    └── file/  (1)

modules/macros/src/            # module cactor_macros

modules/streams/src/            # module cactor_streams
├── core/  (3)
├── dsl/  (1)
└── unified/  (1)

modules/distribution/src/            # module cactor_distribution
├── cluster/  (6)
├── persistence/  (8)
├── remote/  (2)
└── streaming/  (3)

modules/xstate/src/            # module cactor_xstate
├── core/  (1)
│   ├── actor/  (1)
│   ├── engine/  (1)
│   ├── factory/  (1)
│   ├── machine/  (1)
│   └── types/  (3)
├── enterprise/  (1)
│   ├── distributed/  (1)
│   └── event_sourcing/  (1)
├── features/  (1)
│   ├── hierarchical/  (1)
│   └── parallel/  (1)
└── lang/  (1)
    └── macros/  (1)

modules/integration/src/            # module cactor_integration
├── configuration/  (2)
├── logging/  (4)
├── monitoring/  (6)
└── testing/  (4)
    ├── enhanced_dispatcher_test/  (1)
    ├── plan10_actorcontext_test/  (1)
    ├── plan10_advanced_supervision_test/  (1)
    ├── plan10_api_test/  (1)
    ├── plan10_cactor_runtime_test/  (1)
    ├── plan10_comprehensive_test/  (1)
    ├── plan10_router_test/  (1)
    ├── simple_benchmark_test/  (1)
    ├── simple_plan10_test/  (1)
    ├── simple_realistic_benchmark/  (1)
    └── unified_cactor_system_test/  (1)

modules/tests/src/            # module cactor_tests
├── integration/  (4)
└── unit/  (1)
    ├── core/  (1)
    └── runtime/  (1)

modules/examples/src/            # module cactor_examples
└── <45  example packages>/
```

## Appendix: original layered design diagrams

These are the pre-modularization design diagrams, kept as an architectural reference. `src/<layer>/` now maps to `modules/<layer>/src/` and `cactor.<layer>` to `cactor_<layer>`. Some directories (e.g. `foundation/concurrency/`) were planned in the design but never implemented that way; the actual tree above is authoritative.

### 📦 Overall Package Structure

CActor adopts a 6-layer modular architecture with clear responsibilities and boundaries for each layer:

```
src/
├── cactor.cj                    # Main package export file
├── foundation/                  # Foundation Layer
├── core/                       # Core Layer
├── runtime/                    # Runtime Layer
├── patterns/                   # Patterns Layer
├── distribution/               # Distribution Layer
├── integration/                # Integration Layer
├── api/                        # API Layer
├── macros/                     # Macro system
├── config/                     # Configuration files
└── examples/                   # Example code
```

### 🏗️ Detailed Package Structure

#### 1. Foundation Layer

```
foundation/
├── pkg.cj                      # Package export file
├── concurrency/                # Concurrency primitives
│   ├── pkg.cj
│   ├── lockfree_queue.cj      # Lock-free queue implementation
│   ├── spsc_queue.cj          # Single Producer Single Consumer queue
│   ├── mpsc_queue.cj          # Multiple Producer Single Consumer queue
│   ├── atomic_operations.cj    # Atomic operations wrapper
│   └── thread_pool.cj         # Thread pool implementation
├── serialization/              # Serialization framework
│   ├── pkg.cj
│   ├── serializer.cj          # Serialization interface
│   ├── json_serializer.cj     # JSON serializer
│   ├── binary_serializer.cj   # Binary serializer
│   └── serialization_manager.cj # Serialization manager
├── network/                    # Network communication
│   ├── pkg.cj
│   ├── network_transport.cj    # Network transport interface
│   ├── tcp_transport.cj       # TCP transport implementation
│   ├── udp_transport.cj       # UDP transport implementation
│   └── connection_pool.cj     # Connection pool
└── memory/                     # Memory management
    ├── pkg.cj
    ├── object_pool.cj         # Object pool
    ├── memory_pool.cj         # Memory pool
    ├── numa_memory_pool.cj    # NUMA-aware memory pool
    └── smart_gc.cj            # Smart garbage collection
```

#### 2. Core Layer

```
core/
├── pkg.cj                      # Package export file
├── actor/                      # Actor abstractions
│   ├── pkg.cj
│   ├── actor.cj               # Actor interface
│   ├── actor_ref.cj           # Actor reference
│   ├── actor_path.cj          # Actor path
│   ├── props.cj               # Actor properties
│   └── actor_lifecycle.cj     # Actor lifecycle
├── message/                    # Message abstractions
│   ├── pkg.cj
│   ├── message.cj             # Message interface
│   ├── envelope.cj            # Message envelope
│   ├── string_message.cj      # String message
│   ├── json_message.cj        # JSON message
│   ├── network_message.cj     # Network message
│   ├── zerocopy_message.cj    # Zero-copy message
│   ├── enhanced_message.cj    # Enhanced message
│   └── message_serializer.cj  # Message serializer
├── system/                     # System abstractions
│   ├── pkg.cj
│   ├── actor_system.cj        # Actor system interface
│   ├── system_guardian.cj     # System guardian
│   └── dead_letters.cj        # Dead letter handling
├── context/                    # Context
│   ├── pkg.cj
│   ├── actor_context.cj       # Actor context interface
│   └── context_impl.cj        # Context implementation
├── supervision/                # Supervision strategies
│   ├── pkg.cj
│   ├── supervision_strategy.cj # Supervision strategy
│   ├── supervision_directive.cj # Supervision directive
│   └── fault_handling.cj      # Fault handling
└── config/                     # Core configuration
    ├── pkg.cj
    ├── actor_config.cj        # Actor configuration
    ├── mailbox_config.cj      # Mailbox configuration
    ├── dispatcher_config.cj   # Dispatcher configuration
    └── supervision_config.cj  # Supervision configuration
```

#### 3. Runtime Layer

```
runtime/
├── pkg.cj                      # Package export file
├── cactor_runtime.cj          # Main runtime manager
├── plan10_cactor_runtime.cj   # Plan10 runtime implementation
├── dispatcher/                 # Dispatcher implementations
│   ├── pkg.cj
│   ├── message_dispatcher.cj   # Dispatcher interface
│   ├── thread_pool_dispatcher.cj # Thread pool dispatcher
│   ├── work_stealing_dispatcher.cj # Work-stealing dispatcher
│   ├── pinned_dispatcher.cj    # Pinned thread dispatcher
│   ├── numa_dispatcher.cj     # NUMA-aware dispatcher
│   ├── batch_processing/      # Batch processing
│   │   ├── batch_processor.cj
│   │   └── batch_config.cj
│   └── advanced/              # Advanced dispatchers
│       ├── foundation_based_dispatcher.cj
│       └── adaptive_dispatcher.cj
├── mailbox/                    # Mailbox implementations
│   ├── pkg.cj
│   ├── mailbox.cj             # Mailbox interface
│   ├── unbounded_mailbox.cj   # Unbounded mailbox
│   ├── bounded_mailbox.cj     # Bounded mailbox
│   ├── priority_mailbox.cj    # Priority mailbox
│   ├── foundation_mailbox.cj  # Foundation mailbox
│   └── mailbox_factory.cj     # Mailbox factory
├── actor/                      # Runtime actors
│   ├── pkg.cj
│   ├── high_performance_actor.cj # High-performance actor
│   └── actor_system_impl.cj   # Actor system implementation
├── context/                    # Runtime context
│   ├── pkg.cj
│   ├── pooled_actor_context.cj # Pooled actor context
│   └── context_factory.cj     # Context factory
├── lifecycle/                  # Lifecycle management
│   ├── pkg.cj
│   ├── lifecycle_manager.cj   # Lifecycle manager
│   └── actor_lifecycle_state.cj # Actor lifecycle state
├── monitoring/                 # Runtime monitoring
│   ├── pkg.cj
│   ├── actor_system_metrics.cj # System metrics
│   └── performance_monitor.cj  # Performance monitor
├── guardian/                   # Guardian implementations
│   ├── pkg.cj
│   ├── runtime_system_guardian.cj # System guardian
│   └── runtime_user_guardian.cj # User guardian
├── registry/                   # Actor registry
│   ├── pkg.cj
│   └── simple_actor_registry.cj # Simple registry
├── supervision/                # Supervision implementation
│   ├── pkg.cj
│   └── supervision_manager.cj  # Supervision manager
├── events/                     # Event system
│   ├── pkg.cj
│   ├── actor_event_bus.cj     # Actor event bus
│   └── simple_actor_event_bus.cj # Simple event bus
└── message/                    # Runtime messages
    ├── pkg.cj
    └── batch_message_processor.cj # Batch message processor
```

#### 4. Patterns Layer

```
patterns/
├── pkg.cj                      # Package export file
├── ask/                        # Ask pattern
│   ├── pkg.cj
│   ├── ask_pattern.cj         # Ask pattern implementation
│   ├── ask_future.cj          # Ask future
│   └── ask_timeout.cj         # Ask timeout handling
├── routing/                    # Routing patterns
│   ├── pkg.cj
│   ├── router.cj              # Router interface
│   ├── round_robin_router.cj  # Round-robin router
│   ├── random_router.cj       # Random router
│   ├── consistent_hash_router.cj # Consistent hash router
│   └── advanced/              # Advanced routing
│       ├── advanced_routing.cj
│       └── load_balancer.cj
├── circuit_breaker/            # Circuit breaker pattern
│   ├── pkg.cj
│   ├── circuit_breaker.cj     # Circuit breaker implementation
│   └── circuit_breaker_config.cj # Circuit breaker configuration
├── supervision/                # Supervision patterns
│   ├── pkg.cj
│   ├── supervision_patterns.cj # Supervision patterns
│   └── fault_tolerance.cj     # Fault tolerance
└── backpressure/              # Backpressure patterns
    ├── pkg.cj
    ├── backpressure_strategy.cj # Backpressure strategy
    └── flow_control.cj        # Flow control
```

#### 5. Distribution Layer

```
distribution/
├── pkg.cj                      # Package export file
├── remote/                     # Remote communication
│   ├── pkg.cj
│   ├── remote_actor_ref.cj    # Remote actor reference
│   ├── remote_transport.cj    # Remote transport
│   └── remote_deployment.cj   # Remote deployment
├── cluster/                    # Cluster management
│   ├── pkg.cj
│   ├── cluster_manager.cj     # Cluster manager
│   ├── node_discovery.cj      # Node discovery
│   ├── cluster_state.cj       # Cluster state
│   ├── failover.cj            # Failover
│   └── sharding.cj            # Sharding support
├── persistence/                # Persistence
│   ├── pkg.cj
│   ├── persistent_actor.cj    # Persistent actor
│   ├── event_store.cj         # Event store
│   ├── snapshot.cj            # Snapshot mechanism
│   └── recovery.cj            # Recovery mechanism
└── streaming/                  # Stream processing
    ├── pkg.cj
    ├── stream_processing.cj   # Stream processing
    ├── stream_builder.cj      # Stream builder
    └── reactive_streams.cj    # Reactive streams
```

#### 6. Integration Layer

```
integration/
├── pkg.cj                      # Package export file
├── monitoring/                 # Monitoring integration
│   ├── pkg.cj
│   ├── metrics.cj             # Metrics collection
│   ├── distributed_tracing.cj # Distributed tracing
│   ├── memory_monitor.cj      # Memory monitoring
│   └── performance_analyzer.cj # Performance analyzer
├── logging/                    # Logging integration
│   ├── pkg.cj
│   ├── actor_logger.cj        # Actor logger
│   └── structured_logging.cj  # Structured logging
├── configuration/              # Configuration management
│   ├── pkg.cj
│   ├── config_manager.cj      # Configuration manager
│   └── hot_reload.cj          # Hot reload
└── testing/                    # Testing framework
    ├── pkg.cj
    ├── test_actor_system.cj   # Test actor system
    ├── test_probe.cj          # Test probe
    ├── simple_benchmark.cj    # Simple benchmark
    ├── simple_benchmark_test/ # Benchmark test
    ├── plan10_comprehensive_test/ # Comprehensive test
    ├── plan10_cactor_runtime_test/ # Runtime test
    └── plan10_api_test/       # API test
```

#### 7. API Layer

```
api/
├── pkg.cj                      # Package export file
├── cactor.cj                  # Unified API entry
├── cactor_system_api.cj       # Actor system API
├── config/                     # API configuration
│   ├── pkg.cj
│   ├── mailbox_config_api.cj  # Mailbox configuration API
│   ├── dispatcher_config_api.cj # Dispatcher configuration API
│   └── supervision_config_api.cj # Supervision configuration API
└── public/                     # Public APIs
    ├── pkg.cj
    ├── cactor_factory.cj      # CActor factory
    └── cactor_system_builder.cj # System builder
```

#### 8. Other Modules

```
macros/                         # Macro system
├── actor_dsl_macros.cj        # Actor DSL macros

config/                         # Configuration files
└── file/
    ├── config_loader.cj       # Configuration loader
    └── actor_system_config.cj # System configuration

examples/                       # Example code
├── hello_world/               # Basic examples
├── api_demo/                  # API demonstration
├── benchmark_demo/            # Performance examples
├── advanced_dispatcher_examples/ # Advanced dispatcher examples
├── advanced_mailbox_examples/ # Advanced mailbox examples
└── configuration_usage_test/  # Configuration usage test
```

### 🔗 Package Dependencies

#### Dependency Hierarchy

```
API Layer
    ↓ (depends on)
Integration Layer
    ↓ (depends on)
Distribution Layer
    ↓ (depends on)
Patterns Layer
    ↓ (depends on)
Runtime Layer
    ↓ (depends on)
Core Layer
    ↓ (depends on)
Foundation Layer
```

#### Package Import Rules

1. **Downward Dependencies**: Upper layers can import lower layer packages
2. **No Upward Dependencies**: Lower layers cannot import upper layer packages
3. **Same Layer Dependencies**: Packages within the same layer can import each other
4. **API Isolation**: Users only import API layer, not directly using Core layer

### 📋 Package Responsibilities

#### Foundation Layer
- **Responsibility**: Provide infrastructure and utilities
- **Characteristics**: Zero dependencies, can be used independently
- **Contains**: Concurrency primitives, serialization, network, memory management

#### Core Layer
- **Responsibility**: Define core abstractions and interfaces
- **Characteristics**: Pure abstractions, no concrete implementations
- **Contains**: Actor, message, system, context abstractions

#### Runtime Layer
- **Responsibility**: Provide high-performance execution engine
- **Characteristics**: Concrete implementations, performance optimized
- **Contains**: Dispatchers, mailboxes, lifecycle management

#### Patterns Layer
- **Responsibility**: Implement common Actor patterns
- **Characteristics**: Optional features, use as needed
- **Contains**: Ask, routing, circuit breaker, backpressure

#### Distribution Layer
- **Responsibility**: Provide distributed capabilities
- **Characteristics**: Enterprise-grade features
- **Contains**: Remote communication, cluster, persistence, stream processing

#### Integration Layer
- **Responsibility**: Provide integration and tooling
- **Characteristics**: Development and operations support
- **Contains**: Monitoring, logging, configuration, testing

#### API Layer
- **Responsibility**: Provide unified external interface
- **Characteristics**: Simple and easy to use, hide complexity
- **Contains**: Unified API, configuration API, public API

---

**CActor package structure design ensures clear separation of concerns and high modularity!** 🚀

