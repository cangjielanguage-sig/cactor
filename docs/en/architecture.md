# CActor 1.2 Architecture Design

**Version:** 1.2
**Language:** Cangjie
**Inspiration:** Akka / Akka Typed
**Source Files:** 326 `.cj` files
**Test Coverage:** 1616 test cases, 100% pass rate

---

## Table of Contents

1. [Overview](#1-overview)
2. [6+1 Layer Architecture](#2-61-layer-architecture)
3. [Foundation Layer](#3-foundation-layer)
4. [Core Layer](#4-core-layer)
5. [Runtime Layer](#5-runtime-layer)
6. [Patterns Layer](#6-patterns-layer)
7. [Distribution Layer](#7-distribution-layer)
8. [Integration Layer](#8-integration-layer)
9. [API Layer](#9-api-layer)
10. [Core Subsystems](#10-core-subsystems)
11. [Design Principles](#11-design-principles)
12. [Configuration Presets](#12-configuration-presets)

---

## 1. Overview

CActor is a world-class, high-performance Actor system implemented in the Cangjie language. Inspired by Akka and Akka Typed, it provides a robust, scalable, and type-safe concurrency model built on a **6+1 layered architecture**.

### Key Characteristics

- **Actor Model**: Full implementation of the Actor concurrency model with typed and untyped variants.
- **Supervision Hierarchies**: Built-in fault tolerance through hierarchical supervision strategies.
- **High-Performance Mailboxes**: Multiple mailbox implementations including lock-free, batching, and priority queues.
- **Adaptive Dispatching**: Multiple dispatcher strategies including work-stealing and adaptive dispatching.
- **Distribution and Clustering**: Support for remote actors, sharding, and persistence.
- **Zero-Dependency Foundation**: The foundation layer has no external dependencies, ensuring portability and predictability.

### Architecture at a Glance

```
+----------------------------------------------------------------------+
|                         API Layer                                     |
|           CActor · CActorSystem · CActorFactory · Builder            |
+----------------------------------------------------------------------+
|                     Integration Layer                                 |
|            ConfigurationManager · TestActorSystem                    |
+----------------------------------------------------------------------+
|                     Distribution Layer                                 |
|            Cluster · Persistence · Remote · Streaming                 |
+----------------------------------------------------------------------+
|                       Patterns Layer                                   |
|           Ask · Backpressure · CircuitBreaker · Routing               |
+----------------------------------------------------------------------+
|                        Runtime Layer                                  |
|     Actor · Dispatcher · Mailbox · Lifecycle · Events · Typed        |
+----------------------------------------------------------------------+
|                         Core Layer                                    |
|      Actor · Context · Message · Supervision · System · Typed         |
+----------------------------------------------------------------------+
|                      Foundation Layer                                 |
|          Queue · Serialization · Memory · Network · Monitoring · DI  |
+----------------------------------------------------------------------+
```

---

## 2. 6+1 Layer Architecture

The architecture follows a strict **6+1 layered model** where each layer depends only on the layers beneath it. This ensures testability, maintainability, and clear separation of concerns.

| Layer | Files | Purpose |
|---|---|---|
| **Foundation** | 33 | Zero-dependency infrastructure (queues, memory, serialization, DI, monitoring) |
| **Core** | 31 | Abstract interfaces only (Actor, ActorRef, Props, Supervision, System) |
| **Runtime** | 63 | Concrete implementations (dispatchers, mailboxes, lifecycle management) |
| **Patterns** | 13 | Reusable distributed-computing patterns (ask, routing, circuit breaker) |
| **Distribution** | 21 | Cross-node communication, clustering, persistence, streaming |
| **Integration** | 28 | External integrations (configuration management, testing) |
| **API** | 13 | End-user facing public API (CActor, CActorSystem, builders) |

### Dependency Rules

```
API -> Integration -> Distribution -> Patterns -> Runtime -> Core -> Foundation
```

- **Foundation** has **no dependencies** on any other layer.
- **Core** depends only on **Foundation**.
- **Runtime** depends on **Core** and **Foundation**.
- **Patterns** depends on **Core** and **Runtime**.
- **Distribution** depends on **Runtime** and **Patterns**.
- **Integration** depends on all layers below.
- **API** is the public facade, depending on all layers.

---

## 3. Foundation Layer

**Path:** `modules/foundation/src/`
**Files:** 33
**Constraint:** Zero external dependencies.

The Foundation layer provides the essential building blocks that all upper layers depend on. It is designed for maximum portability, predictability, and performance.

### 3.1 Module Structure

```
modules/foundation/src/
|-- abstractions/     # General-purpose abstract interfaces
|-- config/          # Unified configuration framework
|-- contracts/       # Module contracts and invariants
|-- di/             # Dependency injection container
|-- memory/         # Object pools, NUMA pools, shared memory buffers
|-- monitoring/     # Metrics, Tracing, Alerting (MTA)
|-- network/        # Network transport abstraction
|-- queue/          # Concurrent queue implementations
+-- serialization/   # Byte, String, JSON, Binary serializers
```

### 3.2 Queue Subsystem

The queue subsystem is one of the most performance-critical components in CActor, directly affecting message-passing throughput.

#### LockFreeQueue

A lock-free ring-buffer queue supporting multiple concurrency patterns:

- **SPSC**: Single Producer, Single Consumer -- highest throughput, no synchronization overhead.
- **MPSC**: Multiple Producers, Single Consumer -- used for actor mailboxes with multiple senders.
- **SPMC**: Single Producer, Multiple Consumers -- used for event bus subscriptions.
- **MPMC**: Multiple Producers, Multiple Consumers -- general-purpose, most flexible.

**Key Design Decisions:**

- **Ring buffer base**: Fixed-capacity circular buffer eliminates memory allocation during enqueue/dequeue.
- **Auto-switching**: The queue detects its producer/consumer topology and applies the optimal lock-free algorithm.
- **Cache line padding**: Each queue slot is padded to a full cache line (typically 64 bytes) to prevent false sharing between cores.

```
LockFreeQueue Internal Structure:

  RingBuffer (fixed-size array, power-of-2 length)
  +---+---+---+---+---+---+---+---+---
  | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
  +---+---+---+---+---+---+---+---+---
    ^                               ^
    +-- Head (consumer index)       +-- Tail (producer index)
```

### 3.3 Memory Subsystem

Provides memory management primitives optimized for the Actor model:

- **ObjectPool**: Reusable object pool to reduce GC pressure and allocation latency.
- **NUMAMemoryPool**: NUMA-aware memory allocation, pinning memory to the NUMA node where the actor runs.
- **SharedMemoryBuffer**: Shared memory buffers for inter-process communication.

### 3.4 Serialization Subsystem

Four serializer tiers, each optimized for a different use case:

| Serializer | Use Case | Characteristics |
|---|---|---|
| **ByteSerializer** | Raw bytes | Zero overhead, pass-through |
| **StringSerializer** | Text data | UTF-8 optimized |
| **JSONSerializer** | Interoperability | Human-readable, schema-less |
| **BinarySerializer** | Wire protocol | Highest density, versioned schema |

### 3.5 Dependency Injection

A lightweight DI container supporting:

- Constructor injection
- Singleton and transient scopes
- Factory registration
- Circular dependency detection

### 3.6 Monitoring

The MTA (Metrics, Tracing, Alerting) subsystem:

- **Metrics**: Counters, gauges, histograms, timers -- exported for Prometheus, StatsD, etc.
- **Tracing**: Distributed trace propagation across actor message flows.
- **Alerting**: Rule-based alerting on metric thresholds.

---

## 4. Core Layer

**Path:** `modules/core/src/`
**Files:** 31
**Constraint:** Abstract interfaces only. No concrete implementations.

The Core layer defines the **contract** of the Actor system. Every concept in CActor is introduced here with its abstract interface, allowing runtime implementations to be swapped, mocked, or extended.

### 4.1 Module Structure

```
modules/core/src/
|-- actor/       # Actor interface, ActorRef, ActorPath, Props, ActorFactory
|-- config/      # ActorConfig, MailboxConfig, DispatcherConfig, SupervisionConfig
|-- context/     # ActorContext interface
|-- error/       # Error handling
|-- guardian/    # GuardianActor, GuardianType enum
|-- message/     # Message interface, SystemMessage, UserMessage
|-- supervision/ # OneForOne, OneForAll, Escalate, BackoffSupervisor
|-- system/      # ActorSystem interface, ActorSelection, Scheduler
+-- typed/       # TypedBehavior<T>, TypedActorRef<T>, TypedActorContext<T>
```

### 4.2 The Actor Interface

The central abstraction of CActor:

```cangjie
// Core Actor interface
public interface Actor {
    // Main message handler -- returns MessageResult
    func receive(message: Message, context: ActorContext): MessageResult

    // Lifecycle hooks
    func preStart(): Unit
    func postStop(): Unit
    func preRestart(reason: Error, message: Option<Message>): Unit
    func postRestart(reason: Error): Unit
}
```

### 4.3 Actor Lifecycle States

```
                   start
  +---------+   +---------+   +---------+
  | Created |-->| Starting|-->| Running  |
  +---------+   +---------+   +----+----+
                                        |
                     stop               |
  +---------+   +---------+             |
  | Stopped |<--| Stopping |<------------+
  +---------+   +---------+   restart

  +---------+  failure   +-------------+
  |  Failed |----------->| Restarting  |
  +---------+            +-------------+
```

| State | Description |
|---|---|
| **Created** | Actor instance created, not yet started |
| **Starting** | `preStart()` is executing |
| **Running** | Fully operational, processing messages |
| **Stopping** | `postStop()` is executing |
| **Stopped** | Actor has terminated |
| **Failed** | An unhandled exception occurred |
| **Restarting** | Supervision is restarting the actor |

### 4.4 Message Model

CActor distinguishes between two message categories based on priority:

| Message Type | Priority | Description |
|---|---|---|
| **SystemMessage** | 1000 | Internal system messages (e.g., stop, restart, watch) |
| **UserMessage** | 0 | Application-level messages |

System messages always preempt user messages in the mailbox, ensuring administrative messages are never delayed behind application traffic.

### 4.5 Props

`Props` is an immutable configuration object for creating actors:

```cangjie
// Props configuration object
public class Props {
    public let creator: ActorCreator          // Factory function for the actor
    public let mailboxConfig: MailboxConfig    // Mailbox type and capacity
    public let dispatcherConfig: DispatcherConfig  // Dispatcher assignment
    public let supervisionConfig: SupervisionConfig // Supervision strategy
}
```

### 4.6 ActorContext

Provides the actor's view of the system:

```cangjie
public interface ActorContext {
    // Identity
    public prop self: ActorRef { get }       // The actor's own reference
    public prop parent: ActorRef { get }     // Supervisor reference
    public prop children: List<ActorRef> { get }  // Child actor references

    // Message dispatch
    public func tell(ref: ActorRef, message: Message): Unit
    public func ask(ref: ActorRef, message: Message): Future[?]

    // Child actor management
    public func spawn(props: Props, name: String): ActorRef
    public func spawnAnonymous(props: Props): ActorRef
    public func stop(ref: ActorRef): Unit

    // Actor discovery
    public func actorSelection(path: String): ActorSelection

    // Lifecycle
    public func watch(ref: ActorRef): Unit     // Monitor another actor
    public func unwatch(ref: ActorRef): Unit   // Stop monitoring
}
```

### 4.7 Supervision Strategies

CActor implements the "let it crash" philosophy through hierarchical supervision:

| Strategy | Behavior |
|---|---|
| **OneForOne** | Only the failed child is restarted |
| **OneForAll** | All siblings are restarted along with the failed child |
| **Escalate** | The failure is escalated to the supervisor's supervisor |
| **BackoffSupervisor** | Restart with exponential backoff for transient failures |

Each strategy is parameterized with:

- `maxRetries`: Maximum restart attempts before giving up
- `withinTimeRange`: Time window for counting retries
- `decider`: A function `Error -> Directive` mapping errors to directives

### 4.8 Typed Actor System

CActor Typed provides compile-time safety for actor messaging:

```cangjie
// Typed behavior wrapping message handling with type safety
public interface TypedBehavior[T] {
    public func receive(message: T, context: TypedActorContext[T]): MessageResult
}

// Type-safe actor reference
public interface TypedActorRef[T] {
    public func tell(message: T): Unit
    public func ask(message: T): Future[T]
}

// Typed variant of ActorContext
public interface TypedActorContext[T] {
    public prop self: TypedActorRef[T] { get }
}
```

This design ensures an actor can only receive messages of its declared type `T`, preventing entire categories of runtime errors.

### 4.9 ActorSystem Interface

```cangjie
public interface ActorSystem {
    public prop name: String { get }
    public prop startTime: DateTime { get }

    // Actor creation
    public func actorOf(props: Props, name: String): ActorRef
    public func typedActorOf[T](behavior: TypedBehavior[T], name: String): TypedActorRef[T]

    // Guardians
    public prop systemGuardian: ActorRef    // Root guardian for system actors
    public prop userGuardian: ActorRef     // Root guardian for user actors

    // Scheduler
    public prop scheduler: Scheduler

    // Coordinated shutdown
    public func coordinatedShutdown(): CoordinatedShutdown

    // Actor discovery
    public func actorSelection(path: String): ActorSelection
}
```

### 4.10 Guardian Actors

CActor maintains two top-level guardian actors:

| Guardian | Type | Responsibility |
|---|---|---|
| **SystemGuardian** | `GuardianType.System` | All system-level actors (log, event bus, etc.) |
| **UserGuardian** | `GuardianType.User` | All user-created top-level actors |

```
                /user
                   |
        +----------+----------+
        |          |          |
     Actor1    Actor2    Actor3

                /system
                   |
        +----------+----------+
        |          |          |
    LogActor  EventBus  Scheduler
```

---

## 5. Runtime Layer

**Path:** `modules/runtime/src/`
**Files:** 63
**Constraint:** Concrete implementations of Core interfaces.

The Runtime layer is where CActor comes alive. It contains the high-performance implementations that power the actor system.

### 5.1 Module Structure

```
modules/runtime/src/
|-- actor/       # High-performance Actor implementation
|-- context/     # Pooled ActorContext implementation
|-- dispatcher/  # Thread pool, work-stealing, adaptive, batch, dynamic
|-- events/      # Event bus implementation
|-- lifecycle/   # CoordinatedShutdown, ActorLifecycleManager
|-- mailbox/     # Foundation, LockFree, Batching, Priority, RingBuffer
|-- typed/      # Type-safe runtime implementations
+-- wrappers/    # TypedActorWrapper, MessageBridge
```

### 5.2 CActorRuntime -- The Central Integrator

`CActorRuntime` is the central implementation class that wires together all runtime components:

```
CActorRuntime
|-- SystemGuardian             # Root guardian (system)
|-- UserGuardian               # Root guardian (user)
|-- AdvancedMailboxFactory     # Mailbox creation
|-- AdvancedDispatcherFactory  # Dispatcher creation
|-- HighPerformanceActorSystem # Actor system implementation
|-- ActorLifecycleManager      # Lifecycle state machine
|-- SimpleActorRegistry        # Actor path -> ActorRef mapping
+-- SupervisionManager         # Supervision decision execution
```

### 5.3 Mailbox Implementations

CActor provides five mailbox implementations, each optimized for a different scenario:

| Mailbox | Use Case | Key Characteristics |
|---|---|---|
| **FoundationMailbox** | General purpose | Based on `LockFreeQueue`, the default |
| **LockFreeMailbox** | High throughput | Minimal contention, SPSC/MPSC auto-switching |
| **BatchingMailbox** | Batch processing | Aggregates messages before delivery |
| **PriorityMailbox** | Priority-based | Supports message priority (System > User) |
| **RingBufferMailbox** | Extreme throughput | Lock-free ring buffer, single consumer |

### 5.4 Dispatcher Implementations

Six dispatcher strategies covering the full performance spectrum:

| Dispatcher | Description | Best For |
|---|---|---|
| **ThreadPoolDispatcher** | Fixed thread pool | General workloads |
| **WorkStealingDispatcher** | Work-stealing thread pool | Load-balanced, CPU-bound |
| **AdaptiveDispatcher** | Dynamically adjusts pool size | Variable load |
| **DynamicDispatcher** | On-demand thread creation | Burst workloads |
| **BatchProcessingDispatcher** | Optimized for batch jobs | High-throughput batch processing |
| **FoundationBasedDispatcher** | Foundation layer integration | Low-latency requirements |

The dispatcher is responsible for:

1. Picking messages from actor mailboxes.
2. Assigning them to threads for execution.
3. Managing thread pools (creation, scaling, teardown).

### 5.5 Message Flow

```
Producer                          CActorRuntime
   |                                    |
   |  tell(ActorRef, Message)           |
   | --------------------------------> |
   |                                    |
   |                    +----------------------------+
   |                    |     ActorRegistry           |
   |                    |   (path -> ActorRef map)   |
   |                    +------------+---------------+
   |                                 |
   |                    +------------+---------------+
   |                    |  AdvancedMailboxFactory    |
   |                    |  (creates mailbox by Props)|
   |                    +------------+---------------+
   |                                 |
   |                    +------------+---------------+
   |                    |   LockFreeMailbox          |
   |                    |   (SPSC ring buffer)       |
   |                    |   [ SystemMsg ] <--------- priority
   |                    |   [ SystemMsg ]             |
   |                    |   [ UserMsg   ]              |
   |                    |   [ UserMsg   ]              |
   |                    +------------+---------------+
   |                                 |
   |                    +------------+---------------+
   |                    | AdvancedDispatcherFactory  |
   |                    | (work-stealing thread pool)|
   |                    +------------+---------------+
   |                                 |
   |                    +------------+---------------+
   |                    |  HighPerformanceActor       |
   |                    |  receive(msg, context)     |
   |                    |  -> MessageResult          |
   |                    +----------------------------+
```

### 5.6 Lifecycle Management

The `ActorLifecycleManager` manages the state transitions of all actors:

- Validates all state transitions against the lifecycle state machine.
- Coordinates `preStart` / `postStop` hooks with supervision decisions.
- Notifies watchers when an actor terminates.

### 5.7 Typed Runtime

The typed runtime provides type-safe actor interactions:

- `TypedActorWrapper`: Wraps an untyped actor with a type parameter.
- `MessageBridge`: Bridges between typed and untyped actor systems.

---

## 6. Patterns Layer

**Path:** `modules/patterns/src/`
**Files:** 13
**Constraint:** Depends on Core and Runtime.

This layer implements well-known distributed-computing patterns on top of CActor's core abstractions.

### 6.1 Module Structure

```
modules/patterns/src/
|-- ask/             # AskFuture<T>, AskPatternManager, AskReplyActor
|-- backpressure/   # Drop, Block, Buffer, Reject, Throttle strategies
|-- circuit_breaker/ # Three-state circuit breaker
+-- routing/        # Router implementations
```

### 6.2 Ask Pattern

The Ask pattern provides request-response semantics over the actor model:

```cangjie
// ask returns a Future[T] for typed response
let future: Future[Response] = ask(actorRef, Request("data"))
let response: Response = future.await()
```

Components:

- **AskFuture<T>**: A future that resolves when the actor replies.
- **AskPatternManager**: Manages ask timeouts and correlation IDs.
- **AskReplyActor**: A temporary actor that receives the reply and completes the future.

### 6.3 Backpressure Strategies

Backpressure prevents fast producers from overwhelming slow consumers:

| Strategy | Behavior |
|---|---|
| **Drop** | Silently drop messages when the mailbox is full |
| **Block** | Block the sender until space is available |
| **Buffer** | Buffer in an intermediate bounded queue |
| **Reject** | Reject the message and notify the sender |
| **Throttle** | Rate-limit messages to a configured rate |

The `BasicBackpressureController` provides a unified interface to these strategies.

### 6.4 Circuit Breaker

The circuit breaker pattern prevents cascading failures:

```
              +-----------+
              |  Closed   |  Normal operation
              |(All calls |  Failures counted
              |  pass)    |
              +-----+-----+
                    | failure threshold exceeded
                    v
              +-----------+
              |   Open    |  Fast-fail all calls
              |(Rejecting |  Start timeout timer
              | requests) |
              +-----+-----+
                    | timeout elapsed
                    v
              +-----------+
              | HalfOpen  |  Allow one test call
              |(Testing)  |
              +-----+-----+
                    | success
                    v
              +-----------+
              |  Closed   |
              +-----------+
```

- **Closed**: Normal operation. Failures are counted.
- **Open**: After N failures, the circuit opens and all requests fail immediately.
- **HalfOpen**: After a cooldown period, one test request is allowed through.

The `CircuitBreakerDecorator<T>` wraps any actor reference with circuit breaker protection.

### 6.5 Routing

Routers distribute messages across multiple routees:

| Router Type | Algorithm |
|---|---|
| **RoundRobin** | Cyclic distribution |
| **Random** | Uniform random distribution |
| **SmallestMailbox** | Route to the least-busy actor |
| **Broadcast** | Deliver to all routees |
| **ConsistentHash** | Hash-based routing for stateless partitioning |

The router creates a **RouterActorRef** which multiplexes messages to the managed pool of routees.

---

## 7. Distribution Layer

**Path:** `modules/distribution/src/`
**Files:** 21
**Constraint:** Depends on Runtime and Patterns.

The Distribution layer enables CActor to span multiple nodes and persist actor state.

### 7.1 Module Structure

```
modules/distribution/src/
|-- cluster/     # ShardCoordinator, ShardAllocationStrategy
|-- persistence/ # EventStore, PersistentEvent, PersistentActor
|-- remote/      # Simple remote actor communication
+-- streaming/   # Distributed stream processing
```

### 7.2 Remote Communication

The `remote` module provides transparent actor communication across process boundaries:

```
+-----------------+     Network      +-----------------+
|  CActor Node A  |<--------------- >|  CActor Node B  |
|                 | (serialization  |                 |
| +-----------+ +|  + transport)    | +-----------+ +|
| |LocalActor | | |                  | |LocalActor | | |
| +-----+-----+ | |                  | +-----+-----+ | |
|       |       | |                  |       |       | |
| +-----v-----+ | |                  | +-----v-----+ | |
| | RemoteProxy| | |                  | | RemoteProxy| | |
| +-----------+ +|                  | +-----------+ +|
+---------+------+                  +---------+------+
          |                                   |
          +--------- ActorPath over network --+
```

### 7.3 Clustering and Sharding

**ShardCoordinator** manages the distribution of actors (shards) across the cluster:

- **LeastShardsAllocationStrategy**: Assigns shards to the node with the fewest shards.
- Shards are identified by a `ShardId`, enabling location-transparent actor lookups.

```
Cluster Nodes:   Node-1     Node-2     Node-3
                     |          |          |
                 Shard-1   Shard-2   Shard-3
                 Shard-4   Shard-5   Shard-6
                 Shard-7   Shard-8   Shard-9
                             ^
               LeastShards: Node-2 gets assigned next shard
```

### 7.4 Persistence

The persistence subsystem provides at-least-once message delivery and event sourcing:

- **EventPayload**: Wrapper for persisted events with metadata.
- **PersistentEvent**: An event that can be replayed.
- **EnhancedEventStore**: Storage backend for events.
- **PersistentActor**: An actor that can persist and replay its state.

```
PersistentActor Message Flow:

  incoming msg
       |
       v
  +----------------------------------+
  |  PersistentActor                 |
  |  1. persist(event)               |
  |  2. EventStore.write(event)     |
  |  3. apply(event) -> update state |
  |  4. replay() on restart          |
  +----------------------------------+
```

### 7.5 Distributed Streaming

Distributed stream processing enables high-throughput, low-latency data pipelines across the cluster with backpressure support.

---

## 8. Integration Layer

**Path:** `modules/integration/src/`
**Files:** 28
**Constraint:** Depends on all layers below.

The Integration layer connects CActor with external systems and testing infrastructure.

### 8.1 Module Structure

```
modules/integration/src/
|-- configuration/  # ConfigurationManager, ConfigValue, GlobalConfigManager
+-- testing/        # TestActorSystem, TestProbe, TestTimeController
```

### 8.2 Configuration Management

CActor's configuration system is hierarchical and composable:

```cangjie
// Configuration layers (later layers override earlier ones)
GlobalConfigManager
  +-- Reference.conf (classpath resource)
  +-- application.conf (user config)
  +-- Environment variables (overrides)
  +-- Programmatic overrides (highest priority)
```

- **ConfigValue**: A typed, path-based configuration value.
- **ConfigurationManager**: Resolves and merges configuration from multiple sources.
- **GlobalConfigManager**: Process-wide singleton configuration registry.

### 8.3 Testing Framework

CActor provides a complete testing framework for actor systems:

| Component | Purpose |
|---|---|
| **TestActorSystem** | In-process actor system with controlled resources |
| **TestProbe** | A test actor that captures messages for assertions |
| **TestTimeController** | Virtual time for deterministic testing of time-based behavior |

```cangjie
// Test Example:
let probe = TestProbe()
actor.tell("hello", probe.ref)
probe.expectMsg("response")
probe.expectNoMsg(100.ms)
```

---

## 9. API Layer

**Path:** `modules/cactor/src/api/`
**Files:** 13
**Constraint:** Public API. Depends on all layers.

The API layer is the public face of CActor. It provides the cleanest entry points for users.

### 9.1 Module Structure

```
modules/cactor/src/api/
|-- CActor             # Unified entry with system(), props()
|-- CActorSystem       # actorOf(), typedActorOf(), createMessageBridge()
|-- CActorFactory      # Factory pattern implementation
+-- CActorSystemBuilder # Fluent builder for ActorSystem
```

### 9.2 CActor -- The Entry Trait

Every actor extends `CActor`:

```cangjie
public class MyActor <: CActor {
    // Declarative handler registration
    @handler
    public func handleHello(msg: Hello): MessageResult { ... }
}

// Create Props
let props = props!(MyActor)  // @actor macro generates factory
```

### 9.3 CActorSystemBuilder -- Fluent Construction

```cangjie
let system = CActorSystemBuilder()
    .withName("my-system")
    .withDispatcher("default", dispatcherConfig)
    .withMailbox("default", mailboxConfig)
    .withCoordinatedShutdown(true)
    .build()
```

### 9.4 CActorSystem -- Primary Interface

```cangjie
let system = CActorSystem.create("my-system")

// Untyped actor
let actor = system.actorOf(props, "my-actor")
actor.tell(Message)

// Typed actor
let typedActor = system.typedActorOf(MyBehavior(), "typed-actor")
typedActor.tell(TypedMessage("hello"))

// Type-safe bridging
let bridge = system.createMessageBridge(actor, targetType)
```

---

## 10. Core Subsystems

### 10.1 Streams (`modules/streams/src/`)

Reactive stream processing with backpressure:

```
Source -> Flow -> Sink
```

Built on CActor's dispatcher infrastructure, streams provide:

- Fan-in and fan-out operators.
- Materialized values.
- GraphDSL for composing complex pipelines.

### 10.2 XState (`modules/xstate/src/`)

State machine integration providing:

- Finite state machine (FSM) actors.
- State transition logging.
- Timed state transitions.

### 10.3 Macros (`modules/macros/src/`)

Compile-time code generation:

| Macro | Purpose |
|---|---|
| `@handler` | Marks and registers a message handler method |
| `@actor` | Auto-generates Props factory for an actor class |
| `props!()` | Compile-time Props construction |
| `@actor_system` | Declares an actor system dependency |

---

## 11. Design Principles

### 11.1 The Actor Axioms

CActor enforces the fundamental axioms of the Actor model:

1. **Encapsulation**: Actors process one message at a time. No shared mutable state.
2. **Location Transparency**: Actor references are identified by path, not by location. A reference may point to a local or remote actor without the sender knowing.
3. **Hierarchical Supervision**: Actors form a tree. Each actor supervises its children and propagates failures upward.
4. **Let It Crash**: Errors are handled through supervision, not defensive programming.

### 11.2 Layered Dependency Rule

Every layer depends only on layers beneath it. This is enforced by the build system.

### 11.3 Zero-Allocation Hot Path

The foundation and runtime layers are designed to minimize allocation in the hot path:

- Lock-free queues use pre-allocated ring buffers.
- Object pooling eliminates repeated allocations.
- Mailbox implementations minimize object creation per message.

### 11.4 Composability

Every abstraction is designed to be composable:

- Routers can wrap any `ActorRef`.
- Circuit breakers can wrap any `ActorRef`.
- Dispatchers can be assigned per-actor or per-message.
- Mailboxes can be swapped without changing actor code.

### 11.5 Type Safety

CActor Typed provides compile-time guarantees that:

- An actor can only receive messages of its declared type.
- The Ask pattern returns a `Future[T]` where `T` matches the expected response type.
- The `Props` system provides type-safe actor construction.

### 11.6 Predictable Failure Modes

Failures are first-class citizens:

- Every failure is routed through a supervision strategy.
- Failed actors are never silently ignored.
- Cascading failures are contained through circuit breakers.

---

## 12. Configuration Presets

CActor ships with five pre-configured presets optimized for different workloads:

### 12.1 Preset Comparison

| Preset | Mailbox | Dispatcher | Target |
|---|---|---|---|
| `createDefault()` | Unbounded | Thread pool | General development |
| `createProduction()` | Bounded (10K) | Work-stealing | Production workloads |
| `createExtreme()` | Ring buffer | Dynamic | 1M msg/s throughput |
| `createLowLatency()` | Lock-free | Foundation-based | P99 < 100us |
| `createHighThroughput()` | Batching | Batch-processing | 50M msg/s |

### 12.2 Default Preset

```
Mailbox:  FoundationMailbox (unbounded)
Dispatcher: ThreadPoolDispatcher
  - minThreads:  4
  - maxThreads:  64
  - keepAliveTime: 60s
Supervision: OneForOne (maxRetries: 10, within: 5min)
```

### 12.3 Production Preset

```
Mailbox:  LockFreeMailbox (capacity: 10,000)
Dispatcher: WorkStealingDispatcher
  - parallelism: Runtime.availableProcessors()
  - keepAliveTime: 30s
Supervision: OneForAll (maxRetries: 5, within: 10min)
```

### 12.4 Low-Latency Preset

```
Mailbox:  LockFreeMailbox (capacity: 1,000)
Dispatcher: FoundationBasedDispatcher
  - dedicatedThreads: Runtime.availableProcessors() * 2
  - affinity: enabled
  - priority: max
Supervision: OneForOne (maxRetries: 3, within: 1min)
```

---

## Appendix: File Statistics

| Layer | Path | File Count |
|---|---|---|
| Foundation | `modules/foundation/src/` | 33 |
| Core | `modules/core/src/` | 31 |
| Runtime | `modules/runtime/src/` | 63 |
| Patterns | `modules/patterns/src/` | 13 |
| Distribution | `modules/distribution/src/` | 21 |
| Integration | `modules/integration/src/` | 28 |
| API | `modules/cactor/src/api/` | 13 |
| **Total** | | **326** |

---

*Document version 1.2 -- CActor 1.2*
