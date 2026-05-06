# CActor API Reference

**Version:** 7.0
**Language:** Cangjie
**Inspiration:** Akka / Akka Typed

---

## Table of Contents

1. [Unified Entry Point — CActor](#1-unified-entry-point--cactor)
2. [Actor System — CActorSystem](#2-actor-system--cactorsystem)
3. [Actor Reference — ActorRef](#3-actor-reference--actorref)
4. [Actor — Actor Interface](#4-actor--actor-interface)
5. [Message System](#5-message-system)
6. [Props — Actor Configuration](#6-props--actor-configuration)
7. [Typed Actor System](#7-typed-actor-system)
8. [Configuration API](#8-configuration-api)
9. [Ask Pattern](#9-ask-pattern)
10. [Circuit Breaker](#10-circuit-breaker)
11. [Backpressure](#11-backpressure)
12. [Routing](#12-routing)
13. [Testing Framework](#13-testing-framework)
14. [API Extensions](#14-api-extensions)
15. [Usage Examples](#15-usage-examples)

---

## 1. Unified Entry Point — CActor

The `CActor` struct is the primary entry point for all CActor operations. It provides factory methods for creating actor systems and actor props.

### Package

```cangjie
import cactor.api.CActor
```

### Methods

#### `CActor.system(): CActorSystem`

Creates an actor system with the default name `"default"`.

**Returns:** `CActorSystem` — a new default actor system.

---

#### `CActor.system(name: String): CActorSystem`

Creates a named actor system.

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `String` | The unique name of the actor system |

**Returns:** `CActorSystem` — a new named actor system.

---

#### `CActor.system(name: String, config: CActorRuntimeConfig): CActorSystem`

Creates a named actor system with a custom runtime configuration.

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `String` | The unique name of the actor system |
| `config` | `CActorRuntimeConfig` | Custom runtime configuration |

**Returns:** `CActorSystem` — a new configured actor system.

---

#### `CActor.props(creator: () -> Actor): ActorProps`

Creates actor props from a factory function.

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `creator` | `() -> Actor` | A lambda that produces a new Actor instance |

**Returns:** `ActorProps` — props for creating an actor.

---

### CActorSystemFactory

Factory methods for creating `CActorSystem` instances.

```cangjie
public struct CActorSystemFactory {
    public static func create(): CActorSystem
    public static func create(name: String): CActorSystem
    public static func create(name: String, config: CActorRuntimeConfig): CActorSystem
}
```

---

### Example

```cangjie
// Create default system
let system = CActor.system()

// Create named system
let system = CActor.system("MyApp")

// Create configured system
let config = CActorRuntimeConfig.createProduction()
let system = CActor.system("MyApp", config)

// Create actor props
let props = CActor.props({ => MyActor() })
```

---

## 2. Actor System — CActorSystem

The `CActorSystem` class manages the lifecycle of actors and provides methods for actor creation and system management.

### Package

```cangjie
import cactor.api.CActorSystem
```

### Constructors

#### `CActorSystem(name: String)`

Creates an actor system with the default configuration.

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `String` | System name |

---

#### `CActorSystem(name: String, config: CActorRuntimeConfig)`

Creates an actor system with a custom runtime configuration.

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `String` | System name |
| `config` | `CActorRuntimeConfig` | Runtime configuration |

---

### Actor Creation Methods

#### `actorOf(creator: () -> Actor, name: String): ActorRef`

Creates a named actor using a factory lambda.

```cangjie
public func actorOf(creator: () -> Actor, name: String): ActorRef
```

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `creator` | `() -> Actor` | Factory function for the actor |
| `name` | `String` | Unique actor name |

**Returns:** `ActorRef` — a reference to the created actor.

---

#### `actorOf(creator: () -> Actor): ActorRef`

Creates an actor with an auto-generated name.

```cangjie
public func actorOf(creator: () -> Actor): ActorRef
```

**Returns:** `ActorRef` — a reference to the created actor.

---

#### `actorOf(actorProps: ActorProps, name: String): ActorRef`

Creates a named actor using `ActorProps`.

```cangjie
public func actorOf(actorProps: ActorProps, name: String): ActorRef
```

---

#### `actorOf(actorProps: ActorProps): ActorRef`

Creates an actor with auto-generated name using `ActorProps`.

```cangjie
public func actorOf(actorProps: ActorProps): ActorRef
```

---

#### `actorOf(props: Props<Actor>, name: String): ActorRef`

Creates a named actor with full `Props` configuration.

```cangjie
public func actorOf(props: Props<Actor>, name: String): ActorRef
```

---

#### `actorOf(props: Props<Actor>): ActorRef`

Creates an actor with auto-generated name and full `Props` configuration.

```cangjie
public func actorOf(props: Props<Actor>): ActorRef
```

---

#### `actorOfWithMailbox(creator: () -> Actor, name: String, mailboxConfig: CoreMailboxConfig): ActorRef`

Creates a named actor with a custom mailbox configuration.

```cangjie
public func actorOfWithMailbox(
    creator: () -> Actor,
    name: String,
    mailboxConfig: CoreMailboxConfig
): ActorRef
```

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `creator` | `() -> Actor` | Factory function |
| `name` | `String` | Actor name |
| `mailboxConfig` | `CoreMailboxConfig` | Mailbox type and capacity |

---

#### `actorOfWithDispatcher(creator: () -> Actor, name: String, dispatcherConfig: CoreDispatcherConfig): ActorRef`

Creates a named actor with a custom dispatcher configuration.

```cangjie
public func actorOfWithDispatcher(
    creator: () -> Actor,
    name: String,
    dispatcherConfig: CoreDispatcherConfig
): ActorRef
```

---

#### `actorOfConfigured(creator: () -> Actor, name: String, mailboxConfig: CoreMailboxConfig, dispatcherConfig: CoreDispatcherConfig, supervisionConfig: CoreSupervisionConfig): ActorRef`

Creates a named actor with complete configuration (mailbox, dispatcher, and supervision).

```cangjie
public func actorOfConfigured(
    creator: () -> Actor,
    name: String,
    mailboxConfig: CoreMailboxConfig,
    dispatcherConfig: CoreDispatcherConfig,
    supervisionConfig: CoreSupervisionConfig
): ActorRef
```

---

### System Management Methods

#### `typedActorOf<T>(behavior: TypedBehavior<T>, name: String): TypedActorRef<T>`

Creates a type-safe actor with a typed behavior.

```cangjie
public func typedActorOf<T>(behavior: TypedBehavior<T>, name: String): TypedActorRef<T>
```

**Type Parameters:**
| Parameter | Description |
|-----------|-------------|
| `T` | The message type this actor handles |

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `behavior` | `TypedBehavior<T>` | The type-safe behavior definition |
| `name` | `String` | Actor name |

---

#### `typedActorOf<T>(behavior: TypedBehavior<T>): TypedActorRef<T>`

Creates a type-safe actor with auto-generated name.

---

#### `typedActorSelection<T>(path: String): TypedActorSelection<T>`

Selects type-safe actors by path, supporting `*` and `**` wildcards.

```cangjie
public func typedActorSelection<T>(path: String): TypedActorSelection<T>
```

---

#### `wrapAsTypedActor<T>(actorRef: ActorRef, name: String): TypedActorRef<T>`

Wraps a standard `ActorRef` as a `TypedActorRef<T>` for type-safe interaction.

```cangjie
public func wrapAsTypedActor<T>(actorRef: ActorRef, name: String): TypedActorRef<T>
```

---

#### `unwrapTypedActor<T>(typedActorRef: TypedActorRef<T>): ActorRef`

Extracts the underlying `ActorRef` from a `TypedActorRef<T>`.

```cangjie
public func unwrapTypedActor<T>(typedActorRef: TypedActorRef<T>): ActorRef
```

---

#### `createMessageBridge<T>(standardActor: ActorRef, typedActor: TypedActorRef<T>): MessageBridge<T>`

Creates a message bridge enabling communication between standard and typed actors.

```cangjie
public func createMessageBridge<T>(
    standardActor: ActorRef,
    typedActor: TypedActorRef<T>
): MessageBridge<T>
```

---

#### `terminate(): Unit`

Gracefully shuts down the actor system and all its actors.

```cangjie
public func terminate(): Unit
```

---

#### `isTerminated(): Bool`

Checks whether the actor system has been terminated.

```cangjie
public func isTerminated(): Bool
```

**Returns:** `Bool` — `true` if the system is terminated.

---

#### `getName(): String`

Returns the name of the actor system.

```cangjie
public func getName(): String
```

---

#### `getStartTime(): Int64`

Returns the system start time as milliseconds since epoch.

```cangjie
public func getStartTime(): Int64
```

---

#### `getRuntime(): CActorRuntime`

Returns the underlying runtime for advanced operations.

```cangjie
public func getRuntime(): CActorRuntime
```

---

## 3. Actor Reference — ActorRef

`ActorRef` is the primary handle for sending messages to actors. It is an immutable, serializable reference.

### Package

```cangjie
import cactor.core.actor.ActorRef
```

### Interface Definition

```cangjie
public interface ActorRef {
    prop path: ActorPath

    func tell(message: Message): Unit
    func tell(message: Message, sender: Option<String>): Unit
    func equals(other: ActorRef): Bool
    func hashCode(): Int32
}
```

### Properties

#### `path: ActorPath` (read-only)

The actor's unique path within the system. Format: `cactor://{system}{path}`.

### Methods

#### `tell(message: Message): Unit`

Sends a fire-and-forget message to the actor.

```cangjie
func tell(message: Message): Unit
```

This is the primary way to communicate with actors. Delivery is asynchronous and reliable within the same JVM.

---

#### `tell(message: Message, sender: Option<String>): Unit`

Sends a message with an explicit sender path (for reply routing).

```cangjie
func tell(message: Message, sender: Option<String>): Unit
```

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `message` | `Message` | The message to send |
| `sender` | `Option<String>` | The sender's actor path (`Some(path)` or `None`) |

---

#### `equals(other: ActorRef): Bool`

Compares two `ActorRef` instances for equality.

```cangjie
func equals(other: ActorRef): Bool
```

---

#### `hashCode(): Int32`

Returns the hash code of this actor reference.

```cangjie
func hashCode(): Int32
```

---

### ActorPath

```cangjie
public struct ActorPath {
    public init(system: String, path: String)

    public func getSystem(): String
    public func getPath(): String
    public func toString(): String           // Returns "cactor://{system}{path}"
    public func parent(): Option<ActorPath>
    public func child(name: String): ActorPath
    public func equals(other: ActorPath): Bool
    public func hashCode(): Int32
}
```

---

### LocalActorRef

Extended `ActorRef` interface for actors within the same JVM.

```cangjie
public interface LocalActorRef <: ActorRef {
    func getActor(): Actor
    func suspend(): Unit
    func resume(): Unit
    func restart(cause: Exception): Unit
    func stop(): Unit
    func getState(): ActorLifecycleState
}
```

---

### ActorSelection

Selects actors by path pattern (supports `*` and `**` wildcards).

```cangjie
public interface ActorSelection {
    func resolveOne(timeout: Duration): Option<ActorRef>
    func tell(message: Message): Unit
    func tell(message: Message, sender: Option<String>): Unit
}
```

---

## 4. Actor — Actor Interface

The `Actor` interface defines the contract for all actors.

### Package

```cangjie
import cactor.core.actor.Actor
import cactor.core.context.ActorContext
```

### Interface Definition

```cangjie
public interface Actor {
    prop name: String
    prop description: String

    func receive(message: Message, context: ActorContext): MessageResult

    func preStart(): Unit
    func postStart(): Unit
    func preStop(): Unit
    func postStop(): Unit
    func preRestart(reason: Exception): Unit
    func postRestart(reason: Exception): Unit
}
```

### Required Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `String` | The actor's unique name within its parent |
| `description` | `String` | Human-readable description of the actor's purpose |

### Required Method

#### `receive(message: Message, context: ActorContext): MessageResult`

The main message handler. Called for every message received by the actor.

```cangjie
func receive(message: Message, context: ActorContext): MessageResult
```

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `message` | `Message` | The incoming message |
| `context` | `ActorContext` | The actor's execution context |

**Returns:** `MessageResult` — indicates how the message was handled.

---

### Lifecycle Hooks (Optional)

All lifecycle methods have default empty implementations.

#### `preStart(): Unit`

Called before the actor starts processing messages. Use for initialization.

---

#### `postStart(): Unit`

Called after the actor starts processing messages.

---

#### `preStop(): Unit`

Called before the actor stops. Use for cleanup.

---

#### `postStop(): Unit`

Called after the actor has stopped.

---

#### `preRestart(reason: Exception): Unit`

Called before the actor restarts due to a failure. Default calls `preStop()`.

---

#### `postRestart(reason: Exception): Unit`

Called after the actor restarts. Default calls `postStart()`.

---

### Actor Lifecycle States

```cangjie
public enum ActorLifecycleState {
    | Created     // Created but not started
    | Starting    // Starting up
    | Running     // Processing messages
    | Stopping    // Initiating shutdown
    | Stopped     // Fully stopped
    | Failed      // In failed state
    | Restarting  // Restarting after error

    public func toInt64(): Int64
    public static func fromInt64(value: Int64): ActorLifecycleState
}
```

---

## 5. Message System

### Message Interface

```cangjie
public interface Message {
    func messageType(): String

    func priority(): Int32 { 0 }       // Higher = more urgent (system messages: 1000)
    func messageId(): String { "" }   // Unique identifier
    func timestamp(): Int64 { 0 }      // Nanoseconds since epoch
}
```

### SystemMessage Interface

Messages used internally by the actor system. System messages have priority `1000`.

```cangjie
public interface SystemMessage <: Message {
    func priority(): Int32 { 1000 }
}
```

### Built-in Message Types

```cangjie
// System messages
public struct StopMessage <: SystemMessage {
    public init(reason: String)
    public init()
    public func messageType(): String  // "StopMessage"
    public func getReason(): String
}

public struct RestartMessage <: SystemMessage {
    public init(cause: Exception)
    public func messageType(): String  // "RestartMessage"
    public func getCause(): Exception
}

public struct SuspendMessage <: SystemMessage
public struct ResumeMessage <: SystemMessage

public struct TerminatedMessage <: SystemMessage {
    public init(actor: String, reason: String)
    public func messageType(): String  // "TerminatedMessage"
    public func getActor(): String
    public func getReason(): String
}

public struct DeadLetter <: SystemMessage {
    public init(message: Message, sender: String, recipient: String)
    public func messageType(): String  // "DeadLetter"
    public func getMessage(): Message
    public func getSender(): String
    public func getRecipient(): String
}

// User messages
public struct StringMessage <: UserMessage {
    public init(content: String)
    public func messageType(): String  // "StringMessage"
    public func getContent(): String
}

public struct PingMessage <: UserMessage {
    public init(id: Int64)
    public func messageType(): String  // "PingMessage"
    public func getId(): Int64
}

public struct PongMessage <: UserMessage {
    public init(originalId: Int64)
    public func messageType(): String  // "PongMessage"
    public func getOriginalId(): Int64
}
```

### MessageResult Enum

```cangjie
public enum MessageResult {
    | Handled      // Message handled successfully
    | Unhandled    // Message not handled
    | Stop          // Request actor to stop
    | Restart       // Request actor to restart
    | Escalate      // Report problem to supervisor
}
```

### Envelope

```cangjie
public struct Envelope {
    public init(message: Message, sender: Option<String>)
    public func getMessage(): Message
    public func getSender(): Option<String>
    public func getTimestamp(): Int64
}
```

### Enhanced Message (Zero-Copy)

For high-performance scenarios requiring zero-copy message passing:

```cangjie
public class EnhancedZeroCopyMessage {
    public init(
        messageType: UInt16,
        sourceActor: UInt64,
        targetActor: UInt64,
        sharedBuffer: SharedMemoryBuffer,
        payloadSize: UInt64
    )

    public func getMessageType(): UInt16
    public func getSourceActor(): UInt64
    public func getTargetActor(): UInt64
    public func getSharedBuffer(): SharedMemoryBuffer
    public func getPayloadSize(): UInt64
    public func getHopCount(): Int64
    public func getSendTimestamp(): Int64
    public func incrementHopCount(): Unit
    public func acquire(): Unit
    public func release(): Unit
}
```

---

## 6. Props — Actor Configuration

`Props` encapsulates the configuration needed to create an actor instance.

### Package

```cangjie
import cactor.core.actor.Props
import cactor.core.actor.PropsFactory
```

### Props Struct

```cangjie
public struct Props<T> where T <: Actor {
    public init(factory: ActorFactory<T>)

    // Configuration
    public func withDispatcher(dispatcher: String): Props<T>
    public func withMailbox(mailboxType: String): Props<T>
    public func withSupervisionStrategy(strategy: String): Props<T>

    // Creation
    public func create(): T

    // Getters
    public func getDispatcher(): String
    public func getMailboxType(): String
    public func getSupervisionStrategy(): String
    public func getDispatcherName(): String
}
```

### PropsFactory

```cangjie
public struct PropsFactory {
    public static func create<T>(creator: () -> T): Props<T> where T <: Actor
    public static func createWithDispatcher<T>(
        creator: () -> T,
        dispatcher: String
    ): Props<T> where T <: Actor
}
```

### ActorProps (API Layer)

Simplified props wrapper for the API layer.

```cangjie
public struct ActorProps {
    public init(creator: () -> Actor)
    public func getCreator(): () -> Actor
}
```

---

## 7. Typed Actor System

The typed actor system provides compile-time type safety by restricting messages to a specific type.

### Package

```cangjie
import cactor.core.typed.TypedBehavior
import cactor.core.typed.TypedActorRef
import cactor.core.typed.TypedActorContext
import cactor.core.typed.TypedActorSystem
import cactor.core.typed.TypedActorSelection
```

### TypedBehavior<T>

Defines the behavior of a type-safe actor.

```cangjie
public interface TypedBehavior<T> {
    func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T>
    func getDescription(): String { "TypedBehavior" }
}
```

### TypedActorRef<T>

A type-safe reference to an actor.

```cangjie
public interface TypedActorRef<T> {
    func tell(message: T): Unit
    func pathString(): String
    func equals(other: TypedActorRef<T>): Bool
}
```

### TypedActorContext<T>

The execution context for typed actors.

```cangjie
public interface TypedActorContext<T> {
    prop self: TypedActorRef<T>

    func sender(): Option<String>
    func stopSelf(): Unit
    func log(): TypedLogger

    // Child management
    func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any>
    func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any>
    func stopChild(child: TypedActorRef<Any>): Unit

    // Actor watching
    func watchActor(other: TypedActorRef<Any>): Unit
    func unwatchActor(other: TypedActorRef<Any>): Unit

    // Ask pattern
    func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any>
    func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any>

    // System access
    func system(): TypedActorSystem
    func scheduler(): TypedScheduler

    // Child lookup
    func child(name: String): Option<TypedActorRef<Any>>
    func children(): Array<TypedActorRef<Any>>
}
```

### TypedActorSystem

The type-safe actor system.

```cangjie
public interface TypedActorSystem {
    prop name: String

    func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any>
    func actorSelection(path: String): TypedActorSelection<Any>
    func terminate(): Bool
    func isTerminated(): Bool
    func startTime(): Int64
}
```

### TypedActorSelection<T>

Selection of typed actors by path with wildcard support (`*`, `**`).

```cangjie
public interface TypedActorSelection<T> {
    func resolveOne(timeout: Duration): Option<TypedActorRef<T>>
    func tell(message: T): Unit
    func pathString(): String
}
```

### TypedFuture<T>

A future for typed async results.

```cangjie
public interface TypedFuture<T> {
    func isCompleted(): Bool
    func get(): T
    func get(timeout: Duration): T
    func value(): Option<T>
}
```

### TypedScheduler

Scheduler for delayed and periodic tasks.

```cangjie
public interface TypedScheduler {
    func scheduleOnce(
        delay: Duration,
        target: TypedActorRef<Any>,
        message: Any
    ): Unit

    func schedule(
        initialDelay: Duration,
        interval: Duration,
        target: TypedActorRef<Any>,
        message: Any
    ): TypedCancellable
}
```

### TypedCancellable

```cangjie
public interface TypedCancellable {
    func cancel(): Bool
    func isCancelled(): Bool
}
```

### TypedLogger

```cangjie
public interface TypedLogger {
    func debug(message: String): Unit
    func info(message: String): Unit
    func warn(message: String): Unit
    func error(message: String): Unit
}
```

### TypedBehaviors Factory

Factory methods for common typed behaviors.

```cangjie
public struct TypedBehaviors {
    // Receive messages with a handler function
    public static func receive<T>(
        handler: (TypedActorContext<T>, T) -> TypedBehavior<T>
    ): TypedBehavior<T>

    // Keep current behavior unchanged
    public static func same<T>(): TypedBehavior<T>

    // Stop the actor
    public static func stopped<T>(): TypedBehavior<T>

    // Ignore all messages
    public static func ignore<T>(): TypedBehavior<T>

    // Transform incoming message type
    public static func transformMessages<In, T>(
        inner: TypedBehavior<T>,
        f: (In) -> T
    ): TypedBehavior<In>

    // Filter messages by predicate
    public static func filter<In>(
        inner: TypedBehavior<In>,
        p: (In) -> Bool
    ): TypedBehavior<In>

    // Partial receive (predicate-based handler)
    public static func receivePartial<T>(
        p: (T) -> Bool,
        handler: (TypedActorContext<T>, T) -> TypedBehavior<T>
    ): TypedBehavior<T>

    // Aliases
    public static func mapMessages<In, T>(
        inner: TypedBehavior<T>,
        f: (In) -> T
    ): TypedBehavior<In>
    public static func contramap<In, T>(
        inner: TypedBehavior<T>,
        f: (In) -> T
    ): TypedBehavior<In>
}
```

---

## 8. Configuration API

### MailboxConfig

Configures the mailbox for an actor.

### Package

```cangjie
import cactor.api.config.MailboxConfig
import cactor.api.Mailbox  // convenience API
```

### MailboxConfig Struct

```cangjie
public struct MailboxConfig {
    private init(mailboxType: String, capacity: Int32, options: HashMap<String, String>)

    // Factory methods
    public static func unbounded(): MailboxConfig      // "unbounded", capacity=-1
    public static func bounded(capacity: Int32): MailboxConfig  // "bounded"
    public static func priority(): MailboxConfig      // "priority"
    public static func ringBuffer(capacity: Int32): MailboxConfig  // "ring-buffer"
    public static func batching(batchSize: Int32): MailboxConfig   // "batching"

    // Getters
    public func getMailboxType(): String
    public func getCapacity(): Int32
    public func getOptions(): HashMap<String, String>
}
```

### Mailbox Convenience Struct

```cangjie
public struct Mailbox {
    public static func unbounded(): MailboxConfig
    public static func bounded(capacity: Int32): MailboxConfig
    public static func priority(): MailboxConfig
}
```

---

### DispatcherConfig

Configures the execution dispatcher for an actor.

### Package

```cangjie
import cactor.api.config.DispatcherConfig
import cactor.api.Dispatcher  // convenience API
```

### DispatcherConfig Struct

```cangjie
public struct DispatcherConfig {
    private init(
        dispatcherType: String,
        parallelism: Int32,
        throughput: Int32,
        options: HashMap<String, String>
    )

    // Factory methods
    public static func threadPool(size: Int32): DispatcherConfig  // "thread-pool"
    public static func threadPoolDefault(): DispatcherConfig     // threadPool(4)
    public static func workStealing(parallelism: Int32): DispatcherConfig  // "work-stealing"
    public static func workStealingDefault(): DispatcherConfig  // workStealing(8)
    public static func pinned(): DispatcherConfig               // "pinned"
    public static func callingThread(): DispatcherConfig        // "calling-thread"

    // Getters
    public func getDispatcherType(): String
    public func getParallelism(): Int32
    public func getThroughput(): Int32
    public func getOptions(): HashMap<String, String>
}
```

### Dispatcher Convenience Struct

```cangjie
public struct Dispatcher {
    public static func default(): DispatcherConfig
    public static func workStealing(): DispatcherConfig
    public static func workStealing(parallelism: Int32): DispatcherConfig
    public static func threadPool(): DispatcherConfig
    public static func threadPool(size: Int32): DispatcherConfig
}
```

---

### SupervisionConfig

Configures the supervision strategy for an actor.

### Package

```cangjie
import cactor.api.config.SupervisionConfig
import cactor.api.Supervision  // convenience API
```

### SupervisionConfig Struct

```cangjie
public struct SupervisionConfig {
    private init(
        strategyType: String,
        maxRetries: Int32,
        backoffDuration: Duration,
        options: HashMap<String, String>
    )

    // Factory methods
    public static func restart(maxRetries: Int32, backoff: Duration): SupervisionConfig
    public static func restartDefault(): SupervisionConfig  // restart(3, 1s)
    public static func stop(): SupervisionConfig           // "stop"
    public static func escalate(): SupervisionConfig       // "escalate"
    public static func resume(): SupervisionConfig         // "resume"

    // Getters
    public func getStrategyType(): String
    public func getMaxRetries(): Int32
    public func getBackoffDuration(): Duration
    public func getOptions(): HashMap<String, String>
}
```

### Supervision Convenience Struct

```cangjie
public struct Supervision {
    public static func restart(): SupervisionConfig
    public static func restart(maxRetries: Int32): SupervisionConfig
    public static func stop(): SupervisionConfig
    public static func escalate(): SupervisionConfig
}
```

---

### ActorConfig

A composite configuration that combines mailbox, dispatcher, and supervision.

### Package

```cangjie
import cactor.api.config.ActorConfig
```

### ActorConfig Struct

```cangjie
public struct ActorConfig {
    public let mailbox: MailboxConfig
    public let dispatcher: DispatcherConfig
    public let supervision: SupervisionConfig

    public init(
        mailbox: MailboxConfig,
        dispatcher: DispatcherConfig,
        supervision: SupervisionConfig
    )

    // Preset configurations
    public static func default(): ActorConfig
        // Unbounded mailbox + threadPool(4) + restart(3, 1s)

    public static func highPerformance(): ActorConfig
        // Bounded(10K) + workStealing(8) + restart(5, 2s)

    public static func lowLatency(): ActorConfig
        // Unbounded + workStealing(4) + restart(1, 100ms)

    public static func batching(): ActorConfig
        // Bounded(50K) + threadPool(16) + restart(10, 5s)
}
```

---

### CActorRuntimeConfig

Full runtime configuration for the entire actor system.

```cangjie
public struct CActorRuntimeConfig {
    public let mailboxConfig: MailboxConfig
    public let dispatcherConfig: DispatcherConfig
    public let highPerformanceConfig: HighPerformanceActorSystemConfig
    public let enableMonitoring: Bool
    public let enablePersistence: Bool

    public static func createDefault(): CActorRuntimeConfig
    public static func createProduction(): CActorRuntimeConfig
    public static func createExtreme(): CActorRuntimeConfig  // 1M msg/s target
    public static func createMemoryOptimized(): CActorRuntimeConfig  // <1KB/actor
}
```

---

## 9. Ask Pattern

Asynchronous request-response with timeout support.

### Package

```cangjie
import cactor.patterns.ask.Ask
import cactor.patterns.ask.AskFuture
import cactor.patterns.ask.AskPatternManager
```

### Ask Static API

```cangjie
public struct Ask {
    // Send ask request (default 5s timeout)
    public static func ask(target: ActorRef, message: Message): AskFuture<Message>

    // Send ask request with custom timeout
    public static func ask(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): AskFuture<Message>

    // Type-safe ask request
    public static func askTyped(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): AskFuture<Message>

    // Blocking ask-and-wait
    public static func askAndWait(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): Result<Message>

    // Async ask with success/failure callbacks
    public static func askAsync(
        target: ActorRef,
        message: Message,
        timeout: Duration,
        onSuccess: (Message) -> Unit,
        onFailure: (Exception) -> Unit
    ): Unit

    // Shutdown the ask manager
    public static func shutdown(): Unit

    // Get statistics
    public static func getStats(): AskStats
}
```

### AskFuture<T>

Represents the result of an ask operation.

```cangjie
public class AskFuture<T> {
    public init(requestId: String, timeout: Duration)

    // Complete the future
    public func complete(value: T): Unit
    public func completeWithError(error: Exception): Unit

    // Wait for result (blocking)
    public func get(): Result<T>
    public func await(waitTimeout: Duration): Result<T>

    // Async callback
    public func onComplete(callback: (Result<T>) -> Unit): Unit

    // Status
    public func isCompleted(): Bool
    public func isTimedOut(): Bool

    // Accessors
    public func getRequestId(): String
    public func getTimeout(): Duration
}
```

### AskPatternManager

Manages the lifecycle of ask requests.

```cangjie
public class AskPatternManager {
    public init(defaultTimeout: Duration)

    public func start(): Unit
    public func stop(): Unit

    public func ask(target: ActorRef, message: Message): AskFuture<Message>
    public func ask(target: ActorRef, message: Message, timeout: Duration): AskFuture<Message>
    public func handleResponse(response: AskResponse): Unit

    public func getPendingRequestCount(): Int64
    public func getDefaultTimeout(): Duration
}
```

### Result<T> Enum

```cangjie
public enum Result<T> {
    | Success(T)
    | Failure(Exception)

    public func isSuccess(): Bool
    public func isFailure(): Bool
    public func get(): T              // Throws on Failure
    public func getOrElse(defaultValue: T): T
}
```

### AskStats

```cangjie
public struct AskStats {
    public let pendingRequests: Int64
    public let defaultTimeout: Duration

    public func toString(): String
}
```

### Example

```cangjie
let system = CActor.system("ask-demo")
let target = system.actorOf({ => CalculatorActor() }, "calculator")

// Async with callback
Ask.askAsync(
    target,
    AddMessage(10, 20),
    Duration.second * 5,
    onSuccess: { response =>
        println("Result: ${response}")
    },
    onFailure: { error =>
        println("Request failed: ${error.message}")
    }
)

// Blocking wait
let result = Ask.askAndWait(target, QueryMessage(), Duration.second * 3)
match (result) {
    case Result.Success(msg) => println("Got: ${msg.messageType()}")
    case Result.Failure(ex) => println("Failed: ${ex.message}")
}
```

---

## 10. Circuit Breaker

Provides fault isolation and fast-fail behavior.

### Package

```cangjie
import cactor.patterns.circuit_breaker.CircuitBreaker
import cactor.patterns.circuit_breaker.BasicCircuitBreaker
import cactor.patterns.circuit_breaker.CircuitBreakerFactory
```

### CircuitBreakerState Enum

```cangjie
public enum CircuitBreakerState {
    | Closed   // Normal operation
    | Open     // Fast-fail mode
    | HalfOpen // Testing recovery
}
```

### CircuitBreakerConfig

```cangjie
public struct CircuitBreakerConfig {
    public let failureThreshold: Int32   // Failures before Open
    public let successThreshold: Int32  // Successes to close from HalfOpen
    public let timeout: Duration         // Call timeout
    public let resetTimeout: Duration    // Time before attempting HalfOpen

    public init(
        failureThreshold: Int32,
        successThreshold: Int32,
        timeout: Duration,
        resetTimeout: Duration
    )
}
```

### CircuitBreakerStats

```cangjie
public struct CircuitBreakerStats {
    public let state: CircuitBreakerState
    public let failureCount: Int64
    public let successCount: Int64
    public let totalRequests: Int64
    public let lastFailureTime: Int64
    public let lastSuccessTime: Int64

    public func getFailureRate(): Float64
    public func getSuccessRate(): Float64
}
```

### CircuitBreaker<T> Interface

```cangjie
public interface CircuitBreaker<T> {
    func execute(operation: () -> T): T
    func getState(): CircuitBreakerState
    func getStats(): CircuitBreakerStats
    func reset(): Unit
    func trip(): Unit
}
```

### BasicCircuitBreaker<T>

```cangjie
public class BasicCircuitBreaker<T> <: CircuitBreaker<T> {
    public init(config: CircuitBreakerConfig)

    // Execute with circuit breaker protection
    public func execute(operation: () -> T): T
    public func getState(): CircuitBreakerState
    public func getStats(): CircuitBreakerStats
    public func reset(): Unit
    public func trip(): Unit
}
```

### CircuitBreakerFactory

```cangjie
public struct CircuitBreakerFactory {
    // Default: 5 failures -> Open, 3 successes in HalfOpen -> Closed
    // Timeout: 5s, Reset: 30s
    public static func createDefault<T>(): BasicCircuitBreaker<T>

    // FastFail: 3 failures -> Open, 2 successes -> Closed
    // Timeout: 2s, Reset: 10s
    public static func createFastFail<T>(): BasicCircuitBreaker<T>

    // Tolerant: 10 failures -> Open, 5 successes -> Closed
    // Timeout: 10s, Reset: 60s
    public static func createTolerant<T>(): BasicCircuitBreaker<T>
}
```

### CircuitBreakerDecorator<T>

Decorator that adds fallback behavior to a circuit breaker.

```cangjie
public class CircuitBreakerDecorator<T> {
    public init(
        circuitBreaker: CircuitBreaker<T>,
        fallback: Option<() -> T>
    )

    // Execute operation; call fallback if circuit is open
    public func call(operation: () -> T): T
    public func getCircuitBreaker(): CircuitBreaker<T>
}
```

### Example

```cangjie
let breaker = CircuitBreakerFactory.createDefault<Int64>()

let result = breaker.execute({ =>
    // Potentially failing operation
    riskyServiceCall()
})

// With fallback
let decorator = CircuitBreakerDecorator<Int64>(
    breaker,
    Some({ => 0 })  // Return 0 on failure
)
let value = decorator.call({ => riskyServiceCall() })
```

---

## 11. Backpressure

Provides flow control and overload protection.

### Package

```cangjie
import cactor.patterns.backpressure.BasicBackpressureController
import cactor.patterns.backpressure.BackpressureControllerFactory
```

### BackpressureStrategy Enum

```cangjie
public enum BackpressureStrategy <: ToString {
    | Drop       // Drop new messages when overloaded
    | Block      // Block sender until capacity available
    | Buffer     // Buffer messages up to capacity
    | Reject     // Throw exception when overloaded
    | Throttle   // Apply dynamic delay to slow senders

    public func toString(): String
}
```

### BackpressureConfig

```cangjie
public struct BackpressureConfig {
    public let strategy: BackpressureStrategy
    public let maxBufferSize: Int64
    public let maxThroughput: Int64        // Max messages per second
    public let windowSize: Duration       // Time window for throughput measurement
    public let dropThreshold: Float64      // 0.0-1.0, buffer utilization threshold
    public let blockTimeout: Duration      // Max time to wait for buffer space

    public init(
        strategy: BackpressureStrategy,
        maxBufferSize: Int64,
        maxThroughput: Int64,
        windowSize: Duration,
        dropThreshold: Float64,
        blockTimeout: Duration
    )
}
```

### BackpressureStats

```cangjie
public struct BackpressureStats {
    public let totalMessages: Int64
    public let droppedMessages: Int64
    public let blockedMessages: Int64
    public let bufferedMessages: Int64
    public let rejectedMessages: Int64
    public let throttledMessages: Int64
    public let currentThroughput: Float64
    public let bufferUtilization: Float64

    public func getDropRate(): Float64
    public func getSuccessRate(): Float64
}
```

### FlowController Interface

```cangjie
public interface FlowController {
    func trySend(message: Message): Bool
    func getStats(): BackpressureStats
    func reset(): Unit
    func isOverloaded(): Bool
}
```

### BasicBackpressureController

```cangjie
public class BasicBackpressureController <: FlowController {
    public init(config: BackpressureConfig)

    public func trySend(message: Message): Bool
    public func getStats(): BackpressureStats
    public func reset(): Unit
    public func isOverloaded(): Bool
    public func stop(): Unit
    public func getBufferedMessages(): CActorQueue<Message>
}
```

### BackpressureControllerFactory

```cangjie
public struct BackpressureControllerFactory {
    // Drop strategy: 80% threshold, 5s block timeout
    public static func createDropController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController

    // Block strategy: 90% threshold, 10s block timeout
    public static func createBlockController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController

    // Throttle strategy: 70% threshold, 3s block timeout
    public static func createThrottleController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController
}
```

### Example

```cangjie
let controller = BackpressureControllerFactory.createThrottleController(
    10000,   // max buffer size
    1000     // max throughput per second
)

// Attempt to send a message
if (controller.trySend(message)) {
    println("Message accepted")
} else {
    println("Message rejected (overloaded)")
}

// Check overload status
if (controller.isOverloaded()) {
    println("System is overloaded")
}

// Get statistics
let stats = controller.getStats()
println("Drop rate: ${stats.getDropRate()}")
```

---

## 12. Routing

Message routing with multiple strategies and load balancing.

### Package

```cangjie
import cactor.patterns.routing.BasicRouter
import cactor.patterns.routing.RouterFactory
```

### RoutingStrategy Interface

```cangjie
public interface RoutingStrategy {
    func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef>
}
```

### Built-in Routing Strategies

```cangjie
// Round-robin: cycles through routees in order
public class RoundRobinRoutingStrategy <: RoutingStrategy {
    public init()
}

// Random: randomly selects a routee
public class RandomRoutingStrategy <: RoutingStrategy {
    public init()
}

// SmallestMailbox: selects the actor with fewest pending messages
public class SmallestMailboxRoutingStrategy <: RoutingStrategy {
    public init()
}

// Broadcast: sends to all routees
public class BroadcastRoutingStrategy <: RoutingStrategy {
    public init()
    public func broadcast(routees: Array<ActorRef>, message: Message): Unit
}

// ConsistentHash: hash-based selection for request affinity
public class ConsistentHashRoutingStrategy <: RoutingStrategy {
    public init()
}
```

### Router Interface

```cangjie
public interface Router {
    func route(message: Message): Unit
    func addRoutee(routee: ActorRef): Unit
    func removeRoutee(routee: ActorRef): Unit
    func getRoutees(): Array<ActorRef>
    func getStrategy(): RoutingStrategy
}
```

### BasicRouter

```cangjie
public class BasicRouter <: Router {
    public init(strategy: RoutingStrategy)

    public func route(message: Message): Unit
    public func addRoutee(routee: ActorRef): Unit
    public func removeRoutee(routee: ActorRef): Unit
    public func getRoutees(): Array<ActorRef>
    public func getStrategy(): RoutingStrategy
}
```

### RouterFactory

```cangjie
public struct RouterFactory {
    public static func roundRobin(): BasicRouter
    public static func random(): BasicRouter
    public static func smallestMailbox(): BasicRouter
    public static func broadcast(): BasicRouter
    public static func consistentHash(): BasicRouter
}
```

### RouterActorRef

Wraps a `Router` as an `ActorRef` for system integration.

```cangjie
public class RouterActorRef <: ActorRef {
    public init(router: Router, path: String)

    public prop path: ActorPath
    public func tell(message: Message): Unit
    public func tell(message: Message, sender: Option<String>): Unit
    public func equals(other: ActorRef): Bool
    public func hashCode(): Int32
    public func getRouter(): Router
}
```

### ResizePolicy Enum

```cangjie
public enum ResizePolicy {
    | Fixed     // Fixed number of routees
    | Dynamic   // Dynamically adjust routee count
}
```

### RoutingConfig

```cangjie
public struct RoutingConfig {
    public let strategy: RoutingStrategy
    public let routeeCount: Int32
    public let resizePolicy: ResizePolicy

    public init(
        strategy: RoutingStrategy,
        routeeCount: Int32,
        resizePolicy: ResizePolicy
    )
}
```

### RoutingStatistics

```cangjie
public struct RoutingStatistics {
    public let totalMessages: Int64
    public let routeeCount: Int32
    public let averageLatency: Float64
}
```

### Example

```cangjie
let router = RouterFactory.roundRobin()

// Add routees
router.addRoutee(worker1)
router.addRoutee(worker2)
router.addRoutee(worker3)

// Route a message
router.route(workMessage)

// Broadcast to all routees
let broadcastRouter = RouterFactory.broadcast()
broadcastRouter.addRoutee(worker1)
broadcastRouter.addRoutee(worker2)
broadcastRouter.route(broadcastMessage)

// Wrap as ActorRef for system integration
let routerRef = RouterActorRef(router, "worker-pool")
// Can now be used like any other ActorRef
```

---

## 13. Testing Framework

### Package

```cangjie
import cactor.integration.testing.TestActorSystem
import cactor.integration.testing.TestProbe
```

### TestActorSystem

A lightweight actor system for testing.

```cangjie
public class TestActorSystem {
    public init(name: String)

    // Create actors
    public func actorOf(factory: () -> Actor, name: String): ActorRef

    // Create a test probe
    public func createTestProbe(): TestProbe

    // Access time control
    public func getTimeController(): TestTimeController

    // Lifecycle
    public func terminate(): Unit
    public func getName(): String
    public func isTerminated(): Bool
    public func getUnderlyingSystem(): CActorSystem
}
```

### TestProbe

Captures and asserts on messages received by an actor.

```cangjie
public class TestProbe {
    // Create a new probe
    public static func create(testSystem: TestActorSystem): TestProbe

    // Assertion: expect a specific message within timeout
    public func expectMessage(
        expectedMessage: Message,
        timeout: Duration
    ): TestExpectation<Message>

    // Assertion: expect NO message for the given duration
    public func expectNoMessage(duration: Duration): TestExpectation<Unit>

    // Assertion: expect any message within timeout
    public func expectAnyMessage(timeout: Duration): TestExpectation<Message>

    // Get the ActorRef for sending messages to the probe
    public func getRef(): ActorRef

    // Cleanup
    public func cleanup(): Unit

    // Statistics
    public func getReceivedMessageCount(): Int32
}
```

### TestExpectation<T>

Result type for probe assertions.

```cangjie
public enum TestExpectation<T> {
    | Success(T)
    | Timeout
    | UnexpectedMessage(Message)
    | Error(Exception)

    public func isSuccess(): Bool
    public func getValue(): Option<T>
    public func toString(): String
}
```

### TestTimeController

Controls virtual time for deterministic testing.

```cangjie
public class TestTimeController {
    public init()

    // Get current virtual time (nanoseconds)
    public func getCurrentTime(): Int64

    // Advance virtual time by duration
    public func advanceTime(duration: Duration): Unit

    // Reset to real time
    public func reset(): Unit
}
```

### Example

```cangjie
let testSystem = TestActorSystem("test-system")
let probe = testSystem.createTestProbe()

// Send a message
workerActor.tell(workMessage)

// Expect a specific response
let result = probe.expectMessage(
    ExpectedResponseMessage(),
    Duration.second * 5
)
match (result) {
    case TestExpectation.Success(msg) =>
        println("Test passed: ${msg.messageType()}")
    case TestExpectation.Timeout =>
        println("Test failed: timeout")
    case TestExpectation.UnexpectedMessage(msg) =>
        println("Test failed: unexpected message ${msg.messageType()}")
    case TestExpectation.Error(ex) =>
        println("Test failed: ${ex.message}")
}

// Expect no message
let result2 = probe.expectNoMessage(Duration.millisecond * 100)

// Verify count
let count = probe.getReceivedMessageCount()
println("Received ${count} messages")

testSystem.terminate()
```

---

## 14. API Extensions

### Package

```cangjie
import cactor.api.extensions.ActorSystemExtensions
```

### ActorSystemExtensions

Factory methods for common actor creation patterns.

```cangjie
public struct ActorSystemExtensions {
    // Create default system
    public static func createDefault(): ActorSystem

    // Create named system
    public static func create(name: String): ActorSystem

    // Create actor with factory
    public static func actorOf<T>(
        system: ActorSystem,
        creator: () -> T,
        name: String
    ): ActorRef where T <: Actor

    // Create actor with auto-name
    public static func actorOf<T>(
        system: ActorSystem,
        creator: () -> T
    ): ActorRef where T <: Actor

    // Create with mailbox config
    public static func actorOfWithMailbox<T>(
        system: ActorSystem,
        creator: () -> T,
        name: String,
        mailboxConfig: MailboxConfig
    ): ActorRef where T <: Actor

    // Create with dispatcher config
    public static func actorOfWithDispatcher<T>(
        system: ActorSystem,
        creator: () -> T,
        name: String,
        dispatcherConfig: DispatcherConfig
    ): ActorRef where T <: Actor

    // Create with supervision config
    public static func actorOfWithSupervision<T>(
        system: ActorSystem,
        creator: () -> T,
        name: String,
        supervisionConfig: SupervisionConfig
    ): ActorRef where T <: Actor

    // Create with full configuration
    public static func actorOfConfigured<T>(
        system: ActorSystem,
        creator: () -> T,
        name: String,
        mailboxConfig: MailboxConfig,
        dispatcherConfig: DispatcherConfig,
        supervisionConfig: SupervisionConfig
    ): ActorRef where T <: Actor

    // Batch create actors
    public static func createActors<T>(
        system: ActorSystem,
        creator: () -> T,
        namePrefix: String,
        count: Int32
    ): Array<ActorRef> where T <: Actor
}
```

---

### CActorRuntimeStats

Runtime statistics from the underlying `CActorRuntime`.

```cangjie
public struct CActorRuntimeStats {
    public let systemName: String
    public let isRunning: Bool
    public let actorCount: Int64
    public let messageCount: Int64

    public func printReport(): Unit
    public func getUptimeInSeconds(): Float64
}
```

---

## 15. Usage Examples

### Complete Example — Echo Actor

```cangjie
import cactor.api.CActor
import cactor.core.message.StringMessage
import cactor.core.context.ActorContext
import cactor.core.actor.Actor

class EchoActor <: Actor {
    public prop name: String { get() { "echo" } }
    public prop description: String { get() { "Echoes messages back" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                // Get sender from context
                let sender = context.sender()
                sender match {
                    case Some(path) =>
                        // Would need to send response here
                        println("Echo: ${msg.getContent()}")
                    case None =>
                        println("Echo (no sender): ${msg.getContent()}")
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    let system = CActor.system("EchoSystem")
    let echo = system.actorOf({ => EchoActor() }, "echo")

    echo.tell(StringMessage("Hello, World!"))

    system.terminate()
    0
}
```

---

### Complete Example — Typed Actor

```cangjie
import cactor.core.typed.{TypedBehavior, TypedBehaviors, TypedActorSystem}
import cactor.api.CActorSystem

class GreetingActor {
    public func onMessage(message: String): String {
        "Hello, ${message}!"
    }
}

// Define typed behavior
let behavior = TypedBehaviors.receive<String>({ ctx, msg =>
    ctx.log().info("Received: ${msg}")
    TypedBehaviors.same<String>()
})

// Create typed actor
let system = CActor.system("typed-demo")
let ref = system.typedActorOf(behavior, "greeter")

// Send typed message
ref.tell("World")
```

---

### Complete Example — Router Pool

```cangjie
import cactor.patterns.routing.RouterFactory
import cactor.api.CActor

let router = RouterFactory.roundRobin()

// Create worker pool
let system = CActor.system("router-demo")
for (i in 0..4) {
    let worker = system.actorOf({ => WorkerActor() }, "worker-${i}")
    router.addRoutee(worker)
}

// Route messages
for (work in workItems) {
    router.route(work)
}
```

---

### Complete Example — Circuit Breaker with Fallback

```cangjie
import cactor.patterns.circuit_breaker.{CircuitBreakerFactory, CircuitBreakerDecorator}
import cactor.patterns.ask.Ask

let breaker = CircuitBreakerFactory.createDefault<Int64>()
let decorator = CircuitBreakerDecorator<Int64>(
    breaker,
    Some({ => -1 })  // Fallback value
)

// Use decorator for safe execution
let result = decorator.call({ =>
    // Call to unreliable external service
    callExternalService()
})

// Check circuit state
let stats = breaker.getStats()
println("Failure rate: ${stats.getFailureRate()}")
println("Current state: ${breaker.getState()}")
```

---

## Related Links

- [Architecture Design](architecture.md) — CActor's layered architecture
- [Performance Optimization](performance.md) — Performance tuning guide
- [Package Structure](package-structure.md) — Module organization
- [Roadmap](roadmap.md) — Future development plans

---

**CActor — type-safe, high-performance Actor programming for Cangjie.**