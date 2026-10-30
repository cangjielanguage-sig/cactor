<div align="center">

# CActor

**An actor model library for Cangjie · the runtime layer for AI agents**

It carries an agent's state, concurrency, failures and location: private context, serial mailboxes, supervision trees, location transparency.<br/>
It is DSL-first: enums are protocols, `@actorReceive` generates dispatch at compile time, `100.millis` and `<<` shorten timeouts and sends.<br/>
Agents describe *what* to do; CActor handles scheduling, isolation and fault tolerance.

[![Cangjie](https://img.shields.io/badge/Cangjie-1.2.0-blue.svg)](https://cangjie-lang.cn/)
[![Version](https://img.shields.io/badge/version-1.2.0-orange.svg)](CHANGELOG.md)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

English | [中文](README.md)

</div>

> **Status: early development (1.2.0).** APIs may still change, and the project is not yet recommended for production use. See [Current Limitations](#current-limitations) for known issues and the [Roadmap](#roadmap) for what comes next.

---

## Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
- [Using CActor in Your Project](#using-cactor-in-your-project)
- [Usage Examples](#usage-examples)
- [Core Concepts](#core-concepts)
- [Architecture](#architecture)
- [Example Programs](#example-programs)
- [Testing](#testing)
- [Current Limitations](#current-limitations)
- [Roadmap](#roadmap)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

CActor is an actor model library for Cangjie, positioned as **the runtime layer for AI agents**. An agent needs its own state, has to work alongside many other agents, must recover when a model call times out or a tool fails, and should scale from one process to many machines. These are exactly the four problems the actor model addresses:

| What an agent needs | What CActor provides | How |
|---|---|---|
| **State**: memory, conversations, intermediate tool results | Private context | State belongs to one actor; others can only send it messages. Typed actors keep state by returning a new behavior |
| **Concurrency**: hundreds or thousands of agents at once | Serial mailboxes | Each actor handles one message at a time, so no locks inside; the dispatcher runs many actors on a fixed thread pool (measured with one million actors) |
| **Failure**: model timeouts, tool exceptions | Supervision trees | Failures go to the parent, which restarts / resumes / stops / escalates; `watch` reports other agents stopping; backpressure strategies for bounded mailboxes |
| **Location**: from one process to many nodes | Location transparency | Local and remote refs share `tell` / `ask` / `watch`; reliable delivery; cluster membership, singleton, sharding, pub/sub, CRDT replication |

CActor is also **DSL-first**, using Cangjie language features so agent code only describes *what* to do:

- **Enums are protocols**: a typed actor's protocol is an `enum`, and exhaustive `match` catches unhandled messages at compile time
- **`@actorReceive`**: generates a type-dispatching `receive` from `onXxx(msg: T, ctx)` methods at compile time, and rejects two handlers for the same type
- **`100.millis`, `3.seconds`** duration literals; **`ref << a << b`** chained sends
- **Declarative behaviors and workflows**: `TypedBehaviors` (`setup` / `receive` / `supervise` / `withTimers` / `withStash` / `intercept` ...) for actor behavior, Streams for data pipelines, XState-style state machines for workflows (Streams and XState are experimental)

Agents describe *what* to do; scheduling, isolation and fault tolerance are left to CActor.

A minimal agent (complete and runnable):

```cangjie
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*
import cactor_core.dsl.*

// Protocol: the enum is the protocol
enum AgentCmd {
    | Ask(String, TypedActorRef<String>)   // question and reply address
    | Remember(String)
    | Reflect                               // periodic memory review
}

// Behavior: private context (memory) is kept by returning a new behavior, no locks
func agent(memory: Array<String>): TypedBehavior<AgentCmd> {
    TypedBehaviors.receive<AgentCmd>({ ctx, cmd =>
        match (cmd) {
            case AgentCmd.Ask(question, replyTo) =>
                replyTo << "got: ${question} (${memory.size} memories)"
                TypedBehaviors.same<AgentCmd>()
            case AgentCmd.Remember(fact) => agent(memory.concat([fact]))
            case AgentCmd.Reflect =>
                ctx.log().info("reviewing ${memory.size} memories")
                TypedBehaviors.same<AgentCmd>()
        }
    })
}

main() {
    let system = CActorSystemFactory.create("agents")
    // Timers + supervision: restart on failure, at most 3 times per minute
    let behavior = TypedBehaviors.withTimers<AgentCmd>({ timers =>
        timers.startTimerAtFixedRate("reflect", AgentCmd.Reflect, 30.seconds)
        agent([])
    })
    let assistant = system.typedActorOf<AgentCmd>(
        TypedBehaviors.supervise(behavior).onFailure(TypedSupervisorStrategy.restart().withLimit(3, 1.minutes)),
        "assistant")
    assistant << AgentCmd.Remember("prefers English") << AgentCmd.Remember("timezone UTC+8")
    let answer = TypedAsk.ask<AgentCmd, String>(assistant, { r => AgentCmd.Ask("today's plan?", r) }, 3.seconds)
        .get(3.seconds)
    println(answer)   // got: today's plan? (2 memories)
    system.terminate()
}
```

---

## Features

| Area | What it covers | Status |
|------|----------------|--------|
| Actor basics | Actor / ActorRef / ActorSystem, lifecycle hooks, children, `watch`, `become`, timers, stash, receive timeout, lookup by path, dead letters | Usable |
| Typed actors | `TypedBehaviors`, `TypedActorRef`, ask from actors, `messageAdapter`, `pipeToSelf`, Receptionist, behavior interceptors, modeled after Akka Typed | Usable |
| Fault tolerance | Supervision trees (OneForOne / OneForAll / Escalate / Backoff), DeathPact, circuit breaker | Usable |
| Patterns | Ask, routing, bounded-mailbox backpressure (drop / fail / block) | Usable |
| DSL | Enum protocols, `@actorReceive`, duration literals, `<<` | Usable |
| Testing | `TestProbe`, `BehaviorTestKit`, virtual clock `ManualTime` | Usable |
| Observability | Leveled and structured logging, MDC, JSON Lines, Prometheus metrics | Usable |
| Remoting | TCP transport, remote ask / watch, cross-node replies, reliable delivery | Usable, no TLS yet |
| Clustering | Gossip membership, split-brain resolver, Cluster Singleton, distributed pub/sub, cluster sharding (passivation, remember-entities), virtual actors (Grains), CRDTs and Replicator | Usable, verified with multi-node, network-partition and multi-process crash tests |
| Persistence | File event journal, snapshots, event-sourced actors, durable state (optimistic concurrency), persistence query, URI-selected store plugins | Usable |
| Streams | Streams DSL | Experimental |
| State machines | XState-style state machines | Experimental |

---

## Getting Started

### Requirements

- Cangjie SDK 1.2.0 (including `cjc` and `cjpm`)
- Linux x86_64 (the only platform verified so far)

### Build from source

```bash
git clone https://atomgit.com/Cangjie-SIG/cactor.git
cd cactor

cjpm build
```

### Run an example

```bash
./target/release/bin/cactor_examples.hello_world_demo
```

---

## Using CActor in Your Project

CActor modules are currently referenced by local path. Add the following to your `cjpm.toml`:

```toml
[dependencies]
cactor          = { path = "/path/to/cactor/modules/cactor" }    # facade API
cactor_core     = { path = "/path/to/cactor/modules/core" }      # Actor, Message and other core types
cactor_patterns = { path = "/path/to/cactor/modules/patterns" }  # Ask and other patterns (optional)
```

Add other modules (such as `cactor_streams` or `cactor_xstate`) as needed; each lives at `modules/<name>`.

---

## Usage Examples

All three snippets below have been compiled and run in a standalone project using the dependency setup above.

### Classic actor

```cangjie
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

class HelloActor <: Actor {
    public prop name: String { get() { "HelloActor" } }
    public prop description: String { get() { "A simple greeting actor" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                println("Received: ${msg.getContent()}")
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    let system = CActor.system("hello-system")
    let actor = system.actorOf({ => HelloActor() }, "hello")
    actor.tell(StringMessage("Hello, CActor!"))   // prints: Received: Hello, CActor!
    system.terminate()
    return 0
}
```

### Typed actor

Message types are checked by the compiler, and state is updated by returning a new behavior instead of mutating fields.

```cangjie
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.{TypedBehavior, TypedBehaviors}

enum CounterMsg {
    | Increment
    | Print
}

func counter(count: Int64): TypedBehavior<CounterMsg> {
    TypedBehaviors.receive<CounterMsg>({ ctx, msg =>
        match (msg) {
            case Increment => counter(count + 1)
            case Print =>
                println("count = ${count}")
                TypedBehaviors.same<CounterMsg>()
        }
    })
}

main(): Int64 {
    let system = CActorSystemFactory.create("typed-system")
    let ref = system.typedActorOf<CounterMsg>(counter(0), "counter")
    ref.tell(Increment)
    ref.tell(Increment)
    ref.tell(Print)                               // prints: count = 2
    system.terminate()
    return 0
}
```

### Ask (request-response)

```cangjie
import std.time.*
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}
import cactor_patterns.ask.{Ask, AskMessage, AskResponse, Result}

class EchoActor <: Actor {
    public prop name: String { get() { "EchoActor" } }
    public prop description: String { get() { "Echoes Ask requests" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case ask: AskMessage =>
                let reply = match (ask.getOriginalMessage()) {
                    case s: StringMessage => StringMessage("echo: ${s.getContent()}")
                    case _ => StringMessage("unknown")
                }
                ask.getReplyTo().tell(AskResponse(ask.getRequestId(), reply, true))
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    let system = CActor.system("ask-system")
    let echo = system.actorOf({ => EchoActor() }, "echo")

    let future = Ask.ask(echo, StringMessage("hi"), Duration.second * 3)
    match (future.get()) {
        case Result.Success(response) =>
            match (response) {
                case s: StringMessage => println("Reply: ${s.getContent()}")   // prints: Reply: echo: hi
                case _ => ()
            }
        case Result.Failure(error) => println("Failed: ${error.message}")
    }

    Ask.shutdown()
    system.terminate()
    return 0
}
```

See [Example Programs](#example-programs) and the [API Reference](docs/en/api-reference.md) for more.

---

## Core Concepts

### Actor

An actor is the basic unit of concurrent computation. Each actor has:

- **Private state** that can only be changed through messages
- **A mailbox** that receives messages from other actors
- **Behavior** that defines how incoming messages are handled

```
┌─────────────────────────────────────────────────────────────┐
│                        Actor                                │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                      State                          │    │
│  │              messageCount: Int64                    │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                     Mailbox                         │    │
│  │  [msg1] [msg2] [msg3] [msg4] ...                    │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                     Behavior                        │    │
│  │           receive(message, context)                 │    │
│  │              → MessageResult                        │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Message results

`receive` returns a `MessageResult` that tells the runtime what happened to the message, for example `Handled`, `Unhandled`, or `Stop` (stop the actor after this message).

### Lifecycle

States are defined by `ActorLifecycleState`; the `preStart`, `postStop`, `preRestart` and `postRestart` hooks run at the corresponding transitions.

```
    ┌─────────┐
    │ Created │  ← actor instance created
    └────┬────┘
         │ preStart()
         ▼
    ┌─────────┐
    │Starting │  ← initializing
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ Running │  ← processing messages
    └────┬────┘
         │
    ┌────┴────┐
    │         │
 Stopped  Failed ← stopped or failed
    │         │
    │    preRestart()
    │         │
    └────┬────┘
         │
    Restarting
         │
         └──→ Running
```

### Supervision

When an actor fails, its supervision strategy decides what to do: `Resume` (ignore and continue), `Restart`, `Stop`, or `Escalate` (hand the failure to the parent). Built-in strategies include `OneForOneStrategy`, `OneForAllStrategy`, `EscalateStrategy` and the exponential-backoff `BackoffSupervisorStrategy`, all in `cactor_core.supervision`.

---

## Architecture

The project is a cjpm workspace of 15 modules under `modules/`. Dependencies only point from higher layers to lower ones:

```
┌─────────────────────────────────────────────────────────────────┐
│                          User code                              │
│   usually goes through the cactor facade; modules can also be   │
│   used directly when needed                                     │
└─────────────────────────────────────────────────────────────────┘
                                │
     ┌──────────────┬───────────┴──────┬───────────────────┐
     ▼              ▼                  ▼                   ▼
┌───────────┐ ┌──────────────┐ ┌───────────────┐ ┌───────────────┐
│  Streams  │ │ Distribution │ │    XState     │ │  Integration  │
├───────────┤ ├──────────────┤ ├───────────────┤ ├───────────────┤
│ • DSL     │ │ • Sharding   │ │ • Machines    │ │ • Logging     │
│ • Back-   │ │ • Remoting   │ │ • Parallel    │ │ • Metrics     │
│  pressure │ │ • Persistence│ │   states      │ │ • Config      │
│ Experim.  │ │ Prototype    │ │ Experimental  │ │               │
└───────────┘ └──────────────┘ └───────────────┘ └───────────────┘
     │              │                  │                   │
     └──────────────┴────────┬─────────┴───────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor (facade API)                         │
│   CActor.system() | CActorSystem | ActorProps                   │
│   CActorFactory | CActorSystemBuilder                           │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_patterns                             │
│   Ask | Router | Backpressure                                   │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_runtime                              │
│   CActorRuntime | Mailbox | Dispatcher | Guardian               │
│   Registry | ActorLifecycleManager                              │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_core                                 │
│   Actor | ActorRef | Props | ActorSystem                        │
│   Message | ActorContext | Supervision                          │
│   TypedBehavior | TypedActorRef                                 │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_foundation                           │
│   Queue | Serializer | Memory | Monitoring                      │
│   Network | Config | DI | CircuitBreaker                        │
└─────────────────────────────────────────────────────────────────┘

cactor_macros is a standalone macro module; cactor_tests and
cactor_examples sit at the top.
```

| Module | Package | Responsibility |
|--------|---------|----------------|
| foundation | `cactor_foundation` | Queues, memory, serialization, networking, config, DI, monitoring, circuit breaker |
| core | `cactor_core` | Core abstractions: Actor, ActorRef, ActorSystem, Message, supervision, typed actors |
| runtime | `cactor_runtime` | Mailboxes, dispatchers, guardian, registry, lifecycle |
| patterns | `cactor_patterns` | Ask, routing, backpressure |
| cactor | `cactor` | Public facade API (`cactor`, `cactor.api`, `cactor.config`) |
| macros | `cactor_macros` | Actor DSL macros |
| streams | `cactor_streams` | Stream processing DSL |
| remote | `cactor_remote` | Remoting (prototype) |
| persistence | `cactor_persistence` | Event sourcing, snapshots, CQRS (prototype) |
| cluster | `cactor_cluster` | Cluster, sharding, failover (prototype) |
| distribution | `cactor_distribution` | Umbrella: re-exports remote / persistence / cluster, plus streaming (prototype) |
| xstate | `cactor_xstate` | State machines |
| integration | `cactor_integration` | Logging, metrics and config integration, plus integration test programs |
| tests | `cactor_tests` | Unit and integration tests |
| examples | `cactor_examples` | Example programs |

For more detail, see [Architecture](docs/en/architecture.md) and [Package and Module Structure](docs/en/package-structure.md).

---

## Example Programs

Examples live in [`modules/examples/src`](modules/examples/src). After building, each one is available as `target/release/bin/cactor_examples.<example-name>`.

| Example | Topic |
|---------|-------|
| [hello_world_demo](modules/examples/src/hello_world_demo/main.cj) | Creating actors and sending messages |
| [api_demo](modules/examples/src/api_demo/main.cj) | Public API usage |
| [ask_pattern_demo](modules/examples/src/ask_pattern_demo/main.cj) | Ask request-response, timeouts and batch requests |
| [typed_actor_demo](modules/examples/src/typed_actor_demo/main.cj) | Typed actor system |
| [advanced_mailbox_examples](modules/examples/src/advanced_mailbox_examples/main.cj) | Mailbox implementations |
| [advanced_dispatcher_examples](modules/examples/src/advanced_dispatcher_examples/main.cj) | Dispatcher implementations |
| [streams_demo](modules/examples/src/streams_demo/main.cj) | Stream processing (experimental) |
| [xstate_demo](modules/examples/src/xstate_demo) | State machines (experimental) |
| [simple_macro_demo](modules/examples/src/simple_macro_demo/main.cj) | DSL macros (prototype) |
| [cluster_sharding_demo](modules/examples/src/cluster_sharding_demo/main.cj) | Cluster sharding (prototype) |
| [persistence_demo](modules/examples/src/persistence_demo/main.cj) | Event sourcing (prototype) |

---

## Testing

```bash
cjpm test                           # run all tests
python3 tools/check_module_deps.py  # check module layering
```

The workspace currently has 366 test cases (`tools/ci.sh`), all passing on Cangjie 1.2.0. CI also runs the chat room, a 50,000-child scale test, a 20-second soak test, and two 3-process cluster checks (normal traffic and a `kill -9` crash). Besides supervision strategies, typed actors (behavior transforms, stash, timers) and mailboxes, they cover concurrency semantics (serial processing, concurrent create/stop, supervised restart), queue stress, TCP/UDP transport, remote end-to-end messaging, and file-based event journal and event-sourcing recovery.

Benchmark program: `./target/release/bin/cactor_examples.real_benchmark`. Measured on a 32-core machine: 5.6–7.2 M msg/s for a single actor, ping-pong round-trip P50 about 2 µs, about 150–190 k msg/s for remote TCP over loopback. A same-machine comparison with Akka 2.6.21 is in the [performance analysis](docs/plan/20261028-performance.md) (in Chinese): point-to-point scenarios match or beat Akka, while fan-out is still about 3× slower and remoting about 1.6× slower.

---

## Current Limitations

The following issues are known and are being addressed according to the [development plan](docs/plan/20261028.md) (in Chinese):

- **Scheduling**: affinity scheduling without work stealing yet; fan-out throughput is still well below Akka; blocking inside `receive` occupies a worker thread and slows other actors pinned to it.
- **Remoting**: TCP transport on `std.net`. Plain `tell` is at-most-once; `tellReliable` adds sequence numbers, acks, resends and de-duplication. Remote ask, cross-node replies and remote watch are supported. Message codecs are versioned for rolling upgrades (new code reads older versions; old nodes reject newer versions explicitly). No TLS yet.
- **Clustering**: `cactor_cluster.membership` provides gossip membership, a split-brain resolver (KeepMajority / KeepOldest / StaticQuorum), Cluster Singleton, distributed pub/sub and cluster sharding (idle passivation, remember-entities, and state recovery after handoff or a crash when combined with event sourcing); `cactor_cluster.ddata` provides CRDTs and a Replicator. All are covered by multi-node tests and multi-process checks (`tools/cluster_demo.sh`, `tools/cluster_crash_demo.sh`). The split-brain resolver uses a gossiped reachability table, so indirectly connected members in asymmetric partitions are downed; virtual actors (`Grains`, Orleans-style) activate on demand, keep a single activation, deactivate when idle and reactivate after a crash; the Replicator uses digest-based anti-entropy and ships only differing keys. The old classes in the `cactor_cluster` root package (`SimpleClusterManager`, `ShardRegion`, ...) are early prototypes and will be removed in 1.3.0.
- **Macros**: `@actorReceive` and `@Message` (generates the message type name and a codec) work; most other DSL macros do not yet perform real expansion.
- **Logging**: library logging goes through a leveled logger (default WARN, written to stderr, adjustable with the `CACTOR_LOG_LEVEL` environment variable), but the repository still has three logging implementations; they are to be merged in 1.3.0.
- **Old benchmark programs**: apart from `real_benchmark`, the benchmark programs in the repository measure synchronous function-call overhead, not message passing.

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) (in Chinese) for the full roadmap. Release plan in brief:

- [x] **1.2.0**: Cangjie 1.2.0; multi-module workspace; core runtime with classic and typed actor semantics (supervision tree, watch, become, timers, stash, ...); remoting on std.net and file-based persistence; reproducible benchmarks
- [ ] **1.3.0 Consolidation**: remove dead and duplicate code; one logger, one config, one ask kernel and one scheduler; a real JSON codec; API snapshots; no `sleep` in tests; re-run benchmarks on an idle machine
- [ ] **1.4.0 New API and HTTP**: `actorSystem {}` / `spawn` / `ask` entry points, `@Actor` / `@Message` macros, a Ktor-style HTTP server integrated with actors
- [ ] **1.5.0 Performance and reliable remoting**: native HTTP engine, fan-out improvements, remote ask / watch, reliable delivery, TLS
- [ ] **1.6.0 Clustering and production readiness**: membership, singleton, split-brain resolution; HTTP client, WebSocket, production plugins
- [ ] **2.0.0**: sharding, a persistence plugin API, API freeze and publication to the central repository

---

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](docs/en/QUICK_START.md) | Installation and a first program |
| [Getting Started](docs/en/getting-started.md) | Basic concepts and common usage |
| [Architecture](docs/en/architecture.md) | Layered design and key components |
| [Package and Module Structure](docs/en/package-structure.md) | Modules and their dependencies |
| [API Reference](docs/en/api-reference.md) | Main types and interfaces |
| [Performance Tuning Guide](docs/en/performance.md) | Compiler flags, runtime parameters, performance rules for actors, how to measure |
| [Documentation index](docs/en/README.md) | All English documents |
| [Changelog](CHANGELOG.md) | Release history |

Some documents are still being updated alongside the code and may not match the current implementation in every detail. When in doubt, the code is authoritative, and corrections are welcome.

---

## Contributing

Issues and pull requests are welcome, whether they fix bugs, add tests or improve documentation. Please read the [contributing guide](CONTRIBUTING.md) first, and make sure that:

```bash
cjpm build                          # the build succeeds
cjpm test                           # all tests pass
python3 tools/check_module_deps.py  # the layering check passes
```

New features should come with tests, and changes to the public API should update the documentation and the [Changelog](CHANGELOG.md).

---

## License

CActor is released under the [MIT License](LICENSE).

## Acknowledgements

- The [Cangjie programming language](https://cangjie-lang.cn/) team for the language and toolchain
- The [Akka](https://akka.io/) project, which inspired much of CActor's design
- Everyone who has contributed to this project
