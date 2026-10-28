<div align="center">

# CActor

**An actor model library for the Cangjie programming language**

Inspired by Akka / Akka Typed, with classic actors, typed actors, supervision and common patterns such as Ask.

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

The actor model structures concurrent programs as isolated actors that communicate only through messages. Each actor owns its state, processes the messages in its mailbox one at a time, and is handled by its supervisor according to a supervision strategy when it fails.

CActor aims to bring this model to Cangjie. It provides:

- **Two programming styles**: classic actors that extend `Actor`, and typed actors built from `TypedBehavior`
- **Fault tolerance**: supervision strategies and a circuit breaker
- **Common patterns**: Ask (request-response), routing, backpressure
- **A modular layout**: 12 cjpm modules with clear layering, so you depend only on what you use

---

## Features

| Area | What it covers | Status |
|------|----------------|--------|
| Actor basics | Actor / ActorRef / ActorSystem, lifecycle hooks, `tell` | Usable |
| Typed actors | `TypedBehaviors`, `TypedActorRef`, modeled after Akka Typed | Usable |
| Fault tolerance | Supervision (OneForOne / OneForAll / Escalate / Backoff), circuit breaker | Usable |
| Patterns | Ask, routing, backpressure | Usable |
| Runtime components | Several mailbox and dispatcher implementations | Usable, see [limitations](#current-limitations) |
| Streams | Stream processing DSL | Experimental |
| State machines | XState-style state machines | Experimental |
| Distribution | Cluster sharding, remoting, event sourcing / persistence | Prototype; no real network or storage yet |
| Macros | Actor DSL macros | Prototype |

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

The project is a cjpm workspace of 12 modules under `modules/`. Dependencies only point from higher layers to lower ones:

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
| distribution | `cactor_distribution` | Cluster, remoting, persistence (prototype) |
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

There are currently 121 test cases, all passing on Cangjie 1.2.0. They cover supervision strategies, typed actors (behavior transforms, stash, timers), mailboxes and more. Tests for concurrency semantics are still being added.

---

## Current Limitations

The following issues are known and are being addressed according to the [development plan](docs/plan/20261028.md) (in Chinese):

- **Message scheduling**: `tell` currently runs `receive` directly on the caller's thread, so an actor is not yet guaranteed to process one message at a time, and dispatchers are not yet wired into the main message path.
- **Concurrency safety**: some queues and shared containers still lack proper synchronization.
- **Distribution**: remoting, persistence and clustering exist only as interfaces and prototypes, without real network transport or storage.
- **Macros**: most DSL macros do not yet perform real expansion.
- **Log output**: the runtime prints startup and shutdown logs directly to standard output, and this cannot be turned off yet.
- **Performance figures**: the benchmark programs in the repository currently measure synchronous function-call overhead rather than message passing, so no performance numbers are given here.

---

## Roadmap

In priority order; see the [development plan](docs/plan/20261028.md) (in Chinese) for details.

- [x] Upgrade to Cangjie 1.2.0
- [x] Split into 12 cjpm modules with a layering check
- [ ] **Core runtime**: one mailbox per actor, messages processed serially by a dispatcher, `tell` only enqueues, fixed concurrent containers, concurrency-semantics tests
- [ ] **API cleanup and deduplication**: smaller public API per module, remove duplicate and placeholder code
- [ ] **Engineering quality**: a logging abstraction instead of `println`, zero compiler warnings, no `sleep`-based synchronization in tests, CI
- [ ] **Reproducible benchmarks**: nanosecond timing, ping-pong / fan-out scenarios, P50 / P99 statistics
- [ ] **Distribution**: remoting on stdx.net, a file-based event store, then clustering on top of both

---

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](docs/en/QUICK_START.md) | Installation and a first program |
| [Getting Started](docs/en/getting-started.md) | Basic concepts and common usage |
| [Architecture](docs/en/architecture.md) | Layered design and key components |
| [Package and Module Structure](docs/en/package-structure.md) | Modules and their dependencies |
| [API Reference](docs/en/api-reference.md) | Main types and interfaces |
| [Performance Guide](docs/en/performance.md) | Dispatcher, mailbox and other tuning options |
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
