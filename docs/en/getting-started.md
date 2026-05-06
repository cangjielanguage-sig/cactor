# CActor Getting Started Guide

## Environment Preparation

### System Requirements

- **Cangjie Language**: 0.53.4 or higher
- **Operating System**: macOS, Linux, Windows
- **Memory**: 4 GB or more recommended
- **CPU**: Multi-core processor supported

### Installing Cangjie Language

Refer to the [Cangjie official documentation](https://cangjie-lang.cn/) to install the Cangjie language environment.

### Installing cjpm

`cjpm` (Cangjie Package Manager) is included with the Cangjie SDK. Verify your installation:

```bash
cangjie --version
cjpm --version
```

## Project Setup

### 1. Clone the Project

```bash
git clone https://github.com/cangjie-lang/cactor.git
cd cactor
```

### 2. Build the Project

```bash
# Build all components
cjpm build

# Verify the build succeeded
echo "Build completed successfully!"
```

### 3. Run Tests

```bash
# Run the full test suite
cjpm test

# Run the Hello World example
cjpm run --name cactor.examples.hello_world_demo
```

## First Actor Program

### Hello World Example

Create a simple Actor program:

```cangjie
import cactor.api.{CActor}
import cactor.core.actor.{Actor}
import cactor.core.message.{Message, StringMessage, MessageResult}
import cactor.core.context.{ActorContext}

// Define a simple Actor
class HelloWorldActor <: Actor {
    private var messageCount: Int64 = 0

    public prop name: String {
        get() { "HelloWorldActor" }
    }

    public prop description: String {
        get() { "Hello World Actor example" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("Received message #${messageCount}: ${msg.getContent()}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func preStart(): Unit {
        println("Hello World Actor started!")
    }

    public func postStop(): Unit {
        println("Hello World Actor stopped! Processed ${messageCount} messages total")
    }
}

// Main program
func main(): Unit {
    // Create the Actor system
    let system = CActor.system("hello-world")

    // Create an Actor
    let helloActor = system.actorOf(() => HelloWorldActor(), "hello")

    // Send messages
    helloActor.tell(StringMessage("Hello, CActor!"))
    helloActor.tell(StringMessage("This is the second message"))
    helloActor.tell(StringMessage("Welcome to CActor!"))

    // Wait for processing to complete
    Thread.sleep(Duration.fromMilliseconds(100))

    // Shut down the system
    system.shutdown()
}
```

### Running the Program

```bash
cjpm run --name cactor.examples.hello_world_demo
```

Expected output:

```
Hello World Actor started!
Received message #1: Hello, CActor!
Received message #2: This is the second message
Received message #3: Welcome to CActor!
Hello World Actor stopped! Processed 3 messages total
```

## Core Feature Examples

### 1. Ask Pattern (Request-Response)

```bash
cjpm run --name cactor.examples.ask_demo
```

Features demonstrated:

- Synchronous and asynchronous requests
- Timeout handling
- Batch requests
- Performance: 657,894 req/s

### 2. Cluster Sharding

```bash
cjpm run --name cactor.examples.cluster_sharding_demo
```

Features demonstrated:

- Cluster sharding coordination
- Multiple routing strategies
- Load balancing
- Supports 1 million Actors

### 3. Persistence System

```bash
cjpm run --name cactor.examples.persistence_demo
```

Features demonstrated:

- Event sourcing
- State snapshots
- CQRS architecture
- Multiple storage backends

### 4. Stream Processing

```bash
cjpm run --name cactor.examples.streams_demo
```

Features demonstrated:

- High-performance stream processing
- Backpressure control
- Rich operator set
- Performance: 5 million elements/sec

### 5. State Machine (XState)

```bash
cjpm run --name cactor.examples.xstate_demo
```

Features demonstrated:

- Strongly typed state machines
- Pattern matching engine
- CActor integration
- Nanosecond transitions

## Common Configurations

### Actor System Configuration

```cangjie
// Create a configuration
let config = CActorRuntimeConfig.createProduction()

// Create an Actor system with configuration
let system = CActor.system("MySystem", config)
```

### Performance Tuning Configuration

```cangjie
// High-performance configuration
let config = CActorRuntimeConfig.createProduction()
    .withDispatcherThreads(8)
    .withMailboxCapacity(10000)
    .withBatchSize(100)

let system = CActor.system("HighPerformanceSystem", config)
```

### Configuration Options Reference

| Option              | Default | Description                              |
|---------------------|---------|------------------------------------------|
| `dispatcherThreads` | 4       | Number of dispatcher threads             |
| `mailboxCapacity`   | 1000    | Maximum mailbox size per Actor           |
| `batchSize`         | 32      | Number of messages processed per batch   |

## Learning Paths

### Beginner Path

Start here if you are new to the Actor model or CActor:

1. **Hello World** -- Basic Actor concepts and lifecycle
2. **API Demo** -- CActor API usage and message passing
3. **Benchmark Demo** -- Performance testing and profiling

### Intermediate Path

Build on the basics with more advanced patterns:

1. **Ask Pattern** -- Request-response communication
2. **Typed Actor** -- Type-safe Actor definitions
3. **Advanced Dispatcher** -- Custom dispatcher configuration

### Expert Path

Master distributed systems and advanced features:

1. **Cluster Sharding** -- Distributed Actor management
2. **Persistence** -- Event sourcing and CQRS
3. **Streams** -- Reactive stream processing
4. **XState** -- State machine integration

## Development Tools

### Build Commands

```bash
# Full build
cjpm build

# Clean build
cjpm clean && cjpm build

# Run a specific example
cjpm run --name <example-name>
```

### Debugging

```bash
# Enable verbose logging
export CACTOR_LOG_LEVEL=DEBUG

# Run performance benchmarks
cjpm run --name cactor.examples.benchmark_demo
```

### Available Examples

Run any example with `cjpm run --name <name>`:

| Example                        | Name                              |
|--------------------------------|-----------------------------------|
| Hello World                    | `cactor.examples.hello_world_demo`|
| Ask Pattern                    | `cactor.examples.ask_demo`        |
| Cluster Sharding               | `cactor.examples.cluster_sharding_demo`|
| Persistence                    | `cactor.examples.persistence_demo`|
| Streams                        | `cactor.examples.streams_demo`    |
| State Machine                  | `cactor.examples.xstate_demo`     |
| Benchmark                      | `cactor.examples.benchmark_demo`  |
| API Demo                       | `cactor.examples.api_demo`        |

## Related Resources

### Documentation

- [API Reference](../api-reference.md)
- [Architecture Guide](../architecture.md)
- [Performance Guide](../performance.md)
- [Feature Overview](../FEATURES.md)
- [Best Practices](../BEST_PRACTICES.md)

### Community Support

- [GitHub Issues](https://github.com/cangjie-lang/cactor/issues)
- [GitHub Discussions](https://github.com/cangjie-lang/cactor/discussions)

## FAQ

### How do I improve Actor performance?

Use batch message processing, tune mailbox capacity, choose an appropriate dispatcher, and adjust the `batchSize` parameter for your workload.

### How do I handle Actor failures?

Use supervision strategies, configure restart policies, and implement fault isolation. CActor provides built-in supervisor hierarchies for resilient systems.

### How do I scale to multiple nodes?

Use the cluster sharding feature and configure cluster routing strategies. See the [Cluster Sharding example](#2-cluster-sharding) for a working demonstration.

### What Cangjie version is required?

Cangjie 0.53.4 or higher is required. Check your version with `cangjie --version`.

### Where can I find more examples?

All examples are located in `src/examples/`. Run `ls src/examples/` to browse available examples, or refer to the [Feature Overview](../FEATURES.md) for a complete list.

---

**Start your CActor journey!**

For questions, check the [full documentation](../index.md) or open an issue on [GitHub](https://github.com/cangjie-lang/cactor).
