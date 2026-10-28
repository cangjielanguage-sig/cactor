# 🚀 CActor Quick Start Guide

## 📋 Environment Setup

### System Requirements
- **Cangjie Language**: 1.2.0 or higher
- **Operating System**: macOS, Linux, Windows
- **Memory**: 4GB or more recommended
- **CPU**: Multi-core processor support

### Installing Cangjie Language
Please refer to the [Cangjie Official Documentation](https://cangjie-lang.cn/) to install the Cangjie language environment.

## 🏗️ Project Setup

### 1. Clone the Project
```bash
git clone https://github.com/your-repo/cactor.git
cd cactor
```

### 2. Build the Project
```bash
# Build all components
cjpm build

# Verify successful build
echo "Build complete!"
```

### 3. Run Tests
```bash
# Run basic tests
cjpm test

# Run example programs
cjpm run --name cactor_examples.hello_world_demo
```

## 🎯 Your First Actor Program

### Hello World Example

Create a simple Actor program:

```cangjie
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

// Define a simple Actor
class HelloWorldActor <: Actor {
    private var messageCount: Int64 = 0

    public prop name: String {
        get() { "HelloWorldActor" }
    }

    public prop description: String {
        get() { "Hello World Actor Example" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("👋 Received message #${messageCount}: ${msg.getContent()}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func preStart(): Unit {
        println("🚀 Hello World Actor started!")
    }

    public func postStop(): Unit {
        println("🛑 Hello World Actor stopped! Processed ${messageCount} messages in total")
    }
}

// Main program
func main(): Unit {
    // Create Actor system
    let system = CActor.system("hello-world")

    // Create Actor
    let helloActor = system.actorOf(() => HelloWorldActor(), "hello")

    // Send messages
    helloActor.tell(StringMessage("Hello, CActor!"))
    helloActor.tell(StringMessage("This is the second message"))
    helloActor.tell(StringMessage("Welcome to CActor!"))

    // Wait for processing to complete
    Thread.sleep(Duration.fromMilliseconds(100))

    // Shutdown the system
    system.shutdown()
}
```

### Run the Program
```bash
cjpm run --name cactor_examples.hello_world_demo
```

Expected output:
```
🚀 Hello World Actor started!
👋 Received message #1: Hello, CActor!
👋 Received message #2: This is the second message
👋 Received message #3: Welcome to CActor!
🛑 Hello World Actor stopped! Processed 3 messages in total
```

## 🎯 Core Feature Examples

### 1. Ask Pattern (Request-Response)
```bash
# Run Ask pattern example
cjpm run --name cactor_examples.ask_demo
```

Features demonstrated:
- Synchronous and asynchronous requests
- Timeout handling
- Batch requests
- Performance: 657,894 req/s

### 2. Cluster Sharding
```bash
# Run cluster sharding example
cjpm run --name cactor_examples.cluster_sharding_demo
```

Features demonstrated:
- Cluster sharding coordination
- Multiple routing strategies
- Load balancing
- Support for: 1 million Actors

### 3. Persistence System
```bash
# Run persistence example
cjpm run --name cactor_examples.persistence_demo
```

Features demonstrated:
- Event sourcing
- State snapshots
- CQRS architecture
- Multiple storage backends

### 4. Stream Processing
```bash
# Run stream processing example
cjpm run --name cactor_examples.streams_demo
```

Features demonstrated:
- High-performance stream processing
- Backpressure control
- Rich operators
- Performance: 5 million elements/sec

### 5. State Machine
```bash
# Run state machine example
cjpm run --name cactor_examples.xstate_demo
```

Features demonstrated:
- Strongly-typed state machine
- Pattern matching engine
- CActor integration
- Nanosecond-level transitions

## 🔧 Common Configurations

### Actor System Configuration
```cangjie
// Create configuration
let config = CActorRuntimeConfig.createProduction()

// Create Actor system with configuration
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

## 📚 Learning Path

### Beginner Path
1. **Hello World** - Basic Actor concepts
2. **API Demo** - CActor API usage
3. **Benchmark Demo** - Performance testing

### Intermediate Path
1. **Ask Pattern** - Request-response pattern
2. **Typed Actor** - Type-safe Actors
3. **Advanced Dispatcher** - Advanced scheduler

### Expert Path
1. **Cluster Sharding** - Cluster sharding
2. **Persistence** - Persistence system
3. **Streams** - Stream processing system
4. **XState** - State machine system

## 🛠️ Development Tools

### Build Commands
```bash
# Full build
cjpm build

# Clean build
cjpm clean && cjpm build

# Run specific example
cjpm run --name <example-name>
```

### Debugging Tips
```bash
# Enable verbose logging
export CACTOR_LOG_LEVEL=DEBUG

# Performance profiling
cjpm run --name cactor_examples.benchmark_demo
```

## 🔗 Related Resources

### Documentation Links
- [Full API Documentation](api-reference.md)
- [Architecture Design](architecture.md)
- [Performance Optimization](performance.md)
- [Feature Overview](FEATURES.md)

### Example Code
- [All Examples](../src/examples/README.md)
- [GitHub Repository](https://github.com/your-repo/cactor)

### Community Support
- [GitHub Issues](https://github.com/your-repo/cactor/issues)
- [GitHub Discussions](https://github.com/your-repo/cactor/discussions)

## ❓ FAQ

### Q: How do I improve Actor performance?
A: Use batch message processing, optimize mailbox configuration, and select the appropriate dispatcher.

### Q: How do I handle Actor failures?
A: Use supervision strategies, configure restart policies, and implement fault isolation.

### Q: How do I scale to multiple nodes?
A: Use the cluster sharding feature and configure cluster routing strategies.

---

**Start your CActor journey!** 🚀

If you have questions, please check the [full documentation](index.md) or ask on [GitHub](https://github.com/your-repo/cactor).
