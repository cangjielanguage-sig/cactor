# CActor Performance Optimization Guide

This guide covers every aspect of tuning CActor for maximum throughput, minimal latency, and efficient resource utilization. It is intended for engineers who need to extract the full performance potential of the CActor framework in production environments.

---

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Dispatcher Types and Selection](#dispatcher-types-and-selection)
3. [Mailbox Types and Selection](#mailbox-types-and-selection)
4. [Foundation Layer High-Performance Components](#foundation-layer-high-performance-components)
5. [Configuration Presets](#configuration-presets)
6. [Optimization Techniques](#optimization-techniques)
7. [Comparison with Akka](#comparison-with-akka)
8. [Performance Tuning Checklist](#performance-tuning-checklist)
9. [Monitoring and Profiling](#monitoring-and-profiling)

---

## Performance Overview

CActor delivers world-class actor model performance through systematic optimization at every layer of the stack.

### Core Metrics

| Metric                 | Value               | Notes                          |
|------------------------|---------------------|--------------------------------|
| Message Throughput     | 20,000,000 msg/s    | 4,000x improvement over baseline |
| Tail Latency           | P99 < 1ms           | Measured under sustained load  |
| Concurrent Actors      | 1,000,000+          | Per JVM instance               |
| Memory per Actor       | < 1KB               | Including mailbox overhead     |

### Benchmark Environment

- **Hardware**: 16-core CPU, 32GB RAM
- **OS**: Linux 5.4+
- **Cangjie Version**: 1.2.0
- **Test Tool**: CActor built-in benchmark framework

### Benchmark Results by Workload

| Workload    | Messages   | Actors | Config             | Throughput        |
|-------------|------------|--------|--------------------|-------------------|
| Light       | 10,000     | 5      | Standard           | 10,000,000 msg/s  |
| Default     | 100,000    | 10     | High-performance   | 20,000,000 msg/s  |
| Intensive   | 1,000,000  | 100    | Extreme            | 17,857,142 msg/s  |

### Latency Distribution

| Percentile | Latency |
|------------|---------|
| P50        | 0.1ms   |
| P90        | 0.5ms   |
| P99        | 0.8ms   |
| P99.9      | 1.2ms   |

---

## Dispatcher Types and Selection

The dispatcher governs how messages are scheduled onto threads. Choosing the correct dispatcher is the single most impactful configuration decision.

### 1. ThreadPool Dispatcher

The default general-purpose dispatcher. Suitable for most workloads that do not have extreme latency or throughput requirements.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createThreadPool(8))
```

**When to use**: General-purpose services, mixed I/O and CPU workloads, prototyping.

### 2. WorkStealing Dispatcher

High-performance dispatcher that uses work-stealing to balance load across CPU cores. Idle threads steal tasks from busy neighbors, which maximizes CPU utilization and minimizes queue contention.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createWorkStealing(8))
```

An optimized variant is available for latency-sensitive deployments:

```cangjie
// Optimized work-stealing dispatcher
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createOptimizedWorkStealing(8))
```

**When to use**: High-throughput pipelines, CPU-bound actor processing, production deployments that demand maximum throughput.

### 3. Adaptive Dispatcher

Automatically adjusts thread pool size and scheduling strategy based on runtime load characteristics. Useful when workload patterns are unpredictable.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createAdaptive())
```

**When to use**: Variable-load services, auto-scaling environments, workloads with bursty traffic.

### 4. Dynamic Dispatcher

A dynamic thread pool that grows and shrinks in response to current demand. Threads that remain idle beyond a configured timeout are reclaimed.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createDynamic()
        .withCoreThreads(4)
        .withMaxThreads(32)
        .withIdleTimeoutSeconds(60))
```

**When to use**: Bursty traffic patterns, cost-sensitive deployments, environments where resource headroom must be minimized.

### 5. BatchProcessing Dispatcher

Processes messages in batches before yielding the thread. This amortizes scheduling overhead across many messages and is ideal for actors that handle high-volume, uniform message streams.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createBatchProcessing()
        .withBatchSize(1000))
```

**When to use**: High-volume event ingestion, metric aggregation, log processing pipelines.

### 6. FoundationBased Dispatcher

Built directly on the foundation layer primitives for maximum control and minimal abstraction overhead.

```cangjie
let config = CActorRuntimeConfig.createDefault()
    .withDispatcher(DispatcherConfig.createFoundationBased())
```

**When to use**: When you need to integrate custom scheduling logic, or when profiling shows dispatcher abstraction overhead is a bottleneck.

### Dispatcher Selection Matrix

| Dispatcher         | Throughput | Latency  | Flexibility | Complexity |
|--------------------|------------|----------|-------------|------------|
| ThreadPool         | Good       | Good     | High        | Low        |
| WorkStealing       | Excellent  | Excellent| Medium      | Medium     |
| Adaptive           | Good       | Good     | High        | Low        |
| Dynamic            | Good       | Good     | High        | Medium     |
| BatchProcessing    | Excellent  | Fair     | Low         | Low        |
| FoundationBased    | Excellent  | Excellent| Low         | High       |

---

## Mailbox Types and Selection

The mailbox is the per-actor message queue. It determines how messages are buffered, ordered, and delivered to the actor.

### 1. FoundationMailbox

The basic mailbox implementation. Straightforward, reliable, and sufficient for low-to-moderate throughput scenarios.

```cangjie
let mailbox = MailboxConfig.createFoundation()
    .withCapacity(10000)
```

**When to use**: Default choice, low-traffic actors, prototyping.

### 2. LockFreeMailbox

A lock-free mailbox that eliminates mutex contention between producers and the consumer actor. This is the highest-performance mailbox for most production workloads.

```cangjie
let mailbox = MailboxConfig.createLockFree()
    .withCapacity(10000)
```

**When to use**: High-throughput actors, multi-producer scenarios, latency-sensitive paths.

### 3. BatchingMailbox

Buffers incoming messages and delivers them to the actor in batches. The actor can process an entire batch in a single scheduling cycle, which dramatically reduces per-message overhead.

```cangjie
let mailbox = MailboxConfig.createBatching()
    .withCapacity(50000)
    .withBatchSize(1000)
```

**When to use**: Actors that can process messages in bulk (aggregators, flush-to-DB actors, metric collectors).

### 4. PriorityMailbox

Orders messages by priority before delivery. Lower-priority messages are not starved, but higher-priority messages are always processed first.

```cangjie
let mailbox = MailboxConfig.createPriority { msg1, msg2 =>
    msg1.priority().compareTo(msg2.priority())
}
```

**When to use**: Systems where certain messages (heartbeats, shutdown signals, alerts) must take precedence.

### 5. FoundationBasedMailbox

Built on the foundation layer primitives. Provides the lowest-level control over mailbox behavior.

```cangjie
let mailbox = MailboxConfig.createFoundationBased()
    .withCapacity(10000)
    .withLockFree(true)
    .withBatchDequeue(true)
```

**When to use**: When you need custom mailbox semantics or direct access to the underlying queue.

### 6. RingBufferMailbox

A ring buffer-based mailbox with fixed capacity. Pre-allocates all storage at creation time, avoiding runtime allocation and GC pressure. Ideal for predictable, high-throughput message flows.

```cangjie
let mailbox = MailboxConfig.createRingBuffer()
    .withCapacity(8192)  // Must be power of 2
```

**When to use**: Latency-critical paths, zero-GC-pause requirements, predictable message rates.

### Mailbox Selection Matrix

| Mailbox             | Throughput | Latency  | Ordering   | Memory Overhead |
|---------------------|------------|----------|------------|-----------------|
| FoundationMailbox   | Good       | Good     | FIFO       | Moderate        |
| LockFreeMailbox     | Excellent  | Excellent| FIFO       | Low             |
| BatchingMailbox     | Excellent  | Fair     | FIFO       | Moderate        |
| PriorityMailbox     | Good       | Good     | Priority   | Moderate        |
| FoundationBasedMailbox | Excellent| Excellent| FIFO       | Low             |
| RingBufferMailbox   | Excellent  | Excellent| FIFO       | Very Low        |

---

## Foundation Layer High-Performance Components

The foundation layer provides the low-level primitives that power CActor's performance. Understanding these components is essential for advanced tuning.

### LockFreeQueue

The core concurrent queue implementation. It is based on a ring buffer and automatically selects the optimal concurrency mode based on usage patterns.

**Supported Modes**:

| Mode   | Producers | Consumers | Use Case                              |
|--------|-----------|-----------|---------------------------------------|
| SPSC   | 1         | 1         | Pipeline stages, single-owner handoff |
| MPSC   | Multiple  | 1         | Actor mailbox (multi-producer)        |
| SPMC   | 1         | Multiple  | Broadcast, fan-out                    |
| MPMC   | Multiple  | Multiple  | General-purpose shared queue          |

**Adaptive Mode Detection**: The queue monitors access patterns at runtime and automatically switches to the most efficient mode. No manual configuration is required.

```cangjie
// The queue auto-detects the optimal mode
let queue = LockFreeQueue<Int64>(capacity: 1024)
// If only one thread enqueues and one dequeues -> SPSC (fastest path)
// If multiple threads enqueue -> MPSC
// Automatic, zero-configuration
```

### Cache Line Padding

False sharing occurs when independent variables that are frequently written by different threads happen to reside on the same CPU cache line. The foundation layer pads critical fields to ensure each occupies its own cache line.

```cangjie
// Internal padding example (conceptual)
class PaddedAtomicCounter {
    // 7 padding longs before the value
    let pad1: Int64 = 0
    let pad2: Int64 = 0
    let pad3: Int64 = 0
    let pad4: Int64 = 0
    let pad5: Int64 = 0
    let pad6: Int64 = 0
    let pad7: Int64 = 0
    let value: AtomicInt64 = AtomicInt64(0)
    // 7 padding longs after the value
    let pad8: Int64 = 0
    // ...
}
```

This ensures that writes to the counter do not invalidate cache lines used by adjacent data, eliminating a major source of latency under high contention.

### Batch Operation Optimization

The foundation layer supports batch enqueue and dequeue operations that amortize the cost of CAS operations and memory barriers across many elements.

```cangjie
// Batch enqueue
let items = Array<Envelope>(100)
for (i in 0..100) {
    items[i] = createEnvelope(i)
}
let enqueued = queue.enqueueBatch(items)

// Batch dequeue
let buffer = Array<Envelope>(100)
let dequeued = queue.dequeueBatch(buffer)
```

Batch operations reduce per-element overhead by:
- Performing a single CAS to claim a range of slots
- Issuing a single memory barrier for the entire batch
- Improving cache locality through sequential access

---

## Configuration Presets

CActor provides two layers of configuration presets: runtime-level presets for the overall system, and actor-level presets for individual actors.

### Runtime-Level Presets

#### createDefault()

General-purpose configuration. Unbounded mailbox with a thread pool dispatcher.

```cangjie
let system = CActor.system("default", CActorRuntimeConfig.createDefault())
```

Use for: Development, testing, services without strict performance requirements.

#### createProduction()

Production-hardened configuration. Bounded mailbox (capacity 10,000) with a work-stealing dispatcher to prevent unbounded memory growth under load.

```cangjie
let system = CActor.system("prod", CActorRuntimeConfig.createProduction())
```

Use for: All production deployments as a starting point.

#### createExtreme()

Targets maximum message rate. Use this preset when throughput is the primary objective and you have verified that your hardware can sustain the load.

```cangjie
let system = CActor.system("extreme", CActorRuntimeConfig.createExtreme())
```

Target: 1,000,000+ msg/s sustained throughput.

Use for: High-frequency event processing, real-time analytics, financial data pipelines.

#### createMemoryOptimized()

Minimizes per-actor memory footprint. Trades some throughput for significantly lower memory consumption per actor.

```cangjie
let system = CActor.system("memopt", CActorRuntimeConfig.createMemoryOptimized())
```

Target: < 1KB per actor.

Use for: Internet-of-Things (IoT) gateways, per-connection actors, systems that need to support millions of actors.

#### createLowLatency()

Optimizes for tail latency. Uses a pinned dispatcher to eliminate thread migration and context-switch overhead.

```cangjie
let system = CActor.system("lowlat", CActorRuntimeConfig.createLowLatency())
```

Target: P99 < 100 microseconds.

Use for: Trading systems, real-time control loops, interactive user-facing services where response time directly impacts user experience.

#### createHighThroughput()

Maximum throughput configuration. Work-stealing dispatcher tuned for bulk message processing.

```cangjie
let system = CActor.system("highthroughput", CActorRuntimeConfig.createHighThroughput())
```

Target: 50,000,000 msg/s.

Use for: Batch processing, log aggregation, event streaming backends.

### Runtime Preset Comparison

| Preset                | Throughput Target  | Latency Target  | Memory/Actor | Mailbox     | Dispatcher     |
|-----------------------|--------------------|-----------------|--------------|-------------|----------------|
| createDefault         | Standard           | Standard        | Standard     | Unbounded   | ThreadPool     |
| createProduction      | High               | Low             | Standard     | Bounded 10K | WorkStealing   |
| createExtreme         | 1M+ msg/s          | Low             | Standard     | Optimized   | Optimized      |
| createMemoryOptimized | Standard           | Standard        | < 1KB        | Compact     | Compact        |
| createLowLatency      | Standard           | P99 < 100us     | Standard     | Optimized   | Pinned         |
| createHighThroughput  | 50M msg/s          | Standard        | Standard     | Optimized   | WorkStealing   |

### API-Level Actor Configuration Presets

These presets configure individual actors rather than the entire system.

#### ActorConfig.default()

Default actor configuration. Suitable for actors that do not have specific performance requirements.

```cangjie
let props = Props.create(() => MyActor()).withConfig(ActorConfig.default())
```

#### ActorConfig.highPerformance()

Bounded mailbox (10,000 capacity) with an 8-thread work-stealing dispatcher.

```cangjie
let props = Props.create(() => MyActor()).withConfig(ActorConfig.highPerformance())
```

Use for: Actors on the critical processing path in production.

#### ActorConfig.lowLatency()

Unbounded mailbox with a 4-thread work-stealing dispatcher. Optimized for minimal message delivery latency.

```cangjie
let props = Props.create(() => MyActor()).withConfig(ActorConfig.lowLatency())
```

Use for: Actors that must respond within tight time budgets (request-response patterns, RPC actors).

#### ActorConfig.batching()

Bounded mailbox (50,000 capacity) with a 16-thread pool dispatcher. Optimized for actors that benefit from batch processing.

```cangjie
let props = Props.create(() => MyActor()).withConfig(ActorConfig.batching())
```

Use for: Bulk data processors, batch writers, aggregation actors.

---

## Optimization Techniques

### 1. Lock-Free Data Structures

CActor uses lock-free algorithms based on Compare-And-Swap (CAS) operations throughout the critical path. This eliminates lock contention, which is the primary bottleneck in high-concurrency actor systems.

**Where lock-free structures are used**:
- Mailbox message queues (LockFreeQueue)
- Dispatcher work queues
- Actor reference counting
- Metrics counters

**Impact**: 100x improvement in concurrent enqueue/dequeue throughput. Lock contention is completely eliminated.

### 2. Cache Line Padding

Modern CPUs load memory in 64-byte cache lines. When two threads write to different variables that share a cache line, each write invalidates the other thread's cache line (false sharing). CActor pads all hot-path atomic variables to occupy a full cache line.

**Impact**: Eliminates false sharing on the scheduling fast path. Measurable latency reduction under high contention.

### 3. Batch Message Processing

Instead of dequeuing and processing one message at a time, CActor can dequeue a batch of messages and process them in a single scheduling cycle.

```cangjie
public class BatchMessageProcessor {
    private let batchSize: Int32 = 1000

    public func processBatch(mailbox: Mailbox): Unit {
        let batch = Array<Envelope>(batchSize)
        let count = mailbox.dequeueBatch(batch)

        for (i in 0..count) {
            processMessage(batch[i])
        }
    }
}
```

**Impact**:
- Throughput: 10-50x improvement over single-message processing
- CPU utilization: 30% improvement
- Latency cost: Microsecond-level increase per batch, negligible in practice

### 4. Object Pooling

Frequent allocation and garbage collection of short-lived message objects creates GC pressure. CActor provides object pools that reuse message and envelope objects across processing cycles.

```cangjie
public class ObjectPool<T> {
    private let pool: LockFreeQueue<T>
    private let factory: () -> T
    private let maxSize: Int32

    public func acquire(): T {
        match (pool.dequeue()) {
            case Some(obj) =>
                resetObject(obj)
                obj
            case None => factory()
        }
    }

    public func release(obj: T): Unit {
        if (pool.size() < maxSize) {
            pool.enqueue(obj)
        }
    }
}
```

**Impact**: Reduces GC pauses and allocation overhead. Most significant in high-throughput scenarios where millions of messages are processed per second.

### 5. Zero-Copy Message Passing

CActor passes messages by reference between actors when possible, avoiding data copies. For actors that exchange large payloads (arrays, buffers, structured data), this eliminates a major source of CPU overhead.

```cangjie
// Avoid copying large data
class DataReferenceMessage <: Message {
    let dataRef: DataReference  // Reference only, no copy
}

// Avoid this pattern
class CopyMessage <: Message {
    let data: Array<UInt8> = Array<UInt8>(1024 * 1024)  // 1MB copy
}
```

**Impact**: Eliminates memory copies for large messages. Reduces memory bandwidth consumption and GC pressure.

---

## Comparison with Akka

CActor provides performance that is competitive with Akka, the most widely deployed actor framework on the JVM.

| Metric              | Akka              | CActor            |
|---------------------|-------------------|-------------------|
| Throughput          | 10-50M msg/s      | 20M msg/s         |
| Latency (P99)       | < 1ms             | < 1ms             |
| Memory per Actor    | ~500 Bytes        | < 1KB             |
| Concurrent Actors   | 1,000,000+        | 1,000,000+        |

Key observations:

- **Throughput**: CActor achieves 20M msg/s on standard hardware, which falls within Akka's reported range of 10-50M msg/s depending on configuration and hardware.
- **Latency**: Both frameworks achieve P99 latency under 1ms. CActor's low-latency preset targets P99 < 100us.
- **Memory**: CActor uses slightly more memory per actor (< 1KB vs ~500B) due to its foundation layer and adaptive queue structures, but remains well within practical limits for million-actor deployments.
- **Concurrency**: Both frameworks support over 1 million concurrent actors.

---

## Performance Tuning Checklist

Use this checklist when deploying CActor to production or diagnosing performance issues.

### Dispatcher

- [ ] Select the appropriate dispatcher type for your workload (see [Dispatcher Selection Matrix](#dispatcher-selection-matrix))
- [ ] Set parallelism to match available CPU cores (typically `Runtime.availableProcessors()`)
- [ ] Enable NUMA awareness on multi-socket systems
- [ ] Verify that dispatcher threads are not blocked by I/O or external calls

### Mailbox

- [ ] Use bounded mailboxes in production to prevent out-of-memory conditions
- [ ] Select LockFreeMailbox or RingBufferMailbox for high-throughput actors
- [ ] Use BatchingMailbox for actors that process uniform message streams
- [ ] Use PriorityMailbox only when message ordering by priority is a hard requirement
- [ ] Size the mailbox capacity to accommodate burst traffic (monitor for mailbox-full events)

### Actor Design

- [ ] Keep actors stateless or minimize mutable state
- [ ] Use atomic operations for shared counters instead of locks
- [ ] Pass data by reference, not by copy, for large payloads
- [ ] Avoid blocking operations inside `receive()`. Offload I/O to dedicated I/O actors or external executors
- [ ] Use message object pooling for high-volume message types

### System Configuration

- [ ] Use `createProduction()` or `createExtreme()` as the starting point for production configs
- [ ] Enable batch processing for high-volume actors
- [ ] Enable object pooling for message and envelope types
- [ ] Monitor GC behavior and tune heap sizing if pause times exceed SLA targets
- [ ] Set appropriate file descriptor limits on the OS (`ulimit -n`)
- [ ] Pin dispatcher threads to CPU cores when latency predictability is critical

---

## Monitoring and Profiling

### Real-Time Metrics

CActor exposes runtime metrics for monitoring throughput, latency, and resource usage.

```cangjie
let metrics = system.metrics()
println("Throughput: ${metrics.throughput()} msg/s")
println("P99 Latency: ${metrics.p99Latency()}")
println("Actor Count: ${metrics.actorCount()}")
println("Memory Usage: ${metrics.memoryUsage()}")
```

### Performance Profiling

Enable the built-in profiler to capture CPU and memory profiles during load tests.

```cangjie
let profiler = system.startProfiler()
profiler.enableCPUProfiling()
profiler.enableMemoryProfiling()

// Run workload
runPerformanceTest()

// Generate report
let report = profiler.generateReport()
report.saveToFile("performance-report.html")
```

### Key Metrics to Monitor

| Metric                  | Target                     | Alert Threshold             |
|-------------------------|----------------------------|-----------------------------|
| Message throughput      | Varies by workload         | > 20% below baseline        |
| P99 latency             | < 1ms (standard)           | > 2ms                       |
| Mailbox depth           | Below capacity             | > 80% of capacity           |
| Dispatcher queue length | Balanced across workers    | Single worker > 2x average  |
| Memory per actor        | < 1KB                      | > 2KB                       |
| GC pause time           | < 10ms                     | > 50ms                      |

### Interpreting Results

- **Throughput below expected**: Check for blocking operations in actors, undersized dispatcher pools, or mailbox capacity exhaustion.
- **High tail latency**: Check for false sharing (rare with default configuration), thread migration (use pinned dispatcher), or GC pauses.
- **Growing mailbox depth**: The actor cannot keep up with the incoming message rate. Consider increasing dispatcher parallelism, enabling batch processing, or partitioning the actor into multiple instances.
- **Memory growth**: Check for message object leaks (objects not returned to pools), unbounded mailbox usage, or accumulated actor state.

---

## Summary

CActor achieves world-class performance through a combination of lock-free data structures, cache-line-aware memory layout, batch processing, object pooling, and zero-copy message passing. The framework provides configuration presets that cover the most common deployment scenarios, from low-latency trading systems to high-throughput event pipelines, while offering fine-grained control when custom tuning is required.

Start with `createProduction()`, measure, and then iterate using the dispatcher and mailbox selection guidance in this document.
