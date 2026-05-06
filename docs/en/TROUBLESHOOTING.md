# CActor Troubleshooting Guide

## Troubleshooting Overview

This guide helps you quickly diagnose and resolve common issues in CActor applications, including performance problems, cluster issues, memory issues, and more.

## Common Issue Categories

### Performance Issues
- High message processing latency
- Decreased throughput
- Abnormal CPU usage
- Excessive memory usage

### Cluster Issues
- Node unable to join cluster
- Shard rebalancing failure
- Network partition recovery
- Cluster split-brain

### Persistence Issues
- Event store failure
- Snapshot recovery errors
- Data consistency issues

### State Machine Issues
- State transition failure
- Deadlock detection
- State inconsistency

## Diagnostic Tools

### Built-in Diagnostic Commands

```bash
# System status check
cjpm run --name cactor.tools.system_status

# Performance analysis
cjpm run --name cactor.tools.performance_analyzer

# Memory analysis
cjpm run --name cactor.tools.memory_analyzer

# Cluster status check
cjpm run --name cactor.tools.cluster_status
```

### Log Analysis

```bash
# Enable detailed logging
export CACTOR_LOG_LEVEL=DEBUG

# View real-time logs
tail -f logs/cactor.log

# Filter error logs
grep "ERROR\|WARN" logs/cactor.log

# Analyze performance logs
grep "PERF" logs/cactor.log | tail -100
```

### Monitoring Endpoints

```bash
# Health check
curl http://localhost:8080/health

# System metrics
curl http://localhost:8080/metrics

# Actor statistics
curl http://localhost:8080/actors/stats

# Cluster status
curl http://localhost:8080/cluster/status
```

## Performance Troubleshooting

### Issue 1: High Message Processing Latency

**Symptoms**:
- P99 latency > 10ms
- Message queue backlog
- Unstable response times

**Diagnostic Steps**:

```bash
# 1. Check system load
top
htop

# 2. Check Actor mailbox status
curl http://localhost:8080/actors/mailbox-stats

# 3. Analyze message processing time
grep "message-processing-time" logs/cactor.log | tail -50
```

**Solutions**:

```cangjie
// 1. Optimize message processing logic
public func receive(message: Message, context: ActorContext): MessageResult {
    // Avoid blocking operations
    match (message) {
        case heavyTask: HeavyTaskMessage =>
            // Async processing for heavy tasks
            context.scheduleAsync { processHeavyTask(heavyTask) }
            MessageResult.Handled
        case _ =>
            // Fast processing for lightweight messages
            processLightMessage(message)
    }
}

// 2. Adjust mailbox configuration
let config = CActorRuntimeConfig.createProduction()
    .withMailboxCapacity(1000)  // Reduce mailbox capacity
    .withBatchSize(50)          // Adjust batch size
```

### Issue 2: Decreased Throughput

**Symptoms**:
- Message processing rate below expectation
- Low CPU usage
- Insufficient thread pool utilization

**Diagnostic Steps**:

```bash
# 1. Check thread pool status
curl http://localhost:8080/threads/stats

# 2. Analyze message dispatch
curl http://localhost:8080/dispatchers/stats

# 3. Check GC status
jstat -gc <pid> 1s 10  # If using JVM
```

**Solutions**:

```cangjie
// 1. Increase worker threads
let config = CActorRuntimeConfig.createProduction()
    .withDispatcherThreads(16)  // Increase thread count

// 2. Optimize scheduling strategy
let config = config.withSchedulingStrategy(
    SchedulingStrategy.WorkStealing  // Use work-stealing
)

// 3. Enable batch processing
let config = config.withBatchProcessing(true)
    .withBatchSize(100)
```

### Issue 3: Excessive Memory Usage

**Symptoms**:
- Memory usage continuously growing
- Frequent GC
- OutOfMemory errors

**Diagnostic Steps**:

```bash
# 1. Check memory usage
free -h
ps aux | grep cactor

# 2. Analyze memory allocation
curl http://localhost:8080/memory/stats

# 3. Check Actor count
curl http://localhost:8080/actors/count
```

**Solutions**:

```cangjie
// 1. Implement Actor lifecycle management
public func postStop(): Unit {
    // Clean up resources
    cleanupResources()
    releaseMemory()
}

// 2. Use object pools
let messagePool = ObjectPool<Message>(
    factory: { => Message() },
    maxSize: 1000
)

// 3. Configure memory limits
let config = CActorRuntimeConfig.createProduction()
    .withMaxActors(100000)      // Limit Actor count
    .withMemoryThreshold(0.8)   // Memory threshold
```

## Cluster Troubleshooting

### Issue 1: Node Unable to Join Cluster

**Symptoms**:
- New node fails to discover other nodes after startup
- Cluster member list not updating
- Network connection timeout

**Diagnostic Steps**:

```bash
# 1. Check network connectivity
telnet <seed-node-ip> 2551

# 2. Check cluster configuration
cat config/cluster.conf

# 3. View cluster logs
grep "cluster" logs/cactor.log
```

**Solutions**:

```bash
# 1. Verify network configuration
# Ensure firewall opens the port
sudo ufw allow 2551

# 2. Check seed node configuration
cactor {
  cluster {
    seeds = [
      "192.168.1.10:2551",  # Use correct IP
      "192.168.1.11:2551"
    ]
  }
}

# 3. Restart cluster service
sudo systemctl restart cactor-cluster
```

### Issue 2: Shard Rebalancing Failure

**Symptoms**:
- Uneven shard distribution
- Some shards not responding
- Rebalancing process stuck

**Diagnostic Steps**:

```bash
# 1. Check shard status
curl http://localhost:8080/cluster/shards

# 2. View coordinator status
curl http://localhost:8080/cluster/coordinator

# 3. Analyze rebalancing logs
grep "rebalancing" logs/cactor.log
```

**Solutions**:

```cangjie
// 1. Manually trigger rebalancing
let coordinator = system.actorOf(ShardCoordinator.props(), "coordinator")
coordinator.tell(RebalanceShards())

// 2. Adjust rebalancing strategy
let shardingConfig = ShardingConfig.create()
    .withRebalanceThreshold(0.1)    // Lower rebalancing threshold
    .withRebalanceInterval(30.seconds)  // Increase rebalancing interval
```

## Persistence Troubleshooting

### Issue 1: Event Store Failure

**Symptoms**:
- Events cannot be written to storage
- Persistent Actor restart failure
- Data loss

**Diagnostic Steps**:

```bash
# 1. Check storage status
ls -la data/events/
df -h  # Check disk space

# 2. View persistence logs
grep "persistence" logs/cactor.log

# 3. Verify storage permissions
ls -la data/
```

**Solutions**:

```cangjie
// 1. Implement error handling
public func onPersistFailure(cause: Exception, event: Event, seqNr: Int64): Unit {
    println("Persistence failure: ${cause.message}")
    // Implement retry logic
    scheduleRetry(event, seqNr)
}

// 2. Configure storage backend
let persistenceConfig = PersistenceConfig.create()
    .withStorageBackend(StorageBackend.File)
    .withRetryPolicy(RetryPolicy.exponentialBackoff(3))
```

## State Machine Troubleshooting

### Issue 1: State Transition Failure

**Symptoms**:
- State machine stuck in a certain state
- Transition conditions not met
- Event handling exceptions

**Diagnostic Steps**:

```bash
# 1. Check state machine status
curl http://localhost:8080/xstate/status

# 2. View transition logs
grep "state-transition" logs/cactor.log

# 3. Analyze event handling
curl http://localhost:8080/xstate/events
```

**Solutions**:

```cangjie
// 1. Add state transition logging
public func onStateTransition(from: State, to: State, event: Event): Unit {
    println("State transition: ${from} -> ${to}, event: ${event}")
}

// 2. Implement error recovery
public func onTransitionFailure(state: State, event: Event, error: Exception): Unit {
    println("Transition failure: ${error.message}")
    // Fallback to safe state
    transitionTo(SafeState())
}
```

## Debugging Tips

### Enable Debug Mode

```bash
# 1. Set debug level
export CACTOR_LOG_LEVEL=DEBUG
export CACTOR_DEBUG_MODE=true

# 2. Enable profiling
export CACTOR_PROFILE=true

# 3. Enable memory tracking
export CACTOR_MEMORY_TRACKING=true
```

### Using Debugging Tools

```cangjie
// 1. Actor state inspection
public func debugState(): String {
    return "Actor state: ${currentState}, message queue: ${mailboxSize}"
}

// 2. Performance timing
let startTime = System.currentTimeMillis()
processMessage(message)
let duration = System.currentTimeMillis() - startTime
println("Message processing time: ${duration}ms")

// 3. Memory usage monitoring
let memoryBefore = Runtime.getRuntime().totalMemory()
performOperation()
let memoryAfter = Runtime.getRuntime().totalMemory()
println("Memory usage change: ${memoryAfter - memoryBefore} bytes")
```

## Getting Help

### Community Support
- **GitHub Issues**: [Report Issues](https://github.com/your-repo/cactor/issues)
- **GitHub Discussions**: [Technical Discussion](https://github.com/your-repo/cactor/discussions)
- **Documentation**: [Complete Documentation](index.md)

### Professional Support
- **Enterprise Support**: Contact the maintenance team
- **Training Services**: Technical training and consulting
- **Custom Development**: Feature customization and optimization

## Issue Report Template

When encountering problems, please provide the following information:

```
### Environment Information
- CActor Version: v1.1.0
- Cangjie Version: 0.60.5
- Operating System: Ubuntu 20.04
- Hardware: 8-core CPU, 16GB RAM

### Problem Description
[Detailed description of the issue]

### Reproduction Steps
1. [Step 1]
2. [Step 2]
3. [Step 3]

### Expected Behavior
[Description of expected correct behavior]

### Actual Behavior
[Description of actual behavior]

### Log Information
```
[Paste relevant logs]
```

### Configuration Information
```
[Paste relevant configuration]
```
```

---

**CActor v1.1.0 - Quick Diagnosis, Efficient Resolution!**

*Last Updated: July 3, 2025*