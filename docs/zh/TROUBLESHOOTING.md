# 🔧 CActor 故障排除指南

## 📋 故障排除概览

本指南帮助您快速诊断和解决 CActor 应用中的常见问题，包括性能问题、集群问题、内存问题等。

## 🚨 常见问题分类

### 🏃 性能问题
- 消息处理延迟高
- 吞吐量下降
- CPU 使用率异常
- 内存使用过高

### 🌐 集群问题
- 节点无法加入集群
- 分片重平衡失败
- 网络分区恢复
- 集群脑裂

### 💾 持久化问题
- 事件存储失败
- 快照恢复错误
- 数据一致性问题

### 🔄 状态机问题
- 状态转换失败
- 死锁检测
- 状态不一致

## 🔍 诊断工具

### 内置诊断命令

```bash
# 系统状态检查
cjpm run --name cactor.tools.system_status

# 性能分析
cjpm run --name cactor.tools.performance_analyzer

# 内存分析
cjpm run --name cactor.tools.memory_analyzer

# 集群状态检查
cjpm run --name cactor.tools.cluster_status
```

### 日志分析

```bash
# 启用详细日志
export CACTOR_LOG_LEVEL=DEBUG

# 查看实时日志
tail -f logs/cactor.log

# 过滤错误日志
grep "ERROR\|WARN" logs/cactor.log

# 分析性能日志
grep "PERF" logs/cactor.log | tail -100
```

### 监控端点

```bash
# 健康检查
curl http://localhost:8080/health

# 系统指标
curl http://localhost:8080/metrics

# Actor 统计
curl http://localhost:8080/actors/stats

# 集群状态
curl http://localhost:8080/cluster/status
```

## 🏃 性能问题排除

### 问题 1: 消息处理延迟高

**症状**:
- P99 延迟 > 10ms
- 消息队列积压
- 响应时间不稳定

**诊断步骤**:

```bash
# 1. 检查系统负载
top
htop

# 2. 检查 Actor 邮箱状态
curl http://localhost:8080/actors/mailbox-stats

# 3. 分析消息处理时间
grep "message-processing-time" logs/cactor.log | tail -50
```

**解决方案**:

```cangjie
// 1. 优化消息处理逻辑
public func receive(message: Message, context: ActorContext): MessageResult {
    // 避免阻塞操作
    match (message) {
        case heavyTask: HeavyTaskMessage =>
            // 异步处理重任务
            context.scheduleAsync { processHeavyTask(heavyTask) }
            MessageResult.Handled
        case _ =>
            // 快速处理轻量消息
            processLightMessage(message)
    }
}

// 2. 调整邮箱配置
let config = CActorRuntimeConfig.createProduction()
    .withMailboxCapacity(1000)  // 减少邮箱容量
    .withBatchSize(50)          // 调整批处理大小
```

### 问题 2: 吞吐量下降

**症状**:
- 消息处理速率 < 预期
- CPU 使用率低
- 线程池利用率不足

**诊断步骤**:

```bash
# 1. 检查线程池状态
curl http://localhost:8080/threads/stats

# 2. 分析消息分发
curl http://localhost:8080/dispatchers/stats

# 3. 检查 GC 情况
jstat -gc <pid> 1s 10  # 如果使用 JVM
```

**解决方案**:

```cangjie
// 1. 增加工作线程
let config = CActorRuntimeConfig.createProduction()
    .withDispatcherThreads(16)  // 增加线程数

// 2. 优化调度策略
let config = config.withSchedulingStrategy(
    SchedulingStrategy.WorkStealing  // 使用工作窃取
)

// 3. 启用批量处理
let config = config.withBatchProcessing(true)
    .withBatchSize(100)
```

### 问题 3: 内存使用过高

**症状**:
- 内存使用持续增长
- 频繁 GC
- OutOfMemory 错误

**诊断步骤**:

```bash
# 1. 检查内存使用
free -h
ps aux | grep cactor

# 2. 分析内存分配
curl http://localhost:8080/memory/stats

# 3. 检查 Actor 数量
curl http://localhost:8080/actors/count
```

**解决方案**:

```cangjie
// 1. 实现 Actor 生命周期管理
public func postStop(): Unit {
    // 清理资源
    cleanupResources()
    releaseMemory()
}

// 2. 使用对象池
let messagePool = ObjectPool<Message>(
    factory: { => Message() },
    maxSize: 1000
)

// 3. 配置内存限制
let config = CActorRuntimeConfig.createProduction()
    .withMaxActors(100000)      // 限制 Actor 数量
    .withMemoryThreshold(0.8)   // 内存阈值
```

## 🌐 集群问题排除

### 问题 1: 节点无法加入集群

**症状**:
- 新节点启动后无法发现其他节点
- 集群成员列表不更新
- 网络连接超时

**诊断步骤**:

```bash
# 1. 检查网络连接
telnet <seed-node-ip> 2551

# 2. 检查集群配置
cat config/cluster.conf

# 3. 查看集群日志
grep "cluster" logs/cactor.log
```

**解决方案**:

```bash
# 1. 验证网络配置
# 确保防火墙开放端口
sudo ufw allow 2551

# 2. 检查种子节点配置
cactor {
  cluster {
    seeds = [
      "192.168.1.10:2551",  # 使用正确的 IP
      "192.168.1.11:2551"
    ]
  }
}

# 3. 重启集群服务
sudo systemctl restart cactor-cluster
```

### 问题 2: 分片重平衡失败

**症状**:
- 分片分布不均
- 某些分片无响应
- 重平衡过程卡住

**诊断步骤**:

```bash
# 1. 检查分片状态
curl http://localhost:8080/cluster/shards

# 2. 查看协调器状态
curl http://localhost:8080/cluster/coordinator

# 3. 分析重平衡日志
grep "rebalancing" logs/cactor.log
```

**解决方案**:

```cangjie
// 1. 手动触发重平衡
let coordinator = system.actorOf(ShardCoordinator.props(), "coordinator")
coordinator.tell(RebalanceShards())

// 2. 调整重平衡策略
let shardingConfig = ShardingConfig.create()
    .withRebalanceThreshold(0.1)    // 降低重平衡阈值
    .withRebalanceInterval(30.seconds)  // 增加重平衡间隔
```

## 💾 持久化问题排除

### 问题 1: 事件存储失败

**症状**:
- 事件无法写入存储
- 持久化 Actor 重启失败
- 数据丢失

**诊断步骤**:

```bash
# 1. 检查存储状态
ls -la data/events/
df -h  # 检查磁盘空间

# 2. 查看持久化日志
grep "persistence" logs/cactor.log

# 3. 验证存储权限
ls -la data/
```

**解决方案**:

```cangjie
// 1. 实现错误处理
public func onPersistFailure(cause: Exception, event: Event, seqNr: Int64): Unit {
    println("持久化失败: ${cause.message}")
    // 实现重试逻辑
    scheduleRetry(event, seqNr)
}

// 2. 配置存储后端
let persistenceConfig = PersistenceConfig.create()
    .withStorageBackend(StorageBackend.File)
    .withRetryPolicy(RetryPolicy.exponentialBackoff(3))
```

## 🔄 状态机问题排除

### 问题 1: 状态转换失败

**症状**:
- 状态机卡在某个状态
- 转换条件不满足
- 事件处理异常

**诊断步骤**:

```bash
# 1. 检查状态机状态
curl http://localhost:8080/xstate/status

# 2. 查看转换日志
grep "state-transition" logs/cactor.log

# 3. 分析事件处理
curl http://localhost:8080/xstate/events
```

**解决方案**:

```cangjie
// 1. 添加状态转换日志
public func onStateTransition(from: State, to: State, event: Event): Unit {
    println("状态转换: ${from} -> ${to}, 事件: ${event}")
}

// 2. 实现错误恢复
public func onTransitionFailure(state: State, event: Event, error: Exception): Unit {
    println("转换失败: ${error.message}")
    // 回退到安全状态
    transitionTo(SafeState())
}
```

## 🛠️ 调试技巧

### 启用调试模式

```bash
# 1. 设置调试级别
export CACTOR_LOG_LEVEL=DEBUG
export CACTOR_DEBUG_MODE=true

# 2. 启用性能分析
export CACTOR_PROFILE=true

# 3. 启用内存跟踪
export CACTOR_MEMORY_TRACKING=true
```

### 使用调试工具

```cangjie
// 1. Actor 状态检查
public func debugState(): String {
    return "Actor状态: ${currentState}, 消息队列: ${mailboxSize}"
}

// 2. 性能计时
let startTime = System.currentTimeMillis()
processMessage(message)
let duration = System.currentTimeMillis() - startTime
println("消息处理时间: ${duration}ms")

// 3. 内存使用监控
let memoryBefore = Runtime.getRuntime().totalMemory()
performOperation()
let memoryAfter = Runtime.getRuntime().totalMemory()
println("内存使用变化: ${memoryAfter - memoryBefore} bytes")
```

## 📞 获取帮助

### 社区支持
- **GitHub Issues**: [报告问题](https://github.com/your-repo/cactor/issues)
- **GitHub Discussions**: [技术讨论](https://github.com/your-repo/cactor/discussions)
- **文档**: [完整文档](index.md)

### 专业支持
- **企业支持**: 联系维护团队
- **培训服务**: 技术培训和咨询
- **定制开发**: 功能定制和优化

## 📋 故障报告模板

当遇到问题时，请提供以下信息：

```
### 环境信息
- CActor 版本: v7.0.0
- 仓颉版本: 0.60.5
- 操作系统: Ubuntu 20.04
- 硬件配置: 8核 CPU, 16GB RAM

### 问题描述
[详细描述问题现象]

### 复现步骤
1. [步骤 1]
2. [步骤 2]
3. [步骤 3]

### 预期行为
[描述预期的正确行为]

### 实际行为
[描述实际发生的行为]

### 日志信息
```
[粘贴相关日志]
```

### 配置信息
```
[粘贴相关配置]
```
```

---

**CActor v7.0.0 - 快速诊断，高效解决！** 🔧

*最后更新: 2026年5月6日*