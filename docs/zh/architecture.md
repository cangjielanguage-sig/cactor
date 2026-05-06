# CActor 7.0 架构设计文档

> 文档版本: 7.0
> 更新日期: 2026-05-06
> 仓颉语言版本: 0.7.0+

---

## 1. 概述

### 1.1 项目定位

CActor 是基于仓颉(Cangjie)语言的高性能 Actor 模型实现，参考 Akka/Akka Typed 设计理念，为现代高并发分布式系统提供坚实的运行时基础。项目包含 326 个 `.cj` 源文件，覆盖从底层并发原语到上层分布式集群的完整技术栈。

### 1.2 核心设计目标

| 目标 | 描述 | 量化指标 |
|------|------|----------|
| **极致性能** | 高吞吐、低延迟消息处理 | 500万+ msg/s，P99 < 100us |
| **类型安全** | 编译期 Actor 类型约束 | 消除运行时类型错误 |
| **弹性伸缩** | 支持单机到集群无缝扩展 | 线性扩展至百万级 Actor |
| **容错恢复** | 完善的监督与恢复机制 | 故障自动恢复，不丢失消息 |
| **开发效率** | 简洁的 API 与 DSL 支持 | 减少 60%+ 样板代码 |

### 1.3 技术选型

- **语言**: 仓颉 0.7.0+（享有现代语言安全特性）
- **并发模型**: Actor + 协程混合
- **内存管理**: 对象池 + NUMA 感知分配
- **网络通信**: 远程 Actor 透明调用

---

## 2. 架构总览

### 2.1 6+1 层架构体系

CActor 采用 **6+1 层模块化架构**，每层职责清晰，依赖单向传导：

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              API Layer (API 层)                                       │
│   CActor · CActorSystem · CActorFactory · CActorSystemBuilder · Props · Extensions    │
│   负责：统一入口、工厂模式、链式构建、运行时类型派发                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                           Integration Layer (集成层)                                    │
│   Configuration · Logging · Monitoring · Testing (TestActorSystem · TestProbe)       │
│   负责：外部系统集成、统一配置、日志、监控、测试框架                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                          Distribution Layer (分布式层)                                  │
│   Remote · Cluster · Persistence · Streaming                                          │
│   负责：跨进程通信、集群管理、分片、事件溯源、CQRS、分布式流                                  │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                            Patterns Layer (模式层)                                     │
│   Ask · Routing · CircuitBreaker · Backpressure · BatchProcessing                     │
│   负责：请求-响应、负载均衡、熔断降级、背压控制、批量处理                                   │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                            Runtime Layer (运行时层)                                    │
│   Dispatcher · Mailbox · Scheduler · Lifecycle · Supervision · Registry · System      │
│   负责：消息调度、邮箱实现、定时器、生命周期、监督管理、Actor注册、高性能系统实现                │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                             Core Layer (核心层)                                        │
│   Actor · ActorRef · ActorPath · Props · Message · Context · Supervision · System      │
│   负责：核心抽象接口、不可变数据结构、生命周期状态机                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                          Foundation Layer (基础设施层)                                 │
│   Queue · Serialization · Memory · Network · Monitoring · Config · DI · Contracts      │
│   负责：零依赖底层组件、无锁队列、序列化、内存池、网络抽象                                    │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                          ▲
                                          │ 下层为上层提供基础设施
                                          │ 上层依赖下层，禁止反向依赖
                                          │ 便于单元测试和模块替换
                                          └────────────────────────
```

### 2.2 层次依赖规则

```
API Layer          → Integration + Runtime + Core + Foundation
Integration Layer  → Distribution + Runtime + Core + Foundation
Distribution Layer → Patterns + Runtime + Core + Foundation
Patterns Layer     → Runtime + Core + Foundation
Runtime Layer      → Core + Foundation
Core Layer         → Foundation
Foundation Layer   → (无依赖，零依赖基础设施)
```

**原则**: 每层只依赖其下层，禁止跨层依赖或反向依赖。这一约束确保：
- 各层可独立单元测试
- 运行时组件可按需替换（如替换调度器实现）
- 最小化依赖图的复杂度

---

## 3. Foundation Layer (基础设施层)

路径: `src/foundation/`，33 个文件

零依赖基础设施，所有组件不依赖核心层以上的任何模块，可独立使用。

### 3.1 模块列表

| 子模块 | 文件数 | 核心职责 |
|--------|--------|----------|
| `abstractions` | - | 通用抽象接口 |
| `config` | - | 统一配置框架、配置验证器 |
| `contracts` | - | 模块契约定义 |
| `di` | - | 依赖注入容器 |
| `memory` | - | 对象池、NUMA 内存池、共享内存缓冲区 |
| `monitoring` | - | Metrics、Tracing、告警 |
| `network` | - | 网络传输层抽象 |
| `queue` | - | 无锁队列、并发 HashMap |
| `serialization` | - | 字节/字符串/JSON/二进制序列化 |

### 3.2 核心组件详解

#### 3.2.1 无锁队列 (LockFreeQueue)

基于环形缓冲区的无锁队列实现，支持自动模式切换：

```cangjie
// 队列模式枚举
public enum QueueMode {
    SPSC  // Single Producer Single Consumer
    MPSC  // Multiple Producer Single Consumer
    SPMC  // Single Producer Multiple Consumer
    MPMC  // Multiple Producer Multiple Consumer
}

// 核心接口
public interface LockFreeQueue<T> {
    func enqueue(item: T): Bool          // 入队
    func dequeue(): Option<T>            // 出队
    func tryEnqueue(item: T): Bool       // 非阻塞入队
    func tryDequeue(): Option<T>         // 非阻塞出队
    func size(): Int64                    // 近似大小
    func isEmpty(): Bool
}

// 特性：
// 1. 缓存行填充（Cache Line Padding）避免 false sharing
// 2. CAS（Compare-And-Swap）原子操作保证线程安全
// 3. 动态检测生产者/消费者数量，自动切换最优模式
// 4. 无界队列通过指数扩容避免队列满阻塞
```

#### 3.2.2 内存管理

提供三层内存管理：

```cangjie
// 1. 对象池 - 减少 GC 压力
public interface ObjectPool<T> {
    func acquire(): T
    func release(item: T): Unit
    func prewarm(count: Int64): Unit
}

// 2. NUMA 感知内存池 - 优化多核访问
public interface NUMAMemoryPool {
    func allocate(size: Int64, affinity: Int32): MemoryBlock
    func deallocate(block: MemoryBlock): Unit
    func getLocalMemory(): MemoryBlock  // 获取本地 NUMA 节点内存
}

// 3. 共享内存缓冲区 - 跨进程通信
public interface SharedMemoryBuffer {
    func write(data: Array<UInt8>, offset: Int64): Unit
    func read(offset: Int64, length: Int64): Array<UInt8>
    func size(): Int64
}
```

#### 3.2.3 序列化框架

统一的序列化接口，支持多种序列化器插拔：

```cangjie
public interface Serializer<T> {
    func serialize(value: T): Array<UInt8>
    func deserialize(data: Array<UInt8>): T
    func serializerId(): Int16
    func manifest(): String
}

// 序列化器类型
// - BytesSerializer: 原始字节
// - StringSerializer: UTF-8 字符串
// - JSONSerializer: JSON 格式
// - BinarySerializer: 二进制高效序列化
// - ProtobufSerializer: Protocol Buffers (可选)
```

#### 3.2.4 监控原语

```cangjie
// Metrics 指标
public interface Metric {
    func record(value: Float64): Unit
    func count(): Int64
    func sum(): Float64
    func avg(): Float64
    func percentile(p: Float64): Float64  // P50/P90/P99/P999
}

// Tracing 链路追踪
public interface Span {
    func start(name: String): Span
    func end(): Unit
    func addTag(key: String, value: String): Unit
    func setStatus(code: Int32): Unit
    func child(): Span
}
```

---

## 4. Core Layer (核心层)

路径: `src/core/`，31 个文件

定义 Actor 模型的核心抽象接口和数据结构，不包含任何实现。

### 4.1 模块列表

| 子模块 | 核心接口/类 |
|--------|-------------|
| `actor` | Actor, ActorRef, ActorPath, Props, ActorFactory, LocalActorRef |
| `config` | ActorConfig, MailboxConfig, DispatcherConfig, SupervisionConfig |
| `context` | ActorContext |
| `error` | 错误处理机制 |
| `guardian` | GuardianActor, GuardianType |
| `message` | Message, SystemMessage, UserMessage, Envelope, DeadLetter |
| `supervision` | OneForOne, OneForAll, Escalate, BackoffSupervisor |
| `system` | ActorSystem, ActorSelection, Scheduler |
| `typed` | TypedBehavior, TypedActorRef, TypedActorContext, TypedBehaviors |

### 4.2 Actor 核心接口

#### 4.2.1 Actor 接口

```cangjie
// 消息处理核心接口
public interface Actor {
    // 主消息处理方法
    // message: 接收到的消息
    // context: Actor 上下文，包含发送者引用、父子关系等
    // 返回: MessageResult 控制消息处理结果
    func receive(message: Message, context: ActorContext): MessageResult

    // 属性
    prop name: String { get }
    prop description: String { get }
}

// Actor 生命周期钩子
public interface LifecycleActor <: Actor {
    func preStart(): Unit          // Actor 启动前调用
    func postStart(): Unit         // Actor 启动后调用
    func preStop(): Unit           // Actor 停止前调用
    func postStop(): Unit          // Actor 停止后调用
    func preRestart(reason: Throwable, msg: Option<Message>): Unit  // 重启前
    func postRestart(reason: Throwable): Unit                          // 重启后
}

// 生命周期状态机
public enum ActorLifecycleState {
    Created     // 刚创建，未启动
    Starting    // 启动中
    Running     // 运行中
    Stopping   // 停止中
    Stopped    // 已停止
    Failed      // 失败状态
    Restarting // 重启中
}

// 状态转换图:
// Created -> Starting -> Running <-> Restarting
//                    \-> Stopping -> Stopped
//                              \-> Failed
```

#### 4.2.2 ActorRef 接口

Actor 的引用句柄，用于发送消息：

```cangjie
// ActorRef 基础接口
public interface ActorRef {
    // 发送异步消息（fire-and-forget）
    func tell(message: Message): Unit
    // 发送消息并指定回复目标
    func tell(message: Message, sender: ActorRef): Unit
    // 获取 Actor 路径
    func path(): ActorPath
    // 获取关联的 ActorSystem
    func system(): ActorSystem
}

// 本地 ActorRef 扩展接口
public interface LocalActorRef <: ActorRef {
    func getActor(): Option<Actor>       // 获取底层 Actor 实例
    func suspend(): Unit                 // 暂停消息处理
    func resume(exc: Option<Throwable>): Unit  // 恢复消息处理
    func restart(reason: Throwable): Unit // 触发重启
    func stop(): Unit                    // 停止 Actor
    func getState(): ActorLifecycleState // 获取当前状态
}

// ActorPath 路径结构
// 示例: akka://my-system/user/parent/child
// 结构: scheme://system-name/@address/user/path/to/actor
public interface ActorPath {
    prop elements: Array<String> { get }  // 路径元素
    prop name: String { get }            // Actor 名称
    prop parent: Option<ActorPath> { get } // 父路径
    func /(name: String): ActorPath       // 构建子路径
    func toString(): String
}
```

#### 4.2.3 Props 配置

Props 是 Actor 的配置模板，支持创建和复制：

```cangjie
public interface Props {
    // 创建新的 Props 实例
    func withDispatcher(dispatcher: String): Props
    func withMailbox(mailbox: String): Props
    func withSupervisor(supervisor: ActorRef): Props
    func withDeploy(deploy: Deploy): Props

    // 复制并修改
    func copy(): Props

    // 工厂方法创建 Props
    static func create(creator: () -> Actor): Props
    static func create[T <: Actor](factory: ActorFactory[T]): Props
}

// Props 宏简化创建
// @props class MyActor { ... }
// 等价于 Props.create { MyActor() }
```

### 4.3 消息系统

#### 4.3.1 消息类型层次

```
Message (trait)
├── SystemMessage (优先级 1000，系统内部消息)
│   ├── StopMessage
│   ├── RestartMessage
│   ├── SuspendMessage
│   ├── ResumeMessage
│   └── TerminatedMessage
├── UserMessage (优先级 0，用户业务消息)
│   ├── Envelope (消息信封，携带发送者信息)
│   ├── DeadLetter (死信，未能送达的消息)
│   └── 内建消息
│       ├── StringMessage
│       ├── PingMessage / PongMessage
│       └── PoisonPill
└── (用户自定义消息实现 Message 接口)
```

#### 4.3.2 消息信封

```cangjie
// 消息信封 - 封装消息及元数据
public struct Envelope {
    public let message: Message           // 实际消息
    public let sender: ActorRef           // 发送者引用
    public let recipient: ActorRef        // 接收者引用
    public let timestamp: Int64          // 时间戳（纳秒）
    public let priority: Int32            // 优先级（SystemMessage=1000, UserMessage=0)
}

// 按优先级排序的入队
public interface PriorityMailbox <: Mailbox {
    func enqueueFirst(envelope: Envelope): Bool  // 高优先级插队
}
```

### 4.4 监督策略

```cangjie
// 监督策略接口
public interface SupervisionStrategy {
    func handleChildFailure(
        child: ActorRef,
        cause: Throwable,
        stats: ChildStats
    ): SupervisionDirective
}

// 常用策略实现
public enum SupervisionDirective {
    Resume       // 恢复继续运行
    Restart      // 重启 Actor
    Stop         // 停止 Actor
    Escalate     // 向上传递
}

// 1. OneForOne: 只影响失败的子 Actor
public class OneForOneStrategy <: SupervisionStrategy {
    public let maxRetries: Int64          // 最大重试次数，-1 无限
    public let withinTimeRange: Duration   // 时间窗口
    public let decider: (Throwable) -> SupervisionDirective
}

// 2. OneForAll: 影响所有兄弟子 Actor
public class OneForAllStrategy <: SupervisionStrategy {
    public let maxRetries: Int64
    public let withinTimeRange: Duration
    public let decider: (Throwable) -> SupervisionDirective
}

// 3. BackoffSupervisor: 指数退避重启
public class BackoffSupervisor <: Actor {
    public let initialDelay: Duration      // 初始延迟
    public let maxDelay: Duration          // 最大延迟
    public let randomFactor: Float64      // 随机抖动因子
}
```

### 4.5 ActorSystem 核心接口

```cangjie
public interface ActorSystem {
    // Actor 创建
    func actorOf(props: Props, name: String): ActorRef
    func actorOf(props: Props): ActorRef   // 自动命名

    // 类型安全 Actor 创建
    func typedActorOf[T](props: Props, name: String): TypedActorRef[T]
    func typedActorOf[T](props: Props): TypedActorRef[T]

    // Actor 查询
    func actorSelection(path: String): ActorSelection
    func stop(ref: ActorRef): Unit

    // 系统级操作
    func scheduler(): Scheduler
    func registerOnTermination(callback: () -> Unit): Unit
    func terminate(): Future[Unit]
    func awaitTermination(duration: Duration): Bool

    // 配置
    func settings(): Settings
    func eventStream(): EventStream
}

// ActorSelection 支持路径模式匹配
public interface ActorSelection {
    func tell(message: Message): Unit
    func tell(message: Message, sender: ActorRef): Unit
    func !!(message: Message): Unit  // broadcast
}
```

### 4.6 类型安全 Actor

```cangjie
// TypedBehavior: 类型化的 Actor 行为
public interface TypedBehavior[T] {
    func receive(ctx: TypedActorContext[T], msg: T): TypedBehavior[T]
}

// TypedActorRef: 类型化的 Actor 引用
public interface TypedActorRef[T] <: ActorRef {
    func tell(msg: T): Unit
    func tell(msg: T, sender: TypedActorRef[T]): Unit
    // 转换为无类型引用
    func underlying(): ActorRef
}

// TypedBehaviors 工厂
public class TypedBehaviors {
    // 普通接收行为
    public static func receive[T](handler: (TypedActorContext[T], T) -> TypedBehavior[T]): TypedBehavior[T]

    // 保持当前行为不变
    public static func same[T](): TypedBehavior[T]

    // 停止当前 Actor
    public static func stopped[T](): TypedBehavior[T]

    // 忽略所有消息
    public static func ignore[T](): TypedBehavior[T]

    // 消息过滤转换
    public static func transformMessages[T, U](
        behavior: TypedBehavior[U],
        transform: (T) -> U,
        restore: (U) -> T
    ): TypedBehavior[T]
}

// 示例
public class Counter <: Actor {
    private var count: Int64 = 0

    public func receive(msg: Message, ctx: ActorContext): MessageResult {
        match (msg) {
            case Inc => count += 1
            case Dec => count -= 1
            case Get(replyTo) => replyTo.tell(count)
        }
        same()
    }
}

// 类型化版本
public class TypedCounter <: TypedBehavior[Int] {
    private var count: Int64 = 0

    public func receive(ctx: TypedActorContext[Int], msg: Int): TypedBehavior[Int] {
        count += msg
        same()
    }
}
```

---

## 5. Runtime Layer (运行时层)

路径: `src/runtime/`，63 个文件

核心接口的具体实现层，依赖 Core Layer 和 Foundation Layer。

### 5.1 模块列表

| 子模块 | 核心实现 |
|--------|----------|
| `actor` | 高性能 Actor 实现 |
| `context` | 池化 Actor 上下文 |
| `dispatcher` | ThreadPool / WorkStealing / Adaptive / Dynamic / BatchProcessing / FoundationBased |
| `events` | 事件总线 (EventBus) |
| `execution` | 执行引擎 |
| `guardian` | SystemGuardian / UserGuardian |
| `lifecycle` | 生命周期管理、CoordinatedShutdown |
| `mailbox` | FoundationMailbox / LockFreeMailbox / BatchingMailbox / PriorityMailbox / RingBufferMailbox |
| `monitoring` | 运行时监控指标 |
| `registry` | Actor 注册表 |
| `scheduler` | 定时器调度器 |
| `supervision` | 监督管理器 |
| `system` | HighPerformanceActorSystem |
| `typed` | TypedActorRuntime |
| `wrappers` | TypedActorWrapper, MessageBridge |

### 5.2 调度器系统

调度器是消息分发到 Actor 邮箱的核心组件。

#### 5.2.1 调度器类型

| 调度器类型 | 适用场景 | 特性 |
|-----------|----------|------|
| `ThreadPoolDispatcher` | 通用场景 | 固定/可变线程池 |
| `WorkStealingDispatcher` | 计算密集型 | 工作窃取，负载均衡 |
| `AdaptiveDispatcher` | 自适应场景 | 根据负载动态调整 |
| `DynamicDispatcher` | 按需创建线程 | 最小化线程数 |
| `BatchProcessingDispatcher` | 批量处理 | 批量消息聚合 |
| `FoundationBasedDispatcher` | 极致性能 | 基于 Foundation 层优化 |

#### 5.2.2 WorkStealingDispatcher 核心实现

```cangjie
public interface MessageDispatcher {
    func dispatch(envelope: Envelope): Unit
    func throughput(): Int64
    func activeThreads(): Int64
    func shutdown(): Unit
}

public class WorkStealingDispatcher <: MessageDispatcher {
    private let threadCount: Int32
    private let queues: Array[LockFreeQueue[Envelope]]  // 每线程独立队列
    private let executor: Executor

    public func dispatch(envelope: Envelope): Unit {
        // 尝试将消息放入当前线程队列
        val localQueue = getCurrentThreadQueue()
        if (localQueue.enqueue(envelope)) {
            return
        }
        // 负载均衡：从其他线程队列窃取任务
        val victim = selectVictim()
        if (let Some(wrapped) <- victim.trySteal()) {
            // 窃取成功，优先处理被窃取的任务（保持 LIFO 的亲和性）
            executor.execute(wrapped)
        }
        // 放入本地队列
        localQueue.enqueue(envelope)
    }
}
```

### 5.3 邮箱系统

#### 5.3.1 邮箱类型

| 邮箱类型 | 描述 | 使用场景 |
|----------|------|----------|
| `UnboundedMailbox` | 无界队列 | 默认，高吞吐量 |
| `BoundedMailbox` | 有界队列 | 内存受限场景 |
| `PriorityMailbox` | 优先级队列 | 系统消息优先 |
| `ControlAwareMailbox` | 控制消息分离 | 监督消息优先 |
| `BatchingMailbox` | 批量出队 | 批量处理优化 |
| `RingBufferMailbox` | 环形缓冲 | 固定大小缓存 |

#### 5.3.2 LockFreeMailbox 实现

```cangjie
public class LockFreeMailbox <: Mailbox, SizedQueue {
    private let capacity: Int32
    private let buffer: Array[Envelope]     // 环形缓冲区
    private let head: AtomicInt64            // 头指针
    private let tail: AtomicInt64            // 尾指针
    private let highWatermark: Int32
    private let lowWatermark: Int32

    public func enqueue(envelope: Envelope): Bool {
        let tailValue = tail.get()
        let headValue = head.get()
        if (tailValue - headValue >= capacity) {
            return false  // 队列满
        }
        buffer[tailValue % capacity] = envelope
        tail.increment()  // CAS 操作
        return true
    }

    public func dequeue(): Option[Envelope] {
        let headValue = head.get()
        let tailValue = tail.get()
        if (tailValue <= headValue) {
            return None  // 队列空
        }
        let value = buffer[headValue % capacity]
        head.increment()  // CAS 操作
        return Some(value)
    }

    public func dequeueBatch(maxSize: Int32): Int32 {
        // 批量出队，减少系统调用
        // 返回实际出队数量
    }
}
```

### 5.4 生命周期管理

#### 5.4.1 ActorLifecycleManager

```cangjie
public class ActorLifecycleManager {
    // 状态转换
    public func transition(ref: ActorRef, from: ActorLifecycleState, to: ActorLifecycleState): Bool

    // 启动 Actor
    public func start(ref: ActorRef, props: Props): Future[Unit]

    // 停止 Actor（协调关闭）
    public func stop(ref: ActorRef): Future[Unit]
    public func stopAll(refs: Array[ActorRef]): Future[Unit]

    // 重启 Actor
    public func restart(ref: ActorRef, cause: Throwable): Unit

    // 状态查询
    public func getState(ref: ActorRef): ActorLifecycleState
    public func isTerminated(ref: ActorRef): Bool
}
```

#### 5.4.2 CoordinatedShutdown

协调关闭确保系统优雅终止：

``` 1
public class CoordinatedShutdown {
    // 添加清理任务
    public func addTask(
        phase: String,           // 阶段名称
        task: () -> Future[Unit], // 清理任务
        dependsOn: Array[String] = []  // 依赖的阶段
    ): Unit

    // 执行关闭
    public func run(reason: String): Future[Unit]

    // 预定义阶段
    // phase-0: 服务开始停止
    // phase-1: 持久化检查点
    // phase-2: 等待分片迁移完成
    // phase-3: 停止 Actor
    // phase-4: 终止系统
}
```

### 5.5 高性能 ActorSystem

```cangjie
public class HighPerformanceActorSystem <: ActorSystem {
    // 核心组件
    private let systemGuardian: ActorRef      // 系统守护 Actor
    private let userGuardian: ActorRef          // 用户根 Actor
    private let eventStream: EventStream        // 事件总线
    private let dispatcherFactory: DispatcherFactory
    private let mailboxFactory: MailboxFactory
    private let registry: ActorRegistry
    private let supervisionManager: SupervisionManager
    private let lifecycleManager: ActorLifecycleManager
    private let metrics: ActorSystemMetrics

    // 线程本地存储
    private val currentActorContext: ThreadLocal[ActorContext]
    private val currentActor: ThreadLocal[ActorRef]

    public func actorOf(props: Props, name: String): ActorRef {
        // 1. 验证名称唯一性
        // 2. 创建邮箱
        let mailbox = mailboxFactory.create(props.mailboxType)
        // 3. 创建 Actor 实例
        let actor = props.createActor()
        // 4. 注册到 registry
        // 5. 启动生命周期
        lifecycleManager.start(ref, props)
        return ref
    }

    // 消息调度优化
    private func scheduleDispatch(mailbox: Mailbox, dispatcher: MessageDispatcher): Unit {
        // 直接入队，减少中间环节
        // 支持批量调度
    }
}
```

---

## 6. Patterns Layer (模式层)

路径: `src/patterns/`，13 个文件

封装常用分布式计算模式。

### 6.1 Ask Pattern (请求-响应)

```cangjie
// AskFuture - 请求未来结果
public interface AskFuture[T] <: Future[T] {
    func pipeTo(recipient: ActorRef): Unit  // 自动转发结果
}

// AskPatternManager 管理所有 pending ask
public class AskPatternManager {
    private val pending: ConcurrentHashMap[String, Promise[Message]]
    private val timeoutScheduler: Scheduler

    public func ask[T](
        ref: ActorRef,
        request: Message,
        timeout: Duration
    ): Future[T]

    // 内部实现：
    // 1. 生成唯一 correlation ID
    // 2. 创建 Promise
    // 3. 注册超时回调
    // 4. 发送带 ID 的消息
    // 5. 回复时匹配 ID 并完成 Promise
}

// 语法糖
public func ?[T](ref: TypedActorRef[T], msg: T, timeout: Duration): Future[T]
```

### 6.2 Routing (路由策略)

| 路由策略 | 描述 | 特性 |
|----------|------|------|
| `RoundRobinRouter` | 轮询分发 | 均匀分布 |
| `RandomRouter` | 随机选择 | 无状态 |
| `SmallestMailboxRouter` | 最小邮箱优先 | 负载均衡 |
| `BroadcastRouter` | 广播 | 发送给所有路由 |
| `ConsistentHashRouter` | 一致性哈希 | 会话保持 |
| `TailChoppingRouter` | 尾部剪裁 | 取最快响应 |

```cangjie
public interface Router {
    func route(message: Message, routees: Array[ActorRef]): ActorRef
    func updateRoutees(routees: Array[ActorRef]): Unit
}

public interface RouterActorRef <: ActorRef {
    func updateRoutees(newRoutees: Array[ActorRef]): Unit
    func addRoutee(routee: ActorRef): Unit
    func removeRoutee(routee: ActorRef): Unit
}

// Pool Router vs Group Router
// Pool: Router 管理子 Actor 生命周期
// Group: 外部 Actor 作为路由目标
```

### 6.3 Circuit Breaker (断路器)

三态自动机：Closed -> Open -> HalfOpen -> (Closed | Open)

```cangjie
public enum CircuitBreakerState {
    Closed   // 正常，允许请求通过
    Open     // 熔断，拒绝请求快速失败
    HalfOpen // 半开，允许一个请求测试恢复
}

public interface CircuitBreaker[T] {
    // 状态查询
    func state(): CircuitBreakerState
    func isOpen(): Bool
    func isClosed(): Bool

    // 执行受保护调用
    func execute(call: () -> T): T
    func executeFuture(call: () -> Future[T]): Future[T]

    // 手动控制
    func succeed(): Unit    // 记录成功
    func fail(exc: Throwable): Unit  // 记录失败
    func reset(): Unit      // 重置断路器

    // 熔断器装饰器
    func decorate(ref: ActorRef): ActorRef  // 装饰 ActorRef
}

public class BasicCircuitBreaker <: CircuitBreaker[Message] {
    public let failureThreshold: Int32       // 失败次数阈值
    public let successThreshold: Int32      // 成功次数阈值
    public let callTimeout: Duration         // 单次调用超时
    public let resetTimeout: Duration        // Open->HalfOpen 等待时间
}
```

### 6.4 Backpressure (背压控制)

五种背压策略：

| 策略 | 行为 | 适用场景 |
|------|------|----------|
| `Drop` | 直接丢弃 | 允许丢失的消息 |
| `Block` | 阻塞发送者 | 同步背压 |
| `Buffer` | 缓冲 + 溢出丢弃 | 削峰填谷 |
| `Reject` | 拒绝并通知 | 显式拒绝 |
| `Throttle` | 限速发送 | 速率控制 |

```cangjie
public interface BackpressureController {
    func requestPermit(): Bool        // 请求许可
    func releasePermit(): Unit        // 释放许可
    func observe(dropped: Int64): Unit // 记录丢弃数
}

public class TokenBucketRateLimiter <: BackpressureController {
    private let capacity: Int64       // 桶容量
    private let refillRate: Float64  // 补充速率（tokens/秒）
    private var tokens: Float64
    private var lastRefill: Int64

    public func requestPermit(): Bool {
        refill()
        if (tokens >= 1.0) {
            tokens -= 1.0
            return true
        }
        return false
    }
}
```

---

## 7. Distribution Layer (分布式层)

路径: `src/distribution/`，21 个文件

跨进程、跨节点通信能力。

### 7.1 模块列表

| 子模块 | 核心功能 |
|--------|----------|
| `remote` | 简单远程 Actor 通信 |
| `cluster` | ShardCoordinator, ShardAllocationStrategy |
| `persistence` | EventPayload, PersistentEvent, EnhancedEventStore, PersistentActor |
| `streaming` | 分布式流处理 |

### 7.2 远程通信

```cangjie
// 远程 ActorRef
public interface RemoteActorRef <: ActorRef {
    func getAddress(): Address
    func isLocal(): Bool
}

// 远程调用协议
public protocol RemoteProtocol {
    // 序列化消息
    // 通过网络传输
    // 反序列化并投递
}

// 支持的传输层
// - TCP: 可靠有序
// - UDP: 低延迟（需要应用层可靠传输）
```

### 7.3 集群与分片

```cangjie
// 分片协调器
public interface ShardCoordinator {
    func allocateShard(shardId: String, node: Address): Unit
    func locateShard(shardId: String): Address
    func rebalance(): Unit  // 负载再平衡
}

// 分片分配策略
public interface ShardAllocationStrategy {
    func allocate(
        shardId: String,
        currentNodes: Array[Address]
    ): Address
}

// LeastShardsAllocationStrategy: 最小分片数优先
public class LeastShardsAllocationStrategy <: ShardAllocationStrategy {
    public func allocate(
        shardId: String,
        currentNodes: Array[Address]
    ): Address {
        // 选择当前分片数最少的节点
    }
}

// ConsistentMapping: 一致性哈希分片
public class ConsistentMapping <: ShardAllocationStrategy {
    public func allocate(
        shardId: String,
        currentNodes: Array[Address]
    ): Address {
        // 保证同一 shardId 映射到同一节点
        // 节点变更时最小化迁移
    }
}
```

### 7.4 持久化与事件溯源

```cangjie
// 事件溯源核心接口
public interface PersistentEvent {
    let persistenceId: String
    let sequenceNr: Int64
    let payload: EventPayload
    let timestamp: Int64
    let tags: Set[String]
}

public interface PersistentActor <: Actor {
    // 持久化事件
    func persist(event: Message, handler: (Message) -> Unit): Unit
    func persistAll(events: Array[Message], handler: (Message) -> Unit): Unit

    // 异步持久化（更高吞吐）
    func persistAsync(event: Message, handler: (Message) -> Unit): Unit

    // 快照
    func saveSnapshot(state: Any): Unit
    func loadSnapshot(persistenceId: String, nr: Int64): Unit

    // 恢复
    func recovery(): Unit
    prop lastSequenceNr: Int64 { get }
}

// EnhancedEventStore - 高性能事件存储
public interface EnhancedEventStore {
    // 写入
    func append(event: PersistentEvent): Future[Unit]
    func appendBatch(events: Array[PersistentEvent]): Future[Unit]

    // 读取
    func readEvents(
        persistenceId: String,
        from: Int64,
        to: Int64
    ): Array[PersistentEvent]

    func readByTag(tag: String, offset: Int64, limit: Int32): Array[PersistentEvent]

    // 快照
    func saveSnapshot(snapshot: Snapshot): Future[Unit]
    func loadLatestSnapshot(persistenceId: String): Option[Snapshot]

    // 持久化查询（用于 CQRS）
    func currentPersistenceIds(): Iterator[String]
}

// MemoryEnhancedEventStore - 内存增强存储
public class MemoryEnhancedEventStore <: EnhancedEventStore {
    // 写穿缓存 + 异步持久化
    // 高性能读取
}
```

---

## 8. Integration Layer (集成层)

路径: `src/integration/`，28 个文件

### 8.1 模块列表

| 子模块 | 核心功能 |
|--------|----------|
| `configuration` | ConfigurationManager, ConfigValue, GlobalConfigManager |
| `logging` | 日志系统集成 |
| `monitoring` | 监控集成 |
| `testing` | TestActorSystem, TestProbe, TestTimeController, TestExpectation |

### 8.2 测试框架

```cangjie
// 测试 ActorSystem
public interface TestActorSystem <: ActorSystem {
    func testKit(): TestKit
    func timeController(): TestTimeController
}

// TestProbe - 测试探针
public interface TestProbe {
    func ref(): ActorRef
    func expectMsg[T <: Message](msg: T, max: Duration): T
    func expectNoMsg(max: Duration): Unit
    func receiveN(n: Int32, max: Duration): Array[Message]
    func within[T](min: Duration, max: Duration, f: () -> T): T
    func awaitCond(cond: () -> Bool, max: Duration, interval: Duration): Unit
}

// TestExpectation - 期望断言
public class TestExpectation[T] {
    func mustEqual(expected: T): TestExpectation[T]
    func mustBeNull(): TestExpectation[T]
    func mustContain(pred: (T) -> Bool): TestExpectation[T]
}

// TestTimeController - 控制时间
public interface TestTimeController {
    func time(): Int64
    func advance(duration: Duration): Unit
    func setTime(time: Int64): Unit
    // 支持调度器的时间控制
    // Scheduler 不真实等待
}

// 示例
test("ping-pong") {
    val system = TestActorSystem()
    val probe = system.testKit().createTestProbe()

    val pinger = system.actorOf(Props(Pinger), "pinger")
    val ponger = system.actorOf(Props(Ponger), "ponger")

    pinger.tell(Ping(probe.ref()))
    probe.expectMsg(Pong())
}
```

### 8.3 配置管理

```cangjie
// 配置值
public interface ConfigValue {
    func asString(): String
    func asInt(): Int32
    func asBool(): Bool
    func asDuration(): Duration
    func asBytesize(): Int64
    func asConfig(): Config
    func asList(): Array[ConfigValue]
}

// 配置管理器
public interface ConfigurationManager {
    func get(path: String): Option[ConfigValue]
    func getOrElse(path: String, default: ConfigValue): ConfigValue
    func hasPath(path: String): Bool

    // 配置合并
    func withFallback(override: Config): Config

    // 配置验证
    func validate(schema: ConfigSchema): ConfigValidationResult
}

// 全局配置管理器
public class GlobalConfigManager <: ConfigurationManager {
    // 支持多配置源
    // - HOCON 配置文件
    // - 环境变量
    // - 程序参数
    // - 默认配置
}
```

---

## 9. API Layer (API 层)

路径: `src/api/`，13 个文件

对外统一 API，简洁易用。

### 9.1 API 列表

| 文件/类 | 职责 |
|---------|------|
| `CActor` | 统一入口，`system()` 获取系统，`props()` 创建配置 |
| `CActorSystem` | ActorSystem 工厂 |
| `CActorFactory` | Actor 工厂 |
| `CActorSystemBuilder` | 链式构建器 |
| `config/` | MailboxConfig, DispatcherConfig, SupervisionConfig 工厂 |
| `extensions/` | ActorRef/ActorSystem/Props 扩展方法 |
| `public/` | 公共导出 |

### 9.2 统一入口

```cangjie
// 简化入口
public class CActor {
    // 获取默认 ActorSystem
    public static func system(): ActorSystem

    // 创建 ActorSystem
    public static func system(name: String): ActorSystem

    // 创建 Props
    public static func props[T <: Actor](creator: () -> T): Props
}

// 使用示例
let system = CActor.system("my-system")
let props = CActor.props { MyActor() }
let ref = system.actorOf(props, "my-actor")
```

### 9.3 链式构建器

```cangjie
// CActorSystemBuilder - 链式配置
public class CActorSystemBuilder {
    public func withName(name: String): CActorSystemBuilder
    public func withConfig(config: Config): CActorSystemBuilder
    public func withDispatcher(name: String, dispatcher: DispatcherConfig): CActorSystemBuilder
    public func withMailbox(name: String, mailbox: MailboxConfig): CActorSystemBuilder
    public func withSupervervision(strategy: SupervisionConfig): CActorSystemBuilder
    public func withRemote(address: String): CActorSystemBuilder
    public func withCluster(): CActorSystemBuilder
    public func withPersistence(): CActorSystemBuilder
    public func withMetrics(): CActorSystemBuilder
    public func build(): ActorSystem
}

// 使用示例
let system = CActorSystemBuilder()
    .withName("my-cluster")
    .withConfig(ConfigFactory.load("application"))
    .withDispatcher("default", DispatcherConfig().setThreads(16))
    .withCluster()
    .withPersistence()
    .withMetrics()
    .build()
```

### 9.4 扩展方法

```cangjie
// ActorRef 扩展
extension ActorRef {
    // 管道转发
    public func pipeTo(target: ActorRef): Unit

    // 条件发送
    public func forwardIf(condition: Boolean): Unit

    // 超时发送
    public func askWithTimeout[T <: Message](msg: T, timeout: Duration): Future[Any]
}

// 示例
ref.tell(message)  // 普通发送
ref !! message     // 广播
ref |> processMessage  // 函数式处理
```

---

## 10. Streams 与 XState

### 10.1 Streams (流处理)

路径: `src/streams/`

Akka Streams 风格的处理：

```cangjie
// Source - 数据源
// RunnableGraph - 运行图
// ActorMaterializer - 物化执行器

let source = Source(1..100)
let flow = Flow[Int].map((x) => x * 2)
let sink = Sink.fold(0)((acc, x) => acc + x)

let graph = source.via(flow).to(sink)
let result = graph.run(materializer)
```

### 10.2 XState (状态机)

路径: `src/xstate/`

分层状态机，支持 Actor 集成：

```cangjie
// 状态机定义
// core/ - 核心状态机实现
// enterprise/ - 企业级特性
// features/ - 特性模块
// lang/ - DSL 语言
// macros/ - 宏支持

// 状态机 Actor
public class StateMachineActor <: TypedBehavior[StateMachineEvent] {
    // 状态机配置
    // 状态转换处理
    // 上下文管理
}
```

---

## 11. 配置预设

CActor 提供多种开箱即用配置：

### 11.1 配置预设表

| 预设名称 | 适用场景 | 线程数 | 邮箱类型 | 调度器 |
|---------|----------|--------|----------|--------|
| `createDefault()` | 通用场景 | CPU 核数 | 无界 | WorkStealing |
| `createProduction()` | 生产环境 | CPU 核数 × 2 | 有界 | Adaptive |
| `createExtreme()` | 极限性能 | CPU 核数 × 4 | 无界 | FoundationBased |
| `createMemoryOptimized()` | 内存受限 | CPU 核数 / 2 | 有界 | Dynamic |
| `createLowLatency()` | 低延迟 | CPU 核数 | 优先级 | ThreadPool |
| `createHighThroughput()` | 高吞吐 | CPU 核数 × 4 | 无界 | BatchProcessing |

### 11.2 性能配置对比

| 指标 | Default | LowLatency | HighThroughput | Extreme |
|------|---------|------------|----------------|---------|
| 吞吐量 | 100K/s | 50K/s | 500K/s | 1M/s |
| 延迟 P99 | 1ms | 100us | 5ms | 10ms |
| 内存/Actor | 2KB | 2KB | 4KB | 8KB |

### 11.3 使用示例

```cangjie
// 默认配置
let system = CActorSystemBuilder()
    .withName("default-system")
    .withPreset(CActorPreset.Default)
    .build()

// 低延迟配置
let system = CActorSystemBuilder()
    .withName("latency-system")
    .withPreset(CActorPreset.LowLatency)
    .build()

// 自定义覆盖预设
let system = CActorSystemBuilder()
    .withPreset(CActorPreset.Production)
    .withDispatcher("custom", myDispatcher)
    .build()
```

---

## 12. 设计原则与哲学

### 12.1 核心设计原则

#### 12.1.1 性能优先原则

| 优化技术 | 实现方式 | 预期收益 |
|----------|----------|----------|
| **零拷贝** | 消息通过引用传递，不做深拷贝 | 减少 80%+ 内存带宽 |
| **无锁设计** | LockFreeQueue 基于 CAS | 消除锁竞争 |
| **批量处理** | 批量出队/入队 | 减少 90% 系统调用 |
| **对象池** | 复用 Actor/消息对象 | 减少 GC 压力 |
| **NUMA 感知** | 本地内存优先分配 | 提升 30% 缓存命中 |
| **批量消息** | 1000 条/批次聚合 | 提升批量吞吐 |
| **WorkStealing** | 空闲线程窃取任务 | 负载均衡 |

#### 12.1.2 类型安全原则

- **TypedBehavior[T]**: 编译期类型约束
- **TypedActorRef[T]**: 发送错误类型编译报错
- **编译期检查**: 减少运行时类型错误
- **无类型转换**: 通过 TypedActorWrapper 安全桥接

#### 12.1.3 弹性设计原则

```
┌─────────────────────────────────────────────────────────────┐
│                    故障处理层次                              │
├─────────────────────────────────────────────────────────────┤
│ Level 5: 系统级   │ CoordinatedShutdown（协调关闭）           │
├──────────────────┼──────────────────────────────────────────┤
│ Level 4: 集群级   │ ShardCoordinator（分片再平衡）            │
├──────────────────┼──────────────────────────────────────────┤
│ Level 3: Actor级  │ Supervision（监督策略）                   │
├──────────────────┼──────────────────────────────────────────┤
│ Level 2: 消息级   │ CircuitBreaker（断路器）                  │
├──────────────────┼──────────────────────────────────────────┤
│ Level 1: 单次调用 │ AskFuture 超时控制                        │
└──────────────────┴──────────────────────────────────────────┘
```

### 12.2 架构决策记录

| 决策 | 选择 | 备选方案 | 理由 |
|------|------|----------|------|
| 调度器默认实现 | WorkStealing | ThreadPool | 更适合 Actor 模型，减少空闲 |
| 邮箱默认实现 | LockFree | LinkedList | 更好的缓存局部性 |
| 序列化默认 | JSON | Protobuf | 通用性，可调试性 |
| 状态管理 | EventSourcing | 直接状态 | 审计日志，故障恢复 |
| 消息优先级 | 分离 System/User | 单一队列 | 系统消息优先处理 |

---

## 13. 消息流与执行模型

### 13.1 消息完整生命周期

```
┌──────────┐    ┌─────────┐    ┌────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Sender   │───>│ Serialize│───>│ Route   │───>│ Enqueue  │───>│ Dispatch│───>│ Receiver │
│ Actor    │    │         │    │         │    │  Mailbox  │    │          │    │ Actor    │
└──────────┘    └─────────┘    └────────┘    └─────────┘    └─────────┘    └─────────┘
                                                                       │
                                                                       v
                                                               ┌─────────────┐
                                                               │   Handler   │
                                                               │ receive()   │
                                                               └─────────────┘
                                                                       │
                                    ┌──────────────────────────────────┐
                                    │         MessageResult           │
                                    ├──────────────┬───────────────────┤
                                    │   same()    │   become()        │
                                    │  保持行为     │  切换行为           │
                                    └──────────────┴───────────────────┘
```

### 13.2 异步消息处理流程

```
1. 用户调用: ref.tell(message)
                    │
                    v
2. 消息信封创建: Envelope(message, sender, recipient)
                    │
                    v
3. 路由决策: 查找 recipient 的 ActorRef
                    │
                    v
4. 邮箱入队: mailbox.enqueue(envelope)  [LockFreeQueue CAS]
                    │
                    v
5. 调度器通知: dispatcher ! mailboxBecameAvailable()
                    │
                    v
6. 调度器选择: WorkStealing 选择执行线程
                    │
                    v
7. 消息出队: mailbox.dequeueBatch(1000)  [批量]
                    │
                    v
8. Actor 执行: actor.receive(msg, context)
                    │
                    v
9. 结果处理: MessageResult.same() 或 become(newBehavior)
```

### 13.3 线程模型

```
┌─────────────────────────────────────────────────────────────────┐
│                      ActorSystem                                │
├─────────────────────────────────────────────────────────────────┤
│  ThreadPool (N threads)                                         │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                 │
│  │ Thread  │ │ Thread  │ │ Thread  │ │ Thread  │  ...           │
│  │  ────   │ │  ────   │ │  ────   │ │  ────   │                │
│  │  Mailbox│ │  Mailbox│ │  Mailbox│ │  Mailbox│                │
│  │  Queue  │ │  Queue  │ │  Queue  │ │  Queue  │                │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘                 │
│        \         /            \         /                      │
│         Work-Stealing         Work-Stealing                    │
│              \                    /                             │
│               v                  v                              │
│  ┌─────────────────────────────────────────────────┐           │
│  │          ActorInstance (per Mailbox)            │           │
│  │   ┌───────────┐  ┌──────────────────────────┐   │           │
│  │   │ Behavior  │  │   ActorContext           │   │           │
│  │   │ receive()│  │   - self: ActorRef        │   │           │
│  │   │          │  │   - sender: ActorRef      │   │           │
│  │   │          │  │   - children: Set[Ref]     │   │           │
│  │   │          │  │   - parent: ActorRef       │   │           │
│  │   └──────────┘  └──────────────────────────────┘           │
│  └─────────────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────────────┘
```

---

## 14. 测试体系

### 14.1 测试覆盖

| 测试类型 | 数量 | 描述 |
|----------|------|------|
| 单元测试 | 25+ 文件 | Core 和 Runtime 模块测试 |
| 集成测试 | 5+ 文件 | ActorSystem, Cluster 集成 |
| 性能测试 | 基准测试 | 吞吐量、延迟基准 |
| **总计** | **1616 个测试用例** | **100% 通过** |

### 14.2 测试工具

```cangjie
// TestKit 核心工具
let kit = TestKit(system)
let probe = kit.createTestProbe()

// 消息期望
probe.expectMsg[Ping](Ping())
probe.expectNoMsg(100.millis)
probe.receiveN(10)

// 时间控制
kit.timeController().advance(5.seconds)

// 条件等待
probe.awaitCond(() => counter.value > 10)

// 异常捕获
val exc = kit.expectException[RuntimeException] {
    ref.tell(InvalidMessage())
}
```

---

## 15. 附录

### 15.1 文件统计

| 层级 | 路径 | 文件数 |
|------|------|--------|
| Foundation | `src/foundation/` | 33 |
| Core | `src/core/` | 31 |
| Runtime | `src/runtime/` | 63 |
| Patterns | `src/patterns/` | 13 |
| Distribution | `src/distribution/` | 21 |
| Integration | `src/integration/` | 28 |
| API | `src/api/` | 13 |
| Streams | `src/streams/` | - |
| XState | `src/xstate/` | - |
| Macros | `src/macros/` | - |
| **总计** | | **326+** |

### 15.2 核心接口快速索引

| 接口 | 层级 | 文件 |
|------|------|------|
| `Actor` | Core | `core/actor/Actor.cj` |
| `ActorRef` | Core | `core/actor/ActorRef.cj` |
| `ActorSystem` | Core | `core/system/ActorSystem.cj` |
| `Message` | Core | `core/message/Message.cj` |
| `Props` | Core | `core/actor/Props.cj` |
| `SupervisionStrategy` | Core | `core/supervision/SupervisionStrategy.cj` |
| `TypedBehavior[T]` | Core | `core/typed/TypedBehavior.cj` |
| `MessageDispatcher` | Runtime | `runtime/dispatcher/MessageDispatcher.cj` |
| `Mailbox` | Runtime | `runtime/mailbox/Mailbox.cj` |
| `LockFreeQueue[T]` | Foundation | `foundation/queue/LockFreeQueue.cj` |

### 15.3 版本历史

| 版本 | 日期 | 重大变更 |
|------|------|----------|
| 1.0 | 早期 | 基础 Actor 实现 |
| 3.0 | - | 引入 TypedActor |
| 5.0 | - | 性能优化，重新架构 |
| 6.0 | - | 分布式支持 |
| 7.0 | 2026-05 | 类型化增强，6+1 层级重构 |

---

**CActor 7.0 架构设计文档完**

*本文档遵循渐进式披露原则，从高层抽象逐步深入到具体实现。*
