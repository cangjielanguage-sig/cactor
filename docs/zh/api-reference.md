# CActor API 参考文档

CActor 是一个高性能 Actor 模型实现库，为仓颉语言提供简洁、类型安全的 Actor 编程接口。本文档是 CActor 1.2 的完整 API 参考。

---

## 目录

1. [统一入口 - CActor](#1-统一入口---cactor)
2. [Actor 系统 - CActorSystem](#2-actor-系统---cactorsystem)
3. [Actor 引用 - ActorRef](#3-actor-引用---actorref)
4. [Actor 接口 - Actor](#4-actor-接口---actor)
5. [消息系统 - Message](#5-消息系统---message)
6. [Props - Actor 属性配置](#6-props---actor-属性配置)
7. [配置 API](#7-配置-api)
8. [Ask 模式](#8-ask-模式)
9. [断路器模式](#9-断路器模式)
10. [背压控制](#10-背压控制)
11. [路由模式](#11-路由模式)
12. [测试框架](#12-测试框架)
13. [类型安全 Actor](#13-类型安全-actor)

---

## 1. 统一入口 - CActor

`CActor` 是整个库的统一入口点，参考 Akka 的 `ActorSystem` 设计，提供简洁直观的静态方法。

### 包引入

```cangjie
import cactor.api.{CActor}
```

### API 签名

```cangjie
public struct CActor {
    /**
     * 创建默认 Actor 系统
     * 使用 "default" 名称和默认配置
     */
    public static func system(): CActorSystem

    /**
     * 创建命名 Actor 系统
     * @param name 系统名称，用于日志和监控
     */
    public static func system(name: String): CActorSystem

    /**
     * 创建带配置的 Actor 系统
     * @param name 系统名称
     * @param config 运行时配置
     */
    public static func system(name: String, config: CActorRuntimeConfig): CActorSystem

    /**
     * 创建 Actor Props
     * @param creator Actor 实例创建函数
     */
    public static func props(creator: () -> Actor): ActorProps
}
```

### 使用示例

```cangjie
// 创建默认系统
let system = CActor.system()

// 创建命名系统
let system = CActor.system("MySystem")

// 创建带配置的系统
let config = CActorRuntimeConfig.createProduction()
let system = CActor.system("ProductionSystem", config)

// 创建 Actor Props
let props = CActor.props({ => MyActor() })
```

### ActorProps 简化包装

```cangjie
public struct ActorProps {
    public init(creator: () -> Actor)

    public func getCreator(): () -> Actor
}
```

---

## 2. Actor 系统 - CActorSystem

`CActorSystem` 是 Actor 系统的核心类，负责管理 Actor 的创建、生命周期和消息路由。

### 包引入

```cangjie
import cactor.api.{CActorSystem}
```

### 构造方法

```cangjie
public class CActorSystem {
    public init(name: String)
    public init(name: String, config: CActorRuntimeConfig)
}
```

### Actor 创建方法

```cangjie
// ========== 基础创建方法 ==========

/**
 * 使用创建函数创建 Actor（自动命名）
 */
public func actorOf(creator: () -> Actor): ActorRef

/**
 * 使用创建函数创建命名 Actor
 * @param creator Actor 实例创建函数
 * @param name Actor 名称，必须唯一
 */
public func actorOf(creator: () -> Actor, name: String): ActorRef

/**
 * 使用 ActorProps 创建 Actor（自动命名）
 */
public func actorOf(actorProps: ActorProps): ActorRef

/**
 * 使用 ActorProps 创建命名 Actor
 */
public func actorOf(actorProps: ActorProps, name: String): ActorRef

/**
 * 使用 Props 创建 Actor（自动命名）
 */
public func actorOf(props: Props<Actor>): ActorRef

/**
 * 使用 Props 创建命名 Actor
 * @param props 完整的 Actor 属性配置
 * @param name Actor 名称
 */
public func actorOf(props: Props<Actor>, name: String): ActorRef

// ========== 高级创建方法 ==========

/**
 * 使用邮箱配置创建 Actor
 * @param creator Actor 创建函数
 * @param name Actor 名称
 * @param mailboxConfig 邮箱配置
 */
public func actorOfWithMailbox(
    creator: () -> Actor,
    name: String,
    mailboxConfig: CoreMailboxConfig
): ActorRef

/**
 * 使用调度器配置创建 Actor
 * @param creator Actor 创建函数
 * @param name Actor 名称
 * @param dispatcherConfig 调度器配置
 */
public func actorOfWithDispatcher(
    creator: () -> Actor,
    name: String,
    dispatcherConfig: CoreDispatcherConfig
): ActorRef

/**
 * 使用完整配置创建 Actor
 * 支持同时配置邮箱、调度器和监督策略
 */
public func actorOfConfigured(
    creator: () -> Actor,
    name: String,
    mailboxConfig: CoreMailboxConfig,
    dispatcherConfig: CoreDispatcherConfig,
    supervisionConfig: CoreSupervisionConfig
): ActorRef
```

### 类型安全 Actor 创建

```cangjie
// ========== Typed Actor 支持 ==========

/**
 * 创建类型安全 Actor
 * @param behavior 类型安全的 Actor 行为定义
 * @param name Actor 名称
 */
public func typedActorOf<T>(behavior: TypedBehavior<T>, name: String): TypedActorRef<T>

/**
 * 创建类型安全 Actor（自动命名）
 */
public func typedActorOf<T>(behavior: TypedBehavior<T>): TypedActorRef<T>

/**
 * 类型安全 Actor 选择（根据路径查找）
 */
public func typedActorSelection<T>(path: String): TypedActorSelection<T>

/**
 * 将标准 ActorRef 转换为类型安全 ActorRef
 */
public func wrapAsTypedActor<T>(actorRef: ActorRef, name: String): TypedActorRef<T>

/**
 * 从类型安全 ActorRef 获取底层标准 ActorRef
 */
public func unwrapTypedActor<T>(typedActorRef: TypedActorRef<T>): ActorRef

/**
 * 创建混合消息桥接器
 * 允许标准 Actor 和类型安全 Actor 之间发送消息
 */
public func createMessageBridge<T>(
    standardActor: ActorRef,
    typedActor: TypedActorRef<T>
): MessageBridge<T>
```

### 系统管理方法

```cangjie
/**
 * 终止 Actor 系统
 * 会停止所有 Actor 并释放资源
 */
public func terminate(): Unit

/**
 * 检查系统是否已终止
 * @return true 表示已终止
 */
public func isTerminated(): Bool

/**
 * 获取系统名称
 */
public func getName(): String

/**
 * 获取底层运行时（用于高级功能）
 */
public func getRuntime(): CActorRuntime

/**
 * 获取系统启动时间戳
 */
public func getStartTime(): Int64
```

### 使用示例

```cangjie
// 基础使用
let system = CActor.system("DemoSystem")
let actor = system.actorOf({ => EchoActor() }, "echo")

// 使用配置创建
let mailboxConfig = MailboxConfig.createBounded(1000)
let dispatcherConfig = DispatcherConfig.createWorkStealing()
let actor = system.actorOfWithMailbox(
    { => WorkerActor() },
    "worker",
    mailboxConfig
)

// 使用类型安全 Actor
let behavior = TypedBehavior.create<MyProtocol>(handler)
let typedActor = system.typedActorOf(behavior, "typed-actor")

// 系统管理
system.terminate()
```

---

## 3. Actor 引用 - ActorRef

`ActorRef` 是 Actor 的引用句柄，用于向 Actor 发送消息。

### 包引入

```cangjie
import cactor_core.actor.{ActorRef}
```

### 接口定义

```cangjie
public interface ActorRef {
    /**
     * Actor 的路径
     */
    prop path: ActorPath

    /**
     * 发送消息给 Actor（tell 模式）
     * 异步发送，不等待响应
     * @param message 要发送的消息
     */
    func tell(message: Message): Unit

    /**
     * 发送消息给 Actor，指定发送者
     * @param message 要发送的消息
     * @param sender 发送者的 Actor 路径（Option<String>）
     */
    func tell(message: Message, sender: Option<String>): Unit

    /**
     * 比较两个 ActorRef 是否相等
     */
    func equals(other: ActorRef): Bool

    /**
     * 获取哈希码
     */
    func hashCode(): Int32
}
```

### LocalActorRef 子接口

表示同一进程内的 Actor 引用：

```cangjie
public interface LocalActorRef <: ActorRef {
    /**
     * 获取底层的 Actor 实例
     */
    func getActor(): Actor

    /**
     * 暂停 Actor 的消息处理
     */
    func suspend(): Unit

    /**
     * 恢复 Actor 的消息处理
     */
    func resume(): Unit

    /**
     * 重启 Actor
     * @param cause 重启原因（异常）
     */
    func restart(cause: Exception): Unit

    /**
     * 停止 Actor
     */
    func stop(): Unit

    /**
     * 获取 Actor 当前状态
     */
    func getState(): ActorLifecycleState
}
```

### DeadLetterActorRef 子接口

用于处理发送给已停止 Actor 的消息：

```cangjie
public interface DeadLetterActorRef <: ActorRef {
    // 标记接口，表示这是一个死信引用
}
```

### ActorSelection 接口

用于按路径模式查找和选择 Actor：

```cangjie
public interface ActorSelection {
    /**
     * 解析选择器，返回单个 ActorRef
     * @param timeout 超时时间
     */
    func resolveOne(timeout: Duration): Option<ActorRef>

    /**
     * 向选择的所有 Actor 发送消息
     */
    func tell(message: Message): Unit

    /**
     * 向选择的所有 Actor 发送消息，指定发送者
     */
    func tell(message: Message, sender: Option<String>): Unit
}
```

### ActorPath

```cangjie
public struct ActorPath {
    public init(system: String, path: String)

    public func getSystem(): String
    public func getPath(): String
    public func toString(): String              // 返回 "cactor://system/path" 格式
    public func parent(): Option<ActorPath>      // 获取父路径
    public func child(name: String): ActorPath   // 创建子路径
    public func equals(other: ActorPath): Bool
    public func hashCode(): Int32
}
```

### 使用示例

```cangjie
// 发送消息
actorRef.tell(StringMessage("Hello"))

// 发送消息并指定发送者
actorRef.tell(StringMessage("Reply"), Some(senderPath))

// 比较引用
if (ref1.equals(ref2)) {
    println("相同的 Actor")
}

// 获取路径
println(actorRef.path.toString())
```

### API 层增强函数

`cactor.api` 包还提供了一些便利的消息发送函数：

```cangjie
import cactor.api.{send, sendBatch, broadcast}

/**
 * 类型安全的消息发送
 */
public func send<T>(actorRef: ActorRef, message: T): Unit where T <: Message

/**
 * 批量消息发送
 */
public func sendBatch(actorRef: ActorRef, messages: Array<Message>): Unit

/**
 * 广播消息到多个 ActorRef
 */
public func broadcast(actorRefs: Array<ActorRef>, message: Message): Unit

/**
 * 获取 Actor 路径字符串
 */
public func getPathString(actorRef: ActorRef): String
```

---

## 4. Actor 接口 - Actor

`Actor` 是所有 Actor 的基础接口，定义核心行为和生命周期钩子。

### 包引入

```cangjie
import cactor_core.actor.{Actor}
```

### 接口定义

```cangjie
public interface Actor {
    /**
     * Actor 的唯一名称
     */
    prop name: String

    /**
     * Actor 的功能描述
     */
    prop description: String

    /**
     * 处理接收到的消息（必须实现）
     * @param message 接收到的消息
     * @param context Actor 上下文
     * @return 消息处理结果
     */
    func receive(message: Message, context: ActorContext): MessageResult

    // ======== 生命周期钩子（可选实现） ========

    /**
     * Actor 启动前的初始化操作
     * 默认空实现
     */
    func preStart(): Unit

    /**
     * Actor 启动后的操作
     * 默认空实现
     */
    func postStart(): Unit

    /**
     * Actor 停止前的清理操作
     * 默认空实现
     */
    func preStop(): Unit

    /**
     * Actor 停止后的清理操作
     * 默认空实现
     */
    func postStop(): Unit

    /**
     * Actor 重启前的操作
     * 默认调用 preStop()
     * @param reason 重启原因（异常）
     */
    func preRestart(reason: Exception): Unit

    /**
     * Actor 重启后的操作
     * 默认调用 postStart()
     * @param reason 重启原因（异常）
     */
    func postRestart(reason: Exception): Unit
}
```

### Actor 生命周期状态

```cangjie
public enum ActorLifecycleState {
    | Created      // 已创建，未启动
    | Starting     // 启动中
    | Running      // 运行中
    | Stopping     // 停止中
    | Stopped      // 已停止
    | Failed       // 失败状态
    | Restarting   // 重启中
}
```

### 消息处理结果

```cangjie
public enum MessageResult {
    | Handled        // 消息已成功处理
    | Unhandled      // 消息未被处理
    | Stop           // 请求停止 Actor
    | Restart        // 请求重启 Actor
    | Escalate       // 向监督者报告问题
}
```

### Actor 实现示例

```cangjie
class EchoActor <: Actor {
    public prop name: String {
        get() { "EchoActor" }
    }

    public prop description: String {
        get() { "回显消息的 Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                // 获取发送者并回复
                context.sender().map { sender =>
                    sender.tell(StringMessage("Echo: ${msg.content}"))
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }

    // 可选的 lifecycle hooks
    public func preStart(): Unit {
        println("EchoActor 启动")
    }

    public func postStop(): Unit {
        println("EchoActor 停止")
    }
}
```

---

## 5. 消息系统 - Message

### 包引入

```cangjie
import cactor_core.message.{Message, UserMessage, SystemMessage}
```

### Message 接口

```cangjie
public interface Message {
    /**
     * 获取消息类型标识
     */
    func messageType(): String

    /**
     * 获取消息优先级（数值越大优先级越高）
     * 默认返回 0
     */
    func priority(): Int32

    /**
     * 获取消息的唯一标识
     * 默认返回空字符串
     */
    func messageId(): String

    /**
     * 获取消息创建时间戳（纳秒）
     * 默认返回 0
     */
    func timestamp(): Int64
}
```

### 消息分类

#### 系统消息接口

```cangjie
public interface SystemMessage <: Message {
    // 系统消息默认优先级为 1000
    func priority(): Int32  // 默认返回 1000
}
```

#### 用户消息接口

```cangjie
public interface UserMessage <: Message {
    // 用户消息默认优先级为 0
    func priority(): Int32  // 默认返回 0
}
```

### 内置消息类型

#### StopMessage（系统消息）

```cangjie
public struct StopMessage <: SystemMessage {
    public init(reason: String)
    public init()  // 默认原因: "Normal stop"

    public func messageType(): String  // 返回 "StopMessage"
    public func getReason(): String
}
```

#### RestartMessage（系统消息）

```cangjie
public struct RestartMessage <: SystemMessage {
    public init(cause: Exception)

    public func messageType(): String  // 返回 "RestartMessage"
    public func getCause(): Exception
}
```

#### SuspendMessage / ResumeMessage（系统消息）

```cangjie
public struct SuspendMessage <: SystemMessage {
    public init()
}

public struct ResumeMessage <: SystemMessage {
    public init()
}
```

#### TerminatedMessage（系统消息）

```cangjie
public struct TerminatedMessage <: SystemMessage {
    public init(actor: String, reason: String)

    public func messageType(): String  // 返回 "TerminatedMessage"
    public func getActor(): String
    public func getReason(): String
}
```

#### StringMessage（用户消息）

```cangjie
public struct StringMessage <: UserMessage {
    public init(content: String)

    public func messageType(): String  // 返回 "StringMessage"
    public func getContent(): String
}
```

#### PingMessage / PongMessage（用户消息）

```cangjie
public struct PingMessage <: UserMessage {
    public init(id: Int64)

    public func messageType(): String  // 返回 "PingMessage"
    public func getId(): Int64
    public func timestamp(): Int64    // 自动设置时间戳
}

public struct PongMessage <: UserMessage {
    public init(originalId: Int64)

    public func messageType(): String  // 返回 "PongMessage"
    public func getOriginalId(): Int64
    public func timestamp(): Int64
}
```

#### DeadLetter（系统消息）

```cangjie
public struct DeadLetter <: SystemMessage {
    public init(message: Message, sender: String, recipient: String)

    public func messageType(): String  // 返回 "DeadLetter"
    public func getMessage(): Message
    public func getSender(): String
    public func getRecipient(): String
}
```

### 消息信封

```cangjie
public struct Envelope {
    public init(message: Message, sender: Option<String>)

    public func getMessage(): Message
    public func getSender(): Option<String>
    public func getTimestamp(): Int64
}
```

### 自定义消息示例

```cangjie
// 定义业务消息
class OrderMessage <: UserMessage {
    private let orderId: String
    private let amount: Float64

    public init(orderId: String, amount: Float64) {
        this.orderId = orderId
        this.amount = amount
    }

    public func messageType(): String {
        "OrderMessage"
    }

    public func getOrderId(): String { orderId }
    public func getAmount(): Float64 { amount }

    // 可选：设置优先级
    public func priority(): Int32 {
        if (amount > 10000.0) { 10 } else { 0 }
    }
}

// 定义系统消息
class HighPriorityTask <: SystemMessage {
    private let taskId: String

    public init(taskId: String) {
        this.taskId = taskId
    }

    public func messageType(): String {
        "HighPriorityTask"
    }

    public func getTaskId(): String { taskId }
}
```

---

## 6. Props - Actor 属性配置

`Props` 是 Actor 的创建配置，包含调度器、邮箱、监督策略等设置。

### 包引入

```cangjie
import cactor_core.actor.{Props, PropsFactory}
```

### Props 结构体

```cangjie
public struct Props<T> where T <: Actor {
    // 私有构造函数，通过 PropsFactory 创建
    private init(factory: ActorFactory<T>, dispatcher: String,
                 mailboxType: String, supervisionStrategy: String)

    /**
     * 配置调度器
     * @param dispatcher 调度器名称
     */
    public func withDispatcher(dispatcher: String): Props<T>

    /**
     * 配置邮箱类型
     * @param mailboxType 邮箱类型名称（如 "unbounded", "bounded"）
     */
    public func withMailbox(mailboxType: String): Props<T>

    /**
     * 配置监督策略
     * @param strategy 监督策略名称
     */
    public func withSupervisionStrategy(strategy: String): Props<T>

    /**
     * 创建 Actor 实例
     */
    public func create(): T

    /**
     * 获取调度器名称
     */
    public func getDispatcher(): String
    public func getDispatcherName(): String

    /**
     * 获取邮箱类型
     */
    public func getMailboxType(): String

    /**
     * 获取监督策略名称
     */
    public func getSupervisionStrategy(): String
}
```

### PropsFactory

```cangjie
public struct PropsFactory {
    /**
     * 创建 Props
     * @param creator Actor 创建函数
     */
    public static func create<T>(creator: () -> T): Props<T> where T <: Actor

    /**
     * 创建带调度器的 Props
     */
    public static func createWithDispatcher<T>(
        creator: () -> T,
        dispatcher: String
    ): Props<T> where T <: Actor
}
```

### 使用示例

```cangjie
// 基础用法
let props = PropsFactory.create({ => MyActor() })

// 配置调度器
let props = PropsFactory.createWithDispatcher(
    { => MyActor() },
    "work-stealing-dispatcher"
)

// 链式配置
let props = PropsFactory.create({ => MyActor() })
    .withDispatcher("dedicated-dispatcher")
    .withMailbox("bounded")
    .withSupervisionStrategy("one-for-one")

// 使用 Props 创建 Actor
let actor = system.actorOf(props, "my-actor")
```

---

## 7. 配置 API

### 包引入

```cangjie
import cactor_core.config.{MailboxConfig, DispatcherConfig, SupervisionConfig}
```

### MailboxConfig - 邮箱配置

```cangjie
public class MailboxConfig {
    public let mailboxType: MailboxType
    public let capacity: Int64
    public let pushTimeout: Duration
    public let stashCapacity: Int64
    public let name: String

    // ======== 工厂方法 ========

    /**
     * 创建无界邮箱（默认）
     */
    public static func createUnbounded(): MailboxConfig
    public static func createUnboundedWithName(name: String): MailboxConfig

    /**
     * 创建有界邮箱
     * @param capacity 邮箱容量
     */
    public static func createBounded(capacity: Int64): MailboxConfig
    public static func createBoundedWithName(capacity: Int64, name: String): MailboxConfig

    /**
     * 创建优先级邮箱
     * @param comparator 消息比较函数
     */
    public static func createPriority(
        comparator: (Message, Message) -> Int32
    ): MailboxConfig
    public static func createPriorityWithCapacity(
        comparator: (Message, Message) -> Int32,
        capacity: Int64,
        name: String
    ): MailboxConfig

    /**
     * 创建支持暂存的邮箱
     * @param capacity 邮箱容量
     * @param stashCapacity 暂存区容量
     */
    public static func createStashing(
        capacity: Int64,
        stashCapacity: Int64
    ): MailboxConfig
    public static func createStashingWithName(
        capacity: Int64,
        stashCapacity: Int64,
        name: String
    ): MailboxConfig
}
```

### MailboxType 枚举

```cangjie
public enum MailboxType {
    | Unbounded    // 无界邮箱
    | Bounded      // 有界邮箱
    | Priority     // 优先级邮箱
    | Stashing     // 支持暂存的邮箱
    | Custom       // 自定义邮箱
}
```

### DispatcherConfig - 调度器配置

```cangjie
public class DispatcherConfig {
    public let dispatcherType: DispatcherType
    public let throughput: Int32
    public let throughputDeadlineTime: Duration
    public let executorConfig: ExecutorConfig
    public let name: String

    // ======== 工厂方法 ========

    /**
     * 创建默认线程池调度器
     */
    public static func createThreadPool(): DispatcherConfig
    public static func createThreadPoolWithName(name: String): DispatcherConfig

    /**
     * 创建工作窃取调度器（推荐用于高并发）
     */
    public static func createWorkStealing(): DispatcherConfig
    public static func createWorkStealingWithName(name: String): DispatcherConfig

    /**
     * 创建专用线程调度器（Actor 独占线程）
     */
    public static func createPinned(): DispatcherConfig
    public static func createPinnedWithName(name: String): DispatcherConfig
}
```

### DispatcherType 枚举

```cangjie
public enum DispatcherType {
    | ThreadPool       // 线程池调度器
    | WorkStealing     // 工作窃取调度器
    | PinnedDispatcher // 专用线程调度器
    | CallingThread    // 调用线程调度器
    | Custom           // 自定义调度器
}
```

### SupervisionConfig - 监督配置

```cangjie
public class SupervisionConfig {
    public let maxRetries: Int32
    public let withinTimeRange: Duration
    public let strategy: SupervisionStrategyType
    public let name: String

    // ======== 工厂方法 ========

    /**
     * 创建默认监督策略（OneForOne，最多重试3次）
     */
    public static func createDefault(): SupervisionConfig
    public static func createDefaultWithName(name: String): SupervisionConfig
}
```

### SupervisionStrategyType 枚举

```cangjie
public enum SupervisionStrategyType {
    | OneForOne   // 一对一策略：只影响失败的子 Actor
    | AllForOne   // 全部策略：影响所有兄弟 Actor
    | Custom      // 自定义策略
}
```

### SupervisionDirective 枚举

```cangjie
public enum SupervisionDirective {
    | Resume      // 恢复 Actor，忽略异常
    | Restart     // 重启 Actor
    | Stop        // 停止 Actor
    | Escalate    // 向上级监督者报告
}
```

### Actor 配置工厂

```cangjie
import cactor_core.config.{ActorConfigurationFactory, ActorConfigurationBuilder}

/**
 * 预定义配置模板
 */
public class ActorConfigurationFactory {
    /**
     * 创建默认配置
     * 适用于一般用途
     */
    public static func createDefault(): ActorConfiguration

    /**
     * 创建高性能配置
     * 适用于高吞吐量、低延迟场景
     * - 有界邮箱（8192）
     * - 工作窃取调度器
     */
    public static func createHighPerformance(): ActorConfiguration

    /**
     * 创建批处理配置
     * 适用于批量消息处理
     * - 有界邮箱（4096）
     * - 线程池调度器
     */
    public static func createBatching(): ActorConfiguration

    /**
     * 创建关键任务配置
     * 适用于关键任务，使用专用线程
     * - 有界邮箱（1024）
     * - 专用线程调度器
     * - 最多重试1次
     */
    public static func createCritical(): ActorConfiguration

    /**
     * 创建优先级配置
     */
    public static func createPriority(
        comparator: (Message, Message) -> Int32
    ): ActorConfiguration

    /**
     * 创建路由配置
     */
    public static func createRouted(
        routerType: RouterType,
        routeeCount: Int32
    ): ActorConfiguration

    /**
     * 创建自定义配置
     */
    public static func createCustom(
        mailboxConfig: MailboxConfig,
        dispatcherConfig: DispatcherConfig,
        supervisionConfig: SupervisionConfig,
        routingConfig: Option<RoutingConfig>,
        name: String,
        description: String
    ): ActorConfiguration
}

/**
 * 流式配置构建器
 */
public class ActorConfigurationBuilder {
    public init()

    public func withMailbox(config: MailboxConfig): ActorConfigurationBuilder
    public func withDispatcher(config: DispatcherConfig): ActorConfigurationBuilder
    public func withSupervision(config: SupervisionConfig): ActorConfigurationBuilder
    public func withRouting(config: RoutingConfig): ActorConfigurationBuilder
    public func withName(name: String): ActorConfigurationBuilder
    public func withDescription(description: String): ActorConfigurationBuilder
    public func build(): ActorConfiguration
}
```

### RouterType 枚举

```cangjie
public enum RouterType {
    | RoundRobin          // 轮询路由
    | Random              // 随机路由
    | ConsistentHashing   // 一致性哈希路由
    | Broadcast           // 广播路由
    | Custom              // 自定义路由
}
```

### 配置示例

```cangjie
// 使用预定义配置
let config = ActorConfigurationFactory.createHighPerformance()

// 使用流式构建器
let config = ActorConfigurationBuilder()
    .withMailbox(MailboxConfig.createBounded(5000))
    .withDispatcher(DispatcherConfig.createWorkStealing())
    .withName("my-actor-config")
    .build()

// 直接创建组件配置
let mailboxConfig = MailboxConfig.createBounded(10000)
let dispatcherConfig = DispatcherConfig.createWorkStealingWithName("my-dispatcher")

// 创建 Actor 时应用配置
let actor = system.actorOfConfigured(
    { => MyActor() },
    "my-actor",
    mailboxConfig,
    dispatcherConfig,
    SupervisionConfig.createDefault()
)
```

---

## 8. Ask 模式

Ask 模式提供请求-响应式的消息交互，参考 Akka 的 Ask 模式设计。

### 包引入

```cangjie
import cactor_patterns.ask.{Ask, AskFuture, AskStats}
```

### Ask 静态 API

```cangjie
public struct Ask {
    /**
     * 发送 Ask 请求（使用默认超时 5 秒）
     * @param target 目标 Actor
     * @param message 请求消息
     * @return AskFuture 对象
     */
    public static func ask(target: ActorRef, message: Message): AskFuture<Message>

    /**
     * 发送 Ask 请求（指定超时）
     * @param target 目标 Actor
     * @param message 请求消息
     * @param timeout 超时时间
     */
    public static func ask(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): AskFuture<Message>

    /**
     * 类型安全的 Ask 请求
     */
    public static func askTyped(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): AskFuture<Message>

    /**
     * 便捷方法：等待 Ask 响应（同步）
     */
    public static func askAndWait(
        target: ActorRef,
        message: Message,
        timeout: Duration
    ): Result<Message>

    /**
     * 便捷方法：异步 Ask 处理（带回调）
     */
    public static func askAsync(
        target: ActorRef,
        message: Message,
        timeout: Duration,
        onSuccess: (Message) -> Unit,
        onFailure: (Exception) -> Unit
    ): Unit

    /**
     * 关闭 Ask 管理器
     */
    public static func shutdown(): Unit

    /**
     * 获取统计信息
     */
    public static func getStats(): AskStats
}
```

### AskFuture 接口

```cangjie
public interface AskFuture<T> {
    /**
     * 检查是否已完成
     */
    func isCompleted(): Bool

    /**
     * 等待并获取结果（同步）
     * @param timeout 最大等待时间
     */
    func await(timeout: Duration): Result<T>

    /**
     * 注册完成回调
     */
    func onComplete(callback: (Result<T>) -> Unit): Unit
}
```

### AskStats

```cangjie
public struct AskStats {
    public let pendingRequests: Int64   // 待处理的请求数
    public let defaultTimeout: Duration // 默认超时时间

    public func toString(): String
}
```

### 使用示例

```cangjie
// 异步 Ask（推荐）
let future = Ask.ask(actorRef, requestMessage, Duration.second * 3)
future.onComplete { result =>
    match (result) {
        case Result.Success(response) =>
            println("收到响应: ${response}")
        case Result.Failure(error) =>
            println("请求失败: ${error.message}")
    }
}

// 同步 Ask（阻塞等待）
let result = Ask.askAndWait(actorRef, requestMessage, Duration.second * 5)
match (result) {
    case Result.Success(response) => handleResponse(response)
    case Result.Failure(error) => handleError(error)
}

// 使用回调
Ask.askAsync(
    actorRef,
    requestMessage,
    Duration.second * 5,
    onSuccess = { response => handleResponse(response) },
    onFailure = { error => handleError(error) }
)
```

---

## 9. 断路器模式

断路器模式提供故障隔离和快速失败机制，保护系统免受级联故障影响。

### 包引入

```cangjie
import cactor_foundation.circuit_breaker.{
    CircuitBreaker, BasicCircuitBreaker, CircuitBreakerFactory,
    CircuitBreakerConfig, CircuitBreakerStats, CircuitBreakerState,
    CircuitBreakerDecorator
}
```

### 断路器状态

```cangjie
public enum CircuitBreakerState {
    | Closed   // 关闭状态：正常工作
    | Open     // 开启状态：快速失败
    | HalfOpen // 半开状态：尝试恢复
}
```

### 断路器配置

```cangjie
public struct CircuitBreakerConfig {
    public let failureThreshold: Int64   // 触发断路的失败次数
    public let successThreshold: Int64   // 半开状态下恢复所需的成功次数
    public let timeout: Duration         // 单次操作超时时间
    public let resetTimeout: Duration    // 从 Open 转换到 HalfOpen 的等待时间
}
```

### CircuitBreaker 接口

```cangjie
public interface CircuitBreaker<T> {
    /**
     * 执行操作
     * @param operation 要执行的操作
     */
    func execute(operation: () -> T): T

    /**
     * 获取当前状态
     */
    func getState(): CircuitBreakerState

    /**
     * 获取统计信息
     */
    func getStats(): CircuitBreakerStats

    /**
     * 手动重置断路器
     */
    func reset(): Unit

    /**
     * 手动触发断路器
     */
    func trip(): Unit
}
```

### BasicCircuitBreaker 实现

```cangjie
public class BasicCircuitBreaker<T> <: CircuitBreaker<T> {
    public init(config: CircuitBreakerConfig)

    public func execute(operation: () -> T): T
    public func getState(): CircuitBreakerState
    public func getStats(): CircuitBreakerStats
    public func reset(): Unit
    public func trip(): Unit
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

### CircuitBreakerFactory

```cangjie
public struct CircuitBreakerFactory {
    /**
     * 创建默认配置的断路器
     * - 失败阈值: 5
     * - 成功阈值: 3
     * - 超时: 5秒
     * - 重置超时: 30秒
     */
    public static func createDefault<T>(): BasicCircuitBreaker<T>

    /**
     * 创建快速失败的断路器
     * - 失败阈值: 3
     * - 成功阈值: 2
     * - 超时: 2秒
     * - 重置超时: 10秒
     */
    public static func createFastFail<T>(): BasicCircuitBreaker<T>

    /**
     * 创建容错的断路器
     * - 失败阈值: 10
     * - 成功阈值: 5
     * - 超时: 10秒
     * - 重置超时: 1分钟
     */
    public static func createTolerant<T>(): BasicCircuitBreaker<T>
}
```

### CircuitBreakerDecorator（装饰器模式）

```cangjie
public class CircuitBreakerDecorator<T> {
    public init(circuitBreaker: CircuitBreaker<T>, fallback: Option<() -> T>)

    /**
     * 执行带断路器保护的操作
     * @param operation 要执行的操作
     */
    public func call(operation: () -> T): T

    public func getCircuitBreaker(): CircuitBreaker<T>
}
```

### 使用示例

```cangjie
// 基础使用
let breaker = CircuitBreakerFactory.createDefault<String>()
let result = breaker.execute { () =>
    riskyOperation()
}

// 查看状态
match (breaker.getState()) {
    case CircuitBreakerState.Closed => println("正常工作")
    case CircuitBreakerState.Open => println("快速失败中")
    case CircuitBreakerState.HalfOpen => println("尝试恢复")
}

// 使用装饰器模式（带降级）
let decorator = CircuitBreakerDecorator<String>(
    breaker,
    Some({ () => "default_value" })
)
let result = decorator.call({ () => riskyOperation() })

// 手动控制
breaker.trip()   // 手动打开
breaker.reset()  // 手动重置
```

---

## 10. 背压控制

背压控制提供流量控制和过载保护机制。

### 包引入

```cangjie
import cactor_patterns.backpressure.{
    BackpressureStrategy, BackpressureConfig, BackpressureStats,
    BasicBackpressureController, BackpressureControllerFactory,
    FlowController, BackpressureException
}
```

### 背压策略

```cangjie
public enum BackpressureStrategy <: ToString {
    | Drop       // 丢弃新消息
    | Block      // 阻塞发送者
    | Buffer     // 缓冲消息
    | Reject     // 拒绝消息（抛异常）
    | Throttle   // 限流（延迟发送）

    public func toString(): String
}
```

### 背压配置

```cangjie
public struct BackpressureConfig {
    public let strategy: BackpressureStrategy     // 背压策略
    public let maxBufferSize: Int64               // 最大缓冲大小
    public let maxThroughput: Int64                // 每秒最大消息数
    public let windowSize: Duration                // 时间窗口大小
    public let dropThreshold: Float64              // 丢弃阈值 (0.0-1.0)
    public let blockTimeout: Duration              // 阻塞超时时间
}
```

### FlowController 接口

```cangjie
public interface FlowController {
    /**
     * 尝试发送消息
     * @return true 表示成功，false 表示被背压策略处理
     */
    func trySend(message: Message): Bool

    /**
     * 获取当前统计信息
     */
    func getStats(): BackpressureStats

    /**
     * 重置统计信息
     */
    func reset(): Unit

    /**
     * 检查是否过载
     */
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

### BackpressureControllerFactory

```cangjie
public struct BackpressureControllerFactory {
    /**
     * 创建丢弃策略控制器
     */
    public static func createDropController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController

    /**
     * 创建阻塞策略控制器
     */
    public static func createBlockController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController

    /**
     * 创建限流策略控制器
     */
    public static func createThrottleController(
        maxBufferSize: Int64,
        maxThroughput: Int64
    ): BasicBackpressureController
}
```

### 使用示例

```cangjie
// 创建控制器
let controller = BackpressureControllerFactory.createDropController(
    10000,   // 最大缓冲
    1000     // 每秒最大1000条
)

// 尝试发送消息
let success = controller.trySend(message)
if (!success) {
    println("消息被背压策略处理")
}

// 检查状态
let stats = controller.getStats()
println("丢弃率: ${stats.getDropRate()}")

// 检查是否过载
if (controller.isOverloaded()) {
    println("系统过载")
}

// 重置统计
controller.reset()
```

---

## 11. 路由模式

路由模式提供消息负载均衡功能，支持多种路由策略。

### 包引入

```cangjie
import cactor_patterns.routing.{
    Router, BasicRouter, RouterFactory,
    RoutingStrategy, RoundRobinRoutingStrategy, RandomRoutingStrategy,
    SmallestMailboxRoutingStrategy, BroadcastRoutingStrategy,
    ConsistentHashRoutingStrategy, RouterActorRef,
    RoutingConfig, RoutingStatistics, ResizePolicy
}
```

### RoutingStrategy 接口

```cangjie
public interface RoutingStrategy {
    /**
     * 选择目标 Actor
     * @param routees 可用的 Actor 列表
     * @param message 要路由的消息
     * @return 选中的 Actor
     */
    func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>
}
```

### 路由策略实现

#### RoundRobinRoutingStrategy（轮询）

```cangjie
public class RoundRobinRoutingStrategy <: RoutingStrategy {
    public init()
    public func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>
}
```

#### RandomRoutingStrategy（随机）

```cangjie
public class RandomRoutingStrategy <: RoutingStrategy {
    public init()
    public func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>
}
```

#### SmallestMailboxRoutingStrategy（最小邮箱）

```cangjie
public class SmallestMailboxRoutingStrategy <: RoutingStrategy {
    public init()
    public func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>
}
```

#### BroadcastRoutingStrategy（广播）

```cangjie
public class BroadcastRoutingStrategy <: RoutingStrategy {
    public init()

    // 返回 None，使用 broadcast 方法发送
    public func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>

    // 广播消息到所有 Actor
    public func broadcast(routees: Array<ActorRef>, message: Message): Unit
}
```

#### ConsistentHashRoutingStrategy（一致性哈希）

```cangjie
public class ConsistentHashRoutingStrategy <: RoutingStrategy {
    public init()
    public func selectRoutee(
        routees: Array<ActorRef>,
        message: Message
    ): Option<ActorRef>
}
```

### Router 接口

```cangjie
public interface Router {
    /**
     * 路由消息
     */
    func route(message: Message): Unit

    /**
     * 添加路由目标
     */
    func addRoutee(routee: ActorRef): Unit

    /**
     * 移除路由目标
     */
    func removeRoutee(routee: ActorRef): Unit

    /**
     * 获取所有路由目标
     */
    func getRoutees(): Array<ActorRef>

    /**
     * 获取路由策略
     */
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

将路由器包装成 ActorRef：

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

### 辅助类型

```cangjie
public enum ResizePolicy {
    | Fixed     // 固定大小
    | Dynamic   // 动态调整
}

public struct RoutingStatistics {
    public let totalMessages: Int64
    public let routeeCount: Int32
    public let averageLatency: Float64
}

public struct RoutingConfig {
    public let strategy: RoutingStrategy
    public let routeeCount: Int32
    public let resizePolicy: ResizePolicy
}
```

### 使用示例

```cangjie
// 创建路由器
let router = RouterFactory.roundRobin()

// 添加路由目标
router.addRoutee(actor1)
router.addRoutee(actor2)
router.addRoutee(actor3)

// 使用路由器发送消息
router.route(StringMessage("Hello"))

// 包装为 ActorRef
let routerRef = RouterActorRef(router, "my-router")

// 通过 ActorRef 发送消息
routerRef.tell(StringMessage("Hello via router"))

// 广播消息
let broadcastRouter = RouterFactory.broadcast()
broadcastRouter.addRoutee(actor1)
broadcastRouter.addRoutee(actor2)
broadcastRouter.addRoutee(actor3)
broadcastRouter.route(StringMessage("Broadcast message"))
```

---

## 12. 测试框架

测试框架提供 Actor 系统的测试支持，包括虚拟时间和消息探针。

### 包引入

```cangjie
import cactor_integration.testing.{
    TestActorSystem, TestProbe, TestTimeController,
    TestExpectation
}
```

### TestActorSystem

测试专用的 Actor 系统：

```cangjie
public class TestActorSystem {
    public init(name: String)

    /**
     * 创建测试 Actor
     */
    public func actorOf(factory: () -> Actor, name: String): ActorRef

    /**
     * 创建测试探针
     */
    public func createTestProbe(): TestProbe

    /**
     * 获取时间控制器
     */
    public func getTimeController(): TestTimeController

    /**
     * 终止测试系统
     */
    public func terminate(): Unit

    public func getName(): String
    public func isTerminated(): Bool
    public func getUnderlyingSystem(): CActorSystem
}
```

### TestProbe

测试探针，用于验证消息接收：

```cangjie
public class TestProbe {
    /**
     * 创建测试探针
     */
    public static func create(testSystem: TestActorSystem): TestProbe

    /**
     * 期望接收特定消息
     * @param expectedMessage 期望的消息
     * @param timeout 超时时间
     * @return 期望结果
     */
    public func expectMessage(
        expectedMessage: Message,
        timeout: Duration
    ): TestExpectation<Message>

    /**
     * 期望在指定时间内不接收任何消息
     */
    public func expectNoMessage(duration: Duration): TestExpectation<Unit>

    /**
     * 期望接收任意消息
     */
    public func expectAnyMessage(timeout: Duration): TestExpectation<Message>

    /**
     * 获取探针的 ActorRef
     */
    public func getRef(): ActorRef

    /**
     * 获取已接收的消息数量
     */
    public func getReceivedMessageCount(): Int32

    /**
     * 清理资源
     */
    public func cleanup(): Unit
}
```

### TestTimeController

虚拟时间控制器：

```cangjie
public class TestTimeController {
    public init()

    /**
     * 获取当前虚拟时间（纳秒）
     */
    public func getCurrentTime(): Int64

    /**
     * 推进虚拟时间
     */
    public func advanceTime(duration: Duration): Unit

    /**
     * 重置时间
     */
    public func reset(): Unit
}
```

### TestExpectation

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

### 测试示例

```cangjie
// 创建测试系统
let testSystem = TestActorSystem("TestSystem")

// 创建被测试的 Actor
let actor = testSystem.actorOf({ => MyActor() }, "test-actor")

// 创建探针
let probe = testSystem.createTestProbe()

// 发送消息
actor.tell(StringMessage("test"), Some(probe.getRef().path.toString()))

// 期望接收消息
let result = probe.expectMessage(StringMessage("response"), Duration.second * 1)
match (result) {
    case TestExpectation.Success(msg) => println("测试通过")
    case TestExpectation.Timeout => println("超时")
    case TestExpectation.UnexpectedMessage(msg) =>
        println("收到意外消息: ${msg.messageType()}")
    case TestExpectation.Error(ex) =>
        println("错误: ${ex.message}")
}

// 期望不接收消息
let noMsg = probe.expectNoMessage(Duration.millisecond * 100)
if (noMsg.isSuccess()) {
    println("正确：没有收到消息")
}

// 使用虚拟时间
let timeController = testSystem.getTimeController()
timeController.advanceTime(Duration.second * 5)

// 清理
probe.cleanup()
testSystem.terminate()
```

---

## 13. 类型安全 Actor

### 包引入

```cangjie
import cactor_core.typed.{TypedBehavior, TypedActorRef, TypedActorSelection}
import cactor.api.{CActorSystem}
```

### TypedBehavior<T>

类型安全的 Actor 行为定义：

```cangjie
public interface TypedBehavior<T> {
    // 定义类型安全 Actor 的行为
}
```

### TypedActorRef<T>

类型安全的 Actor 引用：

```cangjie
public interface TypedActorRef<T> {
    /**
     * 发送类型安全的消息
     */
    func tell(message: T): Unit

    /**
     * 发送类型安全的消息（带发送者）
     */
    func tell(message: T, sender: ActorRef): Unit

    /**
     * 获取底层 ActorRef
     */
    func getUnderlyingActorRef(): ActorRef
}
```

### TypedActorSelection<T>

类型安全的 Actor 选择：

```cangjie
public interface TypedActorSelection<T> {
    /**
     * 解析选择器
     */
    func resolveOne(timeout: Duration): Option<TypedActorRef<T>>

    /**
     * 向选中的 Actor 发送消息
     */
    func tell(message: T): Unit
}
```

### TypedBehavior 使用示例

```cangjie
// 定义协议
class Ping {}
class Pong { public let from: String }

// 创建类型安全行为
let behavior = TypedBehavior.create<Ping>(handler)

// 创建类型安全 Actor
let typedActor = system.typedActorOf(behavior, "typed-actor")

// 发送类型安全消息
typedActor.tell(Ping())
typedActor.tell(Pong("hello"))

// 类型安全选择
let selection = system.typedActorSelection<Ping>("/user/*")
selection.tell(Ping())

// 类型转换
let normalRef = system.unwrapTypedActor(typedActor)
let typedRef = system.wrapAsTypedActor<Ping>(normalRef, "wrapped")
```

---

## 附录

### A. 完整的 Hello World 示例

```cangjie
import cactor.api.{CActor, CActorSystem}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

class EchoActor <: Actor {
    public prop name: String {
        get() { "EchoActor" }
    }

    public prop description: String {
        get() { "回显消息的 Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                println("收到: ${msg.content}")
                // 回复发送者
                context.sender().map { sender =>
                    sender.tell(StringMessage("Echo: ${msg.content}"))
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    // 创建系统
    let system = CActor.system("HelloWorld")

    // 创建 Actor
    let echo = system.actorOf({ => EchoActor() }, "echo")

    // 发送消息
    echo.tell(StringMessage("Hello, CActor!"))

    // 等待处理
    std.thread.sleep(Duration.second * 1)

    // 终止系统
    system.terminate()

    0
}
```

### B. 配置预设速查表

| 预设配置 | 适用场景 | 邮箱类型 | 调度器 | 监督策略 |
|---------|---------|---------|--------|---------|
| `createDefault()` | 通用 | 无界 | 线程池 | 最多重试3次 |
| `createHighPerformance()` | 高吞吐 | 有界(8K) | 工作窃取 | 最多重试3次 |
| `createBatching()` | 批处理 | 有界(4K) | 线程池 | 最多重试3次 |
| `createCritical()` | 关键任务 | 有界(1K) | 专用线程 | 最多重试1次 |

### C. 异常类型速查

| 异常类型 | 包路径 | 说明 |
|---------|--------|------|
| `CircuitBreakerOpenException` | `cactor_foundation.circuit_breaker` | 断路器开启时抛出 |
| `BackpressureException` | `cactor_patterns.backpressure` | 背压拒绝时抛出 |
| `TypeCastException` | `cactor_patterns.ask` | Ask 模式类型转换失败 |

### D. 迁移指南（从 1.x 到 1.2）

主要变更：

1. **统一入口**：`ActorSystem` → `CActor.system()`
2. **Props 创建**：`Props.create()` → `PropsFactory.create()` 或 `CActor.props()`
3. **类型安全 Actor**：`ActorRef<T>` → `TypedActorRef<T>`
4. **背压 API**：`FlowControl` → `FlowController`

---

**文档版本**: CActor 1.2
**最后更新**: 2026-10-28
