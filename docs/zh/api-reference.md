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
14. [远程可靠投递与持久化失败语义](#14-远程可靠投递与持久化失败语义)

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

/**
 * 停止 Actor（对标 Akka system.stop）：处理完当前消息即停，邮箱中剩余消息计为死信，子 Actor 先停；
 * 名字保留到真正终止（终止前同名 actorOf 报"名称已存在"）。顶层与子 Actor 都适用
 */
public func stop(actorRef: ActorRef): Unit

/**
 * 处理完已入队的消息后停止，并等待终止（对标 Akka Patterns.gracefulStop，发 PoisonPill）：
 * 返回 true 表示已终止且名字已释放，超时返回 false。会阻塞，只能在 Actor 之外调用
 */
public func gracefulStop(actorRef: ActorRef, timeout: Duration): Bool

/** 死信：投递失败的消息，以及包装成 UnhandledMessage 的未处理消息（用 `is UnhandledMessage` 区分） */
public func subscribeDeadLetters(listener: (Message, String) -> Unit): Unit
public func deadLetterCount(): Int64
public func unhandledCount(): Int64
```

与 Akka 的逐条语义对照见 [akka-semantics.md](akka-semantics.md)。

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
     * 重启前在旧实例上调用。默认调用 postStop()（与 Akka 相同）。
     * 之后运行时停止全部子 Actor，等它们全部终止后才创建新实例
     * @param reason 重启原因（异常）
     */
    func preRestart(reason: Exception): Unit

    /**
     * 重启后在新实例上调用（onStarted 之前）。默认调用 preStart()（与 Akka 相同）
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

### 推荐入口（E38 收敛后只有三个）

| 场景 | 写法 |
|---|---|
| 类型化，Actor 之外 | `TypedAsk.ask<Req, Res>(ref, { replyTo => Req(.., replyTo) }, timeout)` → `TypedPromise<Res>` |
| 类型化，Actor 之内 | `ctx.ask(target, factory, timeout) { outcome => ... }`（结果作为消息回到本 Actor，不阻塞） |
| 经典 `ActorRef` | `AskInbox.ask(target, message).get(timeout)`（`cactor_core.dsl`；收件箱就是发送方，目标照常 `context.reply`） |

```cangjie
import cactor_core.dsl.{AskInbox}

let answer = AskInbox.ask(service, Query("天气")).get(3.seconds)       // 超时抛 TypedAskTimeoutException
let maybe = AskInbox.ask(service, Query("天气")).tryGet(100.millis)    // 超时返回 None
```

阻塞等待（`get` / `tryGet` / `TypedPromise.get`）只能在 Actor 之外；在默认调度器的工作线程上调用会立即抛
`IllegalStateException`。`TypedPromise` 可非阻塞组合：`map` / `flatMap` / `recover` / `recoverWith`，
`TypedPromises.successful` / `failed` / `sequence`。

### 旧入口（@Deprecated，2.0.0 移除）

下面的 `cactor_patterns.ask.Ask` / `AskPatternManager` 要求目标 Actor 解包 `AskMessage`、回复 `AskResponse`，
自 1.3.0 起标记 @Deprecated；恒以超时结束的 `TypedAskPattern` 与 patterns 的 `TypedAsk` 已删除。

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
    public let failureThreshold: Int32   // 触发断路的**连续**失败次数
    public let successThreshold: Int32   // 半开状态下恢复所需的试探成功次数
    public let timeout: Duration         // callTimeout：调用耗时超过它按失败计；为 0 时不检查
    public let resetTimeout: Duration    // 从 Open 转换到 HalfOpen 的等待时间
}
```

### 状态机（与 Akka CircuitBreaker 一致）

- **Closed**：计连续失败，任何一次成功清零；达到 `failureThreshold` 转 Open。
- **Open**：调用直接抛 `CircuitBreakerOpenException`；开启满 `resetTimeout` 后 `getState()` 即返回 HalfOpen。
- **HalfOpen**：同一时刻只放行一个试探调用，其余快速失败；试探成功累计到 `successThreshold` 转 Closed，
  试探失败立即回到 Open 并重新计时。
- 调用耗时超过 `timeout` 时抛 `CircuitBreakerTimeoutException` 并计一次失败（结果被丢弃，同 Akka `withSyncCircuitBreaker`）。

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
    // clock 返回单调毫秒数；测试注入虚拟时钟即可确定性地驱动 Open → HalfOpen
    public init(config: CircuitBreakerConfig, clock: () -> Int64)

    public func execute(operation: () -> T): T
    public func getState(): CircuitBreakerState
    public func getStats(): CircuitBreakerStats   // failureCount / successCount 为累计次数
    public func reset(): Unit
    public func trip(): Unit
    public func succeed(): Unit   // 手动记一次成功（结果不经 execute 时使用）
    public func fail(): Unit      // 手动记一次失败；HalfOpen 时立即回到 Open
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

**推荐用法：路由器 Actor**（对标 Akka `Routers.pool` / `Routers.group`）。路由器本身是 Actor：
转发时保留原发送方（目标 `context.reply` 直接回到原发送方）；watch 全部目标，目标终止后摘除，
池路由器的目标全部终止后自行停止。

```cangjie
import cactor_patterns.routing.*

// 池：创建 4 个 Worker 子 Actor，按用户 id 一致性哈希
let workers = system.actorOf(Routers.pool(4, { => Worker() },
    ConsistentHashRoutingStrategy({ m: Message => userIdOf(m) })), "workers")
workers.tell(Job(1), self)           // Worker 里 context.reply(...) 直接回到 self
workers << Broadcast(Reload())       // 发给全部 Worker
workers << AdjustPoolSize(2)         // 扩容 2 个；负数缩容
workers.tell(GetRoutees(), self)     // 回复 Routees(当前目标)

// 组：目标是已存在的 Actor（路由器只 watch，不管理其生命周期）
let group = system.actorOf(Routers.group([a, b, c]), "group")
```

| 工厂 | 说明 |
|---|---|
| `Routers.pool(n, props)` / `pool(n, props, strategy)` / `pool(n, creator, strategy)` | 池路由器，默认轮询 |
| `Routers.group(refs)` / `group(refs, strategy)` | 组路由器，默认轮询 |

管理消息：`Broadcast(message)`、`GetRoutees` → `Routees`、`AddRoutee(ref)`、`RemoveRoutee(ref)`（池目标一并停止）、
`AdjustPoolSize(change)`。没有目标时消息记为未处理。

**类型化路由器**（`cactor_core.typed`，对标 Akka Typed `Routers`）：路由器就是一个 `TypedBehavior<T>`。

```cangjie
let pool = TypedRouters.pool<Job>(4, worker(),
    TypedRouting<Job>().consistentHashing({ j => j.userId }).withBroadcastPredicate({ j => j.isReload }))
let workers = ctx.actorOf(pool, "workers")

// 组：路由到 Receptionist 中登记为 key 的全部实例；实例停止后自动注销、立即生效；路由器停止时退订
let group = ctx.actorOf(TypedRouters.group<Job>(WorkerKey), "worker-group")
```

`TypedRouting<T>` 是不可变配置：`roundRobin()`（默认）、`random()`、`consistentHashing(keyOf)`、
`withBroadcastPredicate(p)`、`withRouteeProps(props)`。

下面的 `BasicRouter` / `RouterActorRef` 是不经过 Actor 的路由对象：不 watch 目标，适合目标生命周期由调用方自行管理的场景。

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
    public func tell(message: Message, sender: ActorRef): Unit   // 保留发送方转发
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

测试工具统一位于 `cactor_testkit`，原 `cactor_integration.testing` 已删除。

```cangjie
import cactor_testkit.{ActorTestKit, TestProbe, BehaviorTestKit, TestInbox, ManualTime}
```

### 真实运行时：ActorTestKit 与 TestProbe

`ActorTestKit(name)` 创建真实 Actor 系统，提供 `actorOf`、`spawnTyped`、`createTestProbe` 和 `shutdown`。测试结束时必须调用 `shutdown()`。

```cangjie
let kit = ActorTestKit("example-test")
try {
    let target = kit.actorOf({ => MyActor() }, "target")
    let probe = kit.createTestProbe("reply")
    probe.send(target, StringMessage("test"))
    let reply = probe.expectMsgType<StringMessage>(Duration.second * 3)
    @Expect(reply.getContent(), "response")
    probe.expectNoMsg(Duration.millisecond * 100)
} finally {
    kit.shutdown()
}
```

`TestProbe.expectMsg(timeout)` 返回消息，`expectMsgType<T>(timeout)` 同时检查类型；超时或类型不符抛 `TestKitAssertionException`。`reply` 向最近一条消息的 sender 回复，`receiveN(n, timeout)` 等待多条消息。等待运行时条件使用 `awaitCondition` / `awaitAssert`，避免固定 sleep。

### 同步行为：BehaviorTestKit 与 ManualTime

`BehaviorTestKit<T>(behavior)` 用 `run(message)` 推进一个回合，`retrieveAllEffects()` 检查 spawn、watch、停止等效果；`childInbox<U>(name)` 与 `TestInbox<T>` 检查类型化消息。`kit.manualTime()` 或 `kit.timePasses(duration)` 推进虚拟时钟，随后 `runAll()` 处理已排入自身 inbox 的消息。**ManualTime 只驱动同步行为测试，不驱动真实 Actor 系统的调度器**（E52 尚未完成）。

兼容的 `TestActorRefFactory` / `TestActorContext` 用于同步经典 Actor 测试。其 watch、行为切换、定时器与选择操作会明确抛异常，不模拟成功；需要这些能力时使用真实运行时或行为测试工具。

### 探针的终止断言与日志断言（E62）

`TestProbe.watch` / `expectTerminated` 在探针自己的 Actor 线程里建立 DeathWatch（`watch` 只能在 Actor 线程调用，探针用内部的控制消息转交），适合断言"某个 Actor 真的停了"，不用 sleep：

```cangjie
let probe = kit.createTestProbe("watcher")
probe.watch(target)                       // 也可以直接调 expectTerminated，它会先 watch
target.tell(PoisonPill())
let t = probe.expectTerminated(target, Duration.second * 5)   // 返回 Terminated
@Expect(t.actor.equals(target), true)
```

对已经停止的目标立即返回（watch 已死目标会立刻投递 `Terminated`）；目标还活着则在超时后抛 `TestKitAssertionException`（消息含"没有停止"），不会静默通过，也不会把 `Terminated` 之外的消息吃掉。

日志断言用 `LoggingTestKit`：临时把全局 sink 换成内存 sink 并调低级别，`close()` 还原（幂等）：

```cangjie
let logs = LoggingTestKit.start(LogLevel.Debug)
try {
    doSomething()
    let r = logs.expectMessage(LogLevel.Info, "分配了 3 个分片", Duration.second * 2)  // 返回 LogRecord
    logs.expectMessage(LogLevel.Debug, "发心跳")            // 默认 3 s
    logs.expectNoMessage(LogLevel.Error, "OOM", Duration.millisecond * 200)
    logs.clear()                                          // 之后只看新记录
} finally {
    logs.close()
}
```

断言只匹配安装或 `clear()` 之后产生的新记录；失败时错误消息里带最近 5 条日志，省掉在 CI 里翻全量输出。级别按 `ordinal()` 比较（Trace 最小、Off 最大），低于阈值的记录不会进入 sink。

### 故障注入：`BlackholeTransport`（`cactor_testkit/network_harness.cj`）

模拟"对端完全收不到包"（黑洞），用于验证故障检测、隔离、恢复。它包装任意 `NetworkTransport`，命中黑名单的地址在 `send` / `sendPriority` 时直接丢帧（普通与优先通道都丢），其余调用全部转发：

```cangjie
import cactor_testkit.*
import cactor_foundation.network.*

let inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
let net = BlackholeTransport(inner)
// 双向黑洞：a 发往 b、b 发往 a 的帧全部丢弃
net.blackhole(aAddr)
net.blackhole(bAddr)
let dropped = net.droppedCount()      // 累计丢弃帧数
net.heal(bAddr)                       // 恢复单个地址
net.healAll()                         // 恢复全部
if (net.isBlackholed(aAddr)) { /* ... */ }
```

### 跨进程同步：`ProcessBarrier`

`ClusterBarrier`（进程内 Mutex 计数）只能同步线程；要在**多个操作系统进程**之间卡住同一步，用 `ProcessBarrier`——共享目录里的 `<name>.ready` 文件就是会合点，到齐后写 `go`：

```cangjie
import cactor_testkit.*

let dir = ProcessBarrier.tempDirectory("chaos")       // /tmp/cactor-barrier-chaos-<随机数>
let barrier = ProcessBarrier(dir, timeout: 30.seconds)
// 每个进程到这一步就登记并等待，全部到齐后一起继续
let ok = barrier.arrive("node-1", expected: 3)        // 自己登记 + 等 3 个
barrier.parties()                                     // ["node-1","node-2","node-3"]，缺谁一眼看到
barrier.isOpen()                                      // go 标记是否已写
barrier.reset()                                       // 同目录复用下一轮
```

shell 侧可以 `touch <dir>/node-2.ready` 直接参与同一个 barrier（协议写在类头注释里），所以脚本与测试可以混用。选了文件而不是 socket：崩溃不会留下半开连接，也不需要谁先启动。

### 等待的写法：`awaitGossipRounds`

负向断言（"迟到的删除没有生效""伪造成员没被加入"）以前靠 `sleep(500.millis)`，高负载下消息可能还没传播完就断言，于是偶发变红。改成等 gossip 真的推进了若干轮：

```cangjie
// 两个节点各自再跑 3 轮 gossip，再断言"什么都没变"
@Expect(awaitGossipRounds([nodes[0].cluster, nodes[1].cluster], 3, CLUSTER_WAIT), true)
@Expect(nodes[0].cluster.membershipStats()[1], 0)
```

只在**没有任何可观察推进量**的场景（三方完全隔离）才退回按 gossip 间隔等轮数，并在用例里写清原因。

---

## 13. 类型安全 Actor

### 持久化 reminder（v2.30）

`cactor_persistence.reminders` 提供 `DurableReminders.behavior(persistenceId, store, deliver, pollInterval: ..., nowMillis: ...)`。它复用异步 `DurableStateBehavior`：启动先恢复，登记、替换、撤销、确认都在存储成功后回复；默认按 UTC 毫秒和一秒轮询，测试可注入时钟。`deliver` 应快速 tell 到业务 Actor，不能阻塞或做长 IO；重启时重新注入当前路由，不持久化 ActorRef 或闭包。

```cangjie
// business 是本地业务 Actor 的 TypedActorRef<ReminderDelivery>。
let reminders = typedSystem.actorOf(DurableReminders.behavior(
    PersistenceId.of("Reminder", "tenant-1"), FileDurableStateStore("/data/reminders"),
    { delivery => business.tell(delivery) }), "reminders")
let registered = TypedAsk.ask<ReminderCommand, Int64>(reminders, { replyTo =>
    ReminderCommand.Register(ReminderDefinition("daily", "stable-business-key", dueAtMillis,
        periodMillis: 86400000), replyTo)
}, Duration.second * 5)
```

| 命令 | 回复及行为 |
|---|---|
| `Register(definition, replyTo)` | 落盘后回复递增的 generation；同名替换产生新代次 |
| `Remove(name, replyTo)` | 落盘后 true；不存在返回 false |
| `Get(replyTo)` | 当前提醒数组的副本 |
| `Acknowledge(name, generation, dueAtMillis, replyTo)` | 匹配当前代次和到期时间且已到期时才接受；落盘后 true，过期/重复确认 false |
| `Tick` | 检查到期提醒；生产由行为内部定时器驱动 |

业务提交副作用后再发送确认。一次性提醒（periodMillis 为 0）确认后删除；周期提醒确认后推进到原周期序列中严格晚于当前时间的下一时刻，错过的周期合并。确认前每轮轮询都可重发，进程重启后仍重发；`(PersistenceId, name, generation, dueAtMillis)` 可作为业务幂等键。取消或替换不撤回已投递消息，旧代次确认不能删除新登记；同一代次不同到期时间的旧确认不能推进新周期。投递与业务副作用不构成原子事务，语义是至少一次，业务必须去重。

每个 PersistenceId 必须有唯一活跃写者。文件存储不提供跨进程锁、复制、选主或自动故障转移；集群部署须由外部所有权协议保障单写者，并在新宿主提供可访问的持久存储和新路由。此接口按表扫描并整体写入状态，适合有限的每实体提醒，不是无限规模的全局调度器。跨节点投递需应用提供可编解码协议与路由；内部含回复引用的命令不能直接当作通用网络协议。墙钟回拨会推迟触发；进程离线时不会执行任务。轮询间隔限制重发频率，但不提供下游背压；慢业务需要有界邮箱或有界去重队列。

`deliver` 抛异常会沿现有持久化行为失败路径终止当前实例；需要自动恢复时，将该行为包在监督退避策略中，并确保恢复后仍为唯一写者。至少一次投递以实例持续运行或最终恢复为前提，长时间离线不会执行任务。

三进程恢复示例：在根目录构建后运行 `bash tools/reminder_demo.sh`，独立执行登记、恢复投递并确认、再次恢复验证已删除。


### 异步退避重试（v2.29）

从 `cactor_core.typed` 导入 `TypedRetry`，从 `cactor_core.supervision` 导入 `BackoffPolicy`。例如，假设 `remote` 是已发现的类型化服务引用，重试同一个幂等读请求：

```cangjie
let token = AskCancellationToken()
let pending = TypedRetry.run<StringMessage>({ attempt =>
    TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage("read"),
        Duration.second, cancellation: Some(token))
}, 3, BackoffPolicy(Duration.millisecond * 100, Duration.second, randomFactor: 0.2),
    cancellation: Some(token),
    shouldRetry: { error => (error as TypedAskTimeoutException).isSome() })
```

`maxAttempts` 必须为正并包含首次调用；`attempt` 从 1 计。操作须迅速返回 Promise，不能在操作工厂中阻塞 `get`。仅失败才重试，前次完成后才调度下一次；默认筛选所有异常，但取消异常始终终止，次数耗尽保留末次失败。筛选器或调度器抛异常时直接结束。`BackoffPolicy` 仅提供延迟、封顶及抖动，`maxRestarts` / `resetBackoffAfter` 不适用于单次请求，次数由 `maxAttempts` 决定。与监督策略一致，抖动向上附加，最终延迟可略超 `maxBackoff`。

生产默认用 Timer；测试可用 `schedule: { delay, action => time.schedule(delay, action) }` 注入 ManualTime。取消会清理等待中的重试任务；要结束正在等待的 ask，应把同一令牌同时传给 ask 和 retry。本接口不提供总 deadline，每次操作须自行保证超时；一个永不完成的 Promise 不会触发重试。令牌取消可能与工厂执行竞争，不保证撤回已经开始的操作。

网络超时可能发生在远端执行之后，重试不提供 exactly-once。写请求需沿每次尝试复用稳定幂等键并在业务侧去重；不要每次重试生成新事务标识。只读请求或幂等命令适合默认重试组合。

### 可取消 ask（v2.28）

本地与跨节点 ask 都接受可选的 `cancellation: Some(token)`：

```cangjie
let token = AskCancellationToken()
let pending = TypedAsk.askRemote<StringMessage, StringMessage>(
    remote, StringMessage("ping"), 3.seconds, cancellation: Some(token))
// 用户取消会话时：
token.cancel()
// pending 以 TypedAskCancelledException 失败，可在 onComplete / recover 中处理。
```

本地调用同样支持 `TypedAsk.ask<Cmd, Reply>(ref, factory, timeout, cancellation: Some(token))`。一个令牌可控制多条请求，取消不可逆且幂等；`isCancellationRequested()` 查询取消状态。预取消跳过工厂和发送；工厂内取消也跳过发送。并发取消与实际投递之间仍有竞态，已发送的请求可能继续在目标节点执行。取消结束调用方等待，不撤回请求、不回滚远端副作用、不终止目标；需要停止业务操作时应在协议中另设取消命令。

回复、超时、发送失败与取消以最先完成 Promise 者为准；后续回复不能覆盖结果。完成时取消超时定时器并解除令牌订阅；远程一次性回复引用随 Promise 完成变为 terminated，沿既有维护周期清理回复入口。`map` / `recover` 派生结果不提供反向取消，应将令牌传入原始 ask。


### 分布式协议期望（v2.27）

为跨节点服务选择稳定、版本化的协议名，在两端创建 Actor 时设置 `TypedProps().withProtocolTypeName("app.Worker.v1")`，服务 key 使用 `ServiceKey<WorkerMsg>("workers", protocolTypeName: Some("app.Worker.v1"))`。`registerTyped` 要求被登记引用声明相同名称，`findTyped` / `findAllTyped` 将名称放入发现引用；服务身份仍由 key.id 决定。普通 enum 使用既有 `TypedPayloadCodecs.register<T>`，也可通过 `declareTypeName<T>` 声明诊断名称。

目标声明期望时，接收端比较信封标签或可推导的载荷类型名：明确错配计入 Unhandled 并限速告警；无法推导名称则计入 `TypedTypeExpectations` 的 unverified，继续由原有 `as T` 检查载荷。名称不是鉴权或 schema 验证。顶层与子 Actor 引用均保留期望；未声明期望的默认路径不增加注册表查询。

`TypedPayloadBoxCodec` v3 新增 kind 3，保存 Message 的显式协议名；新端继续读取 v1/v2，旧端不能读取 kind 3。使用该功能前须升级通信两端。enum 仍需两端注册兼容 codec；协议名相同不代表字节格式兼容。


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

### 类型化语义要点（与 Akka Typed 对齐）

- **监督按行为结构匹配**：`supervise(b).onFailure(...)` 包住的行为在处理中换成新行为后仍在这一层内；
  嵌套时**由内向外**匹配，内层规则优先；`withLimit` / 退避的计数按规则在行为中的位置累计，跨重启保留
  （`setup { supervise(...).withLimit(n, within) }` 的上限照常生效）。`transformMessages` 内部的 supervise 同样生效。
- **未处理消息**：处理方法返回 `unhandled()`、或收到协议之外的消息（`childAs` / lookup 用错了协议类型）时，
  消息以 `UnhandledMessage(message, recipient)` 交给 `subscribeDeadLetters` 的订阅者，`unhandledCount()` 计数，
  并按限速记日志（每 5 分钟前 10 条，`CACTOR_LOG_DEAD_LETTERS` 调整）。
- **子 Actor 的快路径**：`ctx.actorOf` 是接口虚调用，`@Frozen` 无法在调用方包里实例化；子 Actor 的初始行为以
  `setup { ... }`（或 `supervise(setup { ... })`）开头时，由 `setup` 在调用方包里构造热路径桥接，
  吞吐与顶层 `typedActorOf` 接近（`typed_spawn_bench` 实测约 84%，直接传 `receive` 约 45%）。



---

## 14. 远程可靠投递与持久化失败语义

与 Akka 的逐条对照见 [akka-semantics.md](akka-semantics.md) 第 9、10、11 节。

### 远程（`cactor_remote`）

```cangjie
import cactor_remote.*

let node = SimpleRemoteManager(NetworkAddress("127.0.0.1", 2552), TransportProtocol.TCP)
// 心跳 200ms、φ 阈值 8、容忍停顿 1s、重发 200ms、确认 10ms；可靠投递上限与放弃时长另设
node.configure(RemotingSettings.defaults().withLimits(5000, Duration.minute * 10))
node.start()

let sink = node.remoteActorOf(peerAddress, "sink")
node.tellReliable(sink, StringMessage("order-1"))   // 按序、去重；对端重启后在新通道重发
node.watchRemote(sink, localWatcher)                // 目标停止、节点不可达或重启时收到 Terminated

let stats = node.reliableDeliveryStats()
// stats.resent / duplicatesDropped / unacked / overflowDropped / gaveUp / peerRestarts
```

| API | 说明 |
|---|---|
| `RemotingSettings(heartbeat, phi, pause, resend, ack[, maxUnacked, giveUpAfter])` | 五参构造的上限与放弃时长取默认值 20000 条、6 小时（同 Akka） |
| `settings.withLimits(maxUnacked, giveUpAfter)` | 只换可靠投递的上限与放弃时长 |
| `settings.withAskLimit(limit)` | 设置经典/类型化远程 ask 共用的并发上限，默认 1024，必须为正；其他配置构建方法保留该值 |
| `node.pendingAskCount()` | 经典 `node.ask` 当前在途数量 |
| `node.pendingTypedAskCount()` | 类型化远程 ask 当前在途数量；与经典数量之和受 `maxPendingAsks` 限制 |
| `node.replyEntryCount()` | 普通发送方回复入口与类型化临时入口总数；类型化请求终态立即注销 |
| `node.ask(target, message, timeout)` | 正超时必需；满额以 `RemoteAskCapacityException` 完成失败而不发送；未启动/已停止以 `IllegalStateException` 完成失败 |
| `tellReliable(target, message[, sender])` | 通道已有 `maxUnacked` 条未确认消息时丢弃这条并计入 `overflowDropped`，不抛异常 |
| `reliableDeliveryStats(): ReliableDeliveryStats` | 完整统计；旧的 `reliableStats()` 三元组保留 |

- `TypedAsk.askRemote` 经 `SimpleRemoteManager` 的 `RemoteActorRef` 发送时，也受容量与停止管理；保留原 sender 的本地代理转发同样接入。成功、超时、取消及发送失败都即时注销 `$typedask-<启动标识>-<序号>`，`stop()` 立即完成在途失败，不等原超时。`PromiseActorRef.fail(cause)/onComplete(callback)` 供远程层注入终态处理。自定义 ActorRef、本地 fallback、手工可靠转发不会自动获得同一管理器生命周期；两端仍需提供实际载荷编解码。`askRemote` 的非正超时直接抛 `IllegalArgumentException`，计时器创建失败完成失败并解除取消订阅；取消不撤回已发送业务。
- 经典 ask 的 `$ask-<启动标识>-<序号>` 与本地发送方的 `$ref<路径>#<启动标识>-<序号>` 均带管理器启动标识，避免同址新实例重置序号后误收旧回复。名称是内部不透明字符串，远端按收到的 sender 原样回复，无新增帧版本；不提供认证或业务幂等。
- 经典 `node.ask` 的成功、超时、发送/计时器异常均统一注销临时入口并归还容量；`node.stop()` 先释放全部在途入口，再在锁外完成失败，原超时无需到期。回复与停止竞争时以 Promise 首次完成为准；停止不撤回已发送请求或远端副作用。非正超时在登记和发送前直接抛 `IllegalArgumentException`。默认 1024 是经典与类型化共用的限制，可在启动前 `configure(RemotingSettings.defaults().withAskLimit(256))` 调整；不提供全节点传输内存预算。`RemoteAskCapacityException.limit` 可用于观察容量配置；并发满额可搭配 `TypedRetry` 选择性退避，停止不应无限重试。
- `tell` / `sendMessage` 只入出站队列、不阻塞：连接与重连在该地址的写线程里进行；对端不可达或队列满（64 MB）时丢弃并计数
  （`TcpTransport.getDroppedFrames()`），不抛异常。控制消息用 `sendControl(addr, actor, message)` 走优先通道。
- 对端重启后，未确认的消息在新通道重发：跨重启是**至少一次**。
- 每条连接先握手交换身份（地址 + 启动标识）。`RemotingSettings.withSecureCookie("...")` 后握手带 HMAC-SHA256，
  不知道密钥的节点连不上。心跳判定不可达、可靠消息超时放弃、对端重启（旧实例）时对端被**隔离**：再也连不进来，
  须重启才能重新通信；也可 `quarantine(address, reason)` 手动隔离，`isQuarantined(address)` 查询。
- `RemotingSettings.defaults()`：心跳 1 s、φ 阈值 10、容忍停顿 10 s（同 Akka）。
- 最老的未确认消息超过 `giveUpAfter`：放弃发往该节点的全部消息（计入 `gaveUp`），监视该节点上 Actor 的本地 Actor 收到 `Terminated`。

### 管理端点（`ManagementServer`）

```cangjie
let mgmt = ManagementServer(system, "0.0.0.0", 8558)   // 两参构造绑定全部地址
    .withMaxConnections(32)                              // 超出直接回 503
    .withRequestTimeout(Duration.second * 5)             // 从接受连接起的总时限，慢速客户端到时断开
    .addReadinessCheck("cluster", { => cluster.isUp() })
    .start()
```

内置端点为 `/health/live`、`/health/ready`、`/metrics`（路径不区分查询串）。集群的成员与分片视图不是内置的：门面不认识集群类型（`cactor` 不依赖 `cluster`），由集群模块注入：

```cangjie
import cactor.api.{ManagementServer, ManagementRoute}
import cactor_cluster.ops.attachClusterRoutes

// 成员视图 /cluster/status、分片视图 /cluster/shards（不传 sharding 时只挂成员视图）
attachClusterRoutes(mgmt, cluster, sharding: Some(carts))

// 自定义端点：路径必须是绝对的，重复注册抛 IllegalArgumentException；
// 处理器在连接协程里执行，抛异常回 500 并计入 handlerErrors()，不会打断服务
mgmt.addRoute(ManagementRoute("/custom/stats", "application/json", { => jsonOf(system) }))
mgmt.addRoute("/custom/ping", "text/plain", { => "pong" })
```

`/cluster/status` 报出 `self`、`selfUniqueAddress`、`state`、`converged`、`upCount`、`leader`、`sbrDowned`、`gossipCount`、`unreachable` 与逐成员的 `address`/`status`/`upNumber`/`uid`/`reachable`/`self`；`/cluster/shards` 报出 `type`、`shards`、`ownerCount`、`hosted`、`acquiring`、`acquisitions`、`entities`、`delivered`、`forwarded`、`handedOff`、`buffered`、`dropped`、`passivated`、`restarted`、`busiest`（最忙 5 个）与按分片号排序的 `load` 表。两者都只读本地快照，不向其他节点发消息——节点卡住不会拖住管理端口，代价是视图反映的是本节点的知识。

### 集群节点实例与种子加入（`cactor_cluster`）

```cangjie
let settings = ClusterSettings.defaults().withSeedNodeTimeout(Duration.second * 5)   // 默认即 5 s（同 Akka）
let cluster = Cluster(system, remote, [seed1, seed2], settings)
cluster.selfUniqueAddress()          // "127.0.0.1:2552#<启动标识>"
for (m in cluster.members()) {
    println("${m.uniqueAddress()} ${m.status} up#${m.upNumber}")
}
let rep = Replicator(system, remote, Duration.millisecond * 200)   // 节点标识默认 "地址#启动标识"
```

| 情形 | 行为 |
|---|---|
| 进程在同一地址重启 | 新进程有新的启动标识（`SimpleRemoteManager.selfUid()`）。加入时旧实例先被 down、由 Leader 移除，新实例再 Joining → Up，分配新的 upNumber；订阅者先收到旧实例的 `MemberRemoved` |
| 被移除的成员 | 同一实例永远不能再加入；同一地址的新实例可以 |
| 首个种子启动 | 先向其他种子发 JOIN：其他种子全部回 NACK 立即组成集群；已有集群在运行（回 WAIT 或发来 gossip）就加入它；没有任何回应时等 `seedNodeTimeout` 后组成 |
| CRDT 的节点键 | 默认"地址#启动标识"：重启后的写入是新分量，不被旧进程的分量吞掉 |
| `rep.get(key)` | 返回存储里的不可变值本身（O(1)），可交给任意线程遍历、合并、编码；之后的更新生成新实例，不影响已取得的值（E43） |
| `rep.update(key, initial, modify)` | modify 在 Replicator Actor 线程、锁外执行，读线程不会被它挡住；返回的值类型与原值不同时抛 `IllegalArgumentException`，原值不变 |

### 集群分片：许可式移交与加入兼容检查（E13′、E42）

```cangjie
// 每个节点都用相同的类型名、分片数创建（获取分片要等每个成员的区域确认）
let carts = ClusterSharding(cluster, system, remote, "cart", 100, { id => CartActor(id) })
carts.tell("user-42", AddItem("apple"))          // 任何节点调用都送到 user-42 唯一的实例
let (hosted, acquiring, acquired) = carts.shardStats()   // 本节点托管 / 正在获取的分片数、已完成的获取次数
let heaviest = carts.busiestShards(5)                    // 实体数最多的 5 个分片（负载报告）

// 再平衡：rebalance() 只是让领导者立刻评估一次；rebalanceShard 手动迁移（广播给全部成员）
carts.rebalance()
carts.rebalanceShard(42, targetAddress)                  // 分片 42 → targetAddress

// 自动再平衡的策略可配（默认：差 > 5 且不小于 1.5 倍、冷却 3 s、报告 5 s 过期）
let settings = ShardingSettings.passivating(1.minutes)
    .withRebalance(RebalancePolicy().withRatioPercent(200).withMinEntityDelta(10))
let carts2 = ClusterSharding(cluster, system, remote, "cart", 100, { id => CartActor(id) }, settings)
// 压测、演示需要固定放置时关掉自动迁移（手动 rebalanceShard 仍可用）
let fixed = ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled())

// 获取卡死的可见化：等超过 acquireWarnAfter（默认 10 s）会反复告警，并能查"卡了多久、在等谁"
let acq = ShardingSettings.defaults().withAcquireWarnAfter(30.seconds)
let region = ClusterSharding(cluster, system, remote, "cart", 100, { id => CartActor(id) }, acq)
let health = region.acquisitionHealth()
// health.slow（超过阈值的分片数）、health.oldestMs（最久等了多久）、
// health.pendingMembers（还在等谁确认，通常是没跑同类型 ClusterSharding 的节点）、
// health.slowWarnings（累计告警次数，每过一个阈值窗口 +1）

// 加入兼容检查：内置分片哈希版本与移交协议版本总是比对；再加上业务自己的兼容键
let settings = ClusterSettings.defaults().withJoinCompatibility("app.protocol", "3")
match (cluster.joinRejection()) {
    case Some(reason) => println("加入被拒绝：${reason}")   // 例如 "app.protocol：本节点 3，对方 2"
    case None => ()
}
```

| 情形 | 行为 |
|---|---|
| 本节点成为分片拥有者（视图建立、成员加入或离开） | 向其他每个 Up / Leaving / Exiting 成员发 BEGIN；对方停止该分片的实体（先处理完邮箱里的消息），全部终止后回 STOPPED；收齐才创建实体。期间该分片的消息按序缓冲 |
| 两个节点视图不一致、都认为自己是拥有者 | 后请求的一方让先托管的一方停止；任何时刻最多一个节点托管同一分片，视图一致后只剩真正的拥有者 |
| 成员不可达（未 down） | 只有拥有者不可达的分片缓冲消息，恢复后送达；其他分片照常投递。正在进行的获取要等它恢复或被 down |
| 成员被 down / 移除 | 不再等它确认（前提：被 down 的节点自行停止，见 SBR） |
| 转发超过 3 跳仍不在拥有者上 | 挂起，视图变化或每秒重新路由 |
| 本节点离开集群、缓冲区满 | 消息交给死信（`subscribeDeadLetters` 可见，路径 `sharding/<类型>/<实体>`），`stats()` 的丢弃数加一 |
| 某个成员没有创建该类型的区域 | 需要它确认的分片一直获取中，10 s 后告警点名 |
| 注入网络故障（进程级） | 节点进程用 `BlackholeTransport(TcpTransport(...))` 包一层传输，再用 `attachFaultInjectionRoutes(mgmt, transport)` 暴露 `/cluster/blackhole?addr=host:port`、`/cluster/heal[?addr=]`、`/cluster/faults`。单向黑洞只会让**收不到心跳**的一侧报警（我们还能收到对端的心跳，所以我们自己不报）——`tools/cluster_chaos.sh` 就是按这个语义断言的。生产上不要挂这个端点，或传 `allowAll: false` 并只绑 `127.0.0.1` |
| 获取一直不完成 | 获取是许可式两阶段移交：必须等到每个可能托管者确认"已停止"，否则两个节点可能同时托管同一分片。所以**不会超时强推**，而是把等待变成数据——`ShardingSettings.acquireWarnAfter`（默认 10 s）之后每过一个窗口告警一次并累加 `slowWarnings`，`acquisitionHealth()` 给出 `slow` / `oldestMs` / `pendingMembers`，`/cluster/shards` 同样暴露 `slowAcquires`、`oldestAcquireMs`、`pendingAcquireMembers`。最常见的原因是某个 Up 成员没有创建同类型的 `ClusterSharding` |
| 自动再平衡 | 领导者每 2 s tick（及每次收到负载报告）把 Up 成员 `reportTtl`（默认 5 s）内的报告聚合成表；有不可达成员时不动，全部成员都有报告才决策；最忙与最空闲的实体数相差超过 `minEntityDelta`（默认 5）且不小于 `ratioPercent`（默认 150%）时，把最忙节点上最重的分片迁到最空闲节点（源可以是任意成员）；两次自动迁移间隔 `cooldown`（默认 3 s）。非 Up 成员的报告直接丢弃。阈值、冷却与 TTL 由 `RebalancePolicy` 描述，决策由纯函数 `decideRebalance` 给出（返回 `Move` 或带理由的 `Skip`，日志里能看到为什么没搬）。负载不变的节点不重复上报，但每 `reportTtl / 2` 刷新一次 |
| 加入时兼容键不一致，或对方是不带兼容信息的旧版本 | 回 REJECT，永远不会成为成员；被拒的节点不再发 JOIN，首个种子也不会另起集群 |

### 集群工具：ask、异步 Grain、pubsub 确认与成员清理（E12′）

```cangjie
// 分片 / 单例透传 sender：实体照常 context.reply；跨节点时回复直接回到发起方
let total = carts.ask("user-42", StringMessage("total"), 2.seconds).get(3.seconds)   // Actor 外
carts.tell("user-42", StringMessage("total"), ctx.self)                             // Actor 内：回复进自己的邮箱
let leader = ledger.ask(StringMessage("who"), 2.seconds)                            // ClusterSingleton 同理

// Grain 之间互相调用：实现 onCallAsync，用 callAsync 组合，不阻塞工作线程
class OrderGrain <: Grain {
    let carts: Grains
    init(carts: Grains) { this.carts = carts }
    public func reentrant(): Bool { true }          // A→B→A 的调用链需要可重入
    public func onCallAsync(req: Message, ctx: GrainContext): TypedPromise<Message> {
        carts.callAsync(ctx.id, StringMessage("total"), 2.seconds)
            .map<Message>({ t => StringMessage("order:" + (t as StringMessage).getOrThrow().getContent()) })
    }
}

// pubsub：subscribe 不阻塞；需要确认本地已生效时
pubsub.subscribeAsync("news", subscriberRef).get(5.seconds)

// 成员层规模（诊断）与墓碑保留时长
let (records, tombstones, detectors) = cluster.membershipStats()
let settings = ClusterSettings.defaults().withPruneTombstonesAfter(Duration.hour * 24)
```

| 情形 | 行为 |
|---|---|
| 本地 Actor 作为远程消息的发送方 | 普通发送方使用 `$ref<路径>#<启动标识>-<序号>` 并每秒清扫终止引用；类型化 ask 的一次性回复引用使用独立临时入口并在终态即时注销，`replyEntryCount()` 计入两类 |
| 同一路径上重建的 Actor 发远程消息 | 换新入口、注销旧入口：回复送到新实例；发给旧实例的迟到回复计为不可投递 |
| AskInbox 作为远程发送方 | 无法判断终止，入口空闲 `REPLY_ENTRY_IDLE_TTL_MS`（10 分钟）后注销 |
| 分片 / 单例的 ask 超时 | Promise 以 `TypedAskTimeoutException` 失败，迟到的回复被丢弃 |
| Grain 的 `onCallAsync` 结果未完成 | 默认（不可重入）：后续调用排队，完成后按序处理；`reentrant()` 为 true：照常处理其他调用 |
| 在 Actor / Grain 处理方法里调用 `Grains.call` | 立即 `IllegalStateException`（阻塞会占住亲和工作线程），请用 `callAsync` |
| 调用方节点在应答前重启 | 应答发往 `grain-replies-<kind>-<旧 uid>`，新进程没有这个入口，不会串到新调用 |
| 最后一个订阅者退订后立即重新订阅 | 主题 Actor 按顺序发出"删除、添加"，节点留在主题里 |
| 成员被移除 | 记录从 gossip 删除、只留墓碑（带移除时间，24 h 后各节点一致清除）；故障检测器一并删除 |
| 非成员地址发来心跳 | 忽略，不建故障检测器 |
| 被 down 的成员被移除 | 隔离其 uid：断开连接、拒绝再连入；同址重启的新实例不受影响 |

### 分布式补齐：单例 HandOver、集群 Receptionist、一致性级别

```cangjie
// 集群版 Receptionist：每个节点各建一个，共用本节点的 Replicator
let receptionist = ClusterReceptionist(cluster, system, remote, replicator)
receptionist.register("translator", translatorRef)            // 停止后自动注销
for (t in receptionist.find("translator")) {                    // 本节点为本地引用，其他节点为 RemoteActorRef
    t.tell(Translate("hello"), ctx.self)
}
receptionist.subscribe("translator", { refs => println("实例数 ${refs.size}") })

// 一致性级别：不阻塞，返回 TypedPromise
replicator.addToSetWith("orders", "o-42", WriteConsistency.WriteMajority(3.seconds))
    .onComplete({ o => ... })                                   // 少数派一侧以 ReplicatorConsistencyException 失败
let v = replicator.getWith("orders", ReadConsistency.ReadMajority(3.seconds)).get(5.seconds)  // Actor 外
```

| 情形 | 行为 |
|---|---|
| 单例：最老成员正常离开 | 新的最老成员发 HANDOVER_TO_ME，旧实例处理完邮箱后停止、回 HANDOVER_DONE，之后才启动新实例；任何时刻最多一个 |
| 单例：最老成员宕机 | 不可达期间不移交（不排除不可达成员，分区不会多起一个）；被 down 后不需要它确认，立即在下一个最老成员上启动 |
| 单例：正常离开的成员被移除后不回应 | 最多再等 `SINGLETON_HANDOVER_GRACE_MS`（10 s）后按已停止处理 |
| 单例：握手期间发来的消息 | 由目标节点的管理器缓冲（入口 `singleton-<名称>`），实例启动后按序投递 |
| Receptionist：实例停止 / 显式注销 | 登记删除，经 Replicator 传播（通常一个 gossip 间隔） |
| Receptionist：节点不可达 / 被 down | 不可达期间 `find` 不返回它的实例（`findAll` 仍返回）；被 down 或移除后各节点删除它的全部登记 |
| Receptionist：同址重启的新进程 | 登记元素带 uid，旧进程的登记不会被当成新进程的 |
| `WriteMajority(t)` / `WriteAll(t)` | 本地应用并发给对等节点，⌊n/2⌋+1 / n 个副本（含本节点）确认后成功；超时失败，但本地值已更新、之后照常传播 |
| `ReadMajority(t)` / `ReadAll(t)` | 收到足够副本的值后返回合并结果，并写回本地；过半写 + 过半读必然读到 |

### 类型化引用的跨进程能力（E63）

经典引用（`ActorRef`）能跨节点是因为每个 Actor 都有路径；类型化引用要跨节点，缺的是"消息怎么变成字节"：
`TypedPayloadBox` 的载荷在编译期是 `Any`。有两条路：

- 载荷实现了 `Message`：按**它自己的 codec** 嵌成一帧完整消息（v2.23）；
- 载荷是普通 `enum`（类型化协议最常见的形态，本来就不实现 `Message`）：用 `TypedPayloadCodecs` 注册一对编解码闭包（v2.24）。

```cangjie
// 路径一：协议类型实现了 Message
let remoteEcho = a.remote.typedRemoteActorOf<Echo>(bAddress, "typed-echo")
remoteEcho.tell(Echo("跨节点"))        // 载荷经 TypedPayloadBoxCodec 过线

// 路径二：普通 enum 协议
enum Traffic {
    | Ping(String)
    | Count(Int64)
}
TypedPayloadCodecs.register<Traffic>(
    typeName: "demo.Traffic",                       // 双方必须一致，建议带包名
    encode: { m => match (m) { ... } },             // 自己写的二进制编码
    decode: { b => ... })

let remote = a.remote.typedRemoteActorOf<Traffic>(bAddress, "traffic")
remote.tell(Traffic.Ping("hi"))                     // 接收侧仍是用户包的 as T 桥接
```

```cangjie
// 节点 B：暴露一个类型化 Actor（协议类型必须实现 Message 并注册 codec）
let echo = b.system.typed().actorOf(echoBehavior, "typed-echo")
b.remote.exposeActor("typed-echo", echo.toClassic().getOrThrow())

// 节点 A：拿到远程类型化引用直接 tell
let remoteEcho = a.remote.typedRemoteActorOf<Echo>(bAddress, "typed-echo")
remoteEcho.tell(Echo("跨节点"))        // 载荷经 TypedPayloadBoxCodec 过线

// 全集群按 ServiceKey<T> 查找（协议类型由 key 的 T 决定）
let key = ServiceKey<Echo>("echo")
b.receptionist.registerTyped<Echo>(key, echo)          // 只能登记由 Actor 支撑的引用
for (r in a.receptionist.findTyped<Echo>(key)) {        // 本节点本地引用，其他节点远程类型化引用
    r.tell(Echo("hi"))
}
a.receptionist.deregisterTyped<Echo>(key, echo)
```

跨节点 ask：回复引用不放进消息，而是走信封的 sender 通道（v2.26）：

```cangjie
// 节点 B：收到请求就回复（经典上下文；类型化上下文可以 toClassic() 拿到同一个能力）
let echo = b.system.typed().actorOf(
    TypedBehaviors.receive<StringMessage>({ ctx, msg =>
        ctx.toClassic().getOrThrow().reply(StringMessage("pong:${msg.getContent()}"))
        TypedBehaviors.same<StringMessage>()
    }),
    "typed-ask")

// 节点 A：请求发出后，回复沿网络直接回到本节点
let p = TypedAsk.askRemote<StringMessage, StringMessage>(remoteEcho, StringMessage("ping"), 3.seconds)
let reply = p.get(3.seconds)     // 超时抛 TypedAskTimeoutException
```

为什么不复用 `TypedAsk.ask`：`ask` 把一次性回复引用（`/temp/ask-N`）塞进请求消息，进程内最直接；
跨节点时那个引用是本地对象，用户自定义的 codec 不认识它，于是"请求能过线、回复回不来"。
`askRemote` 改用远程层本来就有的 sender 通道（`senderInfoFor` 会给本地引用登记回复入口），
代价是请求里不能顺便携带回复引用——需要多轮对话时用 `tell`。

| 情形 | 行为 |
|---|---|
| 载荷既不是 `Message` 也没有命名编解码器（闭包、连接、任意对象） | 发送侧立即抛 `SerializationException`，明确"这条消息不能跨节点"，不会悄悄丢包 |
| 载荷类型名重复注册 | 立刻抛 `IllegalStateException`：同一字节串在两端会被解成不同类型，属于必须停止的错误（与 `MessageCodecRegistry.register` 的静默覆盖不同） |
| 对端没有注册同名编解码器 | 解码侧抛 `DeserializationException`，并指出缺失的类型名 |
| 同一次注册进程内生效 | 注册表是进程级的；`typeNames()` 用来比对两端配置，`unregister(typeName)` 只在测试里用 |
| 同一个 codec 被重复注册 | `ensureTypedPayloadCodecRegistered()` 幂等；已存在同名 codec 时不覆盖（`SimpleRemoteManager.start()` 调用它） |
| 登记一次性回复引用（`PromiseRef` / `PromiseActorRef`） | 抛 `IllegalArgumentException`：它没有经典路径，跨节点找不到 |
| 跨节点类型化 **ask** | 用 `TypedAsk.askRemote(target, request, timeout)`（v2.26）：回复引用走信封的 sender 通道，请求消息里不需要带任何引用 |
| `askRemote` 的回复类型 | 必须是响应消息本身的类型（`StringMessage` 这类）或由对端类型化引用回传、`PromiseActorRef` 自动解包的载荷；无法还原时抛 `IllegalStateException` |

### 分布式数据：delta 传播与移除节点的 pruning（E64）

```cangjie
// delta 传播默认开启：本地更新只推送变化部分（计数器的变化分量、ORSet 新增 / 删除的点）
let st = replicator.deltaStats()        // deltaPushes / fullPushes / deltasApplied / gaps
replicator.setDeltaPropagation(false)   // 对照与排障：每次推送整值

// pruning：ClusterReplicatorBinding（分片、pubsub、Receptionist 都会调用）自动接上成员视图与移除事件
ClusterReplicatorBinding.bind(cluster, replicator)
replicator.configurePruning(maxDissemination: 60.seconds, markerTimeToLive: 6.hours)   // 只改传入的项
let (initialized, performed, keysWithMarkers) = replicator.pruningStats()

// 不经集群时手工接上（测试或自建成员管理）
replicator.setMembership({ => isLeader() }, { => memberNodeIds() })   // 节点标识为 "地址#uid"
replicator.nodeRemoved("10.0.0.7:25520#8812345")
```

| 情形 | 行为 |
|---|---|
| 本地更新 | 推送增量（带（来源, 键）序号）；键第一次出现、或更新里混入了别的节点的点时推送整值 |
| 增量序号恰好是下一个 | 应用；重复的丢弃 |
| 序号断档（丢包） | 不应用（ORSet 的版本向量不能越过没收到的点），向来源请求整值；1 s 内没收到就重发请求，最多 5 次，之后交给反熵 |
| 节点被移除 | 等 `maxDissemination`（默认 60 s，让它在途的更新传遍）后由 Leader 发起；全部成员登记后发起者执行：计数器分量并入发起者，ORSet 的点改记到发起者，值不变 |
| 发起者失去 Leader 身份 / 离开集群 | 仍由它执行 / 由现任 Leader 重新发起 |
| Leader 没收到移除事件 | 扫描数据里出现、不是成员的 "地址#uid" 节点，同样处理（自定义的、不含 `#` 的节点标识不会被当成已移除） |
| 陈旧副本（含被移除节点的数据）重新连上 | 已执行标记保留 `markerTimeToLive`（默认 6 h），合并前先剥掉该节点的数据，不会重复计数；副本离线超过这个时长则可能重复计数（同 Akka） |
| 被移除节点迟到的增量（僵尸进程） | 丢弃 |
| 与 pruning 并发的删除 | pruning 视同执行者重新添加，按 add-wins 处理（同 Akka） |
| `pruningInterval` | 默认 5 s；扫描只看每个值里出现的节点集合 |

### 远程传输 TLS / mTLS（`RemoteTls`，E65）

```cangjie
import cactor_remote.{SimpleRemoteManager, RemotingSettings, RemoteTls}

// 本节点证书链（叶子在前）、PKCS#8 私钥、信任的 CA；默认 TLS 1.3 双向认证
let tls = RemoteTls.fromPemFiles("/etc/cactor/node.pem", "/etc/cactor/node-key.pem", "/etc/cactor/ca.pem")
let remote = SimpleRemoteManager(NetworkAddress("10.0.0.5", 25520), TransportProtocol.TCP)
remote.configure(RemotingSettings.defaults().withTls(tls).withSecureCookie(cookie))   // 必须在 start 之前
remote.start()
let cluster = Cluster(system, remote, seeds, settings)   // gossip、心跳、分片、ddata 全部经 TLS
```

| 情形 | 行为 |
|---|---|
| 两端证书都由信任的 CA 签发 | TLS 1.3 握手（约 60 ms，连接长期复用）后进行远程层握手与收发；`securedConnections()` 计数 |
| 明文连接（未配 TLS 的节点、裸 TCP） | TLS 握手失败，连接断开，帧不交给远程层；`rejectedConnections()` 计数 |
| TLS 节点连明文节点 | 握手失败、消息按连接失败丢弃，不降级为明文 |
| 对端证书由其他 CA 签发 / 过期 / 客户端不带证书 | 两个方向都断开；`RemoteTls.rejectedPeerCertificates()` 计数 |
| 系统根证书签发的公网证书 | 拒绝：只信任配置的 CA（stdx 的 CustomCA 会同时信任系统根证书，所以用 CustomVerify 自行校验） |
| 主机名 | 不校验（同 Akka 默认）：节点按"持有本集群 CA 签发的证书"认证，身份由其后的握手（地址 + 启动标识）确认 |
| 本节点证书不是信任的 CA 签发 | `RemoteTls.fromPem` 抛 `IllegalArgumentException`（配置错误立即报出） |
| UDP 等不支持加密的传输 / start 之后再配置 | `start` 抛 `UnsupportedException` / `configure` 抛 `IllegalStateException`，不会以明文运行 |
| `withoutMutualAuthentication()` | 单向 TLS：只认证服务端，任何能连上端口的进程都能建立连接，须配合共享密钥 |

构建：根 `cjpm.toml` 以 `${CANGJIE_STDX_PATH}` 静态链接 stdx，先 `source tools/env.sh`（或自行 export 为 stdx 的 `static/stdx` 目录）。
运行时需要 OpenSSL 3（libssl.so.3 / libcrypto.so.3），只在用到 TLS 时加载。
证书：每个节点用本集群 CA 签发的证书（extendedKeyUsage 含 serverAuth 与 clientAuth），私钥 PKCS#8 PEM；EC P-256 握手最快。
`tools/gen_test_tls_certs.sh` 演示了完整的签发步骤（其产物 `tools/tls-test-certs/` 仅供测试）。

### 事件溯源与日志 v2（`EventSourcedActor` + `EventStore`）

```cangjie
let journal = FileEventStore("/var/lib/app/journal")          // 默认 fsync：append 返回即已落盘
let fast = FileEventStore("/tmp/j", fsync: false)               // 只防进程崩溃，不防断电

// 单写者：期望序号不符时抛 JournalConflictException，一条也不写
var seq = journal.highestSequenceNr("order-1")
seq = journal.append("order-1", [BasicEvent("e1", "order-1", "Created", "{...}").withTags(["vip"])], seq)
for (env in journal.replay("order-1", 1, Int64.Max)) {          // 按序号，闭区间
    println("${env.sequenceNr} ${env.event.getEventType()}")
}
journal.deleteTo("order-1", seq)                                 // 最高序号保留
ReadJournal(journal).currentEventsByTag("vip")
```

在 `EventSourcedActor` 子类中：`persist(type, data[, tags])`、`persistAll([(type, data), ...])`（一批一次 fsync）、
`saveSnapshot()` 后 `deleteEventsTo(lastSequenceNr())`。

| 情形 | 行为 |
|---|---|
| 同一标识两个写者 | 后写者 `append` 抛 `JournalConflictException`，事件不写入；Actor 交给监督，重启后从日志恢复 |
| 进程被 kill -9 | 已返回的 append 一条不少；写到一半的尾部在下次打开时截断 |
| 断电后尾部零填充 / 半条记录 | 按尾部残缺截断（它所在的那批从未被确认） |
| 中间损坏、其后仍有完整记录 | 抛 `PersistenceCorruptionException`，不截断，等人工处理 |
| v1 格式的旧日志 | 首次打开时原子迁移为 v2，序号按位置从 1 编号 |
| 并发写同一日志 | 组提交：fsync 进行中到达的写入由下一次 fsync 一并覆盖 |

Enhanced 族（`EnhancedEventStore` / `AbstractPersistentActor`）、CQRS、Plugin / Manager 已弃用，2.0 删除。

### 类型化持久化（`cactor_persistence.typed`，E44，对标 Akka Typed）

```cangjie
import cactor_core.typed.*
import cactor_persistence.{FileEventStore, FileSnapshotStore}
import cactor_persistence.typed.*

enum CartCmd { | AddItem(String, Int64, TypedActorRef<CartReply>) | Checkout(TypedActorRef<CartReply>) | GetCart(TypedActorRef<CartReply>) }
enum CartEvt { | ItemAdded(String, Int64) | CheckedOut(Int64) }

let cart = EventSourcedBehavior<CartCmd, CartEvt, Cart>(PersistenceId.of("ShoppingCart", cartId),
    emptyState: Cart.empty(),
    commandHandler: { cart, cmd, effect =>                      // 只读状态、返回效果；不做 IO
        match (cmd) {
            case GetCart(replyTo) => effect.reply(replyTo, Accepted(cart))
            case AddItem(item, _, replyTo) where cart.has(item) => effect.reply(replyTo, Rejected("已在购物车中"))
            case AddItem(item, qty, replyTo) =>
                effect.persist(ItemAdded(item, qty)).thenReply(replyTo, { c => Accepted(c) })   // 落盘之后回复新状态
            case Checkout(replyTo) => effect.persist(CheckedOut(nowMs())).thenReply(replyTo, { c => Accepted(c) })
        }
    },
    eventHandler: { cart, evt => cart.apply(evt) },             // 纯函数，恢复时重放
    eventAdapter: cartEvents,                                    // 事件 ↔ (manifest, data)
    journal: FileEventStore("/var/lib/app/journal"))
    .withSnapshots(FileSnapshotStore("/var/lib/app/snapshots"), cartCodec)
    .withRetention(RetentionCriteria.snapshotEvery(100, 2).withDeleteEventsOnSnapshot())
    .snapshotWhen({ _, evt, _ =>                                 // 也可按事件触发快照
        match (evt) {
            case CheckedOut(_) => true
            case _ => false
        }
    })
    .receiveSignal({ state, signal => if (signal is RecoveryCompleted) { /* ... */ } })
system.typedActorOf(cart.behavior(), "cart-${cartId}")
```

命令处理器的第三个参数是类型已确定的效果工厂（仓颉不能从返回位置推断静态泛型函数的类型参数）：

| 效果 | 含义 |
|---|---|
| `effect.persist(e)` / `persistAll([e1, e2])` | 写入日志（一批一次 fsync），确认后依次应用事件处理器，再执行副作用 |
| `effect.none()` / `reply(replyTo, msg)` | 不写入；`reply` = `none().thenRun(发送)` |
| `effect.unhandled()` | 计为未处理消息（交给死信订阅者） |
| `effect.stash()` / `unstashAll()` | 用户暂存当前命令；`thenUnstashAll()` 时按原顺序放回、先于此后到达的命令处理 |
| `effect.stop()` | 停止 Actor |
| `.thenRun(f)` / `.thenReply(replyTo, f)` / `.thenStop()` / `.thenUnstashAll()` | 主效果完成后按顺序执行；`f` 拿到的是此时的状态（持久化后为新状态） |

| 情形 | 行为 |
|---|---|
| 启动 / 监督重启 | 在持久化 IO 线程读最新快照（按序号）与之后的事件，Actor 线程应用事件处理器；完成后发 `RecoveryCompleted(最高序号)`，再处理暂存的命令 |
| 恢复中、写入中到达的命令 | 暂存（默认容量 4096），按到达顺序逐条处理：每条命令看到之前全部事件落盘后的状态 |
| 暂存满 | `withStash(n, overflow: DropCommand)`（默认）：丢弃并作为未处理消息上报、限速记日志；`FailActor`：抛 `StashOverflowException` |
| 写入失败（如另一个写者造成序号冲突） | Actor 以该异常失败：默认停止；`TypedBehaviors.supervise(b).onFailure(restartWithBackoff(...))` 重启后重新恢复 |
| 恢复失败（日志损坏、事件处理器抛异常、序号缺口） | 先发 `RecoveryFailed`，再以 `PersistenceException` 失败（默认停止），不带着不完整的状态运行 |
| 重启前在途的 IO 完成 | 完成信号按实例 token 识别并丢弃（新实例已从存储恢复） |
| 保留策略 | 序号跨过 n 的倍数时快照（同时最多一个在途，在途期间只保留最新的待写快照）；保存后删除序号 ≤ 当前 − keep × n 的快照，`withDeleteEventsOnSnapshot` 时连同事件；结果以 `SnapshotCompleted` / `DeleteSnapshotsCompleted` / `DeleteEventsCompleted` / `SnapshotFailed` / `DeleteFailed` 信号通知 |
| schema 演进 | `EventAdapter.fromJournal(manifest, data)` 返回 0..n 个事件：空数组跳过废弃事件，多个用于拆分；写入序号按日志序号接续 |
| 系统关闭 | 不等在途 IO（与 Akka 相同）；需要时 `PersistenceIo.awaitIdle(timeout)`；IO 线程数 `CACTOR_PERSISTENCE_IO_THREADS`（默认 8） |

`DurableStateBehavior<C, S>(persistenceId, emptyState:, commandHandler:, codec:, store:)`：命令处理器用
`effect.persist(newState)` / `effect.delete()`，每次写入修订号加一；删除后状态回到 `emptyState`，
存储留下修订号 +1 的墓碑（`store.revisionOf(id)`），之后的写入接在墓碑之后，持有旧修订号的写者被拒绝（P14）。

完整示例：`modules/examples/src/typed_persistence_demo/main.cj`（`./target/release/bin/cactor_examples.typed_persistence_demo`）。

在类型化 Actor 里把 Future 的结果作为**信号**送回自己（不占命令协议，供持久化这类内部通知使用）：

```cangjie
ctx.pipeSignalToSelf<Int64>(future, { outcome => LoadDone(outcome) })   // LoadDone <: TypedSignal，在 onSignal 中处理
```

### 持久化（`AbstractPersistentActor`，已弃用）

| 情形 | 行为 |
|---|---|
| 启动 / 重启 | `preStart` 自动恢复（快照 + 之后的事件）；重写 `preStart` 时调用 `super.preStart()` |
| 写入存储失败（如版本冲突） | 抛 `PersistenceException`，序号回退 |
| 已落库后处理器抛异常 | 抛 `EventHandlerException`（`PersistenceException` 子类，带 `persistedSequenceNr`），序号**不**回退；默认监督重启后重放该事件 |
| `persistAll` | 等存储确认；冲突抛 `PersistenceException` |
| 恢复失败 | 抛 `PersistenceException`；在 `preStart` 中即为启动失败，默认停止 |

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
