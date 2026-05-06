# CActor 最佳实践指南

## 实践概览

本指南汇总了CActor开发与部署的最佳实践，助您构建高性能、易维护、可扩展的Actor系统。

## 设计原则

### 1. Actor设计原则

#### 单一职责原则

```cangjie
// 好的设计 - 单一职责
class UserManagerActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case createUser: CreateUserMessage =>
                createUser(createUser.userData)
            case updateUser: UpdateUserMessage =>
                updateUser(updateUser.userId, updateUser.userData)
            case deleteUser: DeleteUserMessage =>
                deleteUser(deleteUser.userId)
            case _ => MessageResult.Unhandled
        }
    }
}

// 应避免 - 职责过多
class GodActor <: Actor {
    // 不要让一个Actor处理太多不相关的职责
    // 例如：用户管理、订单处理、支付处理不要放在同一个Actor中
}
```

#### 不可变消息

```cangjie
// 好的设计 - 不可变消息
public struct CreateUserMessage <: Message {
    public let userId: String
    public let userName: String
    public let email: String

    public init(userId: String, userName: String, email: String) {
        this.userId = userId
        this.userName = userName
        this.email = email
    }
}

// 应避免 - 可变消息
public class MutableUserMessage <: Message {
    public var userId: String  // 可变字段可能导致并发问题
    public var userData: UserData
}
```

#### Actor状态封装

```cangjie
// 好的设计 - 状态封装
class BankAccountActor <: Actor {
    private var balance: Decimal = Decimal(0)
    private var transactions: ArrayList<Transaction> = ArrayList<Transaction>()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case deposit: DepositMessage =>
                balance += deposit.amount
                transactions.add(Transaction.deposit(deposit.amount))
                MessageResult.Handled
            case withdraw: WithdrawMessage =>
                if (balance >= withdraw.amount) {
                    balance -= withdraw.amount
                    transactions.add(Transaction.withdraw(withdraw.amount))
                    MessageResult.Handled
                } else {
                    context.sender.tell(InsufficientFundsMessage())
                    MessageResult.Handled
                }
        }
    }
}
```

### 2. 消息设计原则

#### 消息类型层次

```cangjie
// 好的设计 - 清晰的消息层次
public interface UserMessage <: Message {}

public struct CreateUserMessage <: UserMessage {
    public let userData: UserData
}

public struct UpdateUserMessage <: UserMessage {
    public let userId: String
    public let userData: UserData
}

public struct DeleteUserMessage <: UserMessage {
    public let userId: String
}

// 响应消息
public interface UserResponse <: Message {}

public struct UserCreatedResponse <: UserResponse {
    public let userId: String
}

public struct UserErrorResponse <: UserResponse {
    public let error: String
}
```

#### 消息大小优化

```cangjie
// 好的设计 - 轻量级消息
public struct ProcessOrderMessage <: Message {
    public let orderId: String  // 只传递ID，避免传递大对象
}

// 应避免 - 重量级消息
public struct ProcessOrderMessage <: Message {
    public let order: Order  // 包含大量数据的对象
    public let customer: Customer
    public let products: ArrayList<Product>
}
```

## 性能最佳实践

### 1. 邮箱配置优化

```cangjie
// 根据使用场景选择合适的邮箱类型
let config = CActorRuntimeConfig.createProduction()

// 高吞吐量场景
config.withMailboxType(MailboxType.Unbounded)
    .withBatchSize(100)

// 内存敏感场景
config.withMailboxType(MailboxType.Bounded)
    .withMailboxCapacity(1000)

// 优先级消息场景
config.withMailboxType(MailboxType.Priority)
    .withPriorityFunction { message =>
        match (message) {
            case urgent: UrgentMessage => 1
            case normal: NormalMessage => 5
            case low: LowPriorityMessage => 10
        }
    }
```

### 2. 调度器配置

```cangjie
// 为不同类型的Actor选择合适的调度器
let system = CActor.system("ProductionSystem")

// CPU密集型Actor
let cpuIntensiveProps = CActor.props({ => CPUIntensiveActor() })
    .withDispatcher("cpu-intensive-dispatcher")

// IO密集型Actor
let ioIntensiveProps = CActor.props({ => IOIntensiveActor() })
    .withDispatcher("io-intensive-dispatcher")

// 配置调度器
let config = CActorRuntimeConfig.createProduction()
    .withDispatcher("cpu-intensive-dispatcher",
        DispatcherConfig.create()
            .withThreads(Runtime.getRuntime().availableProcessors())
            .withThroughput(1))
    .withDispatcher("io-intensive-dispatcher",
        DispatcherConfig.create()
            .withThreads(100)
            .withThroughput(100))
```

### 3. 批量处理

```cangjie
// 好的设计 - 批量处理
class BatchProcessorActor <: Actor {
    private var batch: ArrayList<Message> = ArrayList<Message>()
    private let batchSize: Int64 = 100

    public func receive(message: Message, context: ActorContext): MessageResult {
        batch.add(message)

        if (batch.size() >= batchSize) {
            processBatch(batch)
            batch.clear()
        }

        MessageResult.Handled
    }

    private func processBatch(messages: ArrayList<Message>): Unit {
        // 批量处理消息，提高效率
        for (message in messages) {
            processMessage(message)
        }
    }
}
```

## 架构最佳实践

### 1. Actor层次结构

```cangjie
// 好的设计 - 清晰的层次结构
/*
UserSystem
├── UserManagerActor (监督者)
│   ├── UserValidatorActor
│   ├── UserPersistenceActor
│   └── UserNotificationActor
└── UserQueryActor (只读查询)
*/

class UserSystemActor <: Actor {
    private var userManager: ActorRef
    private var userQuery: ActorRef

    public func preStart(): Unit {
        userManager = context.actorOf(UserManagerActor.props(), "user-manager")
        userQuery = context.actorOf(UserQueryActor.props(), "user-query")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case cmd: UserCommand =>
                userManager.tell(cmd)
            case query: UserQuery =>
                userQuery.tell(query)
        }
    }
}
```

### 2. 监督策略

```cangjie
// 好的设计 - 合适的监督策略
class UserManagerActor <: Actor {
    public func supervisorStrategy(): SupervisorStrategy {
        return OneForOneStrategy(
            maxNrOfRetries: 3,
            withinTimeRange: Duration.fromMinutes(1),
            decider: { exception =>
                match (exception) {
                    case _: ValidationException => SupervisorDirective.Resume
                    case _: DatabaseException => SupervisorDirective.Restart
                    case _: SecurityException => SupervisorDirective.Stop
                    case _ => SupervisorDirective.Escalate
                }
            }
        )
    }
}
```

### 3. 错误处理

```cangjie
// 好的设计 - 完善的错误处理
class RobustActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        try {
            processMessage(message)
            MessageResult.Handled
        } catch (e: ValidationException) {
            // 业务错误，发送错误响应
            context.sender.tell(ErrorResponse(e.message))
            MessageResult.Handled
        } catch (e: Exception) {
            // 系统错误，记录日志并重新抛出
            println("处理消息时发生错误: ${e.message}")
            throw e
        }
    }

    public func preRestart(reason: Option<Exception>, message: Option<Message>): Unit {
        // 清理资源
        cleanup()
        super.preRestart(reason, message)
    }
}
```

## 集群最佳实践

### 1. 分片策略

```cangjie
// 好的设计 - 合理的分片策略
let shardingConfig = ShardingConfig.create()
    .withNumberOfShards(100)  // 分片数量应为节点数的倍数
    .withEntityExtractor { message =>
        match (message) {
            case userMsg: UserMessage =>
                // 使用用户ID的哈希值进行分片
                let shardId = userMsg.userId.hashCode() % 100
                (userMsg.userId, shardId)
            case _ => throw IllegalArgumentException("不支持的消息类型")
        }
    }
    .withShardResolver { shardId =>
        // 分片到节点的映射
        let nodeIndex = shardId % clusterNodes.size()
        clusterNodes[nodeIndex]
    }
```

### 2. 集群配置

```cangjie
// 好的设计 - 生产环境集群配置
let clusterConfig = ClusterConfig.create()
    .withSeedNodes([
        "node-1.cluster.local:2551",
        "node-2.cluster.local:2551",
        "node-3.cluster.local:2551"
    ])
    .withFailureDetector(
        FailureDetectorConfig.create()
            .withHeartbeatInterval(Duration.fromSeconds(1))
            .withAcceptableHeartbeatPause(Duration.fromSeconds(10))
    )
    .withAutoDownUnreachableAfter(Duration.fromMinutes(5))
```

## 持久化最佳实践

### 1. 事件设计

```cangjie
// 好的设计 - 版本化事件
public interface UserEvent <: Event {
    public func version(): Int64
}

public struct UserCreatedEventV1 <: UserEvent {
    public let userId: String
    public let userName: String

    public func version(): Int64 { return 1 }
}

public struct UserCreatedEventV2 <: UserEvent {
    public let userId: String
    public let userName: String
    public let email: String  // 新增字段

    public func version(): Int64 { return 2 }
}

// 事件升级处理
public func upgradeEvent(event: UserEvent): UserEvent {
    match (event) {
        case v1: UserCreatedEventV1 =>
            UserCreatedEventV2(v1.userId, v1.userName, "")
        case v2: UserCreatedEventV2 => v2
    }
}
```

### 2. 快照策略

```cangjie
// 好的设计 - 智能快照策略
class PersistentUserActor <: PersistentActor {
    private var eventCount: Int64 = 0
    private let snapshotInterval: Int64 = 1000

    public func receiveCommand(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case cmd: UserCommand =>
                let event = processCommand(cmd)
                persist(event) { persistedEvent =>
                    updateState(persistedEvent)
                    eventCount += 1

                    // 定期创建快照
                    if (eventCount % snapshotInterval == 0) {
                        saveSnapshot(currentState)
                    }
                }
                MessageResult.Handled
        }
    }
}
```

## 测试最佳实践

### 1. 单元测试

```cangjie
// 好的设计 - 完整的单元测试
@Test
public func testUserCreation(): Unit {
    let testKit = TestKit.create()
    let userActor = testKit.spawn(UserActor.props())
    let probe = testKit.createTestProbe<UserResponse>()

    // 发送创建用户消息
    userActor.tell(CreateUserMessage("user1", "John", "john@example.com"), probe.ref)

    // 验证响应
    let response = probe.expectMessage<UserCreatedResponse>(Duration.fromSeconds(3))
    assert(response.userId == "user1")

    // 验证状态
    userActor.tell(GetUserMessage("user1"), probe.ref)
    let userInfo = probe.expectMessage<UserInfoResponse>(Duration.fromSeconds(1))
    assert(userInfo.userName == "John")
}
```

### 2. 集成测试

```cangjie
// 好的设计 - 集成测试
@Test
public func testUserWorkflow(): Unit {
    let testKit = TestKit.create()
    let userSystem = testKit.spawn(UserSystemActor.props())
    let probe = testKit.createTestProbe<Message>()

    // 测试完整的用户工作流
    userSystem.tell(CreateUserMessage("user1", "John", "john@example.com"), probe.ref)
    probe.expectMessage<UserCreatedResponse>(Duration.fromSeconds(3))

    userSystem.tell(UpdateUserMessage("user1", UserData("John Doe", "john.doe@example.com")), probe.ref)
    probe.expectMessage<UserUpdatedResponse>(Duration.fromSeconds(3))

    userSystem.tell(DeleteUserMessage("user1"), probe.ref)
    probe.expectMessage<UserDeletedResponse>(Duration.fromSeconds(3))
}
```

## 监控最佳实践

### 1. 关键指标监控

```cangjie
// 好的设计 - 内置监控
class MonitoredActor <: Actor {
    private var messageCount: Int64 = 0
    private var processingTime: Duration = Duration.zero()

    public func receive(message: Message, context: ActorContext): MessageResult {
        let startTime = System.currentTimeMillis()

        try {
            let result = processMessage(message)
            messageCount += 1

            let duration = System.currentTimeMillis() - startTime
            processingTime += Duration.fromMilliseconds(duration)

            // 报告指标
            if (messageCount % 1000 == 0) {
                reportMetrics()
            }

            return result
        } catch (e: Exception) {
            // 报告错误指标
            reportError(e)
            throw e
        }
    }

    private func reportMetrics(): Unit {
        let avgProcessingTime = processingTime.toMilliseconds() / messageCount
        println("Actor指标 - 消息数: ${messageCount}, 平均处理时间: ${avgProcessingTime}ms")
    }
}
```

### 2. 健康检查

```cangjie
// 好的设计 - 健康检查
class HealthCheckActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: HealthCheckMessage =>
                let status = checkSystemHealth()
                context.sender.tell(HealthStatusResponse(status))
                MessageResult.Handled
        }
    }

    private func checkSystemHealth(): HealthStatus {
        let checks = [
            checkMemoryUsage(),
            checkDiskSpace(),
            checkDatabaseConnection(),
            checkClusterStatus()
        ]

        return if (checks.all { it.isHealthy }) {
            HealthStatus.Healthy
        } else {
            HealthStatus.Unhealthy(checks.filter { !it.isHealthy })
        }
    }
}
```

## 安全最佳实践

### 1. 消息验证

```cangjie
// 好的设计 - 输入验证
class SecureUserActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case createUser: CreateUserMessage =>
                if (validateUserInput(createUser)) {
                    processCreateUser(createUser)
                } else {
                    context.sender.tell(ValidationErrorResponse("无效的用户数据"))
                }
                MessageResult.Handled
        }
    }

    private func validateUserInput(message: CreateUserMessage): Bool {
        return message.userName.length() > 0 &&
               message.email.contains("@") &&
               message.userId.matches(Regex("[a-zA-Z0-9]+"))
    }
}
```

### 2. 权限控制

```cangjie
// 好的设计 - 权限检查
class AuthorizedActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (!isAuthorized(context.sender, message)) {
            context.sender.tell(UnauthorizedResponse())
            return MessageResult.Handled
        }

        processAuthorizedMessage(message)
    }

    private func isAuthorized(sender: ActorRef, message: Message): Bool {
        // 实现权限检查逻辑
        return authService.checkPermission(sender, message)
    }
}
```

---

**CActor v7.0.0 - 遵循最佳实践，构建卓越系统！**

*最后更新: 2026年5月6日*