# CActor Best Practices Guide

## Practice Overview

This guide consolidates best practices for CActor development and deployment, helping you build high-performance, maintainable, and scalable Actor systems.

## Design Principles

### 1. Actor Design Principles

#### Single Responsibility Principle
```cangjie
// Good design - Single responsibility
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

// Avoid - Too many responsibilities
class GodActor <: Actor {
    // Do not let one Actor handle too many unrelated responsibilities
    // User management, order processing, payment processing, etc. all in one Actor
}
```

#### Immutable Messages
```cangjie
// Good design - Immutable messages
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

// Avoid - Mutable messages
public class MutableUserMessage <: Message {
    public var userId: String  // Mutable fields may cause concurrency issues
    public var userData: UserData
}
```

#### Actor State Encapsulation
```cangjie
// Good design - State encapsulation
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

### 2. Message Design Principles

#### Message Type Hierarchy
```cangjie
// Good design - Clear message hierarchy
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

// Response messages
public interface UserResponse <: Message {}

public struct UserCreatedResponse <: UserResponse {
    public let userId: String
}

public struct UserErrorResponse <: UserResponse {
    public let error: String
}
```

#### Message Size Optimization
```cangjie
// Good design - Lightweight messages
public struct ProcessOrderMessage <: Message {
    public let orderId: String  // Pass only ID, avoid passing large objects
}

// Avoid - Heavyweight messages
public struct ProcessOrderMessage <: Message {
    public let order: Order  // Object containing large amounts of data
    public let customer: Customer
    public let products: ArrayList<Product>
}
```

## Performance Best Practices

### 1. Mailbox Configuration Optimization

```cangjie
// Choose the appropriate mailbox type based on usage scenario
let config = CActorRuntimeConfig.createProduction()

// High throughput scenarios
config.withMailboxType(MailboxType.Unbounded)
    .withBatchSize(100)

// Memory-sensitive scenarios
config.withMailboxType(MailboxType.Bounded)
    .withMailboxCapacity(1000)

// Priority message scenarios
config.withMailboxType(MailboxType.Priority)
    .withPriorityFunction { message =>
        match (message) {
            case urgent: UrgentMessage => 1
            case normal: NormalMessage => 5
            case low: LowPriorityMessage => 10
        }
    }
```

### 2. Scheduler Configuration

```cangjie
// Choose the appropriate scheduler for different types of Actors
let system = CActor.system("ProductionSystem")

// CPU-intensive Actors
let cpuIntensiveProps = CActor.props({ => CPUIntensiveActor() })
    .withDispatcher("cpu-intensive-dispatcher")

// I/O-intensive Actors
let ioIntensiveProps = CActor.props({ => IOIntensiveActor() })
    .withDispatcher("io-intensive-dispatcher")

// Configure schedulers
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

### 3. Batch Processing

```cangjie
// Good design - Batch processing
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
        // Batch process messages to improve efficiency
        for (message in messages) {
            processMessage(message)
        }
    }
}
```

## Architecture Best Practices

### 1. Actor Hierarchy

```cangjie
// Good design - Clear hierarchy
/*
UserSystem
├── UserManagerActor (supervisor)
│   ├── UserValidatorActor
│   ├── UserPersistenceActor
│   └── UserNotificationActor
└── UserQueryActor (read-only query)
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

### 2. Supervision Strategy

```cangjie
// Good design - Appropriate supervision strategy
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

### 3. Error Handling

```cangjie
// Good design - Comprehensive error handling
class RobustActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        try {
            processMessage(message)
            MessageResult.Handled
        } catch (e: ValidationException) {
            // Business error, send error response
            context.sender.tell(ErrorResponse(e.message))
            MessageResult.Handled
        } catch (e: Exception) {
            // System error, log and re-throw
            println("Error occurred while processing message: ${e.message}")
            throw e
        }
    }

    public func preRestart(reason: Option<Exception>, message: Option<Message>): Unit {
        // Clean up resources
        cleanup()
        super.preRestart(reason, message)
    }
}
```

## Cluster Best Practices

### 1. Sharding Strategy

```cangjie
// Good design - Reasonable sharding strategy
let shardingConfig = ShardingConfig.create()
    .withNumberOfShards(100)  // Number of shards should be a multiple of node count
    .withEntityExtractor { message =>
        match (message) {
            case userMsg: UserMessage =>
                // Shard using hash of user ID
                let shardId = userMsg.userId.hashCode() % 100
                (userMsg.userId, shardId)
            case _ => throw IllegalArgumentException("Unsupported message type")
        }
    }
    .withShardResolver { shardId =>
        // Mapping from shard to node
        let nodeIndex = shardId % clusterNodes.size()
        clusterNodes[nodeIndex]
    }
```

### 2. Cluster Configuration

```cangjie
// Good design - Production cluster configuration
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

## Persistence Best Practices

### 1. Event Design

```cangjie
// Good design - Versioned events
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
    public let email: String  // New field

    public func version(): Int64 { return 2 }
}

// Event upgrade handling
public func upgradeEvent(event: UserEvent): UserEvent {
    match (event) {
        case v1: UserCreatedEventV1 =>
            UserCreatedEventV2(v1.userId, v1.userName, "")
        case v2: UserCreatedEventV2 => v2
    }
}
```

### 2. Snapshot Strategy

```cangjie
// Good design - Smart snapshot strategy
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

                    // Periodically create snapshots
                    if (eventCount % snapshotInterval == 0) {
                        saveSnapshot(currentState)
                    }
                }
                MessageResult.Handled
        }
    }
}
```

## Testing Best Practices

### 1. Unit Testing

```cangjie
// Good design - Complete unit tests
@Test
public func testUserCreation(): Unit {
    let testKit = TestKit.create()
    let userActor = testKit.spawn(UserActor.props())
    let probe = testKit.createTestProbe<UserResponse>()

    // Send create user message
    userActor.tell(CreateUserMessage("user1", "John", "john@example.com"), probe.ref)

    // Verify response
    let response = probe.expectMessage<UserCreatedResponse>(Duration.fromSeconds(3))
    assert(response.userId == "user1")

    // Verify state
    userActor.tell(GetUserMessage("user1"), probe.ref)
    let userInfo = probe.expectMessage<UserInfoResponse>(Duration.fromSeconds(1))
    assert(userInfo.userName == "John")
}
```

### 2. Integration Testing

```cangjie
// Good design - Integration tests
@Test
public func testUserWorkflow(): Unit {
    let testKit = TestKit.create()
    let userSystem = testKit.spawn(UserSystemActor.props())
    let probe = testKit.createTestProbe<Message>()

    // Test complete user workflow
    userSystem.tell(CreateUserMessage("user1", "John", "john@example.com"), probe.ref)
    probe.expectMessage<UserCreatedResponse>(Duration.fromSeconds(3))

    userSystem.tell(UpdateUserMessage("user1", UserData("John Doe", "john.doe@example.com")), probe.ref)
    probe.expectMessage<UserUpdatedResponse>(Duration.fromSeconds(3))

    userSystem.tell(DeleteUserMessage("user1"), probe.ref)
    probe.expectMessage<UserDeletedResponse>(Duration.fromSeconds(3))
}
```

## Monitoring Best Practices

### 1. Key Metrics Monitoring

```cangjie
// Good design - Built-in monitoring
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

            // Report metrics
            if (messageCount % 1000 == 0) {
                reportMetrics()
            }

            return result
        } catch (e: Exception) {
            // Report error metrics
            reportError(e)
            throw e
        }
    }

    private func reportMetrics(): Unit {
        let avgProcessingTime = processingTime.toMilliseconds() / messageCount
        println("Actor metrics - Message count: ${messageCount}, Average processing time: ${avgProcessingTime}ms")
    }
}
```

### 2. Health Checks

```cangjie
// Good design - Health checks
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

## Security Best Practices

### 1. Message Validation

```cangjie
// Good design - Input validation
class SecureUserActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case createUser: CreateUserMessage =>
                if (validateUserInput(createUser)) {
                    processCreateUser(createUser)
                } else {
                    context.sender.tell(ValidationErrorResponse("Invalid user data"))
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

### 2. Authorization Control

```cangjie
// Good design - Authorization checks
class AuthorizedActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (!isAuthorized(context.sender, message)) {
            context.sender.tell(UnauthorizedResponse())
            return MessageResult.Handled
        }

        processAuthorizedMessage(message)
    }

    private func isAuthorized(sender: ActorRef, message: Message): Bool {
        // Implement authorization check logic
        return authService.checkPermission(sender, message)
    }
}
```

---

**CActor v7.0.0 - Build excellent systems by following best practices!**

*Last updated: 2026/05/06*
