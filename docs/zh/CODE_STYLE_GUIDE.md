# CActor 代码风格指南

## 目标概述

本指南定义了CActor项目的代码风格标准，确保代码的一致性、可读性和可维护性。所有团队成员都应遵循这些规范，以维护高质量的代码库。

## 命名约定

### 类名 (PascalCase)

类名采用PascalCase命名法，即每个单词的首字母大写，不使用下划线分隔。

```cangjie
// 正确示例
class ActorSystem
class MessageProcessor
class ErrorHandlingManager

// 错误示例
class actorSystem
class message_processor
class errorhandlingmanager
```

**命名原则：**
- 类名应该清晰表达其职责和用途
- 使用名词或名词短语
- 避免使用缩写（除非是广为人知的标准缩写）
- 复合词中每个组成部分都应首字母大写

### 接口名 (PascalCase)

接口名同样采用PascalCase命名法。建议接口名以"I"开头或使用名词形式明确表示其能力。

```cangjie
// 正确示例
interface Actor
interface MessageHandler
interface ErrorHandlingStrategy
interface IActorFactory

// 错误示例
interface actor
interface messageHandler
interface error_handling_strategy
```

**命名原则：**
- 使用能够描述行为的名称
- 通常以-able、-er或名词形式结尾
- 保持简洁且具有描述性

### 函数名 (camelCase)

函数名采用camelCase命名法，即首字母小写，后续单词首字母大写。

```cangjie
// 正确示例
func processMessage()
func handleError()
func createActorSystem()
func getActorByName()

// 错误示例
func ProcessMessage()
func handle_error()
func CreateActorSystem()
```

**命名原则：**
- 使用动词或动词短语
- 保持简洁，描述函数的主要行为
-布尔值函数可以使用is、has、should等前缀

### 变量名 (camelCase)

变量名采用camelCase命名法，与函数名遵循相同的规则。

```cangjie
// 正确示例
let messageCount: Int64
let processingTime: Duration
let errorHandlingStrategy: ErrorHandlingStrategy
let isActive: Bool
let actorList: ArrayList<ActorRef>

// 错误示例
let MessageCount: Int64
let processing_time: Duration
let error_handaling_strategy: ErrorHandlingStrategy
```

**命名原则：**
- 使用描述性的名称，避免单字母命名（循环变量除外）
- 布尔变量使用is、has、can等前缀
- 集合类型变量使用复数形式或包含元素类型信息

### 常量名 (SCREAMING_SNAKE_CASE)

常量名采用全大写字母，单词之间用下划线分隔。

```cangjie
// 正确示例
let MAX_MESSAGE_SIZE: Int64 = 1024
let DEFAULT_TIMEOUT: Duration = Duration.second * 30
let ACTOR_SYSTEM_NAME: String = "CActor"
let MIN_POOL_SIZE: Int32 = 4

// 错误示例
let maxMessageSize: Int64 = 1024
let defaultTimeout: Duration = Duration.second * 30
let actorSystemName: String = "CActor"
```

**命名原则：**
- 常量应该表示不会改变的值
- 使用描述性的名称，说明常量的用途
- 相关常量可以使用相同的前缀进行分组

### 包名 (snake_case)

包名采用全小写字母，单词之间用点号分隔。

```cangjie
// 正确示例
package cactor.core.actor
package cactor.runtime.dispatcher
package cactor.patterns.ask
package cactor.utils.collection

// 错误示例
package cactor.Core.Actor
package cactor.Runtime.Dispatcher
package cactor.Patterns.Ask
```

**命名原则：**
- 使用简洁的包名，避免冗余
- 按功能模块组织包结构
- 包名应该反映模块的职责

## 注释规范

### 类注释

每个公共类都应包含文档注释，说明类的用途、设计决策和使用注意事项。

```cangjie
/**
 * Actor系统的核心实现
 *
 * 负责管理Actor的生命周期、消息路由和系统配置。
 * 参考Akka设计模式，为仓颉语言量身定制的高性能Actor框架。
 *
 * @since 1.0.0
 * @author CActor Team
 */
class CActorSystemImpl <: ActorSystem {
    // 实现细节
}
```

**注释要素：**
- 简短的描述说明类的职责
- 关键设计决策的说明
- 使用示例（可选）
- 版本信息和作者信息

### 函数注释

公共方法应包含详细的文档注释，说明参数、返回值和可能的异常。

```cangjie
/**
 * 处理接收到的消息
 *
 * @param message 要处理的消息，不能为null
 * @param context Actor上下文，包含发送者信息和当前状态
 * @return 消息处理结果，表示是否成功处理
 * @throws CActorException 当消息处理失败时抛出，包括系统错误和超时情况
 * @throws IllegalArgumentException 当message参数为null时抛出
 */
func receive(message: Message, context: ActorContext): MessageResult {
    // 实现逻辑
}
```

**注释要素：**
- 函数的功能描述
- 所有参数的说明，包括约束条件
- 返回值的说明
- 可能抛出的异常及其触发条件
- 使用示例（对于复杂方法）

### 行内注释

使用行内注释解释复杂的逻辑、标记待办事项或说明非显而易见的实现细节。

```cangjie
// 计算平均处理时间（毫秒）
let avgProcessingTime = totalTime / messageCount

// TODO: 实现更高效的消息路由算法
// FIXME: 修复并发访问时的竞态条件
// NOTE: 这里使用简化实现，生产环境需要优化
// HACK: 临时解决方案，需要在下一版本中重构
```

**注释原则：**
- 注释应该解释"为什么"，而不是"是什么"
- 保持注释与代码同步更新
- 使用标准的标记（TODO、FIXME、NOTE等）便于搜索
- 避免显而易见的注释

## 代码结构

### 文件组织

每个文件应该按照特定的顺序组织内容，以确保代码结构清晰一致。

```cangjie
/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 消息处理模块
 * 提供消息的序列化和反序列化功能，支持多种消息类型
 */

// 1. 标准库导入
import std.time.*
import std.collection.*
import std.convert.*

// 2. CActor核心模块导入
import cactor.core.message.*
import cactor.core.context.*
import cactor.core.actor.*

// 3. 第三方库导入（如果有）

// 4. 类型定义
public enum MessageResult {
    Success
    Failure
    Pending
}

public typealias MessageHandler = Func<Message, MessageResult>

// 5. 接口定义
public interface Actor {
    // 接口方法
    func receive(message: Message, context: ActorContext): MessageResult
    func getName(): String
}

// 6. 类实现
public class ActorImpl <: Actor {
    // 类实现
}
```

**组织原则：**
- 版权信息放在文件开头
- 导入部分按标准库、项目内部、第三方顺序排列
- 类型定义在接口和类之前
- 相关代码块之间用空行分隔

### 类内部结构

类内部的代码应按照以下顺序组织：

```cangjie
class ExampleActor <: Actor {
    // 1. 常量定义
    private static let MAX_RETRIES: Int32 = 3
    private static let DEFAULT_TIMEOUT_MS: Int64 = 5000

    // 2. 静态属性
    private static var instanceCount: Int64 = 0

    // 3. 实例属性
    private var messageCount: Int64 = 0
    private let startTime: Int64
    private let actorRef: ActorRef

    // 4. 构造函数
    public init(name: String) {
        this.startTime = System.currentTimeMillis()
        this.actorRef = ActorRef(name)
        ExampleActor.instanceCount += 1
    }

    // 5. 公共属性
    public prop name: String {
        get() { "ExampleActor" }
    }

    public prop count: Int64 {
        get() { this.messageCount }
    }

    // 6. 公共方法
    public func receive(message: Message, context: ActorContext): MessageResult {
        messageCount += 1
        // 实现
    }

    public func reset(): Unit {
        messageCount = 0
    }

    // 7. 私有方法
    private func processMessage(message: Message): Unit {
        // 实现
    }

    private func validateMessage(message: Message): Bool {
        // 实现
    }

    // 8. 内部类（如果有）
    private class MessageProcessor { }
}
```

## 格式化规范

### 缩进和空格

使用4个空格进行缩进，不要使用Tab字符。保持一致的缩进风格对于代码可读性至关重要。

```cangjie
// 正确示例 - 使用4个空格缩进
class Actor {
    func receive(message: Message): MessageResult {
        match (message) {
            case ping: PingMessage =>
                handlePing(ping)
                MessageResult.Handled
            case pong: PongMessage =>
                handlePong(pong)
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    private func handlePing(ping: PingMessage): Unit {
        // 处理ping消息
    }
}

// 错误示例 - 使用Tab或不一致的缩进
class Actor {
	func receive(message: Message): MessageResult {
		match (message) {
			case ping: PingMessage =>
				MessageResult.Handled
		}
	}
}
```

**空格使用规则：**
- 二元运算符两侧各加一个空格
- 一元运算符与操作数之间不加空格
- 逗号后加一个空格
- 分号后不加空格（单行多条语句时）
- 关键字与左括号之间加一个空格
- 函数调用时函数名与左括号之间不加空格

### 行长度

每行代码不应超过120个字符。长行应该适当换行以提高可读性。

```cangjie
// 正确示例 - 行长度不超过120字符
let result = processMessage(message, context, additionalParam)

// 正确示例 - 长行适当换行
let result = processMessage(
    message,
    context,
    additionalParam,
    anotherParam
)

// 正确示例 - 多层调用换行
let result = innerService
    .process()
    .transform()
    .finalize()

// 错误示例 - 行过长难以阅读
let result = processMessage(message, context, additionalParam, anotherParam, yetAnotherParam, andAnotherOne)
```

**换行原则：**
- 优先在逗号后换行
- 将子句或参数对齐
- 链式调用时每个方法调用单独一行

### 空行使用

合理使用空行分隔相关代码块，提高代码的视觉层次结构。

```cangjie
class ExampleActor <: Actor {
    private var count: Int64 = 0
    private var state: ActorState = ActorState.Idle

    // 构造函数与属性之间空一行
    public init() {
        this.count = 0
        this.state = ActorState.Idle
    }

    // 不同功能的方法组之间空一行
    public func receive(message: Message, context: ActorContext): MessageResult {
        count += 1

        match (message) {
            case ping: PingMessage =>
                handlePing(ping)
                MessageResult.Handled

            case pong: PongMessage =>
                handlePong(pong)
                MessageResult.Handled

            case _ =>
                MessageResult.Unhandled
        }
    }

    // 辅助方法之间空一行
    private func handlePing(ping: PingMessage): Unit {
        state = ActorState.Processing
        // 处理ping消息
    }

    private func handlePong(pong: PongMessage): Unit {
        state = ActorState.Idle
        // 处理pong消息
    }

    // 结束大括号前不加空行
}
```

**空行使用原则：**
- 命名空间导入后空一行
- 类成员分组之间空一行
- 函数定义前后空一行（可选）
- 逻辑相关代码紧凑排列
- 不要过度使用空行

## 错误处理规范

### 异常处理

使用具体的异常类型，避免捕获过于宽泛的异常。对捕获的异常进行适当处理或记录。

```cangjie
// 正确示例 - 具体的异常处理
try {
    processMessage(message)
} catch (e: CActorException) {
    // 处理CActor特定异常，记录错误并恢复
    logger.error("Actor执行失败: ${e.message}")
    handleCActorError(e)
} catch (e: IllegalArgumentException) {
    // 处理参数错误，这是程序员错误，记录并可能终止
    logger.warn("无效参数: ${e.message}")
    throw e
} catch (e: Exception) {
    // 处理其他意外异常
    logger.error("未知错误: ${e.message}")
    handleGenericError(e)
}

// 错误示例 - 捕获所有异常但不处理
try {
    processMessage(message)
} catch (e: Exception) {
    // 空的异常处理，隐藏了错误
}

// 错误示例 - 过宽的异常捕获
try {
    processMessage(message)
} catch (e) {
    // 不指定异常类型，难以定位问题
}
```

**异常处理原则：**
- 只捕获能够恢复的异常
- 不要捕获Exception而什么都不做
- 记录足够的上下文信息以便调试
- 在捕获异常后决定是恢复、转换还是重新抛出

### 错误返回

对于可预期的错误情况，优先使用Result或Option类型而不是抛出异常。这使错误处理更加显式和可控。

```cangjie
// 正确示例 - 使用Result类型
func processMessage(message: Message): Result<Unit, CActorError> {
    if (message.isValid()) {
        // 处理消息
        Ok(())
    } else {
        Err(CActorError.InvalidMessage("消息格式无效"))
    }
}

// 调用时处理Result
let result = processor.processMessage(message)
match (result) {
    case Ok(_) =>
        logger.info("消息处理成功")
    case Err(e) =>
        logger.error("处理失败: ${e.message}")
}

// 正确示例 - 使用Option类型
func findActor(name: String): Option<ActorRef> {
    if (actors.containsKey(name)) {
        Some(actors[name])
    } else {
        None
    }
}

// 调用时处理Option
match (findActor(name)) {
    case Some(actor) =>
        actor.send(message)
    case None =>
        logger.warn("Actor不存在: ${name}")
}

// 错误示例 - 使用异常处理可预期的情况
func findActor(name: String): ActorRef {
    if (!actors.containsKey(name)) {
        throw ActorNotFoundException(name)  // 应该返回Option
    }
    actors[name]
}
```

**错误返回原则：**
- 可预期错误使用Result/Option
- 不可预期错误使用异常
- 在函数签名中明确错误类型
- 避免返回null表示错误

## 性能规范

### 避免不必要的对象创建

在循环中或高频调用处频繁创建对象会产生垃圾回收压力。应该考虑对象重用或预分配。

```cangjie
// 正确示例 - 重用可变对象
class MessageProcessor {
    private let stringBuilder: StringBuilder = StringBuilder()

    func formatMessage(message: Message): String {
        stringBuilder.clear()
        stringBuilder.append("Message: ")
        stringBuilder.append(message.toString())
        stringBuilder.append(" | Time: ")
        stringBuilder.append(System.currentTimeMillis())
        stringBuilder.toString()
    }
}

// 正确示例 - 预分配集合容量
class BatchProcessor {
    private let buffer: ArrayList<Message> = ArrayList<Message>(1000)

    func addMessage(msg: Message): Unit {
        buffer.append(msg)
    }

    func processBatch(): ArrayList<Result> {
        let results = ArrayList<Result>(buffer.size())
        for (msg in buffer) {
            results.append(process(msg))
        }
        return results
    }
}

// 错误示例 - 循环中频繁创建对象
func formatMessages(messages: ArrayList<Message>): ArrayList<String> {
    let results = ArrayList<String>()
    for (msg in messages) {
        let sb = StringBuilder()  // 每次迭代都创建新对象
        sb.append("Message: ")
        sb.append(msg.toString())
        results.append(sb.toString())  // 创建大量临时字符串
    }
    results
}

// 错误示例 - 重复创建相同配置的对象
class RepeatedCreator {
    func process(): Unit {
        let config = Config(timeout: 5000)  // 每次调用都创建新配置
        // 使用配置
    }
}
```

**性能原则：**
- 在循环中避免创建临时对象
- 可变对象考虑在类中预分配
- 字符串拼接使用StringBuilder
- 集合类型考虑预分配容量

### 高效的集合操作

根据使用场景选择合适的集合类型，以获得最佳性能。

```cangjie
// 正确示例 - 根据访问模式选择集合
// 快速随机查找 - 使用HashMap
let actorIndex = HashMap<String, ActorRef>()

// 高效队列操作 - 使用ArrayDeque
let messageQueue = ArrayDeque<Message>()

// 有序遍历 - 使用ArrayList
let orderedActors = ArrayList<ActorRef>()

// 正确示例 - 预分配容量
let messages = ArrayList<Message>(expectedSize)
for (i in 0..expectedSize) {
    messages.append(createMessage(i))
}

// 正确示例 - 使用合适的方法
// 需要唯一性检查时使用HashSet
let actorNames = HashSet<String>()

// 需要按键分组时使用HashMap
let groupedMessages = HashMap<String, ArrayList<Message>>()

// 错误示例 - 使用不合适的集合类型
// 频繁查找但使用ArrayList
let actorIndex = ArrayList<Pair<String, ActorRef>>()  // O(n) 查找

// 错误示例 - 集合容量过小导致扩容
let results = ArrayList<String>()  // 未预分配，可能多次扩容
for (i in 0..10000) {
    results.append(process(i))
}
```

**集合选择原则：**
- 需要O(1)查找时使用HashMap/HashSet
- 需要保持插入顺序时使用LinkedHashMap
- 需要队列操作时使用ArrayDeque
- 高频随机访问时使用ArrayList
- 预估容量使用构造函数预分配

## 测试规范

### 测试命名

测试函数名应该清晰描述测试的场景和预期结果，便于理解测试意图。

```cangjie
// 正确示例 - 描述性的测试名称
@Test
func shouldReturnHandledWhenReceivingValidPingMessage(): Unit {
    // 测试实现
}

@Test
func shouldThrowExceptionWhenMessageIsNull(): Unit {
    // 测试实现
}

@Test
func shouldRetryThreeTimesOnTransientFailure(): Unit {
    // 测试实现
}

@Test
func shouldReturnEmptyListWhenNoActorsRegistered(): Unit {
    // 测试实现
}

// 错误示例 - 不清晰的测试名称
@Test
func test1(): Unit {
    // 测试实现
}

@Test
func messageTest(): Unit {
    // 测试实现
}

@Test
func testError(): Unit {
    // 测试实现
}
```

**命名原则：**
- 使用"should"或"when"前缀描述预期行为
- 包含输入条件和预期结果
- 便于快速定位测试失败原因
- 避免使用test1、test2等无意义名称

### 测试结构

采用Arrange-Act-Assert模式组织测试代码，使测试逻辑清晰易读。

```cangjie
@Test
func shouldProcessMessageCorrectly(): Unit {
    // Arrange - 准备测试数据和环境
    let actor = TestActor()
    let message = PingMessage("test", 123)
    let context = MockActorContext(sender: testRef)
    mockProcessor.expect().toReceive(message).andReturn(Ok(()))

    // Act - 执行被测试的操作
    let result = actor.receive(message, context)

    // Assert - 验证结果符合预期
    assertEquals(MessageResult.Handled, result)
    assertEquals(1, actor.getProcessedCount())
    assertTrue(mockProcessor.wasCalled())
}

@Test
func shouldHandleErrorGracefully(): Unit {
    // Arrange
    let actor = ErrorProneActor()
    let message = InvalidMessage("bad data")
    let context = MockActorContext()

    // Act
    let result = actor.receive(message, context)

    // Assert
    assertEquals(MessageResult.Failed, result)
    assertEquals(1, actor.getErrorCount())
    assertFalse(context.wasNotified())
}
```

**测试原则：**
- 每个测试只验证一个行为
- 测试之间相互独立
- 使用清晰的注释标记测试阶段
- 包含有意义的断言，不只是检查无异常

## 文档规范

### README文件

每个模块都应该有README.md文件，提供模块的概述和使用指南。

**README结构：**
```markdown
# 模块名称

## 功能概述
简要介绍模块的主要功能和用途。

## 核心组件
列出模块的主要类和接口。

## 使用示例
提供代码示例说明如何正确使用模块。

## 注意事项
列出使用时需要注意的事项和限制。
```

### API文档

所有公共接口都应该有完整的文档注释。

**API文档要素：**
- 功能描述
- 参数说明（名称、类型、约束）
- 返回值说明
- 异常说明
- 使用示例
- 版本信息

### 变更日志

使用语义化版本号，按版本组织变更记录。

**变更日志格式：**
```markdown
# 变更日志

## [版本号] - 日期

### 新功能
- 功能描述

### 改进
- 改进描述

### 修复
- 问题修复描述

### 破坏性变更
- 变更说明及迁移指南
```

## 代码审查清单

### 提交前检查

在提交代码之前，请确认已完成以下检查项：

- 代码符合命名约定（类、函数、变量、常量）
- 添加了必要的注释和文档
- 包含相应的单元测试
- 通过所有现有测试
- 性能没有明显下降
- 没有引入新的编译警告
- 遵循错误处理规范
- 没有硬编码敏感信息
- 代码格式符合规范

### 审查要点

代码审查时应关注以下方面：

- 代码逻辑清晰易懂，没有过度复杂化
- 没有重复代码，考虑提取公共逻辑
- 异常处理完善，不会隐藏错误
- 资源管理正确，关闭或释放资源
- 线程安全考虑周全，避免竞态条件
- 边界条件处理完整，避免数组越界等
- 安全漏洞排查，防止注入攻击等
- 可维护性检查，便于后续扩展

## 工具支持

### 格式化工具

使用仓颉格式化工具保持代码风格一致。

```bash
# 格式化源代码
cj fmt src/

# 检查代码风格
cj lint src/

# 格式化并覆盖原文件
cj fmt -w src/
```

### 静态分析

使用静态分析工具检测潜在问题。

```bash
# 运行静态分析工具
cj analyze src/

# 检查潜在问题
cj check src/

# 生成分析报告
cj analyze -o report.html src/
```

### 自动化检查

在CI/CD流程中集成代码质量检查。

```bash
# 运行完整检查
cj fmt --check src/ && cj analyze src/ && cj test
```

---

**注意**：本指南是活文档，会根据项目发展和团队反馈持续更新。所有团队成员都应该遵循这些规范，确保代码质量和一致性。如有任何问题或建议，请提交Issue或联系维护者。