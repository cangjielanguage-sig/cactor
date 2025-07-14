# 📝 CActor 代码风格指南

## 🎯 概述

本指南定义了CActor项目的代码风格标准，确保代码的一致性、可读性和可维护性。

## 📋 命名约定

### 类名 (PascalCase)
```cangjie
// ✅ 正确
class ActorSystem
class MessageProcessor
class ErrorHandlingManager

// ❌ 错误
class actorSystem
class message_processor
class errorhandlingmanager
```

### 接口名 (PascalCase)
```cangjie
// ✅ 正确
interface Actor
interface MessageHandler
interface ErrorHandlingStrategy

// ❌ 错误
interface actor
interface messageHandler
interface error_handling_strategy
```

### 函数名 (camelCase)
```cangjie
// ✅ 正确
func processMessage()
func handleError()
func createActorSystem()

// ❌ 错误
func ProcessMessage()
func handle_error()
func CreateActorSystem()
```

### 变量名 (camelCase)
```cangjie
// ✅ 正确
let messageCount: Int64
let processingTime: Duration
let errorHandlingStrategy: ErrorHandlingStrategy

// ❌ 错误
let MessageCount: Int64
let processing_time: Duration
let error_handling_strategy: ErrorHandlingStrategy
```

### 常量名 (SCREAMING_SNAKE_CASE)
```cangjie
// ✅ 正确
let MAX_MESSAGE_SIZE: Int64 = 1024
let DEFAULT_TIMEOUT: Duration = Duration.second * 30
let ACTOR_SYSTEM_NAME: String = "CActor"

// ❌ 错误
let maxMessageSize: Int64 = 1024
let defaultTimeout: Duration = Duration.second * 30
let actorSystemName: String = "CActor"
```

### 包名 (snake_case)
```cangjie
// ✅ 正确
package cactor.core.actor
package cactor.runtime.dispatcher
package cactor.patterns.ask

// ❌ 错误
package cactor.Core.Actor
package cactor.Runtime.Dispatcher
package cactor.Patterns.Ask
```

## 📝 注释规范

### 类注释
```cangjie
/**
 * Actor系统的核心实现
 * 
 * 负责管理Actor的生命周期、消息路由和系统配置。
 * 参考Akka设计模式，为仓颉语言量身定制。
 * 
 * @since 1.0.0
 * @author CActor Team
 */
class CActorSystemImpl <: ActorSystem {
    // 实现细节
}
```

### 函数注释
```cangjie
/**
 * 处理接收到的消息
 * 
 * @param message 要处理的消息，不能为null
 * @param context Actor上下文，包含发送者信息
 * @return 消息处理结果，表示是否成功处理
 * @throws CActorException 当消息处理失败时抛出
 */
func receive(message: Message, context: ActorContext): MessageResult {
    // 实现逻辑
}
```

### 行内注释
```cangjie
// 计算平均处理时间（毫秒）
let avgProcessingTime = totalTime / messageCount

// TODO: 实现更高效的消息路由算法
// FIXME: 修复并发访问时的竞态条件
// NOTE: 这里使用简化实现，生产环境需要优化
```

## 🏗️ 代码结构

### 文件组织
```cangjie
/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 文件功能描述
 * 详细说明文件的作用和设计思路
 */
package cactor.core.actor

// 1. 标准库导入
import std.time.*
import std.collection.*

// 2. CActor核心模块导入
import cactor.core.message.*
import cactor.core.context.*

// 3. 第三方库导入（如果有）

// 4. 类型定义
public enum MessageResult {
    // 枚举值
}

// 5. 接口定义
public interface Actor {
    // 接口方法
}

// 6. 类实现
public class ActorImpl <: Actor {
    // 类实现
}
```

### 类内部结构
```cangjie
class ExampleActor <: Actor {
    // 1. 常量定义
    private static let MAX_RETRIES: Int32 = 3
    
    // 2. 属性定义
    private var messageCount: Int64 = 0
    private let startTime: Int64
    
    // 3. 构造函数
    public init() {
        this.startTime = System.currentTimeMillis()
    }
    
    // 4. 公共属性
    public prop name: String {
        get() { "ExampleActor" }
    }
    
    // 5. 公共方法
    public func receive(message: Message, context: ActorContext): MessageResult {
        // 实现
    }
    
    // 6. 私有方法
    private func processMessage(message: Message): Unit {
        // 实现
    }
}
```

## 🎨 格式化规范

### 缩进和空格
```cangjie
// ✅ 正确 - 使用4个空格缩进
class Actor {
    func receive(message: Message): MessageResult {
        match (message) {
            case ping: PingMessage =>
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

// ❌ 错误 - 使用Tab或不一致的缩进
class Actor {
	func receive(message: Message): MessageResult {
		// 错误的缩进
	}
}
```

### 行长度
```cangjie
// ✅ 正确 - 行长度不超过120字符
let result = processMessage(message, context, additionalParam)

// ✅ 正确 - 长行适当换行
let result = processMessage(
    message, 
    context, 
    additionalParam,
    anotherParam
)

// ❌ 错误 - 行过长
let result = processMessage(message, context, additionalParam, anotherParam, yetAnotherParam, andAnotherOne)
```

### 空行使用
```cangjie
class ExampleActor <: Actor {
    private var count: Int64 = 0
    
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
    
    private func handlePing(ping: PingMessage): Unit {
        // 处理ping消息
    }
    
    private func handlePong(pong: PongMessage): Unit {
        // 处理pong消息
    }
}
```

## 🔧 错误处理规范

### 异常处理
```cangjie
// ✅ 正确 - 具体的异常处理
try {
    processMessage(message)
} catch (e: CActorException) {
    // 处理CActor特定异常
    handleCActorError(e)
} catch (e: Exception) {
    // 处理通用异常
    handleGenericError(e)
}

// ❌ 错误 - 捕获所有异常但不处理
try {
    processMessage(message)
} catch (e: Exception) {
    // 空的异常处理
}
```

### 错误返回
```cangjie
// ✅ 正确 - 使用Result类型
func processMessage(message: Message): Result<Unit, CActorError> {
    if (message.isValid()) {
        // 处理消息
        Ok(())
    } else {
        Err(CActorError.InvalidMessage("消息格式无效"))
    }
}

// ✅ 正确 - 使用Option类型
func findActor(name: String): Option<ActorRef> {
    if (actors.containsKey(name)) {
        Some(actors[name])
    } else {
        None
    }
}
```

## 📊 性能规范

### 避免不必要的对象创建
```cangjie
// ✅ 正确 - 重用对象
class MessageProcessor {
    private let stringBuilder: StringBuilder = StringBuilder()
    
    func formatMessage(message: Message): String {
        stringBuilder.clear()
        stringBuilder.append("Message: ")
        stringBuilder.append(message.toString())
        stringBuilder.toString()
    }
}

// ❌ 错误 - 频繁创建对象
func formatMessage(message: Message): String {
    let sb = StringBuilder()  // 每次都创建新对象
    sb.append("Message: ")
    sb.append(message.toString())
    sb.toString()
}
```

### 高效的集合操作
```cangjie
// ✅ 正确 - 预分配容量
let messages = ArrayList<Message>(expectedSize)

// ✅ 正确 - 使用合适的集合类型
let actorIndex = HashMap<String, ActorRef>()  // 快速查找
let messageQueue = ArrayDeque<Message>()      // 高效队列操作

// ❌ 错误 - 使用不合适的集合类型
let actorIndex = ArrayList<Pair<String, ActorRef>>()  // 查找效率低
```

## 🧪 测试规范

### 测试命名
```cangjie
// ✅ 正确 - 描述性的测试名称
@Test
func shouldReturnHandledWhenReceivingValidMessage(): Unit {
    // 测试实现
}

@Test
func shouldThrowExceptionWhenMessageIsNull(): Unit {
    // 测试实现
}

// ❌ 错误 - 不清晰的测试名称
@Test
func test1(): Unit {
    // 测试实现
}

@Test
func messageTest(): Unit {
    // 测试实现
}
```

### 测试结构
```cangjie
@Test
func shouldProcessMessageCorrectly(): Unit {
    // Arrange - 准备测试数据
    let actor = TestActor()
    let message = PingMessage("test")
    let context = MockActorContext()
    
    // Act - 执行测试操作
    let result = actor.receive(message, context)
    
    // Assert - 验证结果
    assertEquals(MessageResult.Handled, result)
    assertEquals(1, actor.getProcessedCount())
}
```

## 📚 文档规范

### README文件
- 每个模块都应该有README.md文件
- 包含模块的功能描述、使用示例和API文档链接
- 使用中文编写，保持简洁明了

### API文档
- 所有公共接口都必须有完整的文档注释
- 包含参数说明、返回值说明和使用示例
- 标注版本信息和作者信息

### 变更日志
- 记录所有重要的变更
- 按版本组织，包含新功能、修复和破坏性变更
- 使用语义化版本号

## 🔍 代码审查清单

### 提交前检查
- [ ] 代码符合命名约定
- [ ] 添加了必要的注释和文档
- [ ] 包含相应的单元测试
- [ ] 通过所有现有测试
- [ ] 性能没有明显下降
- [ ] 没有引入新的编译警告
- [ ] 遵循错误处理规范

### 审查要点
- [ ] 代码逻辑清晰易懂
- [ ] 没有重复代码
- [ ] 异常处理完善
- [ ] 资源管理正确
- [ ] 线程安全考虑
- [ ] 边界条件处理

## 🛠️ 工具支持

### 格式化工具
```bash
# 使用仓颉格式化工具（如果可用）
cj fmt src/

# 检查代码风格
cj lint src/
```

### 静态分析
```bash
# 运行静态分析工具
cj analyze src/

# 检查潜在问题
cj check src/
```

---

**注意**: 本指南是活文档，会根据项目发展和团队反馈持续更新。所有团队成员都应该遵循这些规范，确保代码质量和一致性。
