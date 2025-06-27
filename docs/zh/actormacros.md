# CActor宏系统API设计与实现计划

## 🎯 项目目标

基于CActor代码库的深度分析，设计并实现一套完整的Actor宏系统，提供简洁、类型安全、高性能的Actor编程体验。

## 📋 核心设计原则

### 1. Class到Actor的无缝转换
- **零侵入转换**：普通class通过@actor宏自动转换为Actor
- **保持原有结构**：不破坏class的原有方法和属性
- **智能接口实现**：自动实现Actor接口的所有必需方法
- **可选扩展**：支持用户自定义消息处理和生命周期方法

### 2. 基于CActor架构
- **完全兼容**：与CActor的6层架构完美集成
- **类型安全**：基于CActor的类型系统，提供编译时检查
- **性能优化**：宏在编译时展开，零运行时开销

### 3. 参考主流框架
- **Akka模式**：借鉴Akka的Actor转换和宏设计
- **Actix风格**：参考Actix的derive宏和自动实现
- **仓颉特性**：充分利用仓颉语言的宏系统能力

### 4. 渐进式实现
- **基础优先**：先实现核心转换功能，确保稳定性
- **逐步扩展**：根据需求逐步添加高级功能
- **向后兼容**：保持API的向后兼容性

### 5. 智能化设计
- **约定优于配置**：提供合理的默认行为
- **可选覆盖**：用户可以通过定义特定方法来覆盖默认行为
- **自动发现**：宏能够自动发现和调用用户定义的方法

## 🏗️ 宏系统架构

### 宏包结构
```
src/macros/
└── actor_dsl_macros.cj    # 主要宏定义文件
    ├── 核心Actor宏         # @actor, @props, @actor_system
    ├── 消息处理宏          # @message_handler, @message_route
    ├── 生命周期宏          # @lifecycle, @state_machine
    ├── 路由宏              # @router, @routing_strategy
    ├── 性能监控宏          # @perf_monitor, @benchmark
    ├── 错误处理宏          # @safe_execute, @retry
    ├── 日志宏              # @log_info, @log_debug, @log_error
    └── 代码生成宏          # @repeat_times, @generate
```

## � Class到Actor转换机制

### 转换原理

@actor宏的核心功能是将普通的仓颉class自动转换为符合CActor架构的Actor。这个转换过程包括：

1. **接口实现**：自动为class添加`<: Actor`继承
2. **必需方法生成**：自动实现Actor接口要求的所有方法
3. **智能方法调用**：检测并调用用户定义的可选方法
4. **默认行为提供**：为未定义的方法提供合理的默认实现

### 转换前后对比

**转换前（普通class）**：
```cangjie
class WorkerClass {
    private var taskCount: Int32 = 0

    public init() {}

    public func processTask(task: String): Unit {
        taskCount += 1
        println("处理任务: ${task}")
    }
}
```

**转换后（Actor）**：
```cangjie
@actor
class WorkerClass {
    private var taskCount: Int32 = 0

    public init() {}

    public func processTask(task: String): Unit {
        taskCount += 1
        println("处理任务: ${task}")
    }

    // 可选：自定义消息处理
    public func handleMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: TaskMessage =>
                processTask(msg.getTask())
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

// 现在可以作为Actor使用
let system = CActor.system()
let worker = system.actorOf({ => WorkerClass() }, "worker")
worker.tell(TaskMessage("重要任务"))
```

### 智能方法发现

@actor宏会自动检测class中是否定义了以下可选方法：

| 方法名 | 调用时机 | 默认行为 |
|--------|----------|----------|
| `handleMessage(Message, ActorContext): MessageResult` | 收到消息时 | 处理常见消息类型 |
| `onStart(): Unit` | Actor启动前 | 输出启动日志 |
| `onStarted(): Unit` | Actor启动后 | 输出启动完成日志 |
| `onStop(): Unit` | Actor停止前 | 输出停止日志 |
| `onStopped(): Unit` | Actor停止后 | 输出停止完成日志 |
| `onRestart(Exception): Unit` | Actor重启时 | 调用onStop和onStart |

### 自动生成的Actor属性

```cangjie
// 自动生成的name属性
public prop name: String {
    get() { this.getClass().getSimpleName() }  // 使用class名称
}

// 自动生成的description属性
public prop description: String {
    get() { "由@actor宏转换的Actor: ${name}" }
}
```

## �🔧 核心宏API设计

### 1. Actor转换宏

#### `@actor`
**功能**：将普通class自动转换为Actor，实现Actor接口并提供完整的Actor功能

**核心特性**：
- **自动接口实现**：为普通class自动实现Actor接口
- **智能消息处理**：提供默认消息处理逻辑，支持自定义扩展
- **生命周期管理**：自动添加完整的Actor生命周期支持
- **零侵入转换**：不需要修改原有class结构

**使用方式**：
```cangjie
// 普通class自动转换为Actor
@actor
class MyWorker {
    private var taskCount: Int32 = 0

    public init() {}

    // 可选：自定义消息处理方法
    public func handleMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: WorkMessage =>
                taskCount += 1
                println("处理工作任务 #${taskCount}: ${msg.getTask()}")
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }

    // 可选：自定义生命周期钩子
    public func onStart(): Unit {
        println("Worker开始工作")
    }

    public func onStop(): Unit {
        println("Worker停止工作，共完成${taskCount}个任务")
    }
}

// 转换后可以直接作为Actor使用
let system = CActor.system("WorkerSystem")
let worker = system.actorOf({ => MyWorker() }, "worker1")
worker.tell(WorkMessage("处理数据"))
```

**自动生成功能**：
- **Actor接口实现**：自动实现name、description、receive等必需方法
- **默认消息处理**：提供StringMessage、PingMessage等常见消息的默认处理
- **生命周期支持**：自动实现preStart、postStart、preStop、postStop方法
- **智能方法调用**：如果class中定义了handleMessage、onStart等方法，自动调用
- **调试信息**：自动添加生命周期和消息处理的调试输出

**实现计划**：
```cangjie
public macro actor(input: Tokens): Tokens {
    return quote(
        $(input) <: Actor  // 自动实现Actor接口

        extend ActorImplementation {
            // 自动生成Actor必需属性
            public prop name: String {
                get() { this.getClass().getSimpleName() }
            }

            public prop description: String {
                get() { "由@actor宏转换的Actor: ${name}" }
            }

            // 智能消息处理
            public func receive(message: Message, context: ActorContext): MessageResult {
                // 优先调用用户定义的handleMessage方法
                if (this.hasMethod("handleMessage")) {
                    return this.handleMessage(message, context)
                }

                // 提供默认消息处理
                match (message) {
                    case msg: StringMessage =>
                        println("[ACTOR] ${name} 处理字符串: ${msg.getContent()}")
                        MessageResult.Handled
                    case msg: PingMessage =>
                        println("[ACTOR] ${name} 处理Ping")
                        MessageResult.Handled
                    case _ =>
                        println("[ACTOR] ${name} 未知消息: ${message.messageType()}")
                        MessageResult.Unhandled
                }
            }

            // 智能生命周期管理
            public func preStart(): Unit {
                println("[LIFECYCLE] ${name} 启动前")
                if (this.hasMethod("onStart")) {
                    this.onStart()
                }
            }

            public func preStop(): Unit {
                println("[LIFECYCLE] ${name} 停止前")
                if (this.hasMethod("onStop")) {
                    this.onStop()
                }
            }
        }
    )
}
```

### 2. Props创建宏

#### `@props`
**功能**：简化Props创建，自动应用最佳实践配置

**使用方式**：
```cangjie
// 基础Props创建
let props = @props(() -> MyActor())

// 带配置的Props创建
let props = @props(() -> MyActor()) {
    dispatcher = "high-performance"
    mailbox = "bounded-1000"
    supervision = "restart"
}
```

**实现计划**：
```cangjie
public macro props(creator: Tokens): Tokens {
    return quote(
        {
            import cactor.core.actor.{PropsFactory}
            let baseProps = PropsFactory.create($(creator))
            
            // 自动应用性能优化配置
            baseProps
                .withDispatcher("optimized-dispatcher")
                .withMailbox("high-performance-mailbox")
                .withSupervisionStrategy("smart-restart")
        }
    )
}
```

### 3. ActorSystem创建宏

#### `@actor_system`
**功能**：简化ActorSystem创建，自动应用企业级配置

**使用方式**：
```cangjie
// 基础系统创建
let system = @actor_system("MySystem")

// 生产环境系统创建
let system = @actor_system("ProductionSystem") {
    monitoring = true
    persistence = true
    clustering = true
}
```

**实现计划**：
```cangjie
public macro actor_system(name: Tokens): Tokens {
    return quote(
        {
            import cactor.api.{CActor}
            import cactor.runtime.{CActorRuntimeConfig}
            
            let config = CActorRuntimeConfig.createProduction()
                .withMonitoring(true)
                .withPerformanceOptimization(true)
                .withErrorReporting(true)
            
            CActor.system($(name), config)
        }
    )
}
```

### 4. 消息处理宏

#### `@message_handler`
**功能**：自动生成类型安全的消息分发逻辑

**使用方式**：
```cangjie
@message_handler
class MessageProcessor {
    @handle(StringMessage)
    func handleString(msg: StringMessage, context: ActorContext): MessageResult {
        println("处理字符串: ${msg.getContent()}")
        MessageResult.Handled
    }
    
    @handle(PingMessage)
    func handlePing(msg: PingMessage, context: ActorContext): MessageResult {
        println("处理Ping: ${msg.getId()}")
        MessageResult.Handled
    }
}
```

**实现计划**：
```cangjie
public macro message_handler(input: Tokens): Tokens {
    return quote(
        $(input)
        
        extend MessageHandlerEnhanced {
            public func dispatchMessage(message: Message, context: ActorContext): MessageResult {
                match (message) {
                    case msg: StringMessage => handleString(msg, context)
                    case msg: PingMessage => handlePing(msg, context)
                    case msg: StopMessage => handleStop(msg, context)
                    case _ => handleUnknown(message, context)
                }
            }
            
            private func handleUnknown(message: Message, context: ActorContext): MessageResult {
                println("[HANDLER] 未知消息类型: ${message.messageType()}")
                MessageResult.Unhandled
            }
        }
    )
}
```

#### `@message_route`
**功能**：智能消息路由，支持性能监控和错误处理

**使用方式**：
```cangjie
let result = @message_route(message, context) {
    StringMessage => handleStringMessage
    PingMessage => handlePingMessage
    _ => handleDefault
}
```

### 5. 生命周期管理宏

#### `@lifecycle`
**功能**：增强Actor生命周期管理，自动添加监控和日志

**使用方式**：
```cangjie
@lifecycle
class LifecycleActor <: Actor {
    @on_start
    func initialize(): Unit {
        println("Actor初始化")
    }
    
    @on_stop
    func cleanup(): Unit {
        println("Actor清理")
    }
    
    @on_restart(reason: Exception)
    func handleRestart(): Unit {
        println("Actor重启: ${reason}")
    }
}
```

#### `@state_machine`
**功能**：基于CActor.ActorLifecycleState的状态机实现

**使用方式**：
```cangjie
@state_machine
class WorkflowActor <: Actor {
    @state("INITIAL")
    func handleInitial(): Unit { }
    
    @state("PROCESSING")
    func handleProcessing(): Unit { }
    
    @state("COMPLETED")
    func handleCompleted(): Unit { }
    
    @transition("INITIAL" -> "PROCESSING")
    func startProcessing(): Bool { true }
}
```

### 6. 路由宏系统

#### `@router`
**功能**：基于CActor.patterns.routing的智能路由

**使用方式**：
```cangjie
@router(strategy = "round_robin")
class LoadBalancer {
    @routee
    let workers: Array<ActorRef> = []
    
    @route_method
    func distributeWork(message: WorkMessage): ActorRef {
        // 自动生成的路由逻辑
    }
}
```

### 7. 性能监控宏

#### `@perf_monitor`
**功能**：高精度性能监控，支持纳秒级测量

**使用方式**：
```cangjie
let result = @perf_monitor {
    // 需要监控的代码
    complexCalculation()
}

// 带阈值的性能监控
@perf_monitor(threshold = "1ms", alert = true)
func criticalOperation(): Unit {
    // 关键操作
}
```

#### `@benchmark`
**功能**：自动生成基准测试代码

**使用方式**：
```cangjie
@benchmark(iterations = 10000)
func messageProcessingBenchmark(): Unit {
    actor.tell(TestMessage())
}
```

### 8. 错误处理宏

#### `@safe_execute`
**功能**：企业级错误处理，支持重试和恢复

**使用方式**：
```cangjie
let result = @safe_execute(retries = 3, backoff = "exponential") {
    riskyOperation()
}
```

#### `@circuit_breaker`
**功能**：断路器模式实现

**使用方式**：
```cangjie
@circuit_breaker(failure_threshold = 5, timeout = "30s")
func externalServiceCall(): Result<String> {
    // 外部服务调用
}
```

### 9. 日志宏系统

#### 结构化日志宏
**功能**：提供结构化、高性能的日志输出

**使用方式**：
```cangjie
@log_info("用户登录", user_id = userId, timestamp = now())
@log_error("处理失败", error = e.toString(), context = context.path)
@log_debug("性能指标", duration = duration, throughput = tps)
```

### 10. 代码生成宏

#### `@repeat_times`
**功能**：重复代码生成，支持循环和批处理

**使用方式**：
```cangjie
@repeat_times(count = 1000)
{
    actor.tell(BenchmarkMessage())
}
```

#### `@generate_actors`
**功能**：批量生成Actor实例

**使用方式**：
```cangjie
let actors = @generate_actors(count = 100, type = WorkerActor) {
    name_pattern = "worker-{}"
    dispatcher = "worker-pool"
}
```

## 🚀 实施计划

### 阶段1：核心宏实现（1-2周）
1. **@actor宏**：基础Actor增强功能
2. **@props宏**：Props创建简化
3. **@actor_system宏**：ActorSystem创建优化
4. **基础日志宏**：@log_info, @log_debug, @log_error
5. **性能监控宏**：@perf_monitor基础版本

### 阶段2：消息处理宏（2-3周）
1. **@message_handler宏**：消息分发自动化
2. **@message_route宏**：智能消息路由
3. **错误处理宏**：@safe_execute, @retry
4. **状态机宏**：@state_machine基础版本

### 阶段3：高级功能宏（3-4周）
1. **@router宏**：完整路由系统
2. **@lifecycle宏**：生命周期管理
3. **@benchmark宏**：性能基准测试
4. **@circuit_breaker宏**：断路器模式

### 阶段4：企业级特性（4-6周）
1. **分布式宏**：@distributed_actor, @cluster
2. **持久化宏**：@persistent_actor, @event_sourcing
3. **监控集成宏**：@metrics, @tracing
4. **配置宏**：@config, @environment

## 📊 预期效果

### 开发效率提升
- **代码减少70%**：通过宏自动生成样板代码
- **错误减少50%**：编译时类型检查和最佳实践
- **开发速度提升3倍**：简化的API和智能默认配置

### 性能优化
- **零运行时开销**：宏在编译时展开
- **自动优化**：应用性能最佳实践
- **智能监控**：自动添加性能监控代码

### 代码质量
- **类型安全**：编译时类型检查
- **一致性**：统一的代码风格和模式
- **可维护性**：清晰的宏API和文档

## 🎯 成功指标

1. **编译成功率**：100%的宏编译成功
2. **性能基准**：与手写代码性能相当或更优
3. **开发体验**：简化API使用，减少样板代码
4. **错误处理**：完善的错误信息和调试支持
5. **文档完整性**：每个宏都有详细文档和示例

通过这套宏系统，CActor将成为仓颉语言最强大、最易用的Actor框架，为开发者提供企业级的Actor编程体验。

## 📝 详细实现示例

### 1. @actor宏的完整实现

```cangjie
/**
 * Actor增强宏 - 完整实现版本
 */
public macro actor(input: Tokens): Tokens {
    return quote(
        $(input)

        // 为Actor类添加扩展功能
        extend MacroEnhancedActor {
            // 统计信息
            private var _macroMessageCount: Int64 = 0
            private var _macroErrorCount: Int64 = 0
            private var _macroStartTime: Int64 = 0
            private var _macroLastMessageTime: Int64 = 0

            // 性能监控
            private var _macroTotalProcessingTime: Int64 = 0
            private var _macroMaxProcessingTime: Int64 = 0
            private var _macroMinProcessingTime: Int64 = Int64.MAX_VALUE

            // 增强的消息处理
            public func macroEnhancedReceive(message: Message, context: ActorContext): MessageResult {
                let startTime = System.nanoTime()
                _macroMessageCount += 1
                _macroLastMessageTime = startTime

                try {
                    // 调用原始的receive方法
                    let result = receive(message, context)

                    // 计算处理时间
                    let processingTime = System.nanoTime() - startTime
                    _macroTotalProcessingTime += processingTime

                    if (processingTime > _macroMaxProcessingTime) {
                        _macroMaxProcessingTime = processingTime
                    }
                    if (processingTime < _macroMinProcessingTime) {
                        _macroMinProcessingTime = processingTime
                    }

                    // 性能警告
                    if (processingTime > 1000000) { // 超过1ms
                        println("[MACRO-PERF-WARN] ${name} 消息处理耗时: ${processingTime}ns")
                    }

                    return result
                } catch (e: Exception) {
                    _macroErrorCount += 1
                    println("[MACRO-ERROR] ${name} 消息处理失败: ${e.toString()}")
                    return MessageResult.Escalate
                }
            }

            // 统计信息获取
            public func getMacroStats(): String {
                let avgTime = if (_macroMessageCount > 0) {
                    _macroTotalProcessingTime / _macroMessageCount
                } else { 0 }

                return """
                [MACRO-STATS] Actor: ${name}
                  消息数量: ${_macroMessageCount}
                  错误数量: ${_macroErrorCount}
                  平均处理时间: ${avgTime}ns
                  最大处理时间: ${_macroMaxProcessingTime}ns
                  最小处理时间: ${_macroMinProcessingTime}ns
                """
            }

            // 增强的生命周期方法
            public func macroPreStart(): Unit {
                _macroStartTime = System.nanoTime()
                println("[MACRO-LIFECYCLE] ${name} 启动前初始化")
                preStart()
                println("[MACRO-LIFECYCLE] ${name} 启动前初始化完成")
            }

            public func macroPostStart(): Unit {
                println("[MACRO-LIFECYCLE] ${name} 启动后处理")
                postStart()
                println("[MACRO-LIFECYCLE] ${name} 启动完成，运行时间: ${System.nanoTime() - _macroStartTime}ns")
            }

            public func macroPreStop(): Unit {
                let runTime = System.nanoTime() - _macroStartTime
                println("[MACRO-LIFECYCLE] ${name} 停止前清理")
                println("[MACRO-LIFECYCLE] ${name} 运行统计: 处理${_macroMessageCount}条消息，运行${runTime}ns")
                preStop()
            }

            public func macroPostStop(): Unit {
                println("[MACRO-LIFECYCLE] ${name} 停止完成")
                postStop()
            }
        }
    )
}
```

### 2. 完整的使用示例

```cangjie
// 1. 定义增强的Actor
@actor
class EnhancedWorkerActor <: Actor {
    public prop name: String { get() { "EnhancedWorker" } }
    public prop description: String { get() { "使用宏增强的工作Actor" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                // 使用宏增强的日志
                @log_info("处理字符串消息: ${msg.getContent()}")

                // 使用宏增强的性能监控
                @perf_monitor {
                    // 模拟复杂处理
                    var result = 0
                    for (i in 0..1000) {
                        result += i * i
                    }
                }

                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

// 2. 创建Actor系统和Actor
main(): Int64 {
    // 使用宏创建优化的Actor系统
    let system = @actor_system("MacroTestSystem")

    // 使用宏创建优化的Props
    let props = @props(() -> EnhancedWorkerActor())

    // 创建Actor
    let actor = system.actorOf(props, "enhancedWorker")

    // 发送测试消息
    for (i in 0..100) {
        actor.tell(StringMessage("测试消息 ${i}"))
    }

    // 等待处理完成
    Thread.sleep(1000)

    // 获取宏增强的统计信息
    if (let enhancedActor = actor as? MacroEnhancedActor) {
        println(enhancedActor.getMacroStats())
    }

    system.terminate()
    return 0
}
```

### 3. 宏编译配置

在`cjpm.toml`中配置宏包：

```toml
# 宏包配置
[package.package-configuration."cactor.macros"]
output-type = "dynamic"  # 宏包必须编译为动态库
macro-export = true      # 导出宏定义

# 依赖配置
[package.dependencies]
"cactor.macros" = { path = "src/macros" }
```

### 4. 宏测试框架

```cangjie
// 宏功能测试
package cactor.macros.test

import cactor.macros.*

@test
func testActorMacro(): Unit {
    @actor
    class TestActor <: Actor {
        public prop name: String { get() { "TestActor" } }
        public prop description: String { get() { "测试Actor" } }

        public func receive(message: Message, context: ActorContext): MessageResult {
            MessageResult.Handled
        }
    }

    let actor = TestActor()
    assert(actor.name == "TestActor")

    // 验证宏增强功能
    assert(actor is MacroEnhancedActor)
}

@test
func testPerfMonitorMacro(): Unit {
    var executed = false

    let result = @perf_monitor {
        executed = true
        "test result"
    }

    assert(executed)
    assert(result == "test result")
}
```

## 🔧 技术实现细节

### 宏编译流程
1. **宏定义解析**：解析宏定义和参数
2. **AST生成**：生成抽象语法树
3. **代码展开**：将宏展开为具体代码
4. **类型检查**：验证生成代码的类型安全性
5. **优化**：应用编译器优化
6. **动态库生成**：编译为动态库供运行时使用

### 错误处理机制
- **编译时错误**：宏定义错误、类型不匹配等
- **运行时错误**：宏生成代码的运行时异常
- **调试支持**：宏展开的调试信息和错误追踪

### 性能优化策略
- **零运行时开销**：宏在编译时完全展开
- **内联优化**：生成的代码支持内联优化
- **缓存机制**：宏展开结果缓存，避免重复计算

## 📚 最佳实践指南

### 1. 宏使用原则
- **适度使用**：只在能显著简化代码时使用宏
- **清晰命名**：宏名称要清晰表达功能
- **文档完整**：为每个宏提供详细文档和示例

### 2. 性能考虑
- **编译时优化**：尽量在编译时完成工作
- **运行时最小化**：减少宏生成代码的运行时开销
- **内存效率**：避免不必要的内存分配

### 3. 调试技巧
- **宏展开查看**：使用编译器选项查看宏展开结果
- **分步调试**：逐步验证宏的各个功能
- **单元测试**：为宏功能编写完整的单元测试

通过这套完整的宏系统，CActor不仅提供了强大的Actor编程能力，还通过宏的自动化和优化，显著提升了开发效率和代码质量。
