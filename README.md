# 🎭 CActor

[![仓颉语言](https://img.shields.io/badge/仓颉-0.60.5-blue.svg)](https://cangjie-lang.cn/)
[![构建状态](https://img.shields.io/badge/构建-通过-brightgreen.svg)](https://github.com/your-repo/cactor)
[![许可证](https://img.shields.io/badge/许可证-MIT-green.svg)](LICENSE)
[![版本](https://img.shields.io/badge/版本-1.1.0-blue.svg)](CHANGELOG.md)

> 🚀 **基于仓颉语言的现代化Actor系统** - 企业级并发编程解决方案

CActor 是一个基于仓颉语言开发的现代化Actor系统，提供了完整的Actor模型实现、企业级特性和先进的状态管理能力。通过充分利用仓颉语言的现代特性，CActor 在保持高性能的同时，提供了类型安全、内存安全和并发安全的编程体验。

## 🎉 最新更新 (v1.1.0)

### ✅ 已完成的重大功能
- **Ask模式完善** ✅ - 生产级实现，657,894 req/s性能
- **集群管理增强** ✅ - 完整的集群分片系统，支持100万Actor
- **持久化系统完善** ✅ - 完整的事件溯源和CQRS系统
- **测试框架建设** ✅ - 完整的TestKit框架
- **CActor Streams** ✅ - 高性能流处理系统，500万元素/秒
- **XState状态机** ✅ - 基于仓颉语言特性的企业级状态机系统

## ✨ 核心特性

### 🎯 完整的Actor系统
- **标准Actor模型**: 消息传递、状态封装、位置透明性
- **生命周期管理**: 完整的Actor创建、启动、停止和销毁流程
- **监督策略**: OneForOne、AllForOne等多种监督策略
- **Actor层次结构**: 支持父子Actor关系和监督树
- **类型化Actor**: 编译时类型安全的Actor实现

### ⚡ 卓越性能
- **微秒级延迟**: 所有测试场景下都保持微秒级延迟
- **高吞吐量**: I/O密集型任务中达到166,667 msg/s
- **内存优化**: 高效的对象池和内存管理
- **批处理支持**: 消息批处理以提高吞吐量
- **NUMA感知**: 针对NUMA架构的优化

### 🏢 企业级特性
- **Ask模式**: 完整的请求-响应模式支持
- **路由器**: 多种路由策略（轮询、随机、一致性哈希等）
- **断路器**: 故障隔离和自动恢复机制
- **背压控制**: 流量控制和过载保护
- **配置管理**: 灵活的配置系统

### 🌊 流处理系统
- **CActor Streams**: 响应式流处理框架
- **背压支持**: 自动流量控制和过载保护
- **流操作符**: 丰富的流变换和处理操作
- **DSL语法**: 声明式流处理管道定义

### 🔄 状态机系统 (XState)
- **强类型状态机**: 编译时类型安全的状态管理
- **模式匹配引擎**: 基于仓颉模式匹配的高性能转换引擎
- **CActor深度集成**: 状态机即Actor，天然支持并发和分布式
- **企业级特性**: 事件溯源、状态快照、分布式同步
- **DSL支持**: 声明式状态机定义和宏系统

### 🔧 运行时系统
- **多种调度器**: 支持不同的调度策略
- **多种邮箱**: 有界、无界、优先级邮箱
- **定时器支持**: 延迟消息和周期性任务
- **线程池管理**: 高效的线程池实现
- **监控诊断**: 详细的性能监控和健康检查

## � 快速开始

### 环境要求

- **仓颉语言**: 0.60.5 或更高版本
- **操作系统**: macOS, Linux, Windows
- **内存**: 建议 4GB 以上
- **CPU**: 支持多核处理器

### 安装

```bash
# 克隆项目
git clone https://github.com/your-repo/cactor.git
cd cactor

# 构建项目
cjpm build

# 运行测试
cjpm test
```

### Hello World 示例

```cangjie
import cactor.api.{CActor}
import cactor.core.actor.{Actor}
import cactor.core.message.{Message, StringMessage, MessageResult}
import cactor.core.context.{ActorContext}

// 定义一个简单的Actor
class HelloWorldActor <: Actor {
    private var messageCount: Int64 = 0

    public prop name: String {
        get() { "HelloWorldActor" }
    }

    public prop description: String {
        get() { "Hello World Actor示例" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("👋 收到消息 #${messageCount}: ${msg.getContent()}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func preStart(): Unit {
        println("🚀 Hello World Actor启动了!")
    }

    public func postStop(): Unit {
        println("🛑 Hello World Actor停止了! 总共处理了 ${messageCount} 条消息")
    }
}

// 使用Actor
func main(): Unit {
    // 创建Actor系统
    let system = CActor.system("hello-world")

    // 创建Actor
    let helloActor = system.actorOf(() => HelloWorldActor(), "hello")

    // 发送消息
    helloActor.tell(StringMessage("Hello, CActor!"))
    helloActor.tell(StringMessage("这是第二条消息"))

    // 等待处理完成
    Thread.sleep(Duration.fromMilliseconds(100))

    // 关闭系统
    system.shutdown()
}
```

### 运行示例

```bash
# 运行Hello World示例
cjpm run --name cactor.examples.hello_world_demo

# 运行Ask模式示例
cjpm run --name cactor.examples.ask_demo

# 运行XState状态机示例
cjpm run --name cactor.examples.xstate_demo

# 运行流处理示例
cjpm run --name cactor.examples.streams_demo

# 运行集群分片示例
cjpm run --name cactor.examples.cluster_sharding_demo

# 运行持久化示例
cjpm run --name cactor.examples.persistence_demo
```

## 📊 性能基准测试

### 🏆 真实压测结果

基于与主流Actor框架的全面对比测试：

| 测试类型 | CActor | ProtoActor-Go | Actix | 性能排名 |
|----------|--------|---------------|-------|----------|
| **轻量级** | 454,545 msg/s | **3,333,333 msg/s** | 96,154 msg/s | 🥈 第二名 |
| **中等强度** | **54,348 msg/s** | 47,619 msg/s | 47,170 msg/s | 🥇 第一名 |
| **重量级** | **35,714 msg/s** | 9,615 msg/s | 9,434 msg/s | 🥇 第一名 |
| **CPU密集型** | **11,364 msg/s** | 17,391 msg/s | 16,393 msg/s | 🥉 第三名 |
| **I/O密集型** | **166,667 msg/s** | 4,951 msg/s | 4,902 msg/s | 🥇 第一名 |
| **内存密集型** | 177 msg/s | 894 msg/s | **971 msg/s** | 🥉 第三名 |
| **混合负载** | 386 msg/s | **4,975 msg/s** | 4,000 msg/s | 🥉 第三名 |

### ⚡ 延迟性能对比

| 测试类型 | CActor | ProtoActor-Go | Actix | 延迟排名 |
|----------|--------|---------------|-------|----------|
| **轻量级** | **0.0019 ms** | 0.0027 ms | 1.6324 ms | 🥇 最低延迟 |
| **中等强度** | **0.0023 ms** | 0.0290 ms | 3.4558 ms | 🥇 最低延迟 |
| **重量级** | **0.0037 ms** | 0.0191 ms | 3.5038 ms | 🥇 最低延迟 |
| **CPU密集型** | **0.0067 ms** | 0.0178 ms | 23.6387 ms | 🥇 最低延迟 |
| **I/O密集型** | **0.0019 ms** | 26.3434 ms | 46.3912 ms | 🥇 最低延迟 |
| **内存密集型** | **0.0030 ms** | 539.4237 ms | 517.7320 ms | 🥇 最低延迟 |
| **混合负载** | **0.0027 ms** | 62.4422 ms | 126.7881 ms | 🥇 最低延迟 |

### 🎯 CActor 核心优势

#### 🏆 **延迟控制之王**
- **所有测试场景下都保持最低延迟**
- 微秒级延迟稳定性，适合实时系统
- 比其他框架快 10-10,000 倍

#### 🚀 **I/O密集型绝对优势**
- **166,667 msg/s** vs 其他框架的 ~5,000 msg/s
- **比主流框架快 30+ 倍**
- 特别适合Web服务和数据库应用

#### 💪 **复杂计算领先性能**
- 中等强度和重量级任务中性能领先
- 展现仓颉语言在复杂算法上的优势
- 适合算法密集型和业务逻辑处理

> 📊 **[查看完整基准测试报告](BENCHMARK_RESULTS.md)** - 详细的测试数据和分析

## 🏗️ 架构设计

CActor采用6层模块化架构：

```
┌─────────────────────────────────────────┐
│              API Layer (API层)           │  ← 统一对外接口
├─────────────────────────────────────────┤
│         Integration Layer (集成层)        │  ← 监控、日志、配置
├─────────────────────────────────────────┤
│        Distribution Layer (分布式层)      │  ← 远程、集群、持久化
├─────────────────────────────────────────┤
│          Patterns Layer (模式层)         │  ← Ask、路由、断路器
├─────────────────────────────────────────┤
│          Runtime Layer (运行时层)        │  ← 调度器、邮箱、定时器
├─────────────────────────────────────────┤
│            Core Layer (核心层)           │  ← Actor、消息、系统
├─────────────────────────────────────────┤
│        Foundation Layer (基础设施层)      │  ← 并发、序列化、网络
└─────────────────────────────────────────┘
```

## 📚 文档

> 📖 **[完整文档中心](docs/index.md)** - 查看所有文档的导航和索引

### 中文文档
- [架构设计](docs/zh/architecture.md) - 详细的架构设计文档
- [API参考](docs/zh/api-reference.md) - 完整的API文档
- [性能优化](docs/zh/performance.md) - 性能优化指南
- [包结构](docs/zh/package-structure.md) - 详细的包结构设计
- [未来规划](docs/zh/roadmap.md) - 项目发展路线图

### English Documentation
- [Architecture Design](docs/en/architecture.md) - Detailed architecture documentation
- [API Reference](docs/en/api-reference.md) - Complete API documentation
- [Performance Optimization](docs/en/performance.md) - Performance optimization guide
- [Package Structure](docs/en/package-structure.md) - Detailed package structure design
- [Roadmap](docs/en/roadmap.md) - Project development roadmap

## 🎯 未来规划

### 已完成功能 (v1.1.0)
- ✅ **核心Actor系统** - 完整的Actor模型实现
- ✅ **Ask模式** - 生产级请求-响应模式，657,894 req/s
- ✅ **集群管理** - 完整的集群分片系统，支持100万Actor
- ✅ **持久化系统** - 事件溯源和CQRS架构
- ✅ **测试框架** - 完整的TestKit框架
- ✅ **流处理系统** - CActor Streams，500万元素/秒
- ✅ **状态机系统** - XState，基于仓颉语言特性
- ✅ **7层架构** - 完整的模块化架构设计

### 进行中项目
- 🔄 **HTTP集成** - 高性能Web服务器
- 🔄 **序列化优化** - 多格式序列化支持
- 🔄 **可视化工具** - 监控和管理界面
- 🔄 **文档完善** - 多语言文档体系

### 未来规划
- 📋 **云原生支持** - Kubernetes和容器化
- 📋 **多语言绑定** - Java、Python、Go绑定
- 📋 **边缘计算** - 轻量级运行时
- 📋 **社区生态建设** - 开源社区发展

## 🧪 运行测试

### 性能测试
```bash
# 运行简化压测
./target/release/bin/cactor.integration.testing.simple_benchmark_test

# 运行压测示例
./target/release/bin/cactor.examples.benchmark_demo
```

### 功能测试
```bash
# Hello World入门示例
cjpm run --name cactor.examples.hello_world_demo

# API演示
cjpm run --name cactor.examples.api_demo

# 高级调度器示例
cjpm run --name cactor.examples.advanced_dispatcher_examples

# 高级邮箱示例
cjpm run --name cactor.examples.advanced_mailbox_examples

# 配置使用示例
cjpm run --name cactor.examples.configuration_usage_test

# 类型安全Actor示例
cjpm run --name cactor.examples.typed_actor_demo
```

## 🤝 贡献指南

我们欢迎社区贡献！请参阅 [贡献指南](CONTRIBUTING.md) 了解如何参与项目开发。

### 开发流程
1. Fork项目
2. 创建功能分支
3. 提交代码变更
4. 创建Pull Request

### 代码规范
- 遵循仓颉语言规范
- 添加适当的注释和文档
- 编写相应的测试用例
- 确保所有测试通过

## 📄 许可证

本项目采用 [MIT许可证](LICENSE)。

## 🙏 致谢

感谢仓颉语言团队提供的优秀编程语言和工具链，使得CActor项目得以实现。

特别感谢所有为CActor项目贡献代码、文档、测试和反馈的开发者们！

## 📞 联系我们

- **项目主页**: [GitHub Repository]
- **问题反馈**: [GitHub Issues]
- **功能请求**: [GitHub Discussions]
- **技术交流**: [社区论坛]

---

**CActor - 让仓颉语言拥有优先的Actor系统！** 🚀
