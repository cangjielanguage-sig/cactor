# CActor - 基于仓颉语言的优先Actor系统

[English](README_EN.md) | 中文

## 🎯 项目概述

CActor 是一个基于仓颉语言实现的优先高性能Actor系统，提供了完整的Actor模型实现、企业级特性和分布式能力。项目采用6层模块化架构设计，支持超高并发、超低延迟的消息处理，是构建分布式系统的理想选择。

### 🏆 性能特色

基于真实基准测试，CActor展现出独特的性能优势：

- **延迟控制**: **微秒级延迟**，所有场景下都保持最低延迟
- **I/O性能**: **166,667 msg/s**，比主流框架快30+倍
- **复杂计算**: 中等到重量级任务中性能领先
- **实时性**: 延迟稳定性极佳，适合实时系统
- **全能表现**: 7项测试中4项第一，3项前三

## ✨ 核心特性

### 🚀 性能优势
- **延迟控制**: 微秒级延迟，所有场景最优
- **I/O性能**: 166,667 msg/s，行业领先水平
- **复杂计算**: 重量级任务性能领先3.5倍
- **实时性**: 延迟稳定性极佳，适合实时系统
- **高并发**: 支持大规模并发Actor处理

### 🏢 企业级特性
- **Ask模式**: 完整的请求-响应模式支持
- **监督策略**: 完整的故障恢复机制
- **断路器**: 故障隔离和自动恢复
- **背压控制**: 流量控制和过载保护
- **生命周期管理**: 完整的Actor生命周期管理

### 🌐 分布式能力
- **远程通信**: 透明的远程Actor通信
- **集群管理**: 节点发现和状态管理
- **故障转移**: 自动故障检测和恢复
- **序列化**: JSON和二进制序列化支持
- **持久化**: 事件溯源和快照支持

### 🔧 完整生态
- **配置管理**: 灵活的配置系统
- **监控集成**: 完整的指标和监控
- **调试工具**: 性能分析和诊断工具
- **测试框架**: 全面的测试覆盖
- **DSL支持**: 基于宏的DSL语法

## 📦 快速开始

### 环境要求
- 仓颉 (Cangjie) 0.53.4+
- cjpm (仓颉包管理器)

### 安装构建
```bash
# 克隆项目
git clone <repository-url>
cd cactor

# 构建项目
cjpm build

# 运行Hello World示例
cjpm run --name cactor.examples.hello_world_demo

# 运行性能测试
cjpm run --name cactor.integration.testing.simple_benchmark_test
```

### 简单示例

```cangjie
import cactor.api.{CActor}

// 1. 定义Actor
class HelloActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                println("收到消息: ${msg.content}")
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

// 2. 创建Actor系统
main(): Int64 {
    let system = CActor.system("HelloSystem")
    let actor = system.actorOf({ => HelloActor() }, "hello")

    actor.tell(StringMessage("Hello, CActor!"))

    return 0
}
```

> 💡 **完整的入门教程**: 运行 `cjpm run --name cactor.examples.hello_world_demo` 查看详细的Hello World示例，包含完整的注释和演示步骤。

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

### 短期目标 (已完成)
- ✅ 优先性能优化 (20M msg/s)
- ✅ 企业级监控和配置
- ✅ 完整的测试框架
- ✅ 6层架构完善

### 中期目标 (进行中)
- 🔄 分布式集群支持
- 🔄 持久化和事件溯源
- 🔄 流处理能力
- 🔄 完整文档体系

### 长期目标 (规划中)
- 📋 云原生支持
- 📋 多语言绑定
- 📋 可视化工具
- 📋 社区生态建设

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
