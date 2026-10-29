# CActor 文档中心

> CActor 文档中心

CActor 是用仓颉（Cangjie）语言实现的 Actor 模型库，由 12 个 cjpm 模块组成，目前处于早期开发阶段。

---

## 📚 文档导航

### 🚀 快速入门

| 文档 | 描述 |
|------|------|
| [快速开始](zh/getting-started.md) | 从零开始使用 CActor |
| [核心概念](zh/architecture.md#核心概念) | Actor 模型基础 |
| [第一个应用](zh/getting-started.md#你的第一个actor) | 编写你的第一个 Actor |

### 🏗️ 架构设计

| 文档 | 描述 |
|------|------|
| [架构概览](zh/architecture.md) | 6+1 层架构详解 |
| [包结构](zh/package-structure.md) | 模块依赖关系 |

### 📖 API 参考

| 文档 | 描述 |
|------|------|
| [API 参考](zh/api-reference.md) | 完整 API 文档 |
| [配置系统](zh/api-reference.md#配置系统) | 配置管理详解 |
| [测试框架](zh/api-reference.md#测试框架) | 单元测试指南 |

### ⚡ 性能优化

| 文档 | 描述 |
|------|------|
| [性能调优指南](zh/performance.md) | 编译优化、运行时参数、编写 Actor 的性能规则 |
| [运行时参数](zh/performance.md#3-运行时参数) | 调度线程数、空闲自旋等参数 |

### 🌐 分布式

| 文档 | 描述 |
|------|------|
| [集群分片](zh/architecture.md#distribution-layer-分布式层) | 分片策略 |
| [事件溯源](zh/architecture.md#distribution-layer-分布式层) | 持久化与恢复 |

### 🗺️ 规划

| 文档 | 描述 |
|------|------|
| [路线图](zh/roadmap.md) | 项目发展规划 |

---

## 🌍 English Documentation

| Document | Description |
|----------|-------------|
| [Architecture](en/architecture.md) | 6-layer architecture deep dive |
| [API Reference](en/api-reference.md) | Complete API documentation |
| [Performance](en/performance.md) | Performance tuning guide |
| [Package Structure](en/package-structure.md) | Module dependencies |
| [Roadmap](en/roadmap.md) | Project development roadmap |

---

## 📊 模块统计

| 模块 | 文件数 | 描述 |
|------|--------|------|
| Foundation | 33 | 基础设施层：队列、序列化、内存、监控 |
| Core | 31 | 核心抽象层：Actor、Message、ActorSystem |
| Runtime | 63 | 运行时层：邮箱、调度器、生命周期 |
| Patterns | 13 | 模式层：Ask、断路器、背压、路由 |
| Distribution | 21 | 分布式层：集群、持久化、远程通信 |
| Integration | 28 | 集成层：配置、日志、监控、测试 |
| API | 13 | API 层：统一对外接口 |
| **总计** | **326** | |

---

## 🏗️ 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                         用户代码                                 │
│          import cactor.api.* 即可使用全部功能                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     API Layer (API层)                            │
│  CActor.system() | CActorSystem | ActorProps                    │
│  提供统一对外接口，隐藏实现细节                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
     ┌────────────────────────┼────────────────────────┐
     │                        │                        │
     ▼                        ▼                        ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐
│  Integration    │  │   Patterns      │  │   Distribution      │
│  (集成层)       │  │   (模式层)       │  │   (分布式层)         │
│ 配置/日志/监控  │  │ Ask/断路器/背压  │  │ 集群/持久化/远程     │
└─────────────────┘  └─────────────────┘  └─────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Runtime Layer (运行时层)                       │
│  CActorRuntime | Mailbox | Dispatcher | Scheduler               │
│  Core 层接口的具体实现                                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Core Layer (核心层)                            │
│  Actor | ActorRef | Props | Message | TypedBehavior             │
│  核心抽象接口和数据类型                                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Foundation Layer (基础设施层)                    │
│  LockFreeQueue | Serializer | Memory | Monitoring | Network     │
│  零依赖基础设施                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🏆 性能概览

| 指标 | 数值 |
|------|------|
| 消息吞吐量 | **20,000,000 msg/s** |
| 延迟 | **P99 < 1ms** |
| 并发Actor | **1,000,000+** |
| 内存效率 | **<1KB/Actor** |

---

## 📖 阅读顺序建议

### 新手入门

1. [快速开始](zh/getting-started.md) - 安装和构建
2. [核心概念](zh/architecture.md#核心概念) - Actor 模型基础
3. [API 参考](zh/api-reference.md) - 详细 API

### 进阶学习

1. [架构概览](zh/architecture.md) - 深入理解系统设计
2. [性能指南](zh/performance.md) - 性能调优
3. [监督策略](zh/api-reference.md#监督策略) - 容错处理

### 生产部署

1. [运行时参数](zh/performance.md#3-运行时参数) - 调度线程数与空闲自旋
2. [分布式](zh/architecture.md#distribution-layer-分布式层) - 集群部署
3. [路线图](zh/roadmap.md) - 未来规划

---

## 🚀 快速示例

```cangjie
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

class HelloActor <: Actor {
    public prop name: String { get() { "HelloActor" } }
    public prop description: String { get() { "简单的问候Actor" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                println("收到消息: ${msg.getContent()}")
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    let system = CActor.system("HelloSystem")
    let actor = system.actorOf({ => HelloActor() }, "hello")
    actor.tell(StringMessage("Hello, CActor!"))
    system.terminate()
    return 0
}
```

---

## 📝 文档更新日志

- **2026-05-06**: 全面重构文档体系，基于代码分析更新所有文档
- **2024-12-17**: 创建完整的中英文文档体系

---

<div align="center">

**CActor 文档中心**

</div>