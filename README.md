<div align="center">

# CActor

**仓颉语言的 Actor 模型库**

设计参考 Akka / Akka Typed，提供经典 Actor、类型化 Actor、监督容错和 Ask 等常用模式。

[![仓颉](https://img.shields.io/badge/仓颉-1.2.0-blue.svg)](https://cangjie-lang.cn/)
[![版本](https://img.shields.io/badge/版本-1.2.0-orange.svg)](CHANGELOG.md)
[![许可证](https://img.shields.io/badge/许可证-MIT-green.svg)](LICENSE)

[English](README_EN.md) | 中文

</div>

> **项目状态：早期开发阶段（1.2.0）。** API 仍可能调整，暂不建议在生产环境使用。已知问题见 [当前限制](#当前限制)，后续计划见 [路线图](#路线图)。

---

## 目录

- [简介](#简介)
- [功能概览](#功能概览)
- [快速开始](#快速开始)
- [在项目中使用](#在项目中使用)
- [使用示例](#使用示例)
- [核心概念](#核心概念)
- [架构设计](#架构设计)
- [示例程序](#示例程序)
- [测试](#测试)
- [当前限制](#当前限制)
- [路线图](#路线图)
- [文档](#文档)
- [参与贡献](#参与贡献)
- [许可证](#许可证)

---

## 简介

Actor 模型用"彼此隔离、只通过消息通信的 Actor"来组织并发程序：每个 Actor 拥有自己的状态，按顺序处理邮箱里的消息，出错时由上级按监督策略处理。

CActor 尝试在仓颉语言中提供这套模型，主要包括：

- **两种编程风格**：继承 `Actor` 的经典风格，以及基于 `TypedBehavior` 的类型化风格
- **容错机制**：监督策略与断路器
- **常用模式**：Ask 请求-响应、消息路由、背压
- **模块化结构**：12 个 cjpm 模块，分层清晰，可以按需依赖

---

## 功能概览

| 方向 | 内容 | 状态 |
|------|------|------|
| Actor 基础 | Actor / ActorRef / ActorSystem、生命周期回调、`tell` | 可用 |
| 类型化 Actor | `TypedBehaviors`、`TypedActorRef`，参考 Akka Typed | 可用 |
| 容错 | 监督策略（OneForOne / OneForAll / Escalate / Backoff）、断路器 | 可用 |
| 通用模式 | Ask、消息路由、背压控制 | 可用 |
| 运行时组件 | 多种 mailbox 与 dispatcher 实现 | 可用，见 [当前限制](#当前限制) |
| 流处理 | Streams DSL | 实验性 |
| 状态机 | XState 风格的状态机 | 实验性 |
| 分布式 | 集群分片、远程通信、事件溯源 / 持久化 | 原型，尚无真实网络与存储 |
| 宏 | Actor DSL 宏 | 原型 |

---

## 快速开始

### 环境要求

- 仓颉 SDK 1.2.0（含 `cjc` 和 `cjpm`）
- Linux x86_64（目前只在该平台上验证过）

### 从源码构建

```bash
git clone https://atomgit.com/Cangjie-SIG/cactor.git
cd cactor

cjpm build
```

### 运行示例

```bash
./target/release/bin/cactor_examples.hello_world_demo
```

---

## 在项目中使用

目前通过本地路径引用 CActor 的模块。在你的 `cjpm.toml` 中添加：

```toml
[dependencies]
cactor          = { path = "/path/to/cactor/modules/cactor" }    # 门面 API
cactor_core     = { path = "/path/to/cactor/modules/core" }      # Actor、Message 等核心类型
cactor_patterns = { path = "/path/to/cactor/modules/patterns" }  # Ask 等模式（按需）
```

其他模块（如 `cactor_streams`、`cactor_xstate`）按需添加，路径为 `modules/<模块名>`。

---

## 使用示例

下面三段代码都在一个独立工程中按上面的依赖配置编译运行过。

### 经典 Actor

```cangjie
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

class HelloActor <: Actor {
    public prop name: String { get() { "HelloActor" } }
    public prop description: String { get() { "简单的问候 Actor" } }

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
    let system = CActor.system("hello-system")
    let actor = system.actorOf({ => HelloActor() }, "hello")
    actor.tell(StringMessage("Hello, CActor!"))   // 输出：收到消息: Hello, CActor!
    system.terminate()
    return 0
}
```

### 类型化 Actor

消息类型由编译器检查；状态通过返回新的行为来更新，不需要可变字段。

```cangjie
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.{TypedBehavior, TypedBehaviors}

enum CounterMsg {
    | Increment
    | Print
}

func counter(count: Int64): TypedBehavior<CounterMsg> {
    TypedBehaviors.receive<CounterMsg>({ ctx, msg =>
        match (msg) {
            case Increment => counter(count + 1)
            case Print =>
                println("count = ${count}")
                TypedBehaviors.same<CounterMsg>()
        }
    })
}

main(): Int64 {
    let system = CActorSystemFactory.create("typed-system")
    let ref = system.typedActorOf<CounterMsg>(counter(0), "counter")
    ref.tell(Increment)
    ref.tell(Increment)
    ref.tell(Print)                               // 输出：count = 2
    system.terminate()
    return 0
}
```

### Ask 请求-响应

```cangjie
import std.time.*
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}
import cactor_patterns.ask.{Ask, AskMessage, AskResponse, Result}

class EchoActor <: Actor {
    public prop name: String { get() { "EchoActor" } }
    public prop description: String { get() { "回显 Ask 请求" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case ask: AskMessage =>
                let reply = match (ask.getOriginalMessage()) {
                    case s: StringMessage => StringMessage("echo: ${s.getContent()}")
                    case _ => StringMessage("unknown")
                }
                ask.getReplyTo().tell(AskResponse(ask.getRequestId(), reply, true))
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

main(): Int64 {
    let system = CActor.system("ask-system")
    let echo = system.actorOf({ => EchoActor() }, "echo")

    let future = Ask.ask(echo, StringMessage("hi"), Duration.second * 3)
    match (future.get()) {
        case Result.Success(response) =>
            match (response) {
                case s: StringMessage => println("响应: ${s.getContent()}")   // 输出：响应: echo: hi
                case _ => ()
            }
        case Result.Failure(error) => println("失败: ${error.message}")
    }

    Ask.shutdown()
    system.terminate()
    return 0
}
```

更多用法见 [示例程序](#示例程序) 和 [API 参考](docs/zh/api-reference.md)。

---

## 核心概念

### Actor

Actor 是并发计算的基本单元，每个 Actor 包含：

- **私有状态**：只能通过消息修改
- **邮箱**：接收其他 Actor 发来的消息
- **行为**：定义如何处理收到的消息

```
┌─────────────────────────────────────────────────────────────┐
│                        Actor                                │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    状态 (State)                     │    │
│  │              messageCount: Int64                    │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    邮箱 (Mailbox)                   │    │
│  │  [msg1] [msg2] [msg3] [msg4] ...                    │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   行为 (Behavior)                   │    │
│  │           receive(message, context)                 │    │
│  │              → MessageResult                        │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 消息处理结果

`receive` 返回 `MessageResult`，告诉运行时这条消息的处理情况，例如 `Handled`（已处理）、`Unhandled`（未处理）、`Stop`（处理后停止 Actor）。

### 生命周期

状态定义见 `ActorLifecycleState`，各阶段对应 `preStart`、`postStop`、`preRestart`、`postRestart` 回调。

```
    ┌─────────┐
    │ Created │  ← Actor 实例创建
    └────┬────┘
         │ preStart()
         ▼
    ┌─────────┐
    │Starting │  ← 初始化中
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ Running │  ← 正常处理消息
    └────┬────┘
         │
    ┌────┴────┐
    │         │
 Stopped  Failed ← 停止或异常
    │         │
    │    preRestart()
    │         │
    └────┬────┘
         │
    Restarting
         │
         └──→ Running
```

### 监督

Actor 失败时，由监督策略决定如何处理：`Resume`（忽略并继续）、`Restart`（重启）、`Stop`（停止）或 `Escalate`（交给上级）。内置策略包括 `OneForOneStrategy`、`OneForAllStrategy`、`EscalateStrategy` 和带指数退避的 `BackoffSupervisorStrategy`，位于 `cactor_core.supervision`。

---

## 架构设计

项目是一个 cjpm workspace，由 `modules/` 下的 12 个模块组成。依赖只能从上层指向下层：

```
┌─────────────────────────────────────────────────────────────────┐
│                           用户代码                              │
│        一般通过 cactor 门面使用，需要时直接引用各模块              │
└─────────────────────────────────────────────────────────────────┘
                                │
     ┌──────────────┬───────────┴──────┬───────────────────┐
     ▼              ▼                  ▼                   ▼
┌───────────┐ ┌──────────────┐ ┌───────────────┐ ┌───────────────┐
│  Streams  │ │ Distribution │ │    XState     │ │  Integration  │
│ (流处理)  │ │  (分布式)    │ │   (状态机)    │ │    (集成)     │
├───────────┤ ├──────────────┤ ├───────────────┤ ├───────────────┤
│ • 流 DSL  │ │ • 集群分片   │ │ • 状态机定义  │ │ • 日志        │
│ • 背压    │ │ • 远程通信   │ │ • 并行状态    │ │ • 指标        │
│           │ │ • 持久化     │ │               │ │ • 配置        │
│ 实验性    │ │ 原型         │ │ 实验性        │ │               │
└───────────┘ └──────────────┘ └───────────────┘ └───────────────┘
     │              │                  │                   │
     └──────────────┴────────┬─────────┴───────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor（门面 API）                          │
│   CActor.system() | CActorSystem | ActorProps                   │
│   CActorFactory | CActorSystemBuilder                           │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_patterns（通用模式）                 │
│   Ask | Router | Backpressure                                   │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_runtime（运行时）                    │
│   CActorRuntime | Mailbox | Dispatcher | Guardian               │
│   Registry | ActorLifecycleManager                              │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_core（核心抽象）                     │
│   Actor | ActorRef | Props | ActorSystem                        │
│   Message | ActorContext | Supervision                          │
│   TypedBehavior | TypedActorRef                                 │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cactor_foundation（基础设施）               │
│   Queue | Serializer | Memory | Monitoring                      │
│   Network | Config | DI | CircuitBreaker                        │
└─────────────────────────────────────────────────────────────────┘

cactor_macros 是独立的宏模块；cactor_tests 和 cactor_examples 位于最上层。
```

| 模块 | 包名 | 职责 |
|------|------|------|
| foundation | `cactor_foundation` | 队列、内存、序列化、网络、配置、DI、监控、断路器 |
| core | `cactor_core` | Actor、ActorRef、ActorSystem、Message、监督、类型化 Actor 等核心抽象 |
| runtime | `cactor_runtime` | mailbox、dispatcher、guardian、registry、生命周期 |
| patterns | `cactor_patterns` | Ask、路由、背压 |
| cactor | `cactor` | 对外统一 API（`cactor`、`cactor.api`、`cactor.config`） |
| macros | `cactor_macros` | Actor DSL 宏 |
| streams | `cactor_streams` | 流处理 DSL |
| distribution | `cactor_distribution` | 集群、远程、持久化（原型） |
| xstate | `cactor_xstate` | 状态机 |
| integration | `cactor_integration` | 日志、指标、配置集成，以及集成测试程序 |
| tests | `cactor_tests` | 单元测试与集成测试 |
| examples | `cactor_examples` | 示例程序 |

更详细的设计说明见 [架构设计](docs/zh/architecture.md) 和 [包与模块结构](docs/zh/package-structure.md)。

---

## 示例程序

示例位于 [`modules/examples/src`](modules/examples/src)，构建后的可执行文件为 `target/release/bin/cactor_examples.<示例名>`。

| 示例 | 内容 |
|------|------|
| [hello_world_demo](modules/examples/src/hello_world_demo/main.cj) | Actor 创建与消息发送 |
| [api_demo](modules/examples/src/api_demo/main.cj) | 对外 API 用法 |
| [ask_pattern_demo](modules/examples/src/ask_pattern_demo/main.cj) | Ask 请求-响应、超时与批量请求 |
| [typed_actor_demo](modules/examples/src/typed_actor_demo/main.cj) | 类型化 Actor 系统 |
| [advanced_mailbox_examples](modules/examples/src/advanced_mailbox_examples/main.cj) | 不同 mailbox 实现 |
| [advanced_dispatcher_examples](modules/examples/src/advanced_dispatcher_examples/main.cj) | 不同 dispatcher 实现 |
| [streams_demo](modules/examples/src/streams_demo/main.cj) | 流处理（实验性） |
| [xstate_demo](modules/examples/src/xstate_demo) | 状态机（实验性） |
| [simple_macro_demo](modules/examples/src/simple_macro_demo/main.cj) | DSL 宏（原型） |
| [cluster_sharding_demo](modules/examples/src/cluster_sharding_demo/main.cj) | 集群分片（原型） |
| [persistence_demo](modules/examples/src/persistence_demo/main.cj) | 事件溯源（原型） |

---

## 测试

```bash
cjpm test                           # 运行全部测试
python3 tools/check_module_deps.py  # 检查模块分层约束
```

目前共有 121 个测试用例，在仓颉 1.2.0 下全部通过，覆盖监督策略、类型化 Actor（行为变换、Stash、定时器）、mailbox 等模块。并发语义方面的测试还在补充中。

---

## 当前限制

以下问题已经确认，正在按 [开发计划](docs/plan/20261028.md) 逐步处理：

- **消息调度**：`tell` 目前在调用方线程上直接执行 `receive`，还不能保证同一个 Actor 同一时刻只处理一条消息；dispatcher 尚未接入主消息路径。
- **并发安全**：部分队列和共享容器还缺少同步。
- **分布式能力**：远程通信、持久化和集群目前只有接口与原型，没有真实的网络传输和存储。
- **宏**：DSL 宏大多还没有实际展开逻辑。
- **日志输出**：运行时会把启动、停止等日志直接打印到标准输出，暂时无法关闭。
- **性能数据**：仓库里的 benchmark 程序目前测到的是同步函数调用的开销，不能代表消息传递性能，所以这里暂不给出性能数字。

---

## 路线图

按优先级排列，细节见 [开发计划](docs/plan/20261028.md)：

- [x] 升级到仓颉 1.2.0
- [x] 拆分为 12 个 cjpm 模块，并加入分层检查
- [ ] **核心运行时**：每个 Actor 独立 mailbox，由 dispatcher 串行处理消息；`tell` 只负责入队；修复并发容器；补充并发语义测试
- [ ] **API 收口与去重**：精简各模块对外 API，清理重复实现和占位代码
- [ ] **工程质量**：日志抽象替换 `println`，清零编译告警，测试去掉 `sleep` 同步，接入 CI
- [ ] **可复现的基准测试**：纳秒级计时，ping-pong / fan-out 场景，统计 P50 / P99
- [ ] **分布式**：基于 stdx.net 实现远程通信，基于文件系统实现事件存储，再在此基础上实现集群

---

## 文档

| 文档 | 说明 |
|------|------|
| [快速开始](docs/zh/QUICK_START.md) | 安装与第一个程序 |
| [入门指南](docs/zh/getting-started.md) | 基础概念与常见用法 |
| [架构设计](docs/zh/architecture.md) | 分层设计与关键组件 |
| [包与模块结构](docs/zh/package-structure.md) | 模块划分与依赖关系 |
| [API 参考](docs/zh/api-reference.md) | 主要类型与接口 |
| [性能指南](docs/zh/performance.md) | 调度器、邮箱等调优选项 |
| [文档索引](docs/zh/README.md) | 全部中文文档 |
| [更新日志](CHANGELOG.md) | 版本变更记录 |

部分文档还在随代码一起更新，个别内容可能和当前实现不一致，以代码为准，也欢迎指出。

---

## 参与贡献

欢迎提交 Issue 和 Pull Request，无论是修复问题、补充测试还是改进文档。提交前请先阅读 [贡献指南](CONTRIBUTING.md)，并确认：

```bash
cjpm build                          # 构建通过
cjpm test                           # 测试通过
python3 tools/check_module_deps.py  # 分层检查通过
```

新功能请附带测试；涉及对外 API 的改动请同步更新文档和 [更新日志](CHANGELOG.md)。

---

## 许可证

本项目基于 [MIT 许可证](LICENSE) 开源。

## 致谢

- [仓颉编程语言](https://cangjie-lang.cn/) 团队提供的语言和工具链
- [Akka](https://akka.io/) 项目，CActor 的许多设计参考了它
- 所有参与过本项目的贡献者
