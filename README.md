<div align="center">

# CActor

**仓颉 Actor 模型库 · AI Agent 的底层运行时**

承载 Agent 的状态、并发、失败与位置：私有上下文、串行邮箱、监督树、位置透明。<br/>
DSL 化：枚举即协议，`@actorReceive` 编译期分派，`100.millis` 与 `<<` 简化超时和发送。<br/>
Agent 只写“做什么”，调度、隔离与容错交给 CActor。

[![仓颉](https://img.shields.io/badge/仓颉-1.2.0-blue.svg)](https://cangjie-lang.cn/)
[![版本](https://img.shields.io/badge/版本-1.2.0-orange.svg)](CHANGELOG.md)
[![许可证](https://img.shields.io/badge/许可证-MIT-green.svg)](LICENSE)

[English](README_EN.md) | 中文

</div>

> **项目状态：早期开发阶段（1.2.0）。** API 仍可能调整。已知问题见 [当前限制](#当前限制)，后续计划见 [路线图](#路线图)。

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

CActor 是仓颉语言的 Actor 模型库，定位为 **AI Agent 的底层运行时**。一个 Agent 要有自己的状态，要和许多 Agent 并发协作，要在模型超时、工具出错之后恢复，还要能从单进程扩展到多台机器——这四件事正是 Actor 模型要解决的问题：

| Agent 需要承载的 | CActor 提供的 | 具体机制 |
|---|---|---|
| **状态**：记忆、会话、工具调用的中间结果 | 私有上下文 | 状态只属于一个 Actor，外部只能发消息，不能直接读写；类型化 Actor 用“返回新行为”保存状态 |
| **并发**：成百上千个 Agent 同时工作 | 串行邮箱 | 每个 Actor 一次只处理一条消息，内部不需要锁；调度器在固定线程池上并发执行大量 Actor（实测百万个 Actor） |
| **失败**：模型调用超时、工具抛异常 | 监督树 | 失败交给父 Actor 按策略重启 / 恢复 / 停止 / 上报；`watch` 感知其他 Agent 停止；有界邮箱的背压策略 |
| **位置**：从单进程扩展到多节点 | 位置透明 | 本地与远程引用用同一套 `tell` / `ask` / `watch`；可靠投递；集群成员、Singleton、分片、发布订阅、CRDT 同步 |

同时，CActor 是 **DSL 化** 的，用仓颉的语言特性让 Agent 代码只描述“做什么”：

- **枚举即协议**：类型化 Actor 的消息协议就是一个 `enum`，`match` 的穷尽检查在编译期发现漏处理的消息
- **`@actorReceive`**：编译期根据 `onXxx(msg: T, ctx)` 方法生成按消息类型分派的 `receive`；同一类型写了两个处理方法也在编译期报错
- **`100.millis`、`3.seconds`**：时间字面量；**`ref << a << b`**：连续发送
- **声明式描述行为与工作流**：`TypedBehaviors`（`setup` / `receive` / `supervise` / `withTimers` / `withStash` / `intercept` …）描述 Actor 行为，Streams 描述数据流水线，XState 风格状态机描述工作流（Streams 与 XState 目前为实验性）

Agent 只写“做什么”，调度、隔离与容错交给 CActor。

一个最小的 Agent（完整可运行）：

```cangjie
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*
import cactor_core.dsl.*

// 协议：枚举即协议
enum AgentCmd {
    | Ask(String, TypedActorRef<String>)   // 问题与回复地址
    | Remember(String)
    | Reflect                               // 定时整理记忆
}

// 行为：私有上下文（记忆）通过“返回新行为”保存，不需要锁
func agent(memory: Array<String>): TypedBehavior<AgentCmd> {
    TypedBehaviors.receive<AgentCmd>({ ctx, cmd =>
        match (cmd) {
            case AgentCmd.Ask(question, replyTo) =>
                replyTo << "已收到：${question}（记忆 ${memory.size} 条）"
                TypedBehaviors.same<AgentCmd>()
            case AgentCmd.Remember(fact) => agent(memory.concat([fact]))
            case AgentCmd.Reflect =>
                ctx.log().info("整理记忆：${memory.size} 条")
                TypedBehaviors.same<AgentCmd>()
        }
    })
}

main() {
    let system = CActorSystemFactory.create("agents")
    // 定时器 + 监督：失败时重启，1 分钟内最多 3 次
    let behavior = TypedBehaviors.withTimers<AgentCmd>({ timers =>
        timers.startTimerAtFixedRate("reflect", AgentCmd.Reflect, 30.seconds)
        agent([])
    })
    let assistant = system.typedActorOf<AgentCmd>(
        TypedBehaviors.supervise(behavior).onFailure(TypedSupervisorStrategy.restart().withLimit(3, 1.minutes)),
        "assistant")
    assistant << AgentCmd.Remember("用户偏好中文") << AgentCmd.Remember("时区 UTC+8")
    let answer = TypedAsk.ask<AgentCmd, String>(assistant, { r => AgentCmd.Ask("今天的计划？", r) }, 3.seconds)
        .get(3.seconds)
    println(answer)   // 已收到：今天的计划？（记忆 2 条）
    system.terminate()
}
```

更完整的设计理念与 Actor / Agent 对应关系见 [项目介绍](docs/zh/INTRODUCTION.md)。

---

## 功能概览

| 方向 | 内容 | 状态 |
|------|------|------|
| Actor 基础 | Actor / ActorRef / ActorSystem、生命周期回调、子 Actor、`watch`、`become`、定时器、Stash、接收超时、按路径查找、死信 | 可用 |
| 类型化 Actor | `TypedBehaviors`、`TypedActorRef`、Actor 内 ask、`messageAdapter`、`pipeToSelf`、Receptionist、行为拦截器，参考 Akka Typed | 可用 |
| 容错 | 监督树（OneForOne / OneForAll / Escalate / Backoff）、DeathPact、断路器 | 可用 |
| 通用模式 | Ask、消息路由、有界邮箱背压（丢弃 / 失败 / 阻塞） | 可用 |
| DSL | 枚举协议、`@actorReceive`、时间字面量、`<<` | 可用 |
| 测试 | `TestProbe`、`BehaviorTestKit`、虚拟时钟 `ManualTime` | 可用 |
| 可观测性 | 分级与结构化日志、MDC、JSON Lines、Prometheus 指标 | 可用 |
| 远程 | TCP 传输、远程 ask / watch、跨节点回复、可靠投递 | 可用，暂无 TLS |
| 集群 | gossip 成员协议、Cluster Singleton、分布式发布订阅、集群分片、CRDT 与 Replicator | 可用，有多节点与多进程验证；暂无脑裂处理 |
| 持久化 | 文件事件日志、快照、事件溯源 Actor | 可用，暂无插件接口 |
| 流处理 | Streams DSL | 实验性 |
| 状态机 | XState 风格的状态机 | 实验性 |

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

项目是一个 cjpm workspace，由 `modules/` 下的 15 个模块组成。依赖只能从上层指向下层：

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
| remote | `cactor_remote` | 远程通信（原型） |
| persistence | `cactor_persistence` | 事件溯源、快照、CQRS（原型） |
| cluster | `cactor_cluster` | 集群、分片、故障转移（原型） |
| distribution | `cactor_distribution` | 聚合模块：重导出 remote / persistence / cluster，含 streaming（原型） |
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

目前共有 163 个测试用例，在仓颉 1.2.0 下全部通过。除监督策略、类型化 Actor（行为变换、Stash、定时器）、mailbox 外，还覆盖并发语义（串行处理、并发创建/停止、监督重启）、队列压力、TCP/UDP 传输、远程端到端、文件事件日志与事件溯源恢复。

基准程序：`./target/release/bin/cactor_examples.real_benchmark`。在 32 核机器上实测：单 Actor 5.6–7.2 M msg/s，ping-pong 往返 P50 约 2 µs，本机回环远程 TCP 约 150–190 k msg/s。同机与 Akka 2.6.21 的对比见 [性能分析](docs/plan/20261028-performance.md)：点对点场景已追平或超过 Akka，扇出仍慢约 3 倍，远程慢约 1.6 倍。

---

## 当前限制

以下问题已经确认，正在按 [开发计划](docs/plan/20261028.md) 逐步处理：

- **调度**：亲和调度，还没有工作窃取；扇出吞吐仍明显低于 Akka；在 `receive` 中阻塞会占住工作线程，同一线程上的其他 Actor 也会被拖慢。
- **远程通信**：基于 `std.net` 的 TCP 传输。普通 `tell` 是至多一次；需要可靠性时用 `tellReliable`（序号、确认、重发、去重）。支持远程 ask、跨节点回复和远程 watch。暂不支持 TLS。
- **集群**：`cactor_cluster.membership` 提供 gossip 成员协议、Cluster Singleton、分布式发布订阅和集群分片，`cactor_cluster.ddata` 提供 CRDT 与 Replicator，均有多节点测试与多进程验证（`tools/cluster_demo.sh`）。分片实体的状态在移交后不保留（需要时结合事件溯源）；还没有脑裂处理策略（只有手动 down 和超时自动 down）。`cactor_cluster` 根包中的旧类（`SimpleClusterManager`、`ShardRegion` 等）是早期原型，将在 1.3.0 删除。
- **宏**：`@actorReceive` 已可用；其余 DSL 宏大多还没有实际展开逻辑。
- **日志**：库内日志走分级日志（默认 WARN，输出到 stderr，用环境变量 `CACTOR_LOG_LEVEL` 调整），但仓库中还有三套日志实现，计划在 1.3.0 合并。
- **旧基准程序**：除 `real_benchmark` 外，仓库里其他 benchmark 程序测到的是同步函数调用的开销，不代表消息传递性能。

---

## 路线图

完整路线图见 [ROADMAP.md](ROADMAP.md)。版本规划概要：

- [x] **1.2.0**：升级到仓颉 1.2.0；拆分为多模块 workspace；核心运行时、经典与类型化 API 语义（监督树、watch、become、定时器、Stash 等）；基于 std.net 的远程与文件持久化；可复现的基准测试
- [ ] **1.3.0 收敛**：删除死代码与重复实现；统一日志、配置、ask 与 Scheduler；真实 JSON；API 快照；测试去掉 `sleep`；空闲机器上复测基准
- [ ] **1.4.0 新 API 与 HTTP**：`actorSystem {}` / `spawn` / `ask` 新入口，`@Actor` / `@Message` 宏，Ktor 风格 HTTP 服务端与 Actor 融合
- [ ] **1.5.0 性能与可靠远程**：Native HTTP 引擎、扇出优化、远程 ask / watch、可靠投递、TLS
- [ ] **1.6.0 集群与生产化**：成员协议、Singleton、脑裂处理；HTTP 客户端、WebSocket、生产插件
- [ ] **2.0.0**：分片、持久化插件接口、API 冻结并发布到中心仓

---

## 文档

| 文档 | 说明 |
|------|------|
| [项目介绍](docs/zh/INTRODUCTION.md) | 设计理念、模块分层，以及 Actor 与 AI Agent 的对应关系 |
| [快速开始](docs/zh/QUICK_START.md) | 安装与第一个程序 |
| [入门指南](docs/zh/getting-started.md) | 基础概念与常见用法 |
| [架构设计](docs/zh/architecture.md) | 分层设计与关键组件 |
| [包与模块结构](docs/zh/package-structure.md) | 模块划分与依赖关系 |
| [API 参考](docs/zh/api-reference.md) | 主要类型与接口 |
| [性能调优指南](docs/zh/performance.md) | 编译优化、运行时参数、编写 Actor 的性能规则、如何测量 |
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
