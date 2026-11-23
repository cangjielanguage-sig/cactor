<div align="center">

<img src="assets/logo.svg" alt="CActor logo" width="150" />

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
- [发布到中心仓](#发布到中心仓)
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
- **声明式描述行为与工作流**：`TypedBehaviors`（`setup` / `receive` / `supervise` / `withTimers` / `withStash` / `intercept` …）描述 Actor 行为，Streams 描述数据流水线，`@StateMachine` 宏描述工作流（XState 风格状态机模块已冻结，新代码用 `@StateMachine`）

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
| 容错 | 监督树（OneForOne / OneForAll / Escalate / 指数退避重启，经典与类型化 API 均支持）、DeathPact、断路器；默认策略与 Akka 一致（启动失败 / Kill / DeathPact 停止） | 可用 |
| 通用模式 | Ask、消息路由、有界邮箱背压（丢弃 / 失败 / 阻塞） | 可用 |
| DSL | `@actor[cases/protocol]`、`@Message`、`@StateMachine`、`Behaviors`、流管道、时间字面量、`<<` | 可用 |
| 测试 | `TestProbe`、`BehaviorTestKit`、虚拟时钟 `ManualTime` | 可用 |
| 可观测性 | 分级与结构化日志、MDC、JSON Lines、Prometheus 指标 | 可用 |
| 远程 | TCP 传输、远程 ask / watch、跨节点回复、可靠投递、握手鉴权与隔离、TLS 1.3 双向认证 | 可用 |
| 集群 | gossip 成员协议、脑裂处理、Cluster Singleton、分布式发布订阅、集群分片（钝化、remember-entities）、虚拟 Actor（Grains）、CRDT 与 Replicator（一致性级别、delta 传播、移除节点的 pruning） | 可用，有多节点、网络分区与多进程宕机验证 |
| 持久化 | 文件事件日志、快照、事件溯源 Actor、持久化状态（乐观并发）、持久化查询、按 URI 选择存储的插件注册表 | 可用 |
| 流处理 | 背压流（Reactive Streams，E60） | 可用 |

---

## 快速开始

### 环境要求

- 仓颉 SDK 1.2.0（含 `cjc` 和 `cjpm`）
- Linux x86_64（目前只在该平台上验证过）

### 从源码构建

```bash
git clone https://atomgit.com/Cangjie-SIG/cactor.git
cd cactor

source tools/env.sh   # 仓颉环境 + stdx 路径（CANGJIE_STDX_PATH，远程 TLS 依赖 stdx）
cjpm build
```

### 运行示例

```bash
./target/release/bin/cactor_examples.hello_world_demo
```

---

## 在项目中使用

### 从仓颉中心仓安装（推荐）

CActor 的 12 个库模块已发布到 [仓颉中心仓](https://pkg.cangjie-lang.cn)，版本统一为 `1.2.1`：

```toml
[dependencies]
cactor          = "1.2.1"   # 门面 API（cactor / cactor.api / cactor.config）
cactor_core     = "1.2.1"   # Actor、Message 等核心类型
cactor_patterns = "1.2.1"   # Ask 等模式（按需）
```

可用的模块：`cactor_foundation`、`cactor_core`、`cactor_runtime`、`cactor`、`cactor_patterns`、
`cactor_testkit`、`cactor_streams`、`cactor_remote`、`cactor_persistence`、`cactor_cluster`、
`cactor_xstate`、`cactor_macros`。

用到 `cactor_remote` / `cactor_cluster`（TLS 依赖 stdx）时，构建前需把环境变量 `CANGJIE_STDX_PATH`
指向本机 stdx 静态库目录（如 `$HOME/.cangjie/stdx-1.2.0.1/macos_aarch64_cjnative/static/stdx`）：

```bash
export CANGJIE_STDX_PATH=...
cjpm build
```

产物里已经带了 `[target.*.bin-dependencies] path-option = ["${CANGJIE_STDX_PATH}"]`，消费方不必再改
`cjpm.toml`；漏设这个变量会报 `imports package 'stdx.crypto.common' ... but it is not added as a dependency`。

### 本地路径引用（开发本仓库时）

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
import cactor_core.dsl.{AskInbox}
import cactor_core.typed.{TypedAskTimeoutException}

// 经典 ask：AskInbox 就是发送方，目标照常 context.reply（类型化用 TypedAsk.ask / ctx.ask）
class EchoActor <: Actor {
    public prop name: String { get() { "EchoActor" } }
    public prop description: String { get() { "回显 ask 请求" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage => context.reply(StringMessage("echo: ${s.getContent()}"))
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

main(): Int64 {
    let system = CActor.system("ask-system")
    let echo = system.actorOf({ => EchoActor() }, "echo")

    try {
        let reply = AskInbox.ask(echo, StringMessage("hi")).get(Duration.second * 3)
        match (reply) {
            case s: StringMessage => println("响应: ${s.getContent()}")   // 输出：响应: echo: hi
            case _ => ()
        }
    } catch (e: TypedAskTimeoutException) {
        println("超时: ${e.message}")
    }

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

Actor 失败时，由监督策略决定如何处理：`Resume`（忽略并继续）、`Restart`（重启）、`Stop`（停止）或 `Escalate`（交给上级）。内置策略包括 `OneForOneStrategy`、`OneForAllStrategy`、`EscalateStrategy` 和带指数退避的 `BackoffSupervisionStrategy(BackoffPolicy(...))`（返回 `RestartAfter`，延迟期间挂起 Actor），位于 `cactor_core.supervision`。类型化 API 用 `TypedSupervisorStrategy.restartWithBackoff(min, max, randomFactor)`，两者共用同一套退避算法（抖动、自动复位、次数上限）。

---

## 架构设计

项目是一个 cjpm workspace，由 `modules/` 下的 15 个模块组成。能力从底层向上组合，依赖只能指向下层；精确依赖见[包结构](docs/zh/package-structure.md)。

```text
foundation → core → runtime → cactor
                  → patterns
                  → remote → cluster
                  → persistence
cactor → streams / xstate / platform / testkit
独立宏模块：macros；最上层：tests / examples
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
| remote | `cactor_remote` | TCP、可靠通道、远程 ask/watch、TLS |
| persistence | `cactor_persistence` | 日志、快照、类型化持久化 |
| cluster | `cactor_cluster` | 成员协议、分片、单例、服务发现与分布式数据 |
| testkit | `cactor_testkit` | 运行时探针、同步行为测试与故障测试工具 |
| xstate | `cactor_xstate` | 状态机（实验性，已冻结，待 E51 移除；新代码用 `@StateMachine`） |
| platform | `cactor_platform` | 平台原语：受监督执行（SupervisedCall）、可睡眠会话（SleepableActor）、按 sessionId 寻址（SessionRegistry）、持久流（DurableStreams）、无状态 Edge 接入（EdgeRouter）、MCP server（McpServer）、按 actor 授权（AuthGate）、Party/Matchmaker/共享 KV |
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
| [ask_demo](modules/examples/src/ask_demo/main.cj) | 经典 ask（`AskInbox.ask`）：请求-响应、超时、replyTo 写法、多线程并发 ask，带自检 |
| [ask_pattern_demo](modules/examples/src/ask_pattern_demo/main.cj) | 旧 ask API（`Ask` / `AskPatternManager`，已 @Deprecated，2.0.0 移除） |
| [typed_actor_demo](modules/examples/src/typed_actor_demo/main.cj) | 类型化 Actor 系统 |
| [advanced_mailbox_examples](modules/examples/src/advanced_mailbox_examples/main.cj) | 不同 mailbox 实现 |
| [advanced_dispatcher_examples](modules/examples/src/advanced_dispatcher_examples/main.cj) | 不同 dispatcher 实现 |
| [streams_demo](modules/examples/src/streams_demo/main.cj) | 背压流 |
| [xstate_demo](modules/examples/src/xstate_demo) | 状态机（实验性，已冻结；新代码参考 `dsl_state_machine`） |
| [dsl_showcase](modules/examples/src/dsl_showcase/main.cj) | DSL 与经典 Actor 的对照示例 |
| [llm_http_demo](modules/examples/src/llm_http_demo/main.cj) | 本地 HTTP SSE、持久会话恢复续聊、取消停滞读取 |
| [agent_orchestration_demo](modules/examples/src/agent_orchestration_demo/main.cj) | 多 Agent 编排、重试、取消、持久化与流式进度 |
| [best09_cluster_sharding](modules/examples/src/best09_cluster_sharding/main.cj) | 集群分片：gossip 成员协议 + `ClusterSharding` 路由到唯一实体（单进程三节点，带自检） |
| [persistence_demo](modules/examples/src/persistence_demo/main.cj) | 事件溯源（原型） |

### 真实应用示例（`ex01`–`ex57`）

这五十七个示例不是 API 教程，而是**带真实业务语义、真实故障模型、业务断言**的应用；
设计与验收见 [docs/plan/2026117-lz.md](docs/plan/2026117-lz.md)，
与 Akka / protoactor-go 官方示例的逐项对照见
[docs/plan/20261117-porting-akka-protoactor.md](docs/plan/20261117-porting-akka-protoactor.md)。
HTTP/SSE 由 [Lou](https://gitcode.com/catorai/lou) Web 框架承载（`ex06`–`ex09`、`ex11`–`ex56` 都提供 `serve <port>` 模式，可直接 `curl` 验证；`ex57` 为线协议验证，无需 HTTP）。

| 示例 | 内容 | 压到的能力 |
|------|------|------|
| [ex01_agent_runtime](modules/examples/src/ex01_agent_runtime/main.cj) | LLM Agent 运行时：Lou 提供 HTTP/SSE，会话串行 + 工具并发 + 取消 + 崩溃恢复 | `DurableStateBehavior`、`ctx.ask`、流式取消、`PersistenceIo` |
| [ex02_tenant_orders](modules/examples/src/ex02_tenant_orders/main.cj) | 多租户订单：库存不超卖、单写者租约交接、旧 token 被 fencing 拒绝、opId 幂等 | `ClusterSharding`、`FileLease`、`FencedDurableStateStore` |
| [ex03_event_backbone](modules/examples/src/ex03_event_backbone/main.cj) | 跨节点可靠事件骨干：按序恰好一次、断连重发、背压预算、消费端幂等 | `tellReliable`、`RemotingSettings.withLimits`、`reliableDeliveryStats` |
| [ex04_edge_config](modules/examples/src/ex04_edge_config/main.cj) | 边缘–云一致性配置面：离线后反熵收敛、ORSet add-wins、SBR 决策、读一致性 | `Replicator`、CRDT、`sbrDecide`、`ReadMajority` |
| [ex05_durable_workflow](modules/examples/src/ex05_durable_workflow/main.cj) | 可恢复长任务：崩溃后从断点续跑、每步副作用幂等、自动快照、提醒只触发一次 | `EventSourcedBehavior`、`RetentionCriteria`、`DurableReminders` |
| [ex06_http_actor_api](modules/examples/src/ex06_http_actor_api/main.cj) | REST 服务（对标 protoactor `web`）：Lou CRUD + ask 存储 Actor + 监督重启 + 409/404 语义 | `TypedBehaviors`、`TypedAsk`、`supervise.restart`、Lou |
| [ex07_cluster_pubsub_chat](modules/examples/src/ex07_cluster_pubsub_chat/main.cj) | 集群聊天（对标 protoactor `chat`、Akka distributed-pub-sub）：3 节点广播 + SSE + 退订 | `DistributedPubSub`、`Replicator`、Lou `ctx.sse` |
| [ex08_cluster_singleton_job](modules/examples/src/ex08_cluster_singleton_job/main.cj) | 集群单例定时任务（对标 akka-sample-cluster-singleton）：唯一实例、tick、宕机接管 | `ClusterSingleton`、`onStarted` + `timers` |
| [ex09_grains_cart](modules/examples/src/ex09_grains_cart/main.cj) | 虚拟 Actor 购物车（对标 protoactor `cluster-grain` / Orleans）：按需激活、单一激活、空闲停用、存储恢复 | `Grains`、`FileDurableStateStore` |
| [ex10_fsm_order](modules/examples/src/ex10_fsm_order/main.cj) | 订单状态机（对标 akka-sample-fsm）：合法/非法迁移、支付超时自动取消、订单隔离 | `withTimers` + 状态即行为 |
| [ex11_router_pool](modules/examples/src/ex11_router_pool/main.cj) | 路由器池（对标 Akka Routers / protoactor router）：轮询分发、广播、目标动态增删、终止自动摘除 | `Routers.pool/group`、`Broadcast`、`AddRoutee` |
| [ex12_streams_sse](modules/examples/src/ex12_streams_sse/main.cj) | 流式管道 + SSE（对标 akka-sample-streams）：有序输出、async 边界、有界队列背压/明确丢弃 | `Sources/Flows/Sinks`、`async`、`Source.queue` |
| [ex13_projection_read_model](modules/examples/src/ex13_projection_read_model/main.cj) | 事件投影读模型（对标 Akka Projection/CQRS）：全量重建、增量投影、按 (id,seq) 幂等 | `FileEventStore.replay`、`append(expectedSeq)` |
| [ex14_circuit_breaker](modules/examples/src/ex14_circuit_breaker/main.cj) | 断路器 + 退避重启（对标 Akka CircuitBreaker / protoactor middleware）：快速失败、半开恢复、指数退避 | `BasicCircuitBreaker`、`restartWithBackoff` |
| [ex15_work_queue](modules/examples/src/ex15_work_queue/main.cj) | 分布式工作队列（对标 akka-sample-cluster / protoactor work pull）：Receptionist 发现、一次一任务、重试与死信、worker 掉线重排 | `ClusterReceptionist`、租约超时重排 |
| [ex16_persistence_upcast](modules/examples/src/ex16_persistence_upcast/main.cj) | 事件 schema 演进（对标 Akka Persistence）：v1/v2 混存恢复、按 manifest upcast、未知版本明确失败 | `EventAdapter`、`journal_v2` |
| [ex17_remote_watch](modules/examples/src/ex17_remote_watch/main.cj) | 远程监视与隔离（对标 protoactor remote / Akka DeathWatch）：Terminated、停止后明确失败、quarantine | `watchRemote`、`quarantine` |
| [ex18_durable_pubsub](modules/examples/src/ex18_durable_pubsub/main.cj) | 持久化发布订阅（差距 4.8 的应用层补法）：离线消息重放、每订阅者游标、重启不回退 | `FileEventStore` + `FileDurableStateStore` |
| [ex19_cluster_router](modules/examples/src/ex19_cluster_router/main.cj) | 集群感知路由（差距 4.5 的应用层补法）：按实时负载选目标、停用即剔除、无候选明确失败 | `ClusterReceptionist` + `AskInbox` |
| [ex20_singleton_lease](modules/examples/src/ex20_singleton_lease/main.cj) | 租约保护单例（对标 Akka singleton + lease）：单一实例、宕机后租约到期接管、全程至多一个 | `ClusterSingletonSettings.withLease`、`FileLease` |
| [ex21_read_min_cap](modules/examples/src/ex21_read_min_cap/main.cj) | 读一致性 minCap（差距 4.6 的框架修复）：2/4 副本可达时 ReadMinCap(2) 成功、ReadMajority 明确失败 | `ReadConsistency.ReadMinCap`（新增） |
| [ex22_grains_async](modules/examples/src/ex22_grains_async/main.cj) | 虚拟 Actor 异步/可重入（对标 protoactor cluster-grain）：200 个 callAsync、可重入链、非重入明确失败 | `Grains.callAsync`、`Grain.reentrant()` |
| [ex23_sharding_passivation](modules/examples/src/ex23_sharding_passivation/main.cj) | 分片实体空闲钝化（对标 Akka sharding passivation）：空闲 300ms 停用、再访问从持久化状态恢复 | `ShardingSettings.passivating`、`DurableStateStore` |
| [ex24_at_least_once](modules/examples/src/ex24_at_least_once/main.cj) | 至少一次投递 + 幂等消费（差距 4.13 的应用层补法）：重发只生效一次、去重表跨重启、计数可观测 | `deliveryId` + `DurableStateStore` |
| [ex25_broadcast_hub](modules/examples/src/ex25_broadcast_hub/main.cj) | 广播中心与每订阅者背压（差距 4.8 的应用层补法）：快慢订阅者隔离、有界队列明确丢弃、SSE | `Sources.queue` + `DropNew`、Lou SSE |
| [ex26_graceful_shutdown](modules/examples/src/ex26_graceful_shutdown/main.cj) | 集群优雅离开（差距 4.1 的框架修复）：协调关闭先 leave、幸存者看到 MemberLeft/Exited、upCount 3→2 | `Cluster.installCoordinatedShutdown`（新增） |
| [ex27_streamref_pipeline](modules/examples/src/ex27_streamref_pipeline/main.cj) | 跨节点 StreamRef（对标 Akka StreamRefs）：200 条按序传输、buffer=4 背压、取消传播、本地 SSE | `StreamRefs.sourceRef/subscribe`、`StreamRefSettings` |
| [ex28_actor_selection](modules/examples/src/ex28_actor_selection/main.cj) | 远程选择器（对标 Akka ActorSelection，差距 4.1 的框架修复）：`worker-*`/`worker-?` 通配投递、名称解析、resolveOne 唯一性、内部入口隔离 | `SimpleRemoteManager.actorSelection`、`ActorSelection`（新增） |
| [ex29_custom_downing](modules/examples/src/ex29_custom_downing/main.cj) | 可插拔脑裂决策（对标 Akka DowningProvider，差距 4.2 的框架修复）：东区必须可用的业务规则，5 节点真实断网后西区多数派停止、东区少数派存活 | `DowningProvider`、`DowningStrategy.Custom`、`ClusterSettings.withDowningProvider`（新增） |
| [ex30_distributed_config](modules/examples/src/ex30_distributed_config/main.cj) | 分布式配置（对标 Akka DData ORMap/LWWMap，差距 4.6 的框架修复）：并发改不同 key、同 key add-wins、观察-删除不复活、LWW 墓碑 | `ORMap`、`LWWMap`、`Replicator.putInMap/putInLWWMap`（新增） |
| [ex31_cluster_router](modules/examples/src/ex31_cluster_router/main.cj) | 集群感知路由器（对标 Akka cluster-aware router，差距 4.5 的框架修复）：LeastLoaded/RoundRobin、成员 down 自动剔除、无候选明确失败 | `ClusterRouter`（新增） |
| [ex32_distributed_trace](modules/examples/src/ex32_distributed_trace/main.cj) | 跨节点追踪关联（差距 4.10 的传播原语）：W3C traceparent 生成/解析、span 逐跳接续、两侧日志 Mdc 带同一 traceId、坏输入降级 | `toTraceParent/parseTraceParent/continueTrace`、`withTraceMdc`（新增） |
| [ex33_cluster_metrics](modules/examples/src/ex33_cluster_metrics/main.cj) | 集群指标导出（差距 4.10 的指标部分）：成员/收敛/gossip/隔离/可靠投递未确认/分片托管/实体数，Prometheus 文本 + MetricRegistry 同源 | `ClusterMetrics`（新增） |
| [ex34_shard_allocation](modules/examples/src/ex34_shard_allocation/main.cj) | 分片分配策略 SPI（差距 4.4）：同一集群共存两种策略——按分片奇偶落到两端、全部钉在最小地址节点 | `ShardAllocationStrategy`、`withAllocationStrategy`、`FirstMemberAllocation`（新增） |
| [ex35_otlp_export](modules/examples/src/ex35_otlp_export/main.cj) | OTLP 导出（差距 4.10 的导出部分）：批量导出器、失败隔离（下游抛异常不影响业务）、OTLP/JSON、跨节点 span 父子链 | `SpanExporter`、`BatchSpanExporter`、`toOtlpJson`、`Tracer.attachExporter`（新增） |
| [ex36_cluster_client](modules/examples/src/ex36_cluster_client/main.cj) | 集群客户端（差距 4.3 / C2c）：外部进程不加入集群，按联系点查 Up 成员后轮询投递；成员下线自动更新，无联系点明确失败 | `ClusterClient`（新增） |
| [ex37_projection](modules/examples/src/ex37_projection/main.cj) | 持久化投影进度（差距 4.12）：事件投影成读模型、每标识偏移落盘、重启从断点续跑、reset 后全量重建 | `Projection`、`ReadJournal.currentEventsByTag`、`DurableStateStore`（新增） |
| [ex38_schema_upcast](modules/examples/src/ex38_schema_upcast/main.cj) | 事件 schema 演进（差距 4.7 的框架修复）：v1（元）/v2（元;补贴）/当前格式（分）混合日志恢复，冷重启同样生效，旧事件不被改写 | `EventAdapter.upcast`（新增）、`EventSourcedBehavior` 恢复路径 |
| [ex39_grain_storage](modules/examples/src/ex39_grain_storage/main.cj) | Grain 状态存储钩子（差距 4.7 的框架修复，对标 Orleans IGrainStorage）：激活前自动读回、停用后自动写出、跨进程重启续算，业务不写读/写样板 | `GrainStorage`、`Grain.encodeState/decodeState`、`Grains(..., storage: ...)`（新增） |
| [ex40_at_least_once](modules/examples/src/ex40_at_least_once/main.cj) | 至少一次投递 + 幂等扣费（差距 4.13）：稳定 deliveryId、同一信封重发只生效一次、去重表落盘、接收端重启仍识别重复 | `AtLeastOnceDelivery`、`DeliveryEnvelope`、`IdempotentReceiver`、`DurableDeliveryIdStore`（新增） |
| [ex41_durable_subscriber](modules/examples/src/ex41_durable_subscriber/main.cj) | 持久订阅（差距 4.8）：离线期间消息不丢、重连按序补齐、游标落盘重启不重复消费、处理失败不推进游标、SSE 先补历史再接实时 | `DurableTopic`、`DurableSubscription`（新增） |
| [ex42_agent_swarm](modules/examples/src/ex42_agent_swarm/main.cj) | 多 Agent 协作编排（§31 首选）：审批点暂停、崩溃续跑、工具幂等、预算失败、trace 根+步骤 span、SSE 事件回看 | `DurableStateStore`、`DurableTopic`、`AtLeastOnce` 思想、`SpanRecord/toOtlpJson`、Lou |
| [ex43_inference_gateway](modules/examples/src/ex43_inference_gateway/main.cj) | 多租户推理网关（§31 第 2 项）：配额 429、上游故障转移、断路器打开、计费幂等、SSE 逐 token、Prometheus 指标 | `BasicCircuitBreaker`、`AtLeastOnceDelivery`、`FileDurableStateStore`、Lou |
| [ex44_agent_memory](modules/examples/src/ex44_agent_memory/main.cj) | Agent 记忆与画像（§31 第 3 项）：v1/v2/当前混合 schema、时间线与画像双读模型、全量重建、幂等写入、变更流离线补齐、租户隔离 | `Projection`、`EventAdapter.upcast`、`DurableTopic`、`IdempotentReceiver`、Lou |
| [ex45_edge_twin](modules/examples/src/ex45_edge_twin/main.cj) | 边缘-云数字孪生（§31 第 4 项）：离线写、CRDT 双向收敛、墓碑防复活、能力 add-wins、持久化欠账缓冲 | `LWWMap`、`ORMap`、`DurableTopic`、Lou |
| [ex46_live_lakehouse](modules/examples/src/ex46_live_lakehouse/main.cj) | CDC 到实时数仓（§31 第 5 项）：v1/v2/v3 混合 schema、快慢双 sink、慢消费者有界丢弃、离线补齐、全量回填、位点/滞后 | `DurableTopic`、`EventAdapter.upcast`、Lou |
| [ex47_grain_reminder](modules/examples/src/ex47_grain_reminder/main.cj) | Grain 提醒（差距 4.7，对标 Orleans reminder）：到期投递到当前激活、一次性确认后不重复、可取消、跨进程持久化 | `Grains.withReminders`、`registerReminder/cancelReminder`、`Grain.onReminder`（新增） |
| [ex48_metrics_endpoint](modules/examples/src/ex48_metrics_endpoint/main.cj) | 统一指标端点（差距 4.10）：运行时 + 集群 + 应用三段一次渲染，段落无重复换行，Lou `/metrics` 直接输出 | `MetricsExporter`（新增）、`ClusterMetrics`、Lou |
| [ex49_shard_state](modules/examples/src/ex49_shard_state/main.cj) | 分片协调状态持久化（差距 4.4）：手动再平衡覆盖落盘，整集群重启后仍生效，无存储时不回退 | `ShardingSettings.withStateStore`、`loadedOverrideOf`、Lou |
| [ex50_trace_chain](modules/examples/src/ex50_trace_chain/main.cj) | 本地 Actor 链路 trace 自动传播（差距 4.10 剩余部分）：三跳共享 traceId、span 各不相同、无作用域不产生 traceId | runtime 自动 `TracedMessage` 包装、`Mdc.currentTraceParent`（新增）、Lou |
| [ex51_durable_pubsub](modules/examples/src/ex51_durable_pubsub/main.cj) | 持久发布订阅（差距 4.8 剩余部分）：离线只进日志、回放补齐、游标不重复、在线扇出与落日志并存 | `DistributedPubSub.withDurableLog`、`replayDurable`、`DurableTopic.replay`（新增）、Lou |
| [ex52_iot_devices](modules/examples/src/ex52_iot_devices/main.cj) | IoT 设备管理（对标 Akka 官方 IoT 示例）：组→设备层级、重复注册拒绝、组内读数聚合、空组语义 | 类型化 Actor 层级（`TypedBehaviors.setup` + `ctx.actorOf`）、`TypedAsk`、Lou |
| [ex53_distributed_workers](modules/examples/src/ex53_distributed_workers/main.cj) | 分布式计算 Worker（对标 Akka Distributed Workers）：任务拆分、本地/远端交替执行、`context.reply` 跨节点回传、执行节点可观测 | 经典 Actor、远程 sender 回复、`remoteActorOf`、Lou |
| [ex54_remember_entities](modules/examples/src/ex54_remember_entities/main.cj) | 记住实体（对标 Akka remember-entities）：宕机后不发新消息也主动恢复实体、恢复数可观测、恢复后换节点 | `ShardingSettings.remembering`、`restartedCount`、Replicator、Lou |
| [ex55_role_routing](modules/examples/src/ex55_role_routing/main.cj) | 按角色路由（对标 Akka cluster-aware router roles）：角色随 gossip 传播、网关不抢 worker 任务、候选自动收敛 | `Cluster(..., roles: ...)`、`membersWithRole`、`ClusterRouter.withRole`（新增）、Lou |
| [ex56_data_center](modules/examples/src/ex56_data_center/main.cj) | 跨数据中心就近路由（差距 4.3 部分实现）：DC 随 gossip 传播、只选同 DC 候选、跨 DC 零命中 | `Cluster(..., dataCenter: ...)`、`dataCenters`、`ClusterRouter.withDataCenter`（新增）、Lou |
| [ex57_large_messages](modules/examples/src/ex57_large_messages/main.cj) | 大消息压缩与分片（差距 4.9）：1 MiB 文本线上压缩、100 KiB 高熵文本按 32 KiB 分片、内容逐字节一致 | `configureWire(maxFrameBytes)`、压缩/重组计数、真实 TCP |
| [ex58_shard_roles](modules/examples/src/ex58_shard_roles/main.cj) | 分片宿主角色（对标 Akka `ClusterShardingSettings.withRole`）：64 分片只落 worker、网关托管 0 但仍可转发、实体宿主可观测 | `ShardingSettings.withRole`（新增）、`Cluster(..., roles: ...)`、`shardStats`、Lou |
| [ex59_graph_dsl](modules/examples/src/ex59_graph_dsl/main.cj) | 流扇出编排（对标 Akka GraphDSL 的 Broadcast/Partition）：一份数据三份落地、同 key 恒定同分区、全部出口取消则上游结束 | `GraphDSL.broadcast`、`GraphDSL.partitionBy`（新增）、Lou |
| [ex60_graph_balance_zip](modules/examples/src/ex60_graph_balance_zip/main.cj) | 流负载均衡与配对（对标 Akka Balance/Zip）：元素只处理一次且分布均衡、两路按位置配对、长度不等按短侧结束 | `GraphDSL.balance`、`GraphDSL.zip`（新增）、Lou |
| [ex61_stream_actor_fanout](modules/examples/src/ex61_stream_actor_fanout/main.cj) | 流扇出到 Actor（对标 Akka Sink.actorRef）：一份事件驱动审计/指标/归档三个 Actor，各自状态独立且计数一致 | `GraphDSL.broadcast` + `Sinks.foreach` + 经典 Actor、Lou |
| [ex62_shard_role_join](modules/examples/src/ex62_shard_role_join/main.cj) | 分片角色的动态可用性：只有网关时消息缓冲不丢，worker 加入后自动排空；网关始终不托管 | `ShardingSettings.withRole`、`ClusterSharding.stats/ownerCount`、无宿主告警（新增）、Lou |
| [ex63_shard_assignment](modules/examples/src/ex63_shard_assignment/main.cj) | 分片协调状态（全量归属）恢复（差距 4.4 收尾）：预置 10 分片 9/1，两个区域都加载并按存储托管 | `ShardingSettings.withStateStore`、`ClusterSharding.assignmentKey/loadedAssignmentOf`（新增）、Lou |
| [ex64_mcp_lou](modules/examples/src/ex64_mcp_lou/main.cj) | MCP 工具服务挂载在 Lou（对标 protoactor-go web + MCP）：initialize/tools list/call、业务失败 isError、未知工具 -32602，HTTP 全部走 Lou | `cactor_platform.mcp.McpServer.handle`、`McpToolProvider`、Lou `POST /mcp` |
| [ex65_auth_gateway](modules/examples/src/ex65_auth_gateway/main.cj) | Scoped Token 鉴权网关（平台 auth + Lou）：无令牌 401、过期/篡改可区分、无权限 403、路径覆盖有边界、`*` 通配 | `ScopedTokenCodec`、`AuthGate.check`、`AccessOp`、Lou `Authorization` |
| [ex66_kv_ddata](modules/examples/src/ex66_kv_ddata/main.cj) | 基于 DData 的共享 KV（平台 kv + Lou）：双向反熵、PNCounter 合并、删除传播、前缀列举、读多数 | `DDataKvStore`、`Replicator`、Lou `PUT/GET/DELETE /kv` |
| [ex67_sleep_sessions](modules/examples/src/ex67_sleep_sessions/main.cj) | 睡眠会话（平台 lifecycle + Lou）：空闲会话不占 Actor、状态落盘、唤醒后从断点继续、多会话隔离 | `SleepRegistry`、`SleepableActor`、`SleepPolicy.Explicit/Idle`、Lou |
| [ex68_matchmaker](modules/examples/src/ex68_matchmaker/main.cj) | 属性匹配（平台 matchmaker + Lou）：同 mode/属性才配、rank 容差、TTL Expired、按 mode 观测等待数 | `Matchmaker.submit/expireNow/waitingByMode`、Lou |

---

## 测试

```bash
cjpm test                           # 运行全部测试
python3 tools/check_module_deps.py  # 检查模块分层约束
```

workspace 目前共有 366 个测试用例（`tools/ci.sh`），在仓颉 1.2.0 下全部通过；CI 另外运行聊天室、5 万子 Actor 规模测试、20 秒稳定性测试，以及两个 3 进程集群验证（正常收发与 `kill -9` 宕机）。除监督策略、类型化 Actor（行为变换、Stash、定时器）、mailbox 外，还覆盖并发语义（串行处理、并发创建/停止、监督重启）、队列压力、TCP/UDP 传输、远程端到端、文件事件日志与事件溯源恢复。

基准程序：`./target/release/bin/cactor_examples.real_benchmark`。[2026-10-28 历史测量](docs/plan/20261028-performance.md)记录了 Linux / Xeon 8582C（32 逻辑核）、cjc 1.2.0、workspace `-O2`、8 个调度线程下的结果：单 Actor 5.6–7.2 M msg/s，ping-pong 往返 P50 约 2 µs，本机回环远程 TCP 146–194 k msg/s。与 Akka 2.6.21 / OpenJDK 17 的差异仅适用于该报告的场景和预热配置，不能代表当前工作区或其他机器。后续邮箱、调度与远程实现已有变化，当前版本的同机对比仍需在空闲机器复测；[ROADMAP §2.2](ROADMAP.md#22-历史性能记录)保留后续观察及限制。

---

## 当前限制

以下问题已经确认，正在按 [开发计划](docs/plan/20261028.md) 逐步处理：

- **调度**：在默认调度器的 `receive` 中阻塞会占住工作线程，同一线程上的其他 Actor 也会被拖慢。与 Akka 的扇出差距以历史报告为限，当前实现需要同机复测。
- **远程通信**：基于 `std.net` 的 TCP 传输。普通 `tell` 是至多一次；需要可靠性时用 `tellReliable`（序号、确认、重发、去重）。支持远程 ask、跨节点回复和远程 watch。消息编解码器带版本号，可以滚动升级（新字段由新代码按旧版本解析，旧节点明确拒绝更新的版本）。暂不支持 TLS。
- **集群**：`cactor_cluster.membership` 提供 gossip 成员协议、脑裂处理（KeepMajority / KeepOldest / StaticQuorum）、Cluster Singleton、分布式发布订阅和集群分片（空闲钝化、remember-entities，可与事件溯源结合在移交和宕机后恢复状态），`cactor_cluster.ddata` 提供 CRDT 与 Replicator，均有多节点测试与多进程验证（`tools/cluster_demo.sh`、`tools/cluster_crash_demo.sh`）。脑裂处理使用随 gossip 传播的可达性表，非对称分区中的间接连接成员会被 down；虚拟 Actor（`Grains`，对标 Orleans）支持按需激活、单一激活、空闲停用与宕机后重新激活；Replicator 的更新以增量推送（按来源序号、断档向来源补齐），摘要式反熵只传不同的键，支持 Majority / All 读写，节点移除后由 Leader 两阶段 pruning 把它的数据并入存活节点。`cactor_cluster` 根包中的旧原型类（`SimpleClusterManager`、`ShardRegion` 等）已在 2026-11 删除，分片/路由请直接用 `membership` 下的 API。
- **宏**：`@actor`（协议清单编译期核对）、`@Message`（消息类型名 + 编解码器 + registerCodec）、
  `@StateMachine`（状态机骨架 + 编译期检查）已可用；`@actorReceive` 为 `@actor` 的兼容别名。
  详见 [docs/dsl.md](docs/dsl.md)。
- **日志**：库内日志走分级日志（默认 WARN，输出到 stderr，用环境变量 `CACTOR_LOG_LEVEL` 调整），但仓库中还有三套日志实现，计划在 1.3.0 合并。
- **基准口径**：早期部分 benchmark 测的是同步函数调用开销，不能当作消息传递吞吐。使用任何基准数字前须核对计时区间、消息是否实际处理完、编译选项、线程数及机器负载；微基准、存活堆和端到端消息基准分别报告。

---

## 发布到中心仓

完整的版本选择、API 兼容检查、CI 与 `vX.Y.Z` tag 流程见 [发布指南](docs/RELEASE.md)。下列命令会实际发布到中心仓，应在发布检查完成后执行。

`tools/publish.sh` 负责把 12 个库模块按依赖顺序发布到 https://pkg.cangjie-lang.cn 。它把模块拷到
workspace 之外、把 path 依赖改写成中心仓版本号后再 `cjpm bundle` + `cjpm publish`，因为 cjpm
既不允许发布 workspace，也要求发布产物的依赖是版本号。

```bash
# 一次性配置全局 token（写入 $CANGJIE_HOME/tools/config/cangjie-repo.toml 的 [repository.home]）
sed -i '' 's/^token = .*/token = "<你的 token>"/' "$CANGJIE_HOME/tools/config/cangjie-repo.toml"

source tools/env.sh
tools/publish.sh                 # 按依赖顺序发布全部 12 个模块（版本取 modules/foundation/cjpm.toml）
tools/publish.sh core runtime    # 只发布指定模块
```

三个容易踩的坑（脚本已处理）：

1. **不能把 `[replace]` 带进产物**。cjpm 会把 `cjpm.toml` 一起打进 artifact，若其中写着
   `[replace] x = { path = "/绝对/路径" }`，别的机器上就会报
   `the path '/...' does not exist which is listed in 'replace' field`。因此暂存目录的依赖一律从中心仓解析。
2. **索引是异步生成的**。发布成功后约 1–3 分钟索引里才会出现新版本，期间消费方解析报
   `no available version for module 'x' by require '1.2.1'`；`cjpm` 还会把旧索引缓存在
   `~/.cjpm/repository/index/<org>/<2>/<2>/<name>` 且不再回源。脚本会轮询索引并清理本机缓存。
3. **`cjpm publish` 的 lint 门禁**。它的 bundle 用本机 cjlint 默认规则集，`G.FUN.02`（未使用参数）
   会把大量接口实现桩判成 MANDATORY 违规。脚本默认 `--skip-lint`（`LINT=1` 可开启严格模式），
   代码风格由 `tools/ci.sh` 把关。

发布前请确认 `modules/<m>/README.md` 存在（artifact 根目录必须有 README），并把各模块
`cjpm.toml` 的 `version` 一起升版本——中心仓不允许同名同版本覆盖，也没有删除/撤回命令。

---

## 路线图

完整路线图见 [ROADMAP.md](ROADMAP.md)。版本规划概要：

- [x] **1.2.0**：升级到仓颉 1.2.0；拆分为多模块 workspace；核心运行时、经典与类型化 API 语义（监督树、watch、become、定时器、Stash 等）；基于 std.net 的远程与文件持久化；可复现的基准测试
- [x] **1.2.1**：12 个库模块发布到仓颉中心仓（`cactor_*` 1.2.1），修复发布产物携带 `[replace]` 绝对路径的问题

- [ ] **1.3.0 收敛**：删除死代码与重复实现；统一日志、配置、ask 与 Scheduler；真实 JSON；API 快照；测试去掉 `sleep`；空闲机器上复测基准
- [ ] **1.4.0 新 API 与 HTTP**：`actorSystem {}` / `spawn` / `ask` 新入口，`@Actor` / `@Message` 宏，Ktor 风格 HTTP 服务端与 Actor 融合
- [ ] **1.5.0 性能与可靠远程**：Native HTTP 引擎、扇出优化、远程 ask / watch、可靠投递、TLS
- [ ] **1.6.0 集群与生产化**：成员协议、Singleton、脑裂处理；HTTP 客户端、WebSocket、生产插件
- [ ] **2.0.0**：分片、持久化插件接口、API 冻结

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
| [Agent 接入](docs/agent-integration.md) | 面向 pic 等 agent 运行时：会话寻址、睡/醒、MCP server、Edge 与流 |
| [性能调优指南](docs/zh/performance.md) | 编译优化、运行时参数、编写 Actor 的性能规则、如何测量 |
| [文档索引](docs/zh/README.md) | 全部中文文档 |
| [更新日志](CHANGELOG.md) | 版本变更记录 |

部分文档还在随代码一起更新，个别内容可能和当前实现不一致，以代码为准，也欢迎指出。

---

## 参与贡献

欢迎提交 Issue 和 Pull Request，无论是修复问题、补充测试还是改进文档。提交前请先阅读 [贡献指南](CONTRIBUTING.md)，并确认：

```bash
source tools/env.sh                 # 仓颉环境 + stdx 路径
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
