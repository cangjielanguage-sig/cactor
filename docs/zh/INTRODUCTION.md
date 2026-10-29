# CActor 介绍：仓颉里的 Actor，也是 AI Agent 的天然容器

> 面向仓颉（Cangjie）1.2.0 · CActor 1.2.0 · MIT 许可证
>
> 项目处于早期开发阶段，API 仍可能调整。本文描述的是 1.2.0 的能力与边界。

**定位**：CActor 是仓颉 Actor 模型库，定位为 AI Agent 的底层运行时——承载 Agent 的**状态、并发、失败与位置**，提供**私有上下文、串行邮箱、监督树和位置透明**。它同时是 DSL 化的：**枚举即协议**，`@actorReceive` 编译期生成分派，`100.millis` 与 `<<` 简化超时和发送，`TypedBehaviors`、Streams、XState 声明式描述行为与工作流。Agent 只写“做什么”，调度、隔离与容错交给 CActor。

| Agent 需要承载的 | CActor 提供的 |
|---|---|
| 状态（记忆、会话、工具调用的中间结果） | 私有上下文：状态只属于一个 Actor，外部只能发消息 |
| 并发（大量 Agent 同时工作） | 串行邮箱：一次处理一条消息，内部不需要锁；实测百万个 Actor |
| 失败（模型超时、工具出错） | 监督树：父 Actor 按策略重启 / 恢复 / 停止 / 上报 |
| 位置（单进程到多节点） | 位置透明：本地与远程同一套 `tell` / `ask` / `watch`，集群成员、Singleton、分片、发布订阅 |

---

## 一、为什么在仓颉里做 Actor

并发编程真正难的地方，从来不是"把任务丢进线程池"，而是**状态**：谁可以改它、什么时候改、改到一半失败了怎么办。

Actor 模型给了一个很朴素的答案：

- 每个 Actor 拥有**私有状态**，外部改不到；
- 外部只能往它的**邮箱**里投消息；
- Actor **按顺序**处理消息，一次一条；
- 出错不往外抛，交给**上级**按监督策略处理。

于是"共享内存 + 锁"变成了"隔离状态 + 消息"。这恰好也是仓颉擅长的部分：枚举（`enum`）可以干净地描述消息协议，`match` 的穷尽检查能在编译期拦住漏处理的分支，不可变数据和函数式写法让"返回新行为"比"改字段"更自然。

CActor 就是把这套模型落到仓颉里的一个尝试。设计上参考 Akka / Akka Typed，但没有照搬 Java/Scala 的 API 形状，而是尽量用仓颉的语言特性来表达：**协议用枚举，状态迁移用返回值，错误用监督树兜住**。

---

## 二、CActor 是什么

一句话：**CActor 是仓颉的 Actor 运行时 + 常用并发模式库**，由 12 个可独立依赖的 cjpm 模块组成，从单机消息传递一路覆盖到远程通信、集群分片、事件溯源和流处理。

它主要提供四类东西：

| 类别 | 内容 |
|------|------|
| 两种编程风格 | 继承 `Actor` 的经典风格；基于 `TypedBehavior<T>` 的类型化风格（参考 Akka Typed） |
| 容错 | 监督策略（OneForOne / OneForAll / Escalate / Backoff）、断路器、监督树 |
| 常用模式 | Ask 请求-响应、路由、背压、有界邮箱与溢出策略 |
| 运行时与分布式 | 多种 mailbox / dispatcher；远程 TCP（远程 ask / watch、可靠投递）；集群成员协议、Singleton、发布订阅、分片、CRDT；事件溯源；Streams、XState（这两项为实验性） |

它**不**试图做的是：替代语言自带的协程/线程原语，或者提供一个"什么都替你决定"的框架。CActor 更接近一层基础设施——你可以只用 `cactor_core` 写几个 Actor，也可以按需把 `streams`、`remote`、`cluster` 叠上去。

---

## 三、三个最小例子

### 1. 经典 Actor：继承 + `receive`

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
    actor.tell(StringMessage("Hello, CActor!"))
    system.terminate()
    return 0
}
```

`receive` 的返回值是 `MessageResult`，告诉运行时这条消息的去向：`Handled`、`Unhandled`、`Stop`、`Restart`、`Escalate`。生命周期回调 `preStart` / `postStop` / `preRestart` / `postRestart` 与 Akka 的语义一致。

### 2. 类型化 Actor：协议是枚举，状态是行为

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
    ref.tell(Print)                 // 输出：count = 2
    system.terminate()
    return 0
}
```

类型化风格里，**发错消息编译不过**；状态不需要可变字段，而是通过返回下一个 `TypedBehavior` 来演进。`TypedBehaviors` 还提供 `setup`、`withTimers`、`withStash`、`stopped`、`receiveSignal` 等组合子。

### 3. Ask：把消息往返变成一次请求

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
                case s: StringMessage => println("响应: ${s.getContent()}")   // echo: hi
                case _ => ()
            }
        case Result.Failure(error) => println("失败: ${error.message}")
    }

    Ask.shutdown()
    system.terminate()
    return 0
}
```

`Ask` 有同步 `askAndWait`、异步 `askAsync`、批量等入口，超时由 `Duration` 控制。

---

## 四、12 个模块，按需依赖

CActor 是一个 cjpm workspace。库本身拆成 12 个模块，依赖方向**只能由上层指向下层**：

```
            用户代码 / 示例 / 测试
                     │
   ┌────────┬────────┼─────────┬──────────┐
   ▼        ▼        ▼         ▼          ▼
streams  distribution  xstate  integration  testkit
             │
   ┌─────────┼──────────┐
   ▼         ▼          ▼
remote   persistence  cluster
   │         │          │
   └─────────┴────┬─────┘
                  ▼
               cactor（门面）
                  │
                  ▼
           cactor_patterns（Ask / Router / Backpressure）
                  │
                  ▼
           cactor_runtime（Mailbox / Dispatcher / Guardian / Registry）
                  │
                  ▼
           cactor_core（Actor / ActorRef / Message / Supervision / Typed）
                  │
                  ▼
           cactor_foundation（Queue / Serialization / Memory / Network / DI / CircuitBreaker）
```

| 模块 | 包名 | 职责 | 状态 |
|------|------|------|------|
| foundation | `cactor_foundation` | 无锁队列、内存池、序列化、网络抽象、配置、DI、监控、断路器 | 可用 |
| core | `cactor_core` | Actor、ActorRef、ActorSystem、Message、Context、监督、类型化 Actor | 可用 |
| runtime | `cactor_runtime` | mailbox、dispatcher、guardian、registry、生命周期管理 | 可用 |
| patterns | `cactor_patterns` | Ask、路由、背压 | 可用 |
| cactor | `cactor` | 统一对外门面（`CActor`、`CActorSystem`、`ActorProps` 等） | 可用 |
| testkit | `cactor_testkit` | TestActorSystem、TestProbe、断言工具 | 可用 |
| streams | `cactor_streams` | 流处理 DSL、背压、15+ 操作符 | 实验性 |
| remote | `cactor_remote` | 基于 std.net 的 TCP 远程通信 | 原型 |
| persistence | `cactor_persistence` | 事件溯源、快照、CQRS | 原型 |
| cluster | `cactor_cluster` | 成员协议、分片、Singleton、分布式发布订阅、CRDT | 原型 |
| distribution | `cactor_distribution` | 聚合 remote / persistence / cluster 的重导出模块 | 原型 |
| xstate | `cactor_xstate` | XState 风格状态机，可与 Actor 组合 | 实验性 |

> 另外还有 `cactor_macros`（宏，独立）、`cactor_integration`（日志/指标/配置集成）、`cactor_tests` 和 `cactor_examples`，它们不属于这 12 个库模块。

**按需依赖**是这套分层最实际的价值：

```toml
[dependencies]
cactor          = { path = ".../modules/cactor" }        # 门面，通常只需要它
cactor_core     = { path = ".../modules/core" }          # 想直接用核心类型时
cactor_patterns = { path = ".../modules/patterns" }      # 需要 Ask / Router / 背压时
cactor_testkit  = { path = ".../modules/testkit" }       # 写测试时
```

只写单机 Actor 就只依赖 `cactor`（和按需的 `patterns`）；不用流处理就不会把 Streams 拉进构建。

---

## 五、为什么 Actor 天然适合 AI Agent

把"AI Agent"拆开看，它其实是一个长期存活、拥有私有上下文、被事件驱动、经常失败、需要被监督的实体——这几乎就是 Actor 的定义。

| Agent 的需求 | Actor 模型里的对应物 | CActor 中的落点 |
|---|---|---|
| 每个 Agent 有独立上下文，不能互相踩 | 私有状态 + 隔离 | `Actor` 实例 / `TypedBehavior` 闭包 |
| 用户输入、工具结果、定时唤醒都汇入同一个处理循环 | 邮箱 + 串行处理 | `Mailbox` + `tell` |
| 一次工具调用要拿到结果，还要防超时 | 请求-响应 | `Ask.ask` / `askAsync` / `AskFuture` |
| 工具报错、模型超时、上游限流要有章法 | 监督与退避 | `OneForOneStrategy`、`BackoffSupervisorStrategy`、断路器 |
| 上下文不能无限膨胀，忙不过来要拒绝 | 有界邮箱与背压 | `withBoundedMailbox(capacity, overflow)` |
| 多轮决策、等待人工审批、分支工作流 | 状态机 | `xstate`（实验性） |
| Token / 事件要流式吐给前端 | 流处理 | `cactor_streams`（实验性） |
| 对话与决策要可回放、可恢复 | 事件溯源 + 快照 | `cactor_persistence`（原型） |
| 按会话 ID 把 Agent 分布到多台机器 | 分片 + 位置透明 | `cactor_cluster`（原型） |
| Agent 行为要能被断言 | TestProbe | `cactor_testkit` |

更具体地说，有三点特别契合：

**1. 串行邮箱 = 天然的"一次只做一个决策"**

LLM 的推理循环本质是串行的：读上下文 → 决定下一步 → 调工具 → 把结果放回上下文。Actor 的邮箱恰好保证了同一时刻只有一条消息在被处理，不需要为"上下文被并发修改"加锁。并发度来自"很多个 Agent 各自串行"，而不是"一个 Agent 内部并行"。

**2. 监督树 = Agent 的失败处理策略**

Agent 系统里失败是常态：工具 500、网络抖动、JSON 解析失败、模型超时。Actor 的监督策略提供了四种明确动作：

| 指令 | 含义 | Agent 场景 |
|---|---|---|
| `Resume` | 保留状态，继续下一条 | 可忽略的工具错误、单次解析失败 |
| `Restart` | 重建 Actor，清空内部状态 | 上下文被污染，需要从干净状态重来 |
| `Stop` | 停止 Actor | 不可恢复的错误，或任务已完成 |
| `Escalate` | 交给上级决定 | 子 Agent 无法自行决定，交由编排者处理 |

再叠加 `BackoffSupervisorStrategy`（指数退避 + 抖动）和断路器，就能把"重试"从散落在业务代码里的 `try/catch + sleep` 收敛成声明式配置。

**3. 位置透明 = 从单机 Demo 到多 Agent 集群不用重写**

`ActorRef` 是引用而不是对象指针：本地 Actor 和远程 Actor 用同一套 `tell` / `Ask`。原型阶段所有 Agent 跑在一个进程里，需要拆到多机时，改的是部署与地址，而不是业务协议。

---

## 六、一个 Agent 的形状

用 CActor 搭一个最小的"Agent + 工具"结构，消息流大致是这样：

```
        用户 / 网关
            │  tell(UserInput)
            ▼
       ┌───────────┐   Ask(ToolCall, timeout)   ┌───────────┐
       │ AgentActor│ ─────────────────────────▶ │ ToolActor │ ──▶ 外部 API
       │           │ ◀───────────────────────── │           │
       └───────────┘   AskResponse / Failure    └───────────┘
            │
            │ 上下文更新、决策、流式输出
            ▼
        前端 / 下一个 Agent
```

对应到代码里的元素：

- **协议**：用 `enum` 定义 `AgentCommand`（`UserInput` / `ToolResult` / `Tick` / `Approve`），让 `match` 帮你检查分支；
- **AgentActor**：持有对话上下文与待办状态，收到 `UserInput` 后决定调用哪个工具；
- **ToolActor**：把外部 API 包成 Actor，失败就抛异常，让监督策略处理，而不是在 Agent 里写重试；
- **Ask**：需要"调用并等待"时用 `Ask.ask(toolRef, ToolCall(...), timeout)`；不希望阻塞 `receive` 时用 `askAsync`，把结果作为一条新消息投回自己；
- **监督**：给 ToolActor 配 `OneForOneStrategy` 或 `BackoffSupervisorStrategy`；给 AgentActor 配更保守的策略（通常 `Restart` 或 `Stop`，因为上下文重建代价高）；
- **有界邮箱**：入口 Actor 用 `withBoundedMailbox`，忙不过来时按策略丢弃或拒绝，避免请求无限堆积；
- **持久化**：需要恢复对话时，把"用户输入 / 工具结果 / 决策"作为事件写入事件日志，重启后重放。

一个重要的工程提醒：**不要在 `receive` 里做长时间阻塞**。CActor 当前是亲和调度，阻塞会占住工作线程，拖慢同一线程上的其他 Actor。耗时的工具调用应该交给独立的 ToolActor（或用 `blocking` dispatcher），Agent 自己只做编排。

---

## 七、性能与现状

在 32 核机器上的实测（`cactor_examples.real_benchmark`）：

| 场景 | 结果 |
|---|---|
| 单 Actor 消息吞吐 | 5.6–7.2 M msg/s |
| Ping-pong 往返延迟 | P50 约 2 µs |
| 本机回环远程 TCP | 150–190 k msg/s |

同机与 Akka 2.6.21 的对比：点对点场景已追平或超过，扇出（fan-out）仍慢约 3 倍，远程慢约 1.6 倍。这些数字来自早期基准，测试方法和复现步骤见 [性能分析](../plan/20261028-performance.md)。

需要如实说明的边界（1.2.0）：

- **调度**：亲和调度，尚无工作窃取；`receive` 中阻塞会影响同线程的其他 Actor；
- **远程**：普通 `tell` 是至多一次语义；需要可靠性时用 `tellReliable`（序号、确认、重发、去重）；暂不支持 TLS；
- **集群**：成员协议、分片、Singleton 已有实现和多节点测试，但分片实体移交后不保留状态，也还没有脑裂处理策略；
- **持久化 / Streams / XState / 宏**：处于原型或实验阶段，API 可能变化；
- **文档**：部分文档仍在随代码更新，遇到不一致以代码为准。

---

## 八、快速开始

要求：仓颉 SDK 1.2.0（含 `cjc`、`cjpm`），目前主要在 Linux x86_64 验证。

```bash
git clone https://atomgit.com/Cangjie-SIG/cactor.git
cd cactor
cjpm build
./target/release/bin/cactor_examples.hello_world_demo
```

运行测试与分层约束检查：

```bash
cjpm test
python3 tools/check_module_deps.py
```

更多示例在 `modules/examples/src`：`ask_pattern_demo`、`typed_actor_demo`、`chat_room`（多客户端会话，接近多 Agent 协作的形状）、`cluster_sharding_demo`、`persistence_demo` 等。

---

## 九、路线图

- **1.2.0（当前）**：升级仓颉 1.2.0；拆分为多模块 workspace；监督树、watch、become、定时器、Stash 等语义；基于 std.net 的远程与文件持久化；可复现基准
- **1.3.0 收敛**：删除死代码与重复实现；统一日志、配置、Ask 与 Scheduler；真实 JSON；API 快照；测试去掉 `sleep`
- **1.4.0 新 API 与 HTTP**：`actorSystem {}` / `spawn` / `ask` 新入口，`@Actor` / `@Message` 宏，Ktor 风格 HTTP 服务端与 Actor 融合
- **1.5.0 性能与可靠远程**：Native HTTP 引擎、扇出优化、远程 ask / watch、可靠投递、TLS
- **1.6.0 集群与生产化**：成员协议、Singleton、脑裂处理；HTTP 客户端、WebSocket
- **2.0.0**：分片、持久化插件接口、API 冻结并发布到中心仓

Agent 方向要补的，主要是三块：**可靠投递与超时语义**（工具调用的基础）、**可恢复的会话状态**（持久化插件化）、**流式输出**（Streams 与 HTTP 的衔接）。这三块在 1.5–1.6 的规划里。

---

## 十、参与

CActor 采用 MIT 许可证。欢迎 Issue 和 PR，提交前请确认：

```bash
cjpm build
cjpm test
python3 tools/check_module_deps.py
```

新功能请附带测试；涉及对外 API 的改动请同步更新文档和 `CHANGELOG.md`。设计讨论、路线图细节见 [ROADMAP.md](../../ROADMAP.md) 与 [贡献指南](../../CONTRIBUTING.md)。

---

**相关文档**

- [README](../../README.md) · [English README](../../README_EN.md)
- [快速开始](QUICK_START.md) · [入门指南](getting-started.md)
- [架构设计](architecture.md) · [包与模块结构](package-structure.md)
- [API 参考](api-reference.md) · [最佳实践](BEST_PRACTICES.md) · [性能调优](performance.md)
- [更新日志](../../CHANGELOG.md) · [路线图](../../ROADMAP.md)
