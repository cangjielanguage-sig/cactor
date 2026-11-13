# CActor DSL 指南：把消息处理写成自然语言

> 对照实现计划：[docs/plan/dsl20261030.md](plan/dsl20261030.md)（R5「自然语言化消息处理」的落地文档）。
> 本文是面向使用者的指南；计划文档记录设计取舍与验证数据。

## 一句话

用**枚举协议**或 **`@Message` 类**表达消息，用 **`@actor`** 把 `onXxx` 方法编译成类型分派的 `receive`，
用 **`cases` / `protocol` 清单**在**编译期**发现漏处理的消息，用 **`Behaviors`** 组合子表达行为与修饰语，
用 **`@StateMachine`** 声明状态机骨架，用 **`Source |> Flow |> Sink`** 表达流管道。

所有错误（漏处理、重复处理器、非法字段、漏状态、非法转换）都在**编译期**报错；运行时不反射、不装箱。

---

## 1. 三档写法与选型

| 形态 | 适合 | 写法 | 编译期检查 |
|---|---|---|---|
| **A：宏**（推荐） | 有明确消息协议的 Actor | `@actor[cases = [A, B]] class X { func onA(m: A, ctx: ActorContext) {...} }` | ✅ 漏处理 / 重复处理器 / 协议漂移 |
| **B：lambda** | 状态多、需要动态换行为的 Actor | `Behaviors.receive<T> { ctx, msg => ... }` | ✅ 枚举 match 穷尽性（语言保证） |
| **C：经典** | 兜底 / 动态协议 | 手写 `receive` + `match` | ❌ |

**推荐顺序**：形态 A → 形态 B → 形态 C。
形态 A 与形态 B 共享同一套运行时语义（见 §5 的映射表），可按需混用：同一个工程里，
"有稳定协议的 Actor" 用 A，"协议会变、状态多" 的用 B。

> 形态 A 与形态 B 的等价性由测试保证：`modules/tests/src/unit/dsl/actor_macro_test.cj`
> 的 `testMacroPathEqualsLambdaPath` 用同一条消息序列驱动两条路径，断言可观测结果一致。

---

## 2. 协议层：消息即名词

### 2.1 枚举协议（进程内首选）

```cangjie
enum CounterMsg <: Message {
    | Increment(Int64)
    | GetCount

    public func messageType(): String {
        "CounterMsg"
    }
}
```

* 一个枚举表达**整个协议**，枚举内部的穷尽性由仓颉 `match` 在编译期保证；
* 协议里带 `ActorRef`（本地引用）也可以，枚举不做序列化。

### 2.2 `@Message` 类（需要跨网络 / 编解码时）

```cangjie
@Message
class StartJob <: Message {
    let jobId: String

    init(jobId: String) {
        this.jobId = jobId
    }
}
```

`@Message` 自动生成：`messageType()`、二进制编解码器、`registerCodec()`。

支持的字段类型（P2 增强后）：

| 类型 | 说明 |
|---|---|
| `String`、`Int64`、`Float64`、`Bool` | 基本类型 |
| `Array<String>`、`Array<Int64>`、`Array<Float64>`、`Array<Bool>` | 数组 |
| `Option<String>`、`Option<Int64>`、`Option<Float64>`、`Option<Bool>` | 可选值（1 字节标记 + 值） |
| `HashMap<String, T>`（`T` ∈ 上述四种基本类型） | 字符串键值表 |

> **注意**
> 1. 用 `@Message` 的文件需要 `import cactor_core.message.*`（宏展开会用到编解码器类型）。
> 2. 字段类型不支持时，错误会**定位到具体字段**（带行列），例如：
>    `@Message: BadField.payload 的类型 Int32 不支持`。
> 3. 带 `ActorRef` 的消息**不能**加 `@Message`（`ActorRef` 不可序列化）——这是有意为之的限制。

---

## 3. 声明层：`@actor` 即动词

### 3.1 基本形态

```cangjie
@actor[cases = [Increment, GetCount]]
class CounterActor <: Actor {
    private var count: Int64 = 0

    func onIncrement(m: Increment, ctx: ActorContext): MessageResult {
        count += m.n
        MessageResult.Handled
    }

    func onGetCount(m: GetCount, ctx: ActorContext): MessageResult {
        m.replyTo << CountIs(count)
        MessageResult.Handled
    }
}
```

宏生成的内容（等价于手写）：

```cangjie
public func receive(message: Message, context: ActorContext): MessageResult {
    match (message) {
        case m: Increment => this.onIncrement(m, context)
        case m: GetCount => this.onGetCount(m, context)
        case _ => MessageResult.Unhandled
    }
}
```

并补齐缺省的 `name` / `description` 属性。

### 3.2 处理方法签名

| 签名 | 语义 |
|---|---|
| `func onXxx(m: T)` | 无上下文，返回 `Unit` 视为 `Handled` |
| `func onXxx(m: T, ctx: ActorContext)` | 有上下文，返回 `Unit` 视为 `Handled` |
| `func onXxx(m: T, ctx: ActorContext): MessageResult` | 返回值原样透传 |

### 3.3 协议清单：`cases` 与 `protocol`

| 写法 | 含义 | 适用 |
|---|---|---|
| `@actor[cases = [A, B, C]]` | 协议由若干**消息类型**组成，每种类型一个处理方法 | 类式协议（每种消息一个类） |
| `@actor[protocol = E]` | 协议是**单个枚举类型**，一个处理方法 + 内部 `match` | 枚举协议 |

清单在编译期核对三件事：

1. **漏处理**：清单里的类型没有对应处理方法 → 报错
   `@actor: Counter 未处理 Query，缺少 onQuery(m: Query, ctx: ActorContext)`
2. **协议漂移**：处理了清单外的类型 → 报错
   `@actor: Counter 处理了未在协议清单中声明的类型 Foo（onFoo）`
3. **重复处理器**：同一类型两个处理方法 → 报错
   `@actor: Dup 中有多个处理 Inc 的方法（onIncAgain），后一个永远不会被调用`

> **为什么清单要显式写？**
> 宏只能看到自己被标注的那段 token，**无法跨包查到调用方包里的枚举声明**
> （计划文档 §12 假设 A 已证伪）。因此协议清单由调用方给出，宏负责逐项核对。
> 枚举协议内部的穷尽性由仓颉 `match` 保证，不需要额外清单。

### 3.4 属性形式说明

`@actor` 同时提供属性宏与非属性宏两种形态（仓颉允许同名重载），带参数时写在方括号里：

```cangjie
@actor                                     // 无协议声明（旧写法，继续可用）
@actor[]                                   // 无协议声明
@actor[cases = [A, B]]                     // 显式清单（可写限定名 pkg.A）
@actor[protocol = E]                      // 单一协议类型
```

同一个属性键（`cases` / `protocol`）写两遍会在编译期报错，不会静默合并或覆盖。
`@actorReceive` 兼容别名同样支持带/不带方括号两种写法。

---

## 4. 修饰语：`within` / `retry` 组合子

原计划的 `@within` / `@retry` **修饰宏不可行**（属性宏参数为表达式时解析有歧义，计划 §12 假设 B 证伪），
改为**运行时组合子**（语义等价，处理器体内使用）：

```cangjie
func onAskModel(m: AskModel, ctx: ActorContext): MessageResult {
    within(3.seconds, "onAskModel") {     // 超时监控：超过阈值打日志，不改返回值
        retry(3) {                         // 失败重试：Unhandled / 异常时重试
            callModel(m)
        }
    }
}
```

* `within(limit, name) { ... }`：计时监控，超过阈值打警告日志，返回值原样透传；
* `retry(attempts) { ... }`：`Unhandled` 或异常视为失败；耗尽后异常场景返回 `Escalate`，
  否则返回最后一次的 `Unhandled`。

---

## 5. 行为层：`Behaviors` 组合子（形态 B）

`cactor_core.dsl.Behaviors` 是行为层的**唯一推荐入口**（底层仍是 `TypedBehaviors`，语义完全一致）：

```cangjie
import cactor_core.dsl.*

let counter = Behaviors.receive<CounterMsg> { ctx, msg =>
    match (msg) {
        case CounterMsg.Increment(n) => Behaviors.same<CounterMsg>()
        case CounterMsg.GetCount => Behaviors.stopped<CounterMsg>()
    }
}
```

可用组合子：`receive` / `receiveMessage` / `setup` / `same` / `unhandled` / `stopped` / `ignore` / `empty` /
`supervise` / `withTimers` / `withStash` / `intercept` / `monitor` / `logMessages` /
`transformMessages`(= `mapMessages` = `contramap`) / `filter` / `receivePartial`。

### 形态 A 与形态 B 的语义映射

| 形态 A（`MessageResult`） | 形态 B（`TypedBehavior`） |
|---|---|
| `MessageResult.Handled` | `Behaviors.same()` |
| `MessageResult.Unhandled` | `Behaviors.unhandled()` |
| `MessageResult.Stop` | `Behaviors.stopped()` |
| `MessageResult.Restart` / `Escalate` | 抛 `SupervisionRequestedException`，交给监督者（`Behaviors.supervise(...)`） |

代码里可以直接用 `messageResultToBehavior<T>(result)` 做转换（`cactor_core.dsl`）。
经典运行时把 `Restart` / `Escalate` 当作失败交给监督者；类型化路径没有"请求重启"的返回值，
因此映射为抛异常，由 `supervise(...).onFailure(...)` 的策略决定重启、恢复还是停止。

---

## 6. 运算符层

| 写法 | 含义 |
|---|---|
| `ref << msg` | 发送消息（可链式：`ref << a << b`） |
| `100.millis` / `3.seconds` | 时长字面量 |
| `source |> via(flow) |> to(sink)` | 流管道（见 §8） |

---

## 6.1 ask：请求-应答的三条主路径

| 场景 | 写法 | 说明 |
|---|---|---|
| 类型化，进程外 | `TypedAsk.ask<Req, Res>(ref, { r => Req(r) }, timeout).get(timeout)` | `cactor_core.typed` |
| 类型化，Actor 内 | `ctx.ask(target, { r => Req(r) }, timeout) { outcome => ... }` | 响应经 `mapResponse` 回到自己的消息流 |
| 经典 `@actor` | `let inbox = askInbox()` + `m.replyTo << 答复` + `inbox.get(timeout)` | `cactor_core.dsl`，零侵入 |

经典路径示例（目标 Actor 只需按 DSL 标准写法回 `replyTo`，无需解包任何包装消息）：

```cangjie
import cactor_core.dsl.*

@actor[cases = [Query]]
class ServiceActor <: Actor {
    func onQuery(m: Query, ctx: ActorContext): MessageResult {
        m.replyTo << Answer("答复：${m.text}")
        MessageResult.Handled
    }
}

// 请求方（Actor 之外：main、测试线程、blocking 调度器上的 Actor）
let inbox = askInbox()
service << Query("今天天气如何", inbox)
let answer = (inbox.get(3.seconds) as Answer).getOrThrow()
```

* `askInbox()` 是一个进程内的临时"收件箱"（实现了 `ActorRef`，只收不发）；
* **不要在 Actor 处理方法里阻塞等待**：默认调度器是亲和调度且没有工作窃取，`get(timeout)` 会占住工作线程，
  被等的 Actor 若分在同一线程上就是死锁。在亲和工作线程上带超时的 `get` / `tryGet` 会直接抛
  `IllegalStateException`（零超时的 `tryGet(Duration.Zero)` 只是查看，不受限制）。
  Actor 内部的请求-应答请把 `replyTo` 设为 `ctx.self`，在对应的 `onXxx` 里接收回复（见 `dsl_agent` 示例），
  类型化 Actor 用 `ctx.ask`；确实要阻塞的 Actor 用 `Props.withDispatcher("blocking")`；
* 超时抛 `cactor_core.typed.TypedAskTimeoutException`（与 `TypedAsk.ask` / `ctx.ask` 共用同一个超时异常）；
  不想抛异常用 `tryGet(timeout)`（返回 `Option<Message>`）；
* `patterns` 里的 `Ask.ask`（包装 `AskMessage`、需要目标配合解包）保留兼容，新代码建议用 `askInbox()`。

---

## 7. 状态机 DSL：`@StateMachine`

```cangjie
enum JobState {
    | Idle
    | Running
    | Stopped
}

@StateMachine[
    stateType = JobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class JobMachine {
}
```

宏生成：

| 成员 | 说明 |
|---|---|
| `stateNames: Array<String>` | 全部状态名 |
| `initialState: JobState` | 初始状态 |
| `stateName(s: JobState): String` | 状态 → 名字（**穷尽 match**） |
| `allowedTargets(from: JobState): Array<String>` | 允许的目标状态 |
| `allowedTargetNames(from: String): Array<String>` | 同上，字符串索引版（给外部状态机工具用） |
| `canTransition(from: JobState, to: JobState): Bool` | 是否允许迁移 |
| `currentState(): JobState` / `transitionTo(to: JobState): Bool` | 实例状态与迁移（非法迁移返回 `false`） |

### guard / action 骨架

宏为**每个转换**生成一对可覆写方法（同名覆写即生效，类体里已写同名方法时宏跳过生成）：

| 生成的骨架 | 默认行为 | 覆写后语义 |
|---|---|---|
| `guard<From><To>(): Bool` | 返回 `true`（放行） | 返回 `false` 时迁移被拒，状态不变 |
| `onTransition<From><To>(): Unit` | 空操作 | 迁移成功后执行（先更新状态、后执行动作） |

```cangjie
@StateMachine[
    stateType = JobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class JobMachine {
    var acceptingJobs: Bool = true

    // 守卫：Idle -> Running 由业务条件决定
    public func guardIdleRunning(): Bool {
        acceptingJobs
    }

    // 动作：迁移成功后执行
    public func onTransitionRunningStopped(): Unit {
        println("任务收尾")
    }
}
```

方法名规则：状态名按枚举构造器原样拼接（`guard` + 源状态 + 目标状态，如 `guardIdleRunning`）。

### 命名守卫（守卫表达式）

转换表的每一项可以带第三个元素——**守卫方法名**，迁移前调用，返回 `false` 则迁移被拒：

```cangjie
@StateMachine[
    stateType = JobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running, gateOpen), (Running, Stopped)]
                   //            ^^^^^^^^ 类体里必须定义 func gateOpen(): Bool
]
class JobMachine {
    var acceptingJobs: Bool = true

    public func gateOpen(): Bool {
        acceptingJobs
    }
}
```

* 指定了命名守卫的转换不再生成 `guard<From><To>` 骨架；
* 守卫方法在类体里不存在时**编译期报错**（负例 `state_machine_unknown_guard`）。

### 子状态机（复合状态）

某个状态委托给另一台状态机：进入复合状态时子机自动**复位到初始状态**：

```cangjie
@StateMachine[
    stateType = WorkflowState,
    states = [Idle, Working, Done],
    initial = Idle,
    transitions = [(Idle, Working), (Working, Done), (Done, Idle)],
    submachines = [(Working, JobMachine)]
                   //  ^^^^^^^  进入 Working 时 JobMachine 复位
]
class WorkflowMachine {
    // 访问子状态机：sub<状态名>()，子机的迁移/守卫/动作照常用
    // 状态枚举默认没有实现 ==，比较状态用 stateName
    public func checkSub(): Bool {
        JobMachine.stateName(subWorking().currentState()) == "Idle"
    }
}
```

* 子状态机类型本身也要是 `@StateMachine` 类（需要无参构造）；
* 每台机器自动获得 `reset()`（复位到初始状态）；委托的状态必须在 `states` 清单里（编译期核对），
  同一状态只能委托一台子机；
* 宏为每个子机生成私有字段 `__sub_<状态>` 与访问器 `sub<状态名>()`（类体已有同名访问器则跳过）。

**编译期检查**（计划 §12 假设 D 的验证点）：

| 错误 | 报错 |
|---|---|
| `states` 少写一个状态 | 编译器：`non-exhaustive patterns` / `Stopped is not covered` |
| `states` 写了枚举里没有的状态 | 编译器：`'NotExist' is not a member of enum 'OrderState'` |
| 转换表引用了不在 `states` 里的状态 | 宏：`转换 Idle -> Running 中，Running 不在 states 清单里` |
| 转换重复声明 | 宏：`转换 Idle -> Running 重复声明` |
| `initial` 不在 `states` 里 | 宏：`初始状态 X 不在 states 清单里` |
| `states` 里同一状态写两遍 / 属性键写两遍 | 宏：`states 里 X 重复列出` / `属性 states 重复出现` |
| 两条转换拼出同名方法（如 `(AB, C)` 与 `(A, BC)`） | 宏：`生成的方法名相同（guardABC）` |

`canTransition` / `transitionTo` 展开为对 `(from, to)` 的一次枚举 `match`：不分配数组、不做字符串比较。

完整示例见 `modules/examples/src/dsl_state_machine`（事件用 `@Message`，状态机用 `@StateMachine`，
Actor 用 `@actor`，转换表直接喂给 xstate 的 `StateMachineBuilder`）。

---

## 8. 背压流：`Source` / `Flow` / `Sink`（E60，对标 Akka Streams）

```cangjie
import cactor_streams.*

// 蓝图惰性、可重复运行；run(system) 物化后得到物化值（这里是结果的 Promise）
let graph = Sources.range(1, 6)
    .filter({ x => x % 2 == 0 })
    .map<String>({ x => "值${x}" })
    .toMat(Sinks.seq<String>(), { _, r => r })
let result = graph.run(system).get(3.seconds)   // ["值2", "值4", "值6"]
```

* 遵循 Reactive Streams：下游 `request(n)` 才发送，慢消费者面对无限流时内存有界；
* 默认整张图在一个类型化 Actor 内融合执行，`.async()` 切出异步边界（按批请求，默认 16）；
* 常用：`mapAsync` / `ask`（有界并行）、`Sources.queue` / `Sources.actorRef`、`Sinks.actorRefWithBackpressure`、
  `KillSwitches`、流内监督 `withSupervision`；与外部 Reactive Streams 互通用 `Sources.fromPublisher` / `Sinks.asPublisher`。
* 旧的 `StreamSource` / `StreamFlow` / `StreamSink` 与 `|> via / to / keep` 管道写法已在 v2.38 删除（见 CHANGELOG 破坏性变更）。

完整示例见 `modules/examples/src/streams_demo`，API 说明见 `modules/streams/README.md`。

---

## 9. 经典写法 / DSL 写法对照

| 经典 | DSL |
|---|---|
| 手写 `receive` + `match` + `case _ => Unhandled` | `@actor[cases = [...]]` + `onXxx` 方法 |
| 每种消息一个类 + 手写 `messageType()` | 枚举协议，或 `@Message` 自动生成 |
| 手写 `name` / `description` | 宏自动补齐 |
| 协议漏处理只能靠自觉 | `cases` / `protocol` 清单编译期核对 |
| 状态机手写转换表、运行期才发现非法转换 | `@StateMachine` 转换表 + 编译期核对 |
| 流用链式 `source.via(f).to(s)` | `source |> via(f) |> to(s)` |

可运行的三组对照示例（经典 / DSL 各一份，行为完全一致）：

| 场景 | 经典 | DSL |
|---|---|---|
| Hello World | `modules/examples/src/dsl_showcase/classic/hello.cj` | `modules/examples/src/dsl_showcase/dsl/hello.cj` |
| 计数器 | `modules/examples/src/dsl_showcase/classic/counter.cj` | `modules/examples/src/dsl_showcase/dsl/counter.cj` |
| 请求-应答 | `modules/examples/src/dsl_showcase/classic/ask.cj` | `modules/examples/src/dsl_showcase/dsl/ask.cj` |

运行：`./target/release/bin/cactor_examples.dsl_showcase`（两种写法输出逐行一致）。

综合应用示例：`modules/examples/src/dsl_agent`——AI Agent 消息协议（用户输入 / 工具调用 / 工具结果 / 流式增量）
用同一套 DSL 一屏写完：可远程载荷消息用 `@Message`，带本地 `replyTo` 的请求消息用普通类，
Agent 与工具 Actor 用 `@actor`。Agent 调工具时把 `replyTo` 设为 `ctx.self`、按 `requestId` 找回对话，
处理方法从不阻塞；`askInbox()` 只在 main 线程收集回复。自检并发 16 轮对话（4 个 Agent 共用 1 个工具），
用 `CACTOR_DISPATCHER_THREADS=1` 单工作线程运行也能通过。

---

## 10. "一句英语"自检表（设计评审用）

| 代码 | 应读作 |
|---|---|
| `@actor[cases = [PlaceOrder, GetOrder]] class OrderAgent { func onPlaceOrder(m: PlaceOrder, ctx: ActorContext) {...} }` | "OrderAgent，收到 PlaceOrder 就记单并回执" |
| `within(3.seconds, "onAskModel") { ... }` | "处理请求最多三秒" |
| `retry(3) { ... }` | "失败最多重试三次" |
| `ref << msg` | "ref，送出 msg" |
| `m.replyTo << CountIs(count)` | "把计数结果回给请求方" |
| `Behaviors.supervise(child).onFailure(RestartWithBackoff(...))` | "孩子挂了就按退避重启" |
| `@StateMachine[states = [Idle, Running], transitions = [(Idle, Running)]]` | "空闲可以转运行，运行不能转空闲" |
| `source |> via(flow) |> to(sink)` | "数据源经过转换，交给汇聚" |

---

## 11. 行数对照（A1）

命令：

```bash
tools/dsl_line_count.sh
```

结果（2026-10-30 实测，代码行 = 去掉注释、空行与 import）：

| 场景 | 经典（代码行） | DSL（代码行） | 降幅 |
|---|---|---|---|
| hello | 42 | 22 | 47% |
| counter | 36 | 21 | 41% |
| ask | 55 | 32 | 41% |
| **合计** | **133** | **75** | **43%** |

> 整文件行数（含注释与 import 的固定样板）降幅为 36%：小示例里注释与 import 的固定开销占比高，
> 因此以"代码行"作为 A1 的度量口径。

---

## 12. 编译期错误验证（A2）

命令：

```bash
tools/negative_tests.sh
```

覆盖的负例（每个都是独立探测工程，期望**编译失败**且报错可读）：

| 用例 | 期望报错 |
|---|---|
| `actor_missing_case` | `未处理 Query` |
| `actor_extra_handler` | `未在协议清单中声明的类型 Query` |
| `actor_duplicate` | `多个处理 Inc` |
| `actor_unknown_attr` | `不认识属性 protocolx` |
| `message_unsupported_field` | `的类型 Int32 不支持` |
| `state_machine_missing_state` | `non-exhaustive patterns` |
| `state_machine_unknown_state` | `is not a member of enum` |
| `state_machine_bad_transition` | `不在 states 清单里` |
| `state_machine_duplicate_state` | `重复列出` |
| `ask_deprecated`（编译通过但要有告警） | `deprecated` |

---

## 13. 性能（A3）

```bash
./target/release/bin/cactor_examples.dsl_bench
```

基准分两部分：

1. **直接分派（判定 A3 用）**：不经调度器，在循环里直接调用 `receive(msg, ctx)`，4 种消息轮流出现，
   手写 `match` 与 `@actor` 生成的 `receive` 各跑 7 轮、奇偶轮交换先后、取最好成绩。
   宏的开销只可能出现在分派这一层，纯 CPU 循环把调度、唤醒、跨线程投递的噪声都排除在外。
2. **端到端 ping-pong（参考值）**：经过真实邮箱与调度器，确认两条路径端到端行为一致。

2026-10-31 实测（32 核共享开发机，workspace -O2 构建）：

| 指标 | 手写路径 | 宏路径 | 比值 |
|---|---|---|---|
| 直接分派，7 轮取最好（负载约 45） | 17.56 ns/次 | 17.48 ns/次 | **99.5%** |
| 端到端 ping-pong，单轮（参考） | — | — | 47%–168% |

宏展开 = 普通 `match`，**无反射、无装箱**，与手写代码生成的分派逻辑相同；直接分派单次运行的比值
（另做的 4 次运行：99.4% / 132% / 89% / 97.9%）以 100% 为中心随机波动，没有系统性开销。

早先只用端到端 ping-pong 判定：两次实测分别为 311.5 ms / 303.8 ms（97.5%）与 75.8 ms / 69.1 ms（91.1%），
两次之间相差 4 倍、单轮比值在 62%–176% 间波动，噪声远大于要测的差距，不能据此得出"≤2%"。
数字只在空闲机器上、用 workspace 根目录 `cjpm build`（-O2）构建后才有意义（`cjpm build -m` 不带 -O2）。

`@StateMachine` 的迁移在同一组实测中：8 状态 / 16 转换的 `transitionTo`，按 `(from, to)` 枚举 `match` 展开
比早期"查数组 + 字符串比较"的生成代码快 1.8–2.0 倍（4 次运行一致）。

---

## 14. 已知限制

| 限制 | 说明 |
|---|---|
| 协议清单要显式写 | 宏无法跨包解析枚举（计划 §12 假设 A 证伪）；枚举协议的穷尽性靠语言保证 |
| `@within` / `@retry` 不是宏 | 属性宏参数为表达式时解析有歧义（§12 假设 B 证伪），改为运行时组合子 |
| `@Message` 不支持 `ActorRef` 等引用类型 | 引用类型不可序列化；本地 replyTo 消息不要加 `@Message` |
| 枚举协议一个处理方法 | 枚举的所有 case 同类型，无法按 case 分派到不同方法（内部 `match` 即可） |
| guard/action 按转换生成方法名 | 名称由状态名拼接（`guardIdleRunning`），重命名状态需同步改覆写方法名 |
