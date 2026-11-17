# 从 Akka / protoactor-go 移植示例到 CActor（2026-11-17）

> 目标：把 Akka 与 protoactor-go 的**官方示例目录**逐个映射到 CActor 的等价写法，
> 并给出"哪些能直接搬、哪些要换个写法、哪些当前搬不了"的可执行结论。
>
> 关联：
> - 能力差距与验收：`docs/plan/20261117.md`（46 条对标项）、`docs/plan/2026117-lz.md`（10 个真实示例）
> - 实例：`modules/examples/src/best01`–`best10`（API 教程）、`real01`–`real10`（真实应用）
> - 本文不修改任何计分表；缺口编号沿用 `20261117.md` §4。

---

## 1. 方法与口径

1. **对照对象是官方示例目录**，不是逐行对照框架源码：
   - Akka：`akka-samples`（main / remote / cluster / cluster-sharding / cluster-singleton /
     distributed-pub-sub / distributed-data / persistence / fsm / streams …）。
   - protoactor-go：`examples/`（helloworld / chat / cluster-grain / cluster-sharding / remote /
     pubsub / futures / middleware / web / persistence …）。
2. **"能移植"的定义**：示例要表达的语义（消息协议、失败模型、状态演进）在 CActor 里有**等价机制**，
   且能被 `real*` 或 `best*` 中的可运行代码验证。
3. 标记：✅ 有等价实现且有可运行示例 ｜ 🟡 有机制但要换写法/缺产品化 ｜ ❌ 当前没有（引用差距编号）。
4. 本文的"移植"指**示例级移植**（用户把自己的 Actor 应用搬过来），不含二进制/协议兼容。

---

## 2. Akka 示例 → CActor 映射

| Akka 示例 | 它教什么 | CActor 等价物 | 可运行实例 | 判定 |
|---|---|---|---|---|
| `akka-sample-main` | ActorSystem + 顶层 Actor + 配置 | `CActorSystemFactory.create` / `system.actorOf` / `actorSystem {}` DSL | `best01`、`hello_world_demo` | ✅ |
| `akka-sample-remote` | 远程 tell/ask/watch、序列化 | `SimpleRemoteManager`、`remoteActorOf`、`tellReliable`、`MessageSerializer` | `best08`、`real03` | ✅ |
| `akka-sample-cluster`（stats worker + listener） | gossip 成员、集群感知路由 | `Cluster`、`ClusterEvent`、`Receptionist`；**无 cluster-aware router** | `best09`、`real02` | 🟡 差距 4.5 |
| `akka-sample-cluster-sharding` | 实体路由、remember-entities、passivation | `ClusterSharding`、`rememberedEntitiesKey`、`withPassivation` | `best09`、`real02` | ✅ |
| `akka-sample-cluster-singleton` | 唯一实例、故障接管 | `ClusterSingleton`、`isRunningHere`、`location` | **`real08`** | ✅ |
| `akka-sample-distributed-pub-sub` | 跨节点主题广播、退订 | `DistributedPubSub.subscribe/publish` | **`real07`** | ✅ |
| `akka-sample-distributed-data` | CRDT + Replicator + 一致性级别 | `GCounter/PNCounter/GSet/ORSet/LWWRegister`、`Replicator` | `real04`、`ddata_stress` | ✅（Map 系 CRDT 缺，差距 4.6） |
| `akka-sample-persistence` | PersistentActor、快照、恢复 | `EventSourcedBehavior`、`journal_v2`、`snapshot_store`、`RetentionCriteria` | `real05`、`best07` | ✅（upcast 缺，差距 4.7） |
| `akka-sample-fsm`（Buncher） | 状态机、状态超时、stash | `TypedBehaviors.receive` 返回新行为 + `withTimers` + `withStash` | **`real10`**、`best04` | ✅ |
| `akka-sample-streams` | Source/Flow/Sink、GraphDSL、背压 | `cactor_streams`（线性算子、async、buffer、kill switch） | `streams_demo`、`best05` | 🟡 缺 GraphDSL（差距 4.8） |
| `akka-sample-cluster-metrics` | 自适应负载均衡 | 无自适应路由；有 `RebalancePolicy` + 负载报告 | `real02`（再平衡） | 🟡 差距 4.5 |
| Akka Multi-DC | roles、data-center、ClusterClient | 无 | — | ❌ 差距 4.1/4.3 |
| Akka HTTP 示例 | HTTP 路由 + ask | 由 **Lou** 承载（非 CActor 模块） | `real06`/`07`/`08`/`09` | ✅ 组合方案 |
| Akka Projection / CQRS | 事件流投影读模型 | 无 `Projection`；`CQRSBus` 已弃用 | — | ❌ 差距 4.12 |

### 2.1 Akka API 概念的逐条对应

| Akka | CActor | 备注 |
|---|---|---|
| `ActorSystem(name, config)` | `CActorSystemFactory.create(name)` / `create(name, CActorRuntimeConfig)` | |
| `Props` / `Behaviors.setup` | `TypedProps` / `TypedBehaviors.setup`；经典 `Props` | |
| `ActorRef<T>` / `tell` / `!` | `TypedActorRef<T>` / `tell` / `<<` | |
| `ask` → `Future` | `TypedAsk.ask<T,R>(...).get(timeout)`，或 `onComplete`/`map` 异步 | 没有独立 Future 类型，用 `TypedPromise`/`TypedFuture` |
| `context.become` | 处理完返回**新行为** | 状态不可变、无锁 |
| `Stash` | `TypedBehaviors.withStash` | |
| `Timers` | `TypedBehaviors.withTimers` / 经典 `context.timers` | |
| `context.watch` / `Terminated` | `ctx.watch` / `TypedTerminated` | DeathPact 语义一致 |
| `SupervisorStrategy` | `TypedBehaviors.supervise(...).onFailure(restart/resume/stop)`、`restartWithBackoff` | 默认策略与 Akka 一致 |
| `OneForOne/OneForAll` | 类型化监督策略；经典 API 同名策略 | |
| `ClusterSharding.entityRefFor` | `ClusterSharding.tell/ask` + `shardOf/shardOwner` | |
| `ClusterSingletonProxy` | `ClusterSingleton.tell/ask`（任意节点） | |
| `DistributedPubSub`（classic） | `membership.DistributedPubSub`（基于 Replicator 的节点集合） | 消息需可序列化 |
| `Replicator` update/get | `Replicator.incrementCounter/addToSet/getWith` | |
| `PersistentActor.persist` | `EventSourcedBehavior.persist` + `thenRun` | `thenRun` 在落盘后执行副作用 |
| `EventAdapter` + `upcast` | `EventAdapter`（无 upcast） | 差距 4.7 |
| `FSM[S,D]` | "状态即行为" + `withTimers` | 无专用 FSM DSL，但语义等价 |
| `GraphDSL` | 无组合子层 | 差距 4.8 |
| `ActorSelection`（远程） | 无 | 差距 4.1 |
| `DowningProvider` | 5 种策略写死在枚举 | 差距 4.2 |

---

## 3. protoactor-go 示例 → CActor 映射

| protoactor-go 示例 | 它教什么 | CActor 等价物 | 可运行实例 | 判定 |
|---|---|---|---|---|
| `helloworld` | Spawn + Tell | `system.actorOf` / `typedActorOf` + `tell` | `best01`、`hello_world_demo` | ✅ |
| `web` | 每请求一个 Actor / HTTP + 消息 | Lou + `TypedAsk`；存储 Actor 串行 | **`real06`** | ✅ |
| `chat` | 房间广播、跨节点 | `DistributedPubSub` + Lou SSE | **`real07`** | ✅ |
| `cluster-grain` | 虚拟 Actor（Identity/Kind） | `Grains` + `Grain.onCall` | **`real09`** | ✅ |
| `cluster-sharding` | 分片实体 | `ClusterSharding` | `best09`、`real02` | ✅ |
| `remote` | PID 跨进程 | `SimpleRemoteManager.remoteActorOf` | `best08`、`real03` | ✅ |
| `remote/relay`、`cluster pubsub` | 中继/发布订阅 | PubSub 有；**无 ClusterClient/中继网关** | `real07` | 🟡 差距 4.3 |
| `futures`（PipeTo / Recover / Timeout） | 异步编排 | `TypedPromise.map/flatMap`、`pipeToSelf`、`ctx.ask` + 超时 | `best02`、`real01` | ✅ |
| `middleware`（recovery / logging） | 横切关注点 | `TypedBehaviors.intercept`、监督策略、日志 `Mdc` | `best03` | ✅（trace 不跨节点，差距 4.10） |
| `supervision` | 父子监督 | `supervise(...).onFailure(...)` | `best03` | ✅ |
| `persistence` | 事件溯源 | `EventSourcedBehavior` | `real05`、`best07` | ✅ |
| `streams`（如果启用） | Source/Sink | `cactor_streams` | `streams_demo` | 🟡 无 GraphDSL |
| protoactor `Context.Request`（ask） | 请求-响应 | `TypedAsk.ask` / `ctx.ask` | `best02` | ✅ |
| protoactor `Context.Respond` | 回复发送方 | 经典 `context.reply` / 类型化 `replyTo.tell` | `best02` | ✅ |
| protoactor `PID`（含 address） | 位置透明引用 | `ActorRef`/`TypedActorRef`（本地与远程同一套） | `best08` | ✅ |

### 3.1 protoactor-go API 概念的逐条对应

| protoactor-go | CActor |
|---|---|
| `actor.NewActorSystem()` | `CActorSystemFactory.create(name)` |
| `actor.PropsFromProducer` / `PropsFromFunc` | `Props` / `TypedProps` / `TypedBehaviors.receive` |
| `system.Root.Spawn(props)` | `system.actorOf(...)` / `typedActorOf(...)` |
| `PID.Tell(msg)` | `ref.tell(msg)` / `ref << msg` |
| `Context.Request(pid, msg)` | `ctx.ask(...)`（不阻塞邮箱）/ `TypedAsk.ask` |
| `Context.Respond(msg)` | `context.reply(msg)` / `replyTo.tell(msg)` |
| `Receive`（func 里 switch） | `match` 穷尽 + 返回新行为 |
| `actor.SupervisorStrategy` | `TypedSupervisorStrategy` / 经典 `SupervisionStrategy` |
| `cluster.WithClusterProvider` | `Cluster(system, remote, seeds, settings)` |
| `cluster.NewClusterKind` / `Grains` | `Grains(cluster, system, remote, kind, factory, idle)` |
| `plugin/persistence` | `EventSourcedBehavior` / `journal_v2` |
| `middleware.Recovery` / `Sender` | `TypedBehaviors.intercept` + 监督 + `ctx.senderRef()` |
| `remote.Start("host:port")` | `SimpleRemoteManager(NetworkAddress(...), TransportProtocol.TCP)` + `start()` |

---

## 4. 移植配方（before → after）

### 4.1 Hello World

```cangjie
// Akka: system.Spawn(Props.Create(() => new HelloActor()))
// protoactor: root.Spawn(actor.PropsFromProducer(func() actor.Actor { return &Hello{} }))
let system = CActorSystemFactory.create("app")
let ref = system.actorOf({ => HelloActor() }, "hello")
ref.tell(StringMessage("hi"))
```

### 4.2 Ask（Akka `?` / protoactor `Request`）

```cangjie
// 外部线程（阻塞取结果）
let v = TypedAsk.ask<Get, String>(ref, { r => Get(r) }, 3.seconds).get(3.seconds)

// Actor 内部（不阻塞邮箱，结果转成自己的消息）
ctx.ask(ref, { r => Get(r) }, 3.seconds)
    .map<String>({ x => Got(x) })
    .onComplete({ m => self.tell(m) })   // 实际写法见 best02 / real01
```

### 4.3 监督

```cangjie
TypedBehaviors.supervise(behavior)
    .onFailure(TypedSupervisorStrategy.restart().withLimit(3, 1.minutes))
// 退避：TypedSupervisorStrategy.restartWithBackoff(100.millis, 10.seconds, 0.2)
```

### 4.4 分片

```cangjie
let shards = ClusterSharding(cluster, system, remote, "inventory", 64,
    { id => InventoryEntity(id) })
shards.tell("tenant-42", StringMessage("add|sku-a|1"))     // 同 id 串行
```

### 4.5 持久化

```cangjie
let behavior = EventSourcedBehavior<ShoppingCart, Event, State>(
    persistenceId: "cart-1", initialState: State(),
    onCommand: { state, cmd, ctx => ctx.persist(Event.Added(cmd.sku), { s, e => s.apply(e) }) },
    onEvent: { state, e => state.apply(e) },
    retention: RetentionCriteria.snapshotEvery(100, 3))
```

### 4.6 FSM

```cangjie
// 状态即行为：迁移返回新的 Behavior；超时用 withTimers
func order(state: String): TypedBehavior<Cmd> {
    TypedBehaviors.receive<Cmd>({ _, msg => match (msg) {
        case Pay(reply) where state == "created" => { reply.tell("ok"); order("paid") }
        case _ => TypedBehaviors.same<Cmd>()
    }})
}
```

### 4.7 HTTP 服务（Akka HTTP / protoactor web）

CActor 自身不做 HTTP。用 **Lou**（`import lou.*`）把 HTTP 翻译成 Actor 消息：

```cangjie
let app = Lou([withHost("127.0.0.1"), withPort(8080u16)])
app.post("/items/:id", { ctx =>
    let v = TypedAsk.ask<ItemCmd, String>(store, { r => ItemCmd.Create(ctx.param("id"), body(ctx), r) }, 10.seconds)
        .get(10.seconds)
    ctx.writeStringWithCode(201u16, v)
})
spawn { app.listen() }
```

---

## 5. 当前搬不过来的部分（明确边界）

| 能力 | 判据 | 影响 | 建议 |
|---|---|---|---|
| 远程 `ActorSelection` | `grep actorSelection modules/remote` = 0 | 无法按路径向未持有引用的远程 Actor 发消息 | 先用 Receptionist 按名查找（`findTyped`） |
| Multi-DC / roles / ClusterClient | 差距 4.1/4.3 | 跨机房、外部进程访问集群 | 先做 `20261117.md` C1–C2 |
| 分片协调状态持久化 | 差距 4.4 | 全灭重启后分片重排 | 实体用 remember-entities + 事件溯源兜底 |
| DData Map CRDT | 差距 4.6 | 分布式配置只能塞 `LWWRegister` | 用 `ORSet` 自拼或等 C4 |
| Projection / Durable Subscriber | 差距 4.8/4.12 | 读模型、离线订阅重放 | 用 `journal_v2` 自行 replay |
| GraphDSL / BroadcastHub | 差距 4.8 | 多入多出流编排 | 线性算子 + `async`/`buffer` 组合 |
| trace 跨节点 / OTel | 差距 4.10 | 分布式排障只能看日志 | 先做指标统一（C6） |
| 幂等 / at-least-once 高层 API | 差距 4.13 | 重复投递需自建去重表 | 用 `DurableStateStore` 做去重（`real02`/`real03` 已示范） |

---

## 6. HTTP/SSE 实测约束（用 Lou 承载时必须知道）

这三个是 `real01`–`real09` 实跑时踩到并已绕开的，不是猜测：

| 现象 | 触发条件 | 现用规避 | 建议 |
|---|---|---|---|
| 普通响应 500 | 响应体含非 ASCII（如中文） | 在 JSON 层转成 `\uXXXX`（`real06` 的 `jsonEscape`） | 修 Lou 或升级版本 |
| POST 路由 405 | 同时注册 `/x/:param` 与 `/x/:param/child`，POST 打在父子重叠的树上 | 改用静态叶子路径（`real07` 用 `POST /broadcast`） | 用路径前缀隔离（`/api/v1/...`） |
| SSE 无断开回调 | 客户端半途断开，服务端无法感知 | 显式取消端点（`real01`）或 `?limit=`（`real07`） | 见 `2026117-lz.md` G-4 |

---

## 7. 移植步骤清单

1. **消息协议**：把 Akka/protoactor 的 `case class`/`struct` 换成 `enum`；每个请求带 `replyTo`（类型化 `TypedActorRef<R>`）。
2. **状态**：把 `var`/`become` 改成"处理完返回新行为"；需要暂存消息用 `withStash`。
3. **并发**：Actor 内不要阻塞；长 IO 用 `spawn`/协程 + `ctx.ask`，或换 blocking 调度器（`best05`）。
4. **失败**：先写默认监督（restart），再针对业务加 `withLimit`/退避（`real01` 工具调用、`real05` 步骤重试）。
5. **位置透明**：本地先跑通，再换 `remoteActorOf`；跨节点消息要么实现 `Message` + 编解码器，要么用 `StringMessage` 起步。
6. **持久化**：从 `EventSourcedBehavior` 起步；副作用放 `thenRun`，不要在事件回调里做不可重入动作（`real05`）。
7. **集群**：先 `Cluster` 收敛，再上 `Sharding`/`Singleton`/`PubSub`/`Grains`；
   每个用例都等 `state().converged` 或 `ownerCount() == N`，不要裸 sleep。
8. **HTTP**：用 Lou；先把非 ASCII 输出转义、路由按前缀隔离，再谈 SSE。

---

## 8. 与 `real01`–`real10` 的对应关系

| 示例 | 对标示例 | 关键验收（可证伪） |
|---|---|---|
| real01 | protoactor `futures` + Akka streams 教程 | SSE 全量 token、取消无残留、工具失败不污染、重启恢复 |
| real02 | akka-sample-cluster-sharding | 库存不为负、旧 token 被 fencing 拒、opId 幂等、热点不饿死 |
| real03 | protoactor `remote` + reliable delivery | 2000 条按序到达、离线重发、超限计数、消费去重 |
| real04 | akka-sample-distributed-data | 4 节点收敛、离线反熵、ORSet add-wins、SBR、ReadMajority 失败 |
| real05 | akka-sample-persistence | 断点续跑、副作用各 1 次、快照、提醒只触发一次 |
| real06 | protoactor `web` | CRUD 状态码、并发 50 创建、/crash 后可用 |
| real07 | protoactor `chat` / akka distributed-pub-sub | 跨节点各 3 条、退订生效、SSE 3 条结束 |
| real08 | akka-sample-cluster-singleton | 唯一实例、计数 1/2/3、tick、宕机接管 |
| real09 | protoactor `cluster-grain` | 单一激活、跨节点共享、空闲停用后存储恢复 |
| real10 | akka-sample-fsm | 合法/非法迁移、支付超时取消、订单隔离 |

复现命令：

```bash
source tools/env.sh && cjpm build
for e in real01_agent_runtime real02_tenant_orders real03_event_backbone real04_edge_config \
         real05_durable_workflow real06_http_actor_api real07_cluster_pubsub_chat \
         real08_cluster_singleton_job real09_grains_cart real10_fsm_order; do
  CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.$e || echo "FAIL: $e"
done

# HTTP 服务的外部验证（另开终端）
./target/release/bin/cactor_examples.real06_http_actor_api serve 18086
curl -i -X POST --data 'red-fuji' http://127.0.0.1:18086/items/apple
curl -i http://127.0.0.1:18086/items/apple
```

---

## 9. 局限

1. 对照基于官方示例目录与公开文档，不做源码级逐行比对；个别示例名可能随上游版本调整。
2. 性能、内存、成本未评估；本文只讨论语义可移植性。
3. HTTP 侧依赖 Lou 2.0.0；§6 的约束来自实际运行，Lou 升级后应重新复核。
