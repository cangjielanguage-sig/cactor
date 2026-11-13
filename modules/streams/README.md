# cactor_streams

按 Reactive Streams 实现的背压流（对标 Akka Streams）：惰性、可重复物化的 `Source` / `Flow` / `Sink` 蓝图，
运行在类型化 Actor 上，慢消费者面对无限流时内存有界。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_streams = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_streams` | `Source` / `Flow` / `Sink` / `RunnableGraph`，工厂 `Sources` / `Flows` / `Sinks`，`Materializer`、`StreamSettings`、`KillSwitches`、流内监督、`OverflowStrategy`、`SourceQueue` / `SinkQueue` |
| `cactor_streams.reactive` | `Publisher` / `Subscriber` / `Subscription` / `Processor` 接口与规范辅助（不依赖运行时） |

## 快速上手

```cangjie
import cactor_core.dsl.*
import cactor.api.CActor
import cactor_streams.*

let system = CActor.system("demo")
// 蓝图是惰性的：构建时不执行任何东西，run 时才物化
let graph = Sources.range(1, 100)
    .filter({ x => x % 2 == 0 })
    .mapAsync<Int64>(4, { x => lookup(x) })          // 最多 4 个在途，按顺序发出
    .toMat(Sinks.seq<Int64>(), { _, r => r })
let result = graph.run(system).get(3.seconds)

// 询问 Actor：Sources/Flows 的 ask 以有界并行度调用 TypedAsk.ask，流结束时撤掉未回复的 ask
Sources.from<String>(items).ask<PriceCmd, Int64>(4, pricer, { item, r => PriceCmd.Quote(item, r) }, 3.seconds)

// 无限流 + 外部结束
let (ks, done) = Sources.tick<String>(0.millis, 1.seconds, "tick")
    .viaMat<String, UniqueKillSwitch, UniqueKillSwitch>(KillSwitches.single<String>(), { _, k => k })
    .toMat(Sinks.foreach<String>({ t => println(t) }), { k, d => (k, d) })
    .run(system)
ks.shutdown()
```

完整示例：`modules/examples/src/streams_demo`。

## 执行模型

- **阶段与连接**：每两个相邻阶段之间是一条连接，下游 `pull` 一次上游才能 `push` 一个元素，连接上最多 1 个在途元素。
- **岛（island）**：默认整张图融合在一个类型化 Actor 里执行（事件队列驱动，无重入、无锁）；`.async()` 把图切开，
  之前的阶段与之后的阶段在两个 Actor 里执行。岛每处理 `syncProcessingLimit`（默认 1000）个事件让出一次调度线程。
- **需求协议**：岛之间用 Reactive Streams 的 `request(n)` 连接，下游一次请求 `inputBufferSize`（默认 16）个，
  消耗一半再补，所以边界上在途 + 缓冲的元素永远不超过 `inputBufferSize`。
- **物化**：`run(system)` / `run(materializer)` 为每个岛创建一个 Actor，返回物化值（完成 Promise、kill switch、
  `SourceQueue`、`Publisher` 等）。`Materializer.create(ctx)` 把岛建成当前 Actor 的子 Actor，宿主停止时流以
  `AbruptStageTerminationException` 终止。
- **结束与取消**：完成 / 失败向下游传播，取消向上游传播，跨岛同样成立。

## 算子一览

- 源：`single`、`from`、`fromIterator`、`empty`、`failed`、`range`、`repeat`、`unfold`、`iterate`、`fromPromise`、
  `tick`、`queue(bufferSize, overflow[, maxConcurrentOffers])`、`actorRef(bufferSize, overflow, completeWhen, failWhen)`、
  `fromPublisher`、`asSubscriber`
- 变换：`map`、`filter`、`filterNot`、`collect`、`mapConcat`、`take`、`takeWhile`、`drop`、`dropWhile`、
  `Flows.grouped`、`scan`、`fold`、`mapAsync`、`mapAsyncUnordered`、`ask`、`throttle`、`buffer(n, OverflowStrategy)`、
  `recover`、`recoverWithRetries`、`flatMapConcat`、`watchTermination`、`async`、`withSupervision`
- 汇：`foreach`、`ignore`、`fold`、`seq([maxElements])`、`head`、`headOption`、`cancelled`、`queue`、
  `actorRefWithBackpressure`、`asPublisher`、`fromSubscriber`
- 其他：`KillSwitches.single` / `shared`、`Flow.toProcessor`

## 流内监督

`withSupervision(decider)` 作用于蓝图内执行用户函数的阶段（map / filter / collect / mapConcat / takeWhile /
dropWhile / scan / fold / mapAsync / ask / flatMapConcat / Sinks.foreach / Sinks.fold）：`Stop`（默认，流失败）、
`Resume`（丢弃出错元素）、`Restart`（丢弃出错元素并把 scan / fold 的状态重置为初始值）。内层设置优先。

## 与旧实现的差异

旧的 `cactor_streams.core` / `dsl` / `reactive` / `unified` 四套实现（对数组急切求值、没有背压）已删除，
`StreamSource` / `StreamFlow` / `StreamSink` / `SourceFactory` 等不再提供；迁移到 `Sources` / `Flows` / `Sinks`。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/streams
cjpm test -m modules/streams
```

## License

MIT
