# cactor_cluster

集群。成员管理、故障检测、ddata、分片与运维操作（实验性），构建在 remote 之上。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_cluster = "1.2.1"
```

（`stdx` 相关能力需自行声明 stdx 依赖，见根仓库说明。）
## 包

| 包 | 内容 |
| --- | --- |
| `cactor_cluster.membership` | 集群子包 |
| `cactor_cluster.detector` | 集群子包 |
| `cactor_cluster.ddata` | 集群子包 |
| `cactor_cluster.ops` | 集群子包 |
| `cactor_cluster.streamref` | 有界信用制 SourceRef / SinkRef、StreamProducer / StreamConsumer 与 Reactive Streams 桥接 |

模块根包 `cactor_cluster` 提供对外门面。

## StreamRef 与流 DSL

```cangjie
import cactor_cluster.streamref.*
import cactor_streams.*

let refs = StreamRefs(system)
let ref = refs.fromPublisher<String>(publisher, settings: StreamRefSettings().withBufferSize(4))
let result = Sources.fromPublisher<String>(refs.asPublisher<String>(ref))
    .runWith(Sinks.seq<String>(), system)
```

`asPublisher` 将单次挂接的 SourceRef 接入 `Sources.fromPublisher`；`asSubscriber(producer)`
可接入 `Sinks.fromSubscriber`，必须独占生产句柄，只在生产端有空位时向上游请求一个元素。
跨节点传递 `ref.toWireString()` 后用 `SourceRef<String>.fromWireString(...)` 还原。
背压由生产端有界缓冲、消费端信用窗口和流物化器输入缓冲共同限定；取消可传回原始 Publisher。
线上仍使用现有 StreamRef 协议；不提供断线可靠重放。

### SinkRef（方向相反：本节点消费，远端推送）

```cangjie
// 节点 B：拥有 sink，交给远端生产方
let (sinkRef, consumer) = b.refs.sinkRef<String>(settings: StreamRefSettings().withBufferSize(4))
classic.reply(StringMessage(sinkRef.toWireString()))

// 消费：pull 一次取一个；缓冲满即停止授信，背压传回远端生产方
match (consumer.pull().get(timeout)) {
    case Some(v) => ...      // 元素
    case None => ...         // 正常完成 / 已取消
}
consumer.cancel()             // 远端生产方 termination() 变为 Cancelled

// 节点 A：把元素推过来（既可直接 offer，也可作为 Subscriber 接入流 DSL）
let producer = a.refs.asSink<String>(SinkRef<String>.fromWireString(reply.getContent()))
producer.offer("token")       // Enqueued / Backpressured / Closed
Sources.from<String>(tokens).runWith(Sinks.fromSubscriber<String>(producer), system)
producer.termination().onComplete { t => ... }  // Cancelled：sink 取消/失败/属主停止
```

- 信用由 sink 侧按本地缓冲空位授予（累计上限序号），生产方只发送 `seq ≤ granted` 且序号连续；
  本地缓冲 + 在途元素 ≤ `bufferSize`。
- 单次挂接：一个 SinkRef 只接受一个生产方，第二个收到 Rejected。
- 完成/失败恰好一次；失败、消费方取消、所属 Actor 停止都会双向传播；
  元素编码失败在发送侧大声失败（`Failed(reason)` 传回 sink）。
- 订阅超时、空闲超时沿用 `StreamRefSettings`；`finalTerminationSignalDeadline`
  同时用于本地与远程，避免 Completed 与 DEATH 走不同通道时的竞态。
- 线上复用 StreamRefMsg 的 1..8 号帧，不改变协议版本（`STREAM_REF_PROTOCOL_VERSION = 1`）；
  新增的只是本地控制消息 `ConsumerPull`，不参与编码。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/cluster
cjpm test -m modules/cluster
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
