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
| `cactor_cluster.streamref` | 有界信用制 SourceRef、StreamProducer 与 Reactive Streams 桥接 |

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
线上仍使用现有 StreamRef 协议；不提供 SinkRef 或断线可靠重放。

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
