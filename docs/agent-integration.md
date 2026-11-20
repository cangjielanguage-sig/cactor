# CActor 与 Agent 运行时的接缝（G4）

本文面向 `~/pix/pic` 这类 **agent 运行时**：说明它如何把 CActor 当底层运行时使用。
CActor **不做 Agent 语义**（LLM 客户端、agent loop、提示词、工具定义、记忆都不在本仓库），
只提供四类原语：会话寻址、睡/醒、受监督执行、接入与流。

> 本文引用的每个公开类型都必须存在于 `docs/api-snapshot/`（`tools/check_agent_docs.py` 在 CI 校验）。
> 端点与协议方法（`tools/list`、`GET /v1/edge/{sessionId}` 等）是实现中的固定字符串。

## 1. 按 sessionId 取 Actor

一个会话 = 一个按 id 寻址的长期 Actor。分片负责"同一 id 落在同一实体"，无需自己维护路由表。

```cangjie
// 每个节点建一次（消息需要可远程序列化）
let sharding = ClusterSharding(system, remote, "chat-session", props)
let sessions = SessionRegistry(sharding)

sessions.tell("user-42", ChatMessage.Hello("hi"))          // 直接进分片
let ref = sessions.session<ChatMessage>("user-42")         // 路由型引用，不占本地 Actor
let p = sessions.ask("user-42", ChatMessage.Query(), 3.seconds)
```

类型：`cactor_platform.session.SessionRegistry`、`cactor_cluster.membership.ClusterSharding`。

## 2. 会话睡/醒

空闲会话不应长期占 Actor。`SleepPolicy.Idle` 到期后把状态写入 `DurableStateStore` 并停止实例；
新消息唤醒时读回状态，唤醒期间的消息不丢、不乱序、不重复。

```cangjie
class ChatSession <: SleepableActor {
    var turns: Int64 = 0

    public func state(): String { turns.toString() }
    public func restore(s: String): Unit { turns = Int64.parse(s) }

    public func onMessage(message: Message, ctx: ActorContext): MessageResult {
        // 业务处理；返回 Handled / Unhandled
        MessageResult.Handled
    }
}

let registry = SleepRegistry(system, store)
let policy = SleepPolicy.Idle(30.minutes)
let handle = registry.handle("user-42", policy, { => ChatSession() })
handle.tell(StringMessage("hello"))     // 睡着时先唤醒再投递
```

类型：`cactor_platform.lifecycle.SleepableActor`、`cactor_platform.lifecycle.SleepPolicy`、`cactor_platform.lifecycle.SleepRegistry`。

## 3. 工具执行受监督

工具调用（HTTP、数据库、外部进程）可能失败或阻塞。`SupervisedCall` 把它放进受监督 Actor：
失败重启并重跑，超时取消，隔离级别选 `ThreadPerTask` 时不占亲和调度线程。

```cangjie
let exec = SupervisedCall<String>(system.typed())
let result = exec.submit("web-search", { token => search(token) }).get(5.seconds)
```

工具实现里可把业务失败包成 `McpToolBusinessException`（见 §4），协议层会将其作为内容返回。

类型：`cactor_platform.exec.SupervisedCall`、`cactor_platform.exec.IsolationLevel`。

## 4. MCP server：让 pic 的 MCP client 连进来

CActor 提供 MCP server，pic 已有的 MCP client 无需改造即可把 Actor、实体、流当工具调用。
只做协议与调度；**工具 schema 与实现由使用方给**。

```cangjie
class MyTools <: McpToolProvider {
    public func list(): Array<ToolDescriptor> {
        [ToolDescriptor("get_weather", "查询天气",
            #"{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}"#)]
    }

    public func call(name: String, argsJson: String, ctx: McpCallContext): TypedPromise<String> {
        let p = TypedPromise<String>()
        // 参数不合法等业务失败：作为内容返回（isError=true）
        // p.tryFailure(McpToolBusinessException("city 不能为空"))
        p.trySuccess(#"{"temp":21}"#)
        p
    }
}

McpServer().mount("/mcp", MyTools()).start()   // 后台 stdx.net.http 服务
```

协议方法：`initialize`、`notifications/initialized`（通知，202）、`ping`、`tools/list`、`tools/call`。

错误形态（可区分）：

| 情况 | 响应 |
|---|---|
| 未知工具 / `params` 不是对象 | JSON-RPC `-32602`（invalid_params） |
| 执行超时 / provider 抛异常 | JSON-RPC `-32603`（internal_error） |
| 工具业务失败 | 正常 `result`，`content[].text` + `isError: true` |
| 未知方法 / 坏 JSON | `-32601` / `-32700` |

类型：`cactor_platform.mcp.McpServer`、`cactor_platform.mcp.McpToolProvider`、
`cactor_platform.mcp.ToolDescriptor`、`cactor_platform.mcp.McpCallContext`、
`cactor_platform.mcp.McpToolBusinessException`。

## 5. Edge 接入：长连接与业务状态分离

客户端用 SSE / WebSocket 接 Edge；连接只落在 Edge 节点，会话 Actor 在集群任意节点，
断线凭恢复令牌从 `lastAck + 1` 续传。

```cangjie
// Edge 节点：Receptionist 负责跨节点解析；存储保存出站流
let resolver = ReceptionistEdgeResolver(receptionist)          // service: edge-session-<id>
let router = EdgeRouter(system, store, resolver, "edge-secret")

match (router.connect("user-42")) {
    case Connected(conn) =>
        conn.deliver("hello")                                   // 入站 → 会话 Actor
        let batch = conn.poll()                                 // 出站：从 lastAck+1 读
        for (r in batch.records) {
            conn.ack(r.offset)                                  // 确认后断线不重放
        }
        let token = conn.recoveryToken()                        // 客户端保存
    case Unauthorized(status, reason) => ()
}
```

- 握手：`GET /v1/edge/{sessionId}`，头 `edge-resume-token`；
  成功头 `edge-session-id` / `edge-last-ack` / `edge-next-offset` / `edge-resume-token`；
  过期 401、令牌与会话不匹配或签名不符 403、无会话 404、非 GET 405。
- SSE：`EdgeSse` 编码，`id:` 是 offset，多行 `data:`；单连接 60 秒驻留上限，
  到期结束让客户端带令牌重连。
- WebSocket：`EdgeWs` 文本帧，首行 offset，其后为载荷。

类型：`cactor_platform.edge.EdgeRouter`、`cactor_platform.edge.ReceptionistEdgeResolver`、
`cactor_platform.edge.LocalEdgeResolver`、`cactor_platform.edge.EdgeConnection`、
`cactor_platform.edge.EdgeGateway`、`cactor_platform.edge.EdgeSse`、`cactor_platform.edge.EdgeWs`、
`cactor_platform.edge.EdgeRecoveryToken`。

会话登记的约定：会话 Actor 启动时用 `ClusterReceptionist.register("edge-session-<id>", ref)`
登记（`ReceptionistEdgeResolver` 默认前缀 `edge-session`）。

## 6. Durable Streams：可续读、可 fork 的事件流

```cangjie
let streams = DurableStreamRegistry(store)
let s = streams.stream("chat/user-42", ttlSeconds: 7 * 24 * 3600)
let offset = s.append("token:hello", producerId: "agent")
let read = s.read(fromOffset: 1, maxRows: 64)
```

HTTP 协议（与框架无关，`DurableStreamProtocol` 只映射头）：

- `POST /v1/stream/{path}`：追加，头 `producer-id` / `stream-ttl`；
- `GET /v1/stream/{path}`：长轮询读，头 `stream-next-offset`；
- fork：`POST` 目标路径 + 头 `stream-forked-from` / `stream-fork-offset`；
- 响应头 `stream-seq` / `stream-next-offset` / `stream-cursor` / `stream-expires-at`；
- 过期返回 410，未知方法 405。

类型：`cactor_platform.durable.DurableStreamRegistry`、`cactor_platform.durable.DurableStream`、
`cactor_platform.durable.DurableStreamProtocol`。

## 7. 一条完整的 pic 接入路径（AR11 目标）

1. 启动时建 `ClusterSharding` + `SessionRegistry` + `SleepRegistry`；
2. 每个会话的 tool 调用经 `SupervisedCall` 执行；
3. 会话事件写入 `DurableStream`，客户端经 `EdgeRouter` 的 SSE/WS 续读；
4. 外部 agent（或 pic 里的其它角色）经 `McpServer` 的 `tools/list`/`tools/call` 调用；
5. pic 侧只改三处：tool 执行换 `SupervisedCall`、会话换 `SleepableActor`、远程接入换 `EdgeRouter`。

## 8. 明确不在本仓库

LLM 客户端、agent loop、提示词、工具 schema 的生成、记忆/向量、OpenAI/Anthropic 兼容网关、
A2A —— 都由 pic 实现。CActor 保证上述原语的公开 API 稳定（快照门禁），且不反向依赖 pic。
