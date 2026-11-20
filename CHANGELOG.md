# 📝 CActor 更新日志

本文档记录了 CActor 项目的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### ✨ 新增：Party / Matchmaker / 共享 KV（AR9：F2 / F3 / F6）

- `cactor_platform/party`：`PartyHost` + `PartyMember`。成员表、加入幂等、上限明确拒绝
  （`accepted=false, reason="party is full"`）、广播可排除发起者；主持人 = 最早加入的存活成员，
  本地成员失联（DeathWatch）或显式离开后自动顺延。远程成员不做本地 watch（需显式 leave）。
- `cactor_platform/matchmaker`：`Matchmaker` + `MatchTicket`。按 mode 分桶（不全表扫），
  兼容条件为共同属性键取值相同（可选 `rankTolerance` 区间）；匹配与入队同锁完成，
  同一张票不会匹配两次；TTL 到期由后台 50ms 清扫并返回 `Expired`。
- `cactor_platform/kv`：`KvStore` 接口（get/getMajority/put/delete/keys/list/incr），
  `DDataKvStore`（LWWMap 存值、PNCounter 计数、`ReadMajority` 读多数，副本不足抛
  `KvReadConsistencyException`）与 `MemoryKvStore`（单节点）。
- 测试：`PartyTest` 3/3、`PartySpanTest`（跨节点广播走真实 TCP）、`MatchmakerTest` 3/3
  （含 200 票 8 线程并发恰好各匹配一次）、`KvStoreTest` 2/2、`DDataKvTest` 2/2。
- **未做**：F5 版本化 socket 协议（帧长协商/GoAway/背压容量/租约到期）需要 2.0.0 线协议授权，
  AR9 保持 🟡；F6 的 Sqlite 实现未引入（接外部 KV 实现同一接口即可）。

### ✨ 新增：Scoped 授权与 trace 跨节点传播（AR8：P6/G1 + P7/G2）

**按 actor 授权（`cactor_platform/auth`）**

- `ScopedToken`（subject / actorPath / scopes / expiresAt）+ 紧凑 HMAC 令牌
  `scim-v1|hex(subject)|hex(actorPath)|hex(scopes)|exp|hmac`，签名复用 `foundation.hashing`，
  常数时间比较；密钥可复用远程的 `secureCookie`；
- `TokenAuthorizer` 可插拔，默认实现按路径前缀（带边界）+ scope（`*` 全允许）判断；
- `AuthGate.authenticate/check` 区分 401（缺失/过期/签名非法）与 403（路径或 scope 不匹配）；
- Edge 可选 `auth: Some(AuthGate(...))`：握手读 `edge-access-token`；能解析到 Actor 时立即按真实
  路径授权，否则投递时再判并返回 `EdgeDeliverResult.Forbidden`；默认不开启时行为不变；
- 测试 `ScopedTokenTest` 4/4（只允许自己的 actor、过期 401、篡改拒绝、Edge 401/403/合法投递、网关头）。

**trace 跨节点传播（`core` / `remote` / `runtime` / `foundation`）**

- `NetworkMessage` 帧尾可选 W3C `traceparent` 块：发送线程 MDC 有 traceId/spanId 时才写；
  旧读者忽略尾部、新读者读不到按无追踪处理，**不升线协议版本**；
- 接收侧用 `TracedMessage`（仅本地投递、不序列化）把 traceparent 带进邮箱；
  `ActorCell` 拆包后用 `continueTrace` + `withTraceMdc` 在 `receive` 期间续接本地子 span；
- 测试 `RemoteTraceTest.traceIdPropagatesAcrossNodes`（B 侧 Actor 的 MDC traceId 与 A 侧一致，
  无追踪时不产生包装）；`WireCompatTest` 10/10 保持通过；
- OTel 导出与集群指标沿用既有实现（`BatchSpanExporter`/`toOtlpJson`/`ClusterMetrics`，
  `SpanExporterTest` 与 `ClusterMetricsTest` 覆盖失败不影响业务、集群指标齐全）。

### ✨ 新增：MCP server 与 Agent 接入文档（AR7：P5 / G4）

`cactor_platform/mcp` 让 pic 这类已有 MCP client 的运行时把 CActor 里的 actor、实体、流当工具调用。
只做协议与调度，工具 schema 与实现由使用方提供：

- JSON-RPC 方法：`initialize`（协议版本 `2024-11-05` 与 tools 能力）、`notifications/initialized`
  （通知返回 202，无响应体）、`ping`、`tools/list`、`tools/call`；
- `McpServer.mount(path, provider)` 支持多个挂载点；`handle(path, body)` 是纯函数式单次处理，
  `start(address, port)` 用 `stdx.net.http` 后台 serve，`port(0)` 时返回实际分配端口；
- 错误映射显式：未知工具/`params` 非对象 → `-32602`；超时或 provider 抛异常 → `-32603`；
  `McpToolBusinessException` → 正常 `result` 且 `content` 带 `isError: true`；
  未知方法 `-32601`、坏 JSON `-32700`；
- 工具 schema 由 `ToolDescriptor.inputSchemaJson` 提供；单个坏 schema 退化为空对象，不影响 `tools/list`；
- 测试 `McpServerTest` 3/3：真实 stdx HTTP 客户端完成 initialize + tools/list（含 schema）、
  tools/call 经 `AskInbox` 触发真实 Actor 并连续两次计数递增、五类错误映射；
- G4：新增 `docs/agent-integration.md`（sessionId 寻址、睡/醒、受监督执行、MCP、Edge、Durable Streams、
  pic 接入路径），`tools/check_agent_docs.py` 校验文中 23 个公开类型都在 API 快照里，已接入 `tools/ci.sh`。

### ✨ 新增：Edge 无状态接入层（AR6：P3 / F4）

`cactor_platform/edge` 把"长连接"与"业务状态"分离：连接只落在 Edge 节点，会话状态留在
集群 Actor 里，断线凭令牌从 `lastAck + 1` 续传。

- `EdgeRouter`：连接 ↔ 集群 Actor。`EdgeActorResolver` 可插拔，
  `ReceptionistEdgeResolver` 经 `ClusterReceptionist` 全集群解析（跨节点得到 `RemoteActorRef`），
  `LocalEdgeResolver` 供单机/测试；每次投递重新解析，实例迁移后自动跟随；
- 无状态出站：入站消息以本节点回复 sink 为 sender，会话 Actor 的 `reply` 回到 sink，
  sink 追加到本节点 `DurableStream`（复用 G3），断线期间消息不丢；
- 恢复令牌 `EdgeRecoveryToken`：`sessionId + lastAck + expiresAt` 的 HMAC-SHA256 签名
  （复用 `foundation.hashing`，常数时间比较）；`EdgeConnection.poll` 从 `lastAck + 1` 续读，
  `ack` 只增不减；令牌过期 401、跨会话或不匹配 403；
- 帧与握手（与 HTTP 框架无关）：`EdgeSse`（`id:` = offset、多行 `data:`、60s 驻留上限
  `shouldRecycle`）、`EdgeWs`（首行 offset + 载荷）、`EdgeGateway.upgrade`
  （`GET /v1/edge/{sessionId}`，401/403/404/405 明确映射）；
- 测试 `EdgeRouterTest` 4/4：跨节点路由（连接 node0、Actor node1、回复回流）、
  令牌续传（不重放已确认、不丢未确认）、令牌拒绝（过期/跨会话/篡改）、握手与帧编码。

### ✨ 新增：Durable Streams（AR6：P4 / G3）

`cactor_platform/durable` 提供可续读、可 fork、带 TTL 的持久事件流语义（存储复用
`DurableStateStore`，不新建日志）：

- offset 从 1 单调递增，`ZERO_OFFSET = 0`；单次读上限 128 条；
- 游标续读 `stream-next-offset` / `stream-cursor` / `stream-seq`；
- 长轮询：无新消息最多等 2s，期间有新消息立即返回；
- fork：从第 N 条复制前缀到新流（`stream-forked-from` / `stream-fork-offset`），之后两条流独立；
- TTL：`stream-ttl` / `stream-expires-at`，到期读写抛 `DurableStreamExpiredException`，协议返回 410；
- 多生产者：`producer-id` 标识每条记录，offset 仍连续；
- `DurableStreamProtocol.handle(DurableRequest)`：与 HTTP 框架无关的
  `GET/POST /v1/stream/{path}` 与头映射，便于挂到任意 HTTP 服务端；
- 测试 `DurableStreamTest` 6/6（续读、长轮询、fork、多生产者、TTL、协议头/404/405/410）。


### ✨ 新增：显式背压与负载自报（AR5：C2）

- `ActorRef.isBusy()`：本地引用"正在处理"时为真；`ActorRef.mailboxBacklogSeconds()`：
  最老一条待处理消息的等待秒数（`CellMailbox` 空→非空记时刻、排空清零）；
- Prometheus 每 Actor `cactor_actor_mailbox_backlog_seconds`（积压可观测，不自动丢弃）；
- `cactor_patterns.backpressure.BackpressuredRef<T>`：非阻塞 `offer` 返回
  `OfferResult { Enqueued | Backpressured(inFlight, limit) | Closed }`，
  在途到水位时消息不入队并带回容量；`whenWritable(timeout)` 供上游降速；不做隐式阻塞发送；
- 测试 `BackpressureC2Test`：处理中 isBusy/积压时长上升与归零；慢消费者 5000 条、
  水位 32、在途始终 ≤32、零丢弃、停止后 Closed。

### ✨ 新增：集群优雅离开等待分片移交（AR5：C3）

- `Cluster.installCoordinatedShutdown(waitForLeave!, shardings!)` 新增可选 `shardings`：
  在 cluster-leave 阶段等本地托管实体清零（分片移交），预算超时只记日志、继续关闭；
- 测试 `leaveWithShardingsCompletesWithinBudget`：3 节点 + 分片，关闭在预算内完成、
  离开节点实体数为 0、幸存节点收敛为 2。

### ✨ 新增：可睡眠会话 `SleepableActor`（AR5：P2 / F1）

空闲会话不再长期占一个 Actor。`cactor_platform/lifecycle` 提供：

- `SleepableActor` 基类（`state()` / `restore()` / `onMessage()`）与
  `SleepPolicy { Idle(Duration) | Explicit }`；
- `SleepRegistry` + `SleepableHandle`：handle 不是 ActorRef，不占 Actor；
- 睡 = 向会话要状态 → 写入 `DurableStateStore`（修订号递增）→ 停止会话实例；
  管理器只保留 key/代数/定时器，状态不常驻内存；
- 醒 = 同一 `receive` 内读回状态、创建实例并投递触发消息：唤醒期间的消息不丢、不乱序、不重复；
- `Idle(d)` 每条消息重置定时器；`handle.sleep()` 显式睡眠；
- 管理器跑在 `DispatcherMode.ThreadPerActor`，状态文件 IO 不占亲和池；
- 测试 `SleepableTest` 3/3（状态恢复、50 条唤醒消息恰好一次、Idle 自动睡）。

### ✨ 新增：`cactor_platform` 模块与受监督执行 / 会话寻址（AR4：P1、P8、D4）

新增第 13 个库模块 `cactor_platform`（L6，平台原语，不含 Agent 语义）：

- `platform/exec`：`SupervisedCall<T>` 把任意可失败调用放进受监督 Actor；失败按
  `TypedSupervisorStrategy` 重启并重新执行（默认最多 3 次 / 60s）；超时置位
  `AskCancellationToken` 并以 `SupervisedCallTimeoutException` 失败；`IsolationLevel.ThreadPerTask`
  映射 `DispatcherMode.ThreadPerActor`（A1），阻塞调用只占自己的线程；成功/失败后协调者与工作者都停止，
  不泄漏 Actor；
- `platform/session`：`SessionRegistry` 按 sessionId 经 `ClusterSharding` 路由，
  `session<T>(id)` 返回路由型 `TypedActorRef`（不占本地 Actor）；同一 id 跨节点解析到同一实体；
- 模块门禁：`check_module_deps.py` 增加 `cactor_platform`（L6）与 stdx 白名单，
  `api_snapshot.py` 纳入 platform（快照 13 个模块）；反例（core→platform、platform→xstate 同层）均报红；
- 测试：`SupervisedCallTest`（重启、超时取消、失败隔离、1000 次调用无泄漏）、
  `SessionRegistryTest`（2 节点、单宿主、跨节点计数连续）。

### ✨ 新增：ex43 多租户推理网关（§31 第 2 项真实示例）

- 租户配额持久化：超额 429，拒绝请求不触达上游；
- 每上游一个 `BasicCircuitBreaker`：连续失败打开、健康副本转移；
- 计费 `AtLeastOnceDelivery` + `IdempotentReceiver`：发送方重试不重复扣费；
- SSE 逐 token 输出；Prometheus 文本含请求/拒绝/失败/断路器/租户费用；
- Lou：`/infer`、`/stream`、`/upstream/fail`、`/metrics`、`/state`。

### ✨ 新增：全局 Actor 预算（C1）

单邮箱有界只能限制一个 Actor 的积压，拦不住"创建了太多 Actor"导致的 OOM。现在提供系统级预算：

- `CActorRuntimeConfig.withMaxActors(n)` / `withActorMemoryBudget(bytes)`；内存预算按
  `ESTIMATED_BYTES_PER_ACTOR = 1200` 换算成 Actor 上限，与 `maxActors` 取小（`effectiveActorLimit()`）；
- 超限抛 `ActorBudgetExceededException(limit, liveActors)`；名额在构造前原子占用、构造失败回滚，
  顶层/子 Actor/类型化 Actor 共用同一份预算；已有 Actor 不受影响，停止后名额释放；
- `CActorSystem.actorBudgetUsage()` / `CActorRuntime.actorBudgetUsage()`：存活数、上限、内存预算、
  拒绝次数与剩余名额；
- 指标 `cactor_actor_budget_limit`、`cactor_actor_budget_rejections_total`；
- 测试 `ActorBudgetTest` 覆盖拒绝、已建可用、停止后释放、子 Actor 共享预算与内存预算换算。

### ✨ 新增：ex42 多 Agent 协作编排（§31 首选真实示例）

- 每任务一个经典 Actor + `DurableStateStore`：审批点暂停、进程崩溃后新 Actor 从存储续跑；
- `ToolExecutor` 用 (task, step) 作幂等键，重复批准/恢复不重复执行工具；
- 预算上限：耗尽后任务失败，不越界执行；
- 任务事件写 `DurableTopic`，SSE 先补历史再接实时；
- 每个任务一条 trace（根 span + 步骤 span），可导出 OTLP/JSON；
- Lou：`/tasks`、`/tasks/:id/approve`、`/tasks/:id`、`/tasks/:id/events`、`/traces`、`/effects`。

### ✨ 新增：调度模式 `DispatcherMode` 与 ThreadPerActor（A1）

仓颉线程是用户态轻量线程（M:N、可抢占），阻塞时运行时会切去执行其他就绪线程；此前框架只暴露了
亲和池与共享阻塞池，AI/IO 密集负载（LLM 等待、同步 IO、阻塞式 `foreign`）落在亲和池上会拖慢
同一 worker 的其他 Actor。现在：

- `DispatcherMode { Affinity | ThreadPerActor | Blocking }`，`Props.withDispatcherMode(...)` /
  `TypedProps.withDispatcherMode(...)`（等价于 `withDispatcher(dispatcherName(mode))`）；
- `ThreadPerActor`：每个 Cell 一根专属线程，懒创建、空闲回收（`CACTOR_THREAD_PER_ACTOR_IDLE_MS`，
  默认 10s）、总量上限（`CACTOR_THREAD_PER_ACTOR_MAX`，默认 1024，超限回退亲和池）；
- 默认仍是 `Affinity`，既有行为不变；
- 诊断：`DispatcherDiagnostics.threadPerActorThreads/Created/Reaped()`。

### ✨ 新增：阻塞可观测与 CI 扫描（A1b）

- `cactor_actor_run_duration_seconds{mode="default|blocking|thread-per-actor"}` 直方图，区分
  "默认池上的长运行（疑似阻塞）"与"ThreadPerActor 上的长运行（符合预期）"；
- `cactor_thread_per_actor_threads{,_created_total,_reaped_total}` 指标；
- `tools/check_blocking_in_actor.py`：扫描 `receive` / `onXxx` 中的 `sleep(`、同步文件 IO、
  `awaitCondition(`、阻塞式 `foreign`，CI（`tools/ci.sh`）命中即失败；误报走行内 `// blocking-ok`
  或 `tools/blocking_allowlist.txt`。

### ✨ 新增：持久化主题 `DurableTopic`（差距 4.8 的 Durable Subscriber）

`DistributedPubSub` 是内存扇出，订阅者离线期间的消息会丢。新增 `persistence/durable_topic.cj`：

- `publish(payload)` 返回主题内单调序号，每条消息按序号写入 `DurableStateStore`；
- `subscribe(name, offsets, handler, fromSeq!)`：先补齐游标之后的历史再接实时投递，
  handler 成功后才推进游标（失败不推进，重连重放，至少一次）；
- 游标单独存 `DurableStateStore`，`unsubscribe` 只停实时投递、保留进度；
- `retention` 只保留最近 N 条，旧消息按序号删除；
- 示例 `ex41_durable_subscriber`（离线补齐、重启不重复、SSE 历史+实时）、
  测试 `DurableTopicTest` 覆盖。

### ✨ 新增：至少一次投递高层 API `cactor_patterns.delivery`（差距 4.13）

远程层原本只有通道级可靠投递（重发/ACK/按通道序号去重），用户拿不到业务幂等键。现在：

- `DeliveryEnvelope(deliveryId, payload)`：用户可见的包装消息，经 `tellReliable` 发送；
- `AtLeastOnceDelivery(remote)`：生成稳定 `${实例}-${序号}` deliveryId，`tell/envelope/resend` 三种用法；
- `DeliveryIdStore`（`MemoryDeliveryIdStore` / `DurableDeliveryIdStore`）：去重表；
- `IdempotentReceiver.handle(envelope, handler)`：首次执行并标记，重复直接丢弃；handler 失败不标记；
- 语义明确为"可能重复、不会静默丢失"，不承诺恰好一次；
- 示例 `ex40_at_least_once`（同信封重发只扣一次、接收端重启后仍识别重复、Lou 端点）、
  测试 `AtLeastOnceTest` 覆盖。

### ✨ 新增：事件 schema 演进（框架级 `EventAdapter.upcast`，差距 4.7）

此前 schema 升级只能写进 `fromJournal`，恢复代码被迫同时处理所有历史格式。现在：

- `EventAdapter.upcast(manifest, data): (String, String)` 默认原样返回；恢复路径先 upcast 再
  `fromJournal`，多级演进由适配器链式升级（v1 → v2 → 当前）；
- `FunctionEventAdapter` 增加可选 `upcast` 参数，函数式适配器同样可用；
- 示例 `ex38_schema_upcast`（v1 元 / v2 元;补贴 / 当前分，混合日志恢复 + 冷重启 + 旧事件原样保留）、
  测试 `eventAdapterUpcastChainMigratesOldEvents` 覆盖。

### ✨ 新增：Grain 状态存储 `GrainStorage`（差距 4.7，对标 Orleans IGrainStorage）

Grain 此前需要自己在 `onActivate/onDeactivate` 读写 `DurableStateStore`。现在框架提供边界钩子：

- `GrainStorage { read(kind, id): Option<String>; write(kind, id, state); remove(kind, id) }`；
- `Grain.encodeState(): Option<String>` / `decodeState(state)` 默认不持久化，配置存储后由
  `GrainHost` 在激活前读回、停用后写出（写失败不阻止停用）；
- `Grains(..., storage: Some(...))` 显式开启，不配置时行为完全不变；
- 示例 `ex39_grain_storage`（空闲停用自动写、重激活自动读、跨进程重启续算）、
  测试 `testGrainStoragePersistsAcrossDeactivation` 覆盖。

### ✨ 新增：持久化投影进度 `Projection`（`docs/plan/20261117.md` 差距 4.12）

CQRS 的读模型需要记录"处理到哪"，此前只有示例层的临时实现。新增 `persistence/projection.cj`：

- `Projection(name, journal, offsetStore, apply)`：按 tag 从 `ReadJournal.currentEventsByTag` 取事件，
  对每个 persistenceId 只应用序号大于已处理偏移的事件；偏移整体写入 `DurableStateStore`
  键 `projection-offset-<name>`，重启后从断点续跑；
- `runOnce(tag): Int64` 返回本次应用条数；无新事件不写存储；`offsetOf(pid)` 查询已处理序号；
- `reset()` 清除全部偏移，调用方清空读模型后重放即完成全量重建；
- 不复活已弃用的 `CQRSBus`，只驱动"事件 → 读模型"与进度，读模型类型由调用方决定；
- 示例 `ex37_projection`（20 条事件投影、重复运行幂等、重开目录续跑、rebuild 重放 22 条）、
  测试 `ProjectionTest` 覆盖。

### ✨ 新增：集群客户端 `ClusterClient`（`docs/plan/20261117.md` 差距 4.3 / C2c）

外部进程（网关、BFF、运维脚本）无需加入集群即可按逻辑名访问集群内服务：

- `cactor_cluster.membership.ClusterClient(remote, contacts, timeout)`：向任一联系点的 "cluster" 守护
  查询当前 Up 成员（普通远程消息，协议 `$cc-members|...`，不新增线协议版本）；
- `members()` 逐个尝试联系点、全部失败抛 `IllegalStateException`；`tell/ref(actorName)` 按成员轮询投递；
- 集群侧由 `ClusterDaemon` 响应查询（up 成员排序返回）；
- 示例 `ex36_cluster_client`（3 节点业务集群 + 未加入集群的客户端节点，down 成员后查询自动收敛）、
  测试 `ClusterClientTest` 覆盖；
- 仍待做：Multi-DC（roles / data-center / 跨 DC gossip 隔离）。

### ✨ 新增：Span 导出器与 OTLP/JSON（`docs/plan/20261117.md` 差距 4.10 的导出部分）

Tracer 此前只有内存环形缓冲，无导出能力。新增 `foundation/monitoring/span_exporter.cj`：

- `SpanExporter` 接口；`InMemorySpanExporter`（测试/示例）、`ThrowingSpanExporter`（故障注入）；
- `BatchSpanExporter`：业务线程只入队，达到 batchSize 或 `flush()` 时批量下发；
  **下游异常被吞掉并计数**（`failureCount()`），不会沿调用栈影响业务；
- `Tracer.attachExporter` / `detachExporter`：默认不导出，行为与之前完全一致；
- `toOtlpJson(serviceName, spans)`：OTLP/JSON（`resourceSpans`）编码，属性做 JSON 转义；
  foundation 不依赖 stdx，HTTP 发送由应用完成；
- 示例 `ex35_otlp_export`（2 节点跨节点 span 父子链 + 失败隔离 + `/v1/traces` OTLP JSON）、
  测试 `SpanExporterTest` 覆盖。

### ✨ 新增：分片分配策略 SPI `ShardAllocationStrategy`（`docs/plan/20261117.md` 差距 4.4 的分配部分）

分片归属此前写死为 rendezvous 哈希，想按业务亲和性/容量隔离分配必须改框架源码。现在可插拔：

- 新接口 `cactor_cluster.membership.ShardAllocationStrategy { allocate(shard, members): Option<String> }`，
  必须是纯函数（各节点独立计算需一致）；
- 内置 `RendezvousAllocation`（默认，行为与之前完全相同）与 `FirstMemberAllocation`；
- 新配置 `ShardingSettings.withAllocationStrategy(strategy)`；区域的两处归属计算都走该策略；
- 示例 `ex34_shard_allocation`（同一集群共存"按分片奇偶分配"与"全部钉在最小地址节点"两种策略）、
  测试 `ShardAllocationStrategyTest` 覆盖；
- 仍待做：分片协调状态持久化（全灭重启后保持原宿主）。

### ✨ 新增：集群 / 远程侧 Prometheus 指标 `ClusterMetrics`（`docs/plan/20261117.md` 差距 4.10 的指标部分）

`/metrics` 此前只有运行时指标，集群侧 0 指标。新增 `cactor_cluster.membership.ClusterMetrics`：

- 抓取时读实时快照并渲染 Prometheus 文本，同时写入 foundation `MetricRegistry`（程序化可读）；
- 指标：`cactor_cluster_members_up/total`、`unreachable`、`converged`、`gossip_total`、
  `quarantined`、`pending_asks`、`cactor_remote_reliable_{resent,duplicates,unacked}_*`；
- 可选挂接 `ClusterSharding`：`shards_owned`、`shards_acquiring`、`shards_acquisitions_total`、
  `entities_local`、`entities_passivated_total`；挂接 `Replicator`：`ddata_keys`；
- 示例 `ex33_cluster_metrics`（3 节点 + 分片，/metrics /metrics/all /state）、测试 `ClusterMetricsTest` 覆盖；
- 仍待做：与运行时指标合并成单一渲染入口、OTLP/Jaeger 导出。

### ✨ 新增：W3C traceparent 传播原语与 Mdc 绑定（`docs/plan/20261117.md` 差距 4.10 的传播部分）

`foundation/monitoring/tracing.cj` 补齐上下文传播所需的最小原语（此前只有 ID 生成）：

- `toTraceParent(ctx)`：生成 W3C `traceparent`（`00-<trace-id>-<parent-id>-01`）；
- `parseTraceParent(text)`：严格解析（版本/长度/小写十六进制），坏输入返回 `None` 不抛异常；
- `continueTrace(text)`：接续上游 trace，生成子 span（同 traceId、parent 指向调用方）；
- `traceMdcFields(ctx)` / `withTraceMdc(ctx, block)`：把 traceId/spanId/parentSpanId
  注入当前线程日志字段，块结束（含异常）自动恢复；
- 示例 `ex32_distributed_trace`（2 节点：W3C traceparent 逐跳接续、两侧日志 Mdc 同 traceId、
  坏输入降级）、测试 `TraceContextTest` 覆盖。
- 仍待做：线协议自动携带 traceparent、集群侧指标、OTLP/Jaeger 导出。

### ✨ 新增：集群感知路由器 `ClusterRouter`（`docs/plan/20261117.md` 差距 4.5）

跨节点分发不再需要应用自己拼成员列表与远程引用：

- `cactor_cluster.routing.ClusterRouter(cluster, remote, actorName, strategy, loadOf)`；
- `RoundRobin`：候选按地址排序后轮询；`LeastLoaded`：按应用提供的 `loadOf(address)`
  选负载最低者，相同按地址最小者；
- 每次路由读取实时成员视图（`cluster.state()`），被 down / 移除的成员自动剔除，
  不存在缓存过期竞态；
- 无候选（集群终止 / 全部 down / 路由器 `stop()`）时 `route()`/`tell()` 抛
  `IllegalStateException`，不静默丢消息；
- 示例 `ex31_cluster_router`、测试 `ClusterRouterTest` 覆盖。

### ✨ 新增：Map 系 CRDT `ORMap` / `LWWMap`（`docs/plan/20261117.md` 差距 4.6）

Distributed Data 新增两种最常用的 Map 类型（对标 Akka `ORMap` / `LWWMap`）：

- `ORMap`：键值对集合的观察-删除语义。并发改不同 key 互不干扰；同一 key 并发 put 不同值 add-wins；
  `removeKey`/`removeValue` 在合并后不回退（旧副本不会复活已删除的 key）。
  编码为 `${key.size}:${key}${value}` 作为 `ORSet` 元素，直接复用点标记与版本向量；
- `LWWMap`：每键一个 `LWWRegister`，按 (timestamp, node) 决胜；删除写墓碑，合并后删除不回退；
- `ReplicatedValue` 新分支 `ORMapValue` / `LWWMapValue`，线编码复用 `ORSet` 的规范编码；
- `Replicator` 便捷方法：`putInMap`、`removeFromMap`、`removeMapKey`、`mapValues`、
  `putInLWWMap`、`removeFromLWWMap`、`lwwMapGet`；
- 示例 `ex30_distributed_config`（3 节点真实 gossip：并发写、add-wins、观察-删除、LWW 墓碑）、
  测试 `CrdtMapTest` 与 `MapReplicatorTest` 覆盖。

### ✨ 新增：可插拔脑裂决策 `DowningProvider`（`docs/plan/20261117.md` 差距 4.2）

内置策略（KeepMajority / KeepOldest / StaticQuorum / AutoDownAfter）之外，用户现在可以注入
自己的存活规则（例如"指定业务区必须至少一个节点可达"）：

- 新接口 `cactor_cluster.membership.DowningProvider`：
  `stableAfter()` + `decide(members, unreachable, indirect)`；`decide` 必须是纯函数，
  保证分区两侧在各自视图上算出一致的结论；
- 新枚举分支 `DowningStrategy.Custom(DowningProvider)`；内置策略的 `sbrDecide` 纯函数语义不变；
- 新配置方法 `ClusterSettings.withDowningProvider(provider)`；从配置文件读取时仍只接受内置策略名；
- **注意**：对 `DowningStrategy` 做穷尽 `match` 的外部代码需要处理新分支（源级破坏）；
- 示例 `ex29_custom_downing`（5 节点真实断网：多数派西区停止、少数派东区存活）、
  测试 `testCustomDowningProviderIsConsulted` 与 `testCustomProviderOverridesMajority` 覆盖。

### ✨ 新增：远程 `ActorSelection`（`docs/plan/20261117.md` 差距 4.1）

按路径/通配寻址远程 Actor，不再要求调用方事先持有引用：

- `SimpleRemoteManager.actorSelection(address, path)` 创建选择器，路径支持
  `cactor://host:port/user/<pattern>`、`/user/<pattern>` 或裸 pattern；
- `ActorSelection.tell(msg)` 把目标写成 `$sel:<pattern>` 发一帧，由对端本地展开通配并逐个投递
  （零额外往返、不阻塞）；`resolveNames()` 走 `$sys SELECT` 排序返回业务名称；
  `resolveOne()` 要求恰好一个匹配；
- 通配语义 `*` / `?`；以 `$` 开头的内部处理器（`$sys`、ask/回复临时入口）永远不参与匹配；
- 无匹配计入对端死信计数。示例 `ex28_actor_selection`、测试 `ActorSelectionTest` 覆盖。

### ✨ 新增：`Cluster.installCoordinatedShutdown()`（`docs/plan/20261117.md` 差距 4.1）

把集群优雅离开接入协调关闭：在 `CoordinatedShutdown.PhaseClusterLeave` 阶段发送 `leave()`
并等待本地离开 Up，后续 `actor-system-terminate` 才停止守护 Actor；其余成员因此收到
`MemberLeft`/`MemberExited`，而不是把正常下线判成不可达。幂等、带等待上限（默认 2s），
不调用时行为完全不变。示例 `ex26_graceful_shutdown`、测试 `GracefulLeaveTest` 覆盖。

### ✨ 新增：`ReadConsistency.ReadMinCap` / `WriteConsistency.WriteMinCap`（`docs/plan/20261117.md` 差距 4.6）

Distributed Data 的读写一致性新增 minCap：至少 n 个副本回应/确认即返回（对标 Akka 的 `minCap` / `WriteTo(n)`），
用于"写方与读方成员视图不一致"时缓解过半读写不相交的问题。

- 新增枚举分支 `cactor_cluster.ddata.ReadConsistency.ReadMinCap(Int64, Duration)`
  与 `WriteConsistency.WriteMinCap(Int64, Duration)`；
- `n <= 0` 抛 `IllegalArgumentException`；`n` 超过当前视图副本数时按实际副本数处理；
- 示例 `ex21_read_min_cap` 与测试 `ReadMinCapTest` 覆盖"总副本 4、可达 2：Majority 失败、minCap(2) 成功"。

### 💥 破坏性变更（`docs/plan/20261116.md` B4c：runtime 的第二套实现，生产代码 −2,333 行）

B4c 继续"删假"，但这批与 B4a/B4b 性质不同：**上一批删的是零引用代码，这一批删的是"有实现、但从来没人用的第二套"**。
判定依据都是"仓库里已有活的等价实现 + 零消费者"，删的都是平行实现，不动唯一在用的那套。

| 删除项 | 行数 | 为什么是死的（证据） |
|---|---|---|
| `runtime/supervision/`（`SupervisionManager` / `SupervisionConfig` / `ActorSupervisionInfo` / `SupervisionStats` / `simple_supervision_strategy.cj`） | 611 | `CActorRuntime` 只把它存成字段 + `getSupervisionManager()`，该 getter **全仓零调用**；真正的监督在 `core/supervision/`（`Supervisor` + `SupervisionStrategy` + 8 种策略），`cactor_runtime.cj` 自己也注明"监督由 Actor Cell 与用户 Guardian 完成" |
| `runtime/events/`（`ActorEventBus` + `SimpleActorEventBus` + 9 种生命周期事件类型 + 两套监听器接口） | 729 | **两套平行的事件总线**，且 `subscribe()` 在生产代码里**没有任何调用点**——事件发出去无人接收；`core` 里还有第三套 `EventBus` 接口（主题式）同样无实现 |
| `runtime/monitoring/actor_system_metrics.cj`（`ActorSystemMetrics` + `PerformanceMetrics` + `SystemHealthInfo`） | 485 | 20 个公开方法里只有 `incrementActorCount`/`decrementActorCount` 被调用；算出来的吞吐/时延分位/错误率/CPU/内存**无人读取**。真实指标来自 `runtime/prometheus_metrics.cj`（直接取 guardian 的真实计数，如 `cactor_actors_live`） |
| `runtime/registry/actor_registry.cj`（`ActorRegistry` + `ActorRegistryEntry`） | 465 | 与同目录的 `SimpleActorRegistry` **API 几乎完全重复**（`register`/`unregister`/`findByName`/`getTotalActorCount`…），而 `ActorRegistry` 零消费者；`SimpleActorRegistry` **真被用**（`cactor_runtime.cj` 的登记/注销/按名查找 + 两个测试），已保留 |

连带改动：
- `CActorRuntime`：去掉 `supervisionManager` / `systemMetrics` / `eventBus` 三个字段与三个公开 getter
  （**公开 API 破坏**），以及两处 `systemMetrics.incrementActorCount/decrementActorCount` 调用
  （actor 计数由 `actorCount` 字段与 Prometheus 的 guardian 计数承担，无能力损失）；
- `ActorLifecycleManager`：摘掉 `eventBus` 字段与 7 处 `eventBus.publish(...)`（注册/启动/停止/重启及其失败分支），
  以及 `getEventBus()` getter；生命周期状态机本身（`registerActor`/`startActor`/`unregisterActor`）**全部保留**——
  它是被 `CActorRuntime` 真实调用的；
- 公开 API 快照 `docs/api-snapshot/runtime.txt` 的 diff **只含删除、无新增**；
- 度量：库模块生产代码 78,177 → **75,844**；runtime 11,921 → **9,588**（自 B4a 起 19,703 → 9,588，−51.3%）；
  占位关键词 130 → **122**（runtime 36 → 28）。

**保留（并写明理由）**：`runtime/system/`（`HighPerformanceActorSystem` 被 `start`/`shutdown`/`getSystemStats` 真实调用，
且 `HighPerformanceActorSystemConfig` 是 `CActorRuntimeConfig` 五个预设的组成部分）、`runtime/registry/simple_actor_registry.cj`
（真被用）、`runtime/lifecycle/`（真被用）、`runtime/monitoring`（计数真被用）、`runtime/wrappers/`（门面反解包用）、
`runtime/actor` + `runtime/context`（被 `system/high_performance_actor_system.cj` 使用）——
这些与 `ActorSystem` 门面收敛（E49）同批处理，见 `docs/plan/20261116.md` §10.6。

### 💥 破坏性变更（`docs/plan/20261116.md` B4a+B4b：runtime 死代码，生产代码 −6,522 行）

runtime 是 12 个库里最大的一个（19,703 → **11,921** 行）。本批删掉两类已经失效的实现；删前逐个用“删掉再 `cjpm build`”确认。

**B4a 死链 A+B（2,102 行）**

| 删除项 | 行数 | 依据 |
|---|---|---|
| `dispatcher/factory/enhanced_dispatcher_factory.cj` | 333 | `EnhancedDispatcherFactory` 全仓只出现 1 次（自己的声明） |
| `dispatcher/monitoring/*` | 731 | 唯一消费者是上面那个零引用类 |
| `dispatcher/adaptive/*` | 437 | 同上 |
| `dispatcher/dynamic/*` | 302 | 同上 |
| `dispatcher/dispatcher_pool.cj` | 380 | `cactor_runtime.cj` 原注释已说明构造点被摘除（旧 4 调度器池空转、约占 10% CPU） |
| `dispatcher/high_performance_actor_system_config.cj` | 221 | 同类名的 `system/high_performance_actor_system.cj` 才是活的那个 |

**B4b 高级调度器/邮箱工厂链（4,420 行）**

`dispatcher/{factory,advanced,optimized,work_stealing}` 与 `mailbox/{factory,advanced,lockfree,batching}` 整条依赖链，共 8 个包。依据：它们唯一的入口是 `CActorRuntime` 的两个字段 + `getMailboxFactory()` / `getDispatcherFactory()`，而这两个公开方法在全仓**零调用**；`CActorRuntime.actorOf` 从头到尾没碰过这两个工厂（邮箱与调度实际由 core 的 `ActorCellDispatcher` 与 `runtime.mailbox` 根包承担）。

- 公开 API 删除：`cactor_runtime.CActorRuntime.getMailboxFactory()` / `getDispatcherFactory()`，以及 8 个包的公开类型（`AdvancedMailboxFactory`、`AdvancedDispatcherFactory`、`Foundation*Mailbox`、`SPSCMailbox`/`MPSCMailbox`、`WorkStealingDispatcher`、`OptimizedWorkStealingDispatcher` 等）。快照 `docs/api-snapshot/runtime.txt` 的 diff **只含删除、无新增**。
- 保留 `system/high_performance_actor_system.cj`（及其 `runtime/actor`、`runtime/context` 链）：它虽只被 `CActorRuntime.start()` 调一次（`contextPool.warmup()` + 4 行 debug 日志，从不用来创建 Actor），但 `HighPerformanceActorSystemConfig` 是 `CActorRuntimeConfig` 五个预设的一部分，属 E49 门面收敛范围，本批不动。
- 连带删除：`tests/unit/runtime/mailbox/batching_mailbox_test.cj`（2 例，测的就是被删的 `BatchingMailbox`）、示例 `mailbox_refactor_demo`（213 行，not-built）；`priority_mailbox_test.cj` 去掉对已删包的 `import`（用例只测仍在的 `PriorityMailbox`，**3/3 继续通过**）。
- 度量：库模块生产代码 84,699 → **78,177**；占位关键词 144 → **130**；runtime 占位词 50 → **36**。

### ⚙️ 工程记录（删包时必须清理的两个 cjpm 缓存）

删除包/文件后，`cjpm build` 可能报 `can not find the following dependencies: <已删包>`，而源码里已无任何 import。实测两个缓存都会造成这种“幽灵依赖”：

1. `target/.cjpm-history`——cjpm 的 resolve 缓存，按包名记录了上次解析出的依赖图；
2. `target/release/incremental-cache.json` 与 `target/release/<模块>/*.cjo`——增量构建产物；尤其**已删除模块**（`cactor_integration`、`cactor_distribution`）遗留的 `.cjo` 仍会被当成依赖来源。

复现：删掉 8 个包后仅 `rm -rf target/release/cactor_runtime` 仍报依赖缺失；`rm -f target/.cjpm-history target/release/incremental-cache.json` + `cjpm clean` 后才 `cjpm build success`。结论写进 `docs/plan/20261116.md` §10.3：删除类批次必须在干净构建目录上验证，否则“删掉再构建”这个权威判据会被缓存污染成假绿/假红。

### 💥 破坏性变更（`docs/plan/20261106.md` E46/E47：删除无消费者的平行实现，生产代码 -5,642 行）

原则是“能复用就不留第二套”：下列包在库代码中**零消费者**，能力均由仍然健在的实现覆盖，因此整体删除而不是写适配层。回退点：`git revert` 本提交即可全部恢复。

| 删除项 | 行数 | 替代实现（沿用，不新建） |
|---|---|---|
| `cactor_foundation.log` | 178 | `cactor_foundation.logging`（`Logging` / `Logger` / `LogLevel` / `MemorySink` / `Mdc` / `LogRateLimiter`） |
| `cactor_foundation.abstractions` | 395 | `cactor_core` 的真实 `Actor` / `ActorRef` / `ActorContext`（旧包是一组从未被绑定的平行接口） |
| `cactor_foundation.contracts` | 474 | `tools/check_module_deps.py`（真实的分层门禁）+ `docs/zh/package-structure.md` |
| `cactor_foundation.di` | 358 | 构造函数 / `Props` 注入（仓库里没有服务定位器的消费者） |
| `cactor_foundation.simple_config` | 344 | `cactor_foundation.config.LayeredConfig`（唯一被生产代码使用的配置系统） |
| `cactor_foundation.plugin` | 186 | `cactor_core.typed.typed_interceptor`（`BehaviorInterceptor`）+ `cactor_persistence.plugin_registry` |
| `cactor_foundation.attributes` | 122 | 暂无宿主：等 `ActorSystem` 扩展点定案后按需求重建（不留在树里当摆设） |
| `cactor_core.error` | 331 | `cactor_core.actor.signals`（`ActorErrorException` / `DeathPactException`）+ 监督策略 |
| `cactor_patterns.backpressure` | 399 | `cactor_streams`（Reactive Streams 背压） |
| `cactor_patterns.routing.factory` | 435 | `cactor_patterns.routing` 的真实四种路由（Random / RoundRobin / ConsistentHash / SmallestMailbox） |
| `cactor.config.file` | 821 | `cactor_foundation.config.LayeredConfig` + `cactor_core.config` |
| `cactor_core.message.enhanced_message` | 310 | `cactor_core.message.zerocopy_message`（真实实现） |
| `cactor_runtime.mailbox.queue` | 236 | `cactor_runtime.mailbox` 在用的邮箱实现（`priority_mailbox` / `foundation_mailbox`） |
| `cactor_runtime.message` | 334 | 无替代需求：批处理消息路径未接入任何调度器 |
| `cactor_runtime.mailbox.ringbuffer` | 254 | 同上（未接入邮箱工厂） |
| `cactor_runtime.dispatcher.batch_processing` | 434 | 同上（仅被已删文件引用） |
| `cactor_runtime.scheduler` / `.execution` | 10 | 只有 `package` 声明的空包 |

- 同时删除 `foundation/pkg.cj` 里对 `simple_config` 的 `public import`，以及 `patterns/pkg.cj` 里对 `backpressure` 的 `public import`。
- 删除只演示上述被删能力的两个示例目录：`dependency_optimization_demo`（`di` + `contracts`）与 `simple_config_demo`（`simple_config`）；`modules/examples/src/README.md` 清单由 `tools/check_examples.py --update-readme` 重新生成。
- 被删测试的覆盖面没有丢：`foundation/log` 的语义用例（级别过滤必须在拼接之前发生）迁到 `modules/tests/src/unit/observability/structured_logging_test.cj` 的 `testDisabledLevelSkipsLazyMessageAndOutput`，断言对象换成仍然存在的 `logging.MemorySink`。
- 公开 API 快照同步更新（`foundation` / `core` / `runtime` / `patterns` / `cactor` 五个文件），diff **只含删除、无新增**。
- 度量（`tools/quality_metrics.py`）：库模块生产代码 90,341 → **84,699** 行，占位关键词 165 → **144**，零引用包 23 → **9**。

### ⚙️ 工程体系（门禁：修掉两个“按文档用 `/bin/bash` 就必红”的缺陷）

- **`tools/ci.sh` 不再用 `env -u` 包装 `cjpm test`**。macOS 在执行平台二进制（`/usr/bin/env`、`/bin/bash`、`/usr/bin/timeout`，均在 dyld 共享缓存中）时会把环境里的 `DYLD_*` 全部剔除，于是 `env -u CACTOR_LOG_LEVEL cjpm test` 不仅删了日志变量，连 `tools/env.sh` 刚准备的 `DYLD_LIBRARY_PATH` / `DYLD_FALLBACK_LIBRARY_PATH` 也一起丢掉，`cjpm` 起的测试进程只能找到 `/usr/lib/libcrypto.dylib`（LibreSSL 桩）而 SIGABRT。表现为 7 个 TLS 用例 `Crashed with exit code 134`。改为子 shell 内 `unset`（不 exec 新程序）。已在 HEAD 干净 worktree 上复现同一失败，确认与本次删除无关。
- **6 个 `tools/*.sh` 改为无条件 `source tools/env.sh`**（`cluster_demo` / `cluster_crash_demo` / `ddata_stress` / `journal_crash_demo` / `lease_contend_demo` / `bench_gate`）。它们原来只在 `cjpm` 不在 PATH 时 source 一个写死路径 `~/.cangjie/1.2.0/cangjie/envsetup.sh`（本机实际在 `~/cangjie`，且门禁里 `cjpm` 在 PATH，于是整段跳过），完全依赖继承的 `DYLD_*`；经 `guard`（`timeout --foreground /bin/bash script.sh`）调用时会被剔除，节点进程报 `no LC_RPATH's found` / `Abort trap: 6`。修复后 `caffeinate -i /bin/bash tools/ci.sh` 首次在系统自带 bash 3.2 下走完整链路并退出码 0。
- 教训（已写入 `docs/plan/20261116.md` §9.5）：任何新增的 `tools/*.sh` 只要启动仓颉二进制，开头必须 `source tools/env.sh`；不要用 `env` 包装。

### ✨ 新增（DA7：StreamRef 可靠模式与 streams 跨节点 ask）

- `StreamRefSettings` 新增 `reliable`（默认 `false`）与 `reconnectWindow`（默认 10 s），配套 `withReliable` / `withReconnectWindow`。可靠模式开启后，生产方保留未确认元素（保留量受 `bufferSize` 约束），消费方按累计序号 ACK；短暂断连不立即失败，而是在 `reconnectWindow` 内用同一 streamId + 恢复令牌重新挂接并从 `lastAck + 1` 重发，消费方按序号去重。恢复重试间隔为 `min(1s, max(100ms, reconnectWindow/4))`，重连窗口耗尽抛新增的 `StreamRefReconnectTimeoutException`。本进程内的流（SourceRef 带本地引用）没有传输可断，仍走单次挂接。
- 线上新增版本 2 帧 `SubscribeReliable(9)` / `Ack(10)` / `AckCompleted(11)` / `ReliableProbeOk(12)`；`Completed` 由生产方等 `AckCompleted` 后才真正终止，避免完成标记后继续无限保留元素。版本协商复用既有 `Subscribe` / `Rejected`：可靠消费方首次挂接先发带 version=2 的既有 `Subscribe`，旧生产方按其既有版本检查明确回 `Rejected("协议版本不符")`；可靠生产方收到旧消费方的 version=1 同样用既有 `Rejected` 明确拒绝，两端都不会静默半挂接。`reliable = false`（默认）时线格式逐字节不变，`STREAM_REF_PROTOCOL_VERSION` 保持 1，只有可靠模式使用新增常量 `STREAM_REF_PROTOCOL_VERSION_RELIABLE = 2`。
- `cactor_core.typed.TypedActorRefs.isRemote(ref)`：按 `RemoteActorRef` 固定的 `remote@host:port` system 段判断引用是否由远程层承载（core 不依赖 remote 模块）。
- `cactor_streams` 的 `ask` 算子对远端 `TypedActorRef` 改走 `TypedAsk.askRemote`（回复经信封 sender 返回），本地引用仍走 `TypedAsk.ask`；并行度、顺序与取消令牌语义不变。远端路径的第二个参数是占位回复引用，使用它会明确抛 `IllegalArgumentException`，要求 `createRequest` 返回自包含、可过线的请求消息。
- 新增 6 例 `stream_ref_reliable_test.cj`（真实 TCP 双向黑洞恢复不丢不重、重连窗口超时、协议级重复序号去重、双向版本不匹配明确拒绝）与 3 例 `ask_remote_stream_test.cj`；StreamRef 合计 43/43，streams 42/42。公开 API 快照已更新（仅新增，无删除）。

### 🐛 修复（DA8：Q6 单调性误报与持久化原子替换竞态）

- 修正 `ddata_consistency_matrix_test` Q6 的误报：读到尚未经推送/反熵到达的键时 `counterOf(None)` 返回 `-1`，与初值 `last = 0` 比较被当成"读回退"；扩容刚开始、`ReadMajority` 合法返回 `None` 即可触发。改为只在读到实际值后比较单调，并新增 observed > 0、maxSeen ≤ acked、typeMismatch == 0 断言（收紧了断言，不是放宽）。未改动的基线裸跑 550 次失败 23 次（≈4.2%）；诊断 450 次中 28 个回退事件 100% 为 `v=-1 last=0 kind=none`，无真实下降；修复后 300/300 + 20/20 通过。
- **修复（真实缺陷）**：`cactor_persistence` 的 `writeFileAtomically` 固定使用 `<path>.tmp`，同一目标文件的并发写者会共享临时文件；`std.fs.rename(overwrite: true)` 先删目标再改名，留下"路径暂时不存在"的窗口。混合版本长压中 driver 实测 durable 修订号 30 → 0。改为每次写使用唯一临时名（进程启动随机 nonce + 原子递增序号）并直接调用 POSIX `rename(2)` 原子替换。新增 `durable_state_atomic_test.cj` 2 例（旧实现 5/5 稳定失败，修复后 20/20）。

### ⚙️ 工程体系（E51 版本决策与 tag 证据 / E50 PR 门禁 / DA8 混合版本长压）

- E51：`docs/RELEASE.md` 记录本地 annotated tag `v1.2.0`（`a26d048`，2026-10-28）与 `v1.2.1`（`66b6b14`，2026-11-11）的证据——12 个库模块 manifest 版本与 CHANGELOG 标题一致，tag 未推送。新增 `docs/RELEASE.md` §1「版本决策」：下一发行版必须为 **2.0.0**（不允许 1.3.0 / 1.2.2），依据是 E60 streams 按 Reactive Streams 重写并移除旧公开 API / 子包 / 8 个示例、包与二进制类型身份变化；列出 5 项 2.0.0 前置条件。未创建 v2.0.0 tag、未修改任何 `cjpm.toml`，E51 仍 🟡。
- E50：新增 `tools/ci_pr.sh`（`source tools/env.sh` → `timeout 2700s bash tools/ci.sh`，退出码原样透传；`--dry-run` 与 `CI_PR_TIMEOUT` 可调；无平台专用语法）。平台侧流水线与 main 分支保护仍未配置，未伪造 `.github` / `.atomgit` YAML，E50 仍 🟡。
- DA8：`tools/mixed_version_soak.sh` 增加 `MIXED_VERSION_DURATION` 长压模式——driver 循环 kill → 校验 → 重启 → 校验，每轮断言已确认过半写不丢、旧 fencing token 被 `StaleFencingTokenException` 拒、durable 修订号不回退、remember-entities = 30；`tools/ci_nightly.sh` 透传时长并以"时长 + 600 s"设置步超时。`MIXED_VERSION_DURATION=600` 实跑 46 轮全部通过（exit 0，fencing 拒旧 token 552 次，租约文件 64/64）。

### ✨ 新增（DA7：SinkRef 反向流与 StreamConsumer）

- `cactor_cluster.streamref` 新增 `SinkRef<T>`（`toWireString` / `fromWireString`，前缀 `sinkref:v1|`）、`StreamConsumer<T>`（逐次 `pull`、单次未完成 pull、终止恰好一次）、`SinkRefProducer<T>`（`offer` / `whenReady` / `complete` / `fail` / `termination`，同时是 Reactive Streams `Subscriber`）与 `StreamWriter<T>`；`StreamRefs.sinkRef<T>(settings!, owner!)` 返回 `(SinkRef<T>, StreamConsumer<T>)`，`StreamRefs.asSink<T>(ref, settings!, owner!)` 返回生产句柄。
- 背压：sink 授予累计需求 = 已交付 + `bufferSize`（半窗补信用），本地缓冲 + 在途 ≤ `bufferSize`；生产方 `offer` 先预留槽位，缓冲满返回 `Backpressured`。单次挂接（第二个生产方 `Rejected`）、完成/失败恰好一次、双向取消、属主停止、订阅/空闲超时均沿用 `StreamRefSettings`。
- 线上复用 `StreamRefMsg` 的 1..8 号帧，仅新增本地控制消息 `ConsumerPull`，`STREAM_REF_PROTOCOL_VERSION` 保持 1，旧帧可读。新增 11 个回归（`stream_ref_sink_test.cj`，含 3 个真实回环 TCP），StreamRef 合计 37/37；公开 API 快照已更新。

### ⚡ 性能（DA8：Replicator 本地更新/合并临界区合并）

- `modules/cluster/src/ddata/replicator.cj` 合并本地更新与合并路径的临界区：`onLocalUpdate` 单节点加锁 5→2 次（3 节点集群 7→3），`mergeEnvelope` 3→2，`onConsistentUpdate` 6+2N→3+N。用户 `modify`、编码、摘要与网络发送、订阅回调全部留在锁外；锁 2 内一次性发布新值与 pruning 标记并快照订阅者/对等节点/计数。序号、delta/整值判定、pruning 与推送计数语义不变，公开 API 与线协议未改。
- 唯一可见差异：`deltaEnabled` 的读取从 apply 之后提前到更新开始时，并发 `setDeltaPropagation` 的生效边界移动几微秒；两种 payload 都合法且序号连续。新增高并发读写回归（计数正确、无死锁）。

### ⚙️ 工程体系（E50 / E51 / DA8）

- 新增 `tools/ci_nightly.sh`：串联完整 CI（`--soak 600`）、压测矩阵、并行集群 10 轮与混合版本多进程验证，每步带 timeout、挂死即失败；`--dry-run` 仅打印步骤，`--skip-ci` 可跳过第一步。`docs/RELEASE.md` 补充 PR 门禁、nightly 与 main 分支保护约定（平台侧尚未配置）。
- `docs/deprecations.md` 盘点 46 处 `@Deprecated`（persistence 39、patterns 3、cactor 2、core 1、runtime 1），全部带 `since` 并统一声明 2.0.0 移除；`tools/check_deprecations.py` 接入 CI；`docs/RELEASE.md` 补充弃用生命周期与 semver 规则。实际 tag 与中心仓发布未执行。
- 新增 `tools/mixed_version_soak.sh`：3 个真实进程以旧线协议（`CACTOR_WIRE_LEGACY=1`）与分片租约 / 实体 fencing 运行，kill -9 后在原端口以新节点重启，验证混合版本组集群、实体在存活进程恢复、旧 token 写入被拒（`FENCE_REJECT`）、remember-entities 完整、全部进程正常退出；已接入 `tools/ci.sh`。

### 📚 文档（E30 / E51，部分）

- 新增 `docs/RELEASE.md`：12 个库同步版本、公开 API 快照与破坏性变更记录、完整 CI、semver tag、中心仓发布顺序及产物守卫；列出弃用注解与发布自动化的剩余缺口，不表示已创建 tag 或完成发布。
- README 中英文版与 ROADMAP 的性能数字补充历史测量日期、机器和配置范围；旧更新日志缺少可复现实验信息的性能宣传标为未验证，不作为当前版本指标。本次仅整理文档，没有新增测量。

### 🐛 修复（macOS CI 脚本启动）

- `tools/ci.sh` 的 `guard` 显式用当前 Bash 执行 `.sh` 步骤，避免 `/usr/bin/env` shebang 清除 `DYLD_*`。多进程示例因此保留 `tools/env.sh` 准备的仓颉运行时路径，不再于启动时因缺少 LC_RPATH 而 abort。

### ✨ 新增（DA7：StreamRef 与 Reactive Streams 桥接）

- `StreamRefs.asPublisher` / `fromPublisher` / `asSubscriber` 将单次挂接的 SourceRef 和生产句柄接入 `Sources.fromPublisher` / `Sinks.fromSubscriber`。按生产端空位请求上游，串行控制 Actor 避免同步 Publisher 递归；传播完成、失败及取消，并保留消费者挂接前的原始失败原因至订阅超时。
- Reactive Streams 接口及规范辅助下沉到 `cactor_foundation.reactive`，`cactor_streams.reactive` 公开重导出，保留原导入写法的源码兼容。声明包及二进制类型身份改变，依赖方须重新编译；同步更新公开 API 快照。
- 新增 10 项适配器回归，覆盖实际信用上界、同步回调深度、非法需求、取消、失败、挂接前立即失败及真实 TCP；SinkRef 与可靠重连仍是后续工作。

### ✨ 新增（E61：HTTP / LLM 与多 Agent 编排示例）

- `llm_http_demo`：类型化 HTTP 客户端 Actor，后台协程执行 stdx HTTP IO，每个 token 经 `DurableStateBehavior` 落盘后确认继续读取；取消通过 `ClientBuilder.connector` 暴露的原始 TCP 句柄关闭连接，唤醒停滞的 HTTP 读取。本地真实 TCP mock 返回 OpenAI 风格 chunked SSE，验证响应完成前增量到达、跨 chunk 的中文 UTF-8、`[DONE]`、token 边界取消，以及无待确认 token 时的停滞读取取消。新系统与新文件存储恢复 token 历史及完成/取消终态，再沿同一会话继续下一轮 HTTP 生成并保留历史，无外部服务或凭据。
- `agent_orchestration_demo`：Planner / Worker / Reporter 组合类型化监督、`TypedRetry`、取消令牌、`DurableStateBehavior` 与 StreamRef。新 ActorSystem 与新文件存储实例恢复同一会话，跳过已提交步骤；验证两次真实异常后第 3 次成功、慢请求登记后取消、实际发送的迟到回复不覆盖取消结果，以及按单条需求消费的 3 条提交进度。会话是单写者；外部工具副作用仍需业务幂等键，示例不提供集群存储复制。
- 两个可执行示例登记到 examples manifest 和 `tools/ci.sh`，失败以非零退出码报告。

### ✨ 新增（计划 v2.39：流的管道写法）

- `cactor_streams` 恢复管道写法：`source |> filter { x => ... } |> via(flow) |> to(sink) |> run(system)`。每个算子都有同名自由函数，返回"蓝图 → 蓝图"的管道段，类型参数（含物化值类型）与尾随 lambda 的参数类型由 `|>` 左侧推断；`a |> f(参数)` 与 `a.f(参数)` 完全等价（物化值规则相同）。
- 管道段：`via` / `viaMat` / `to` / `toMat` / `run` / `runWith` / `runForeach` / `runFold`，`map` / `filter` / `filterNot` / `collect` / `mapConcat` / `takeWhile` / `dropWhile` / `scan` / `fold` / `mapAsync` / `mapAsyncUnordered` / `ask` / `recover` / `flatMapConcat`（Source 与 Flow 都可用），`take` / `drop` / `grouped` / `throttle` / `buffer` / `async` / `withSupervision`（只用于 Source，Flow 版与之参数相同会被判为重载冲突）。与 `std.collection` 的同名管道函数同时导入不冲突。
- 已知限制：cjc 1.2.0 下 `|>` 右侧管道段的参数里嵌套含 lambda 的调用（如 `|> to(Sinks.foreach<Int64>({ x => ... }))`）会触发编译器内部错误，先绑定到变量再接入。`streams_demo` 新增第 7 段管道写法。

### 💥 破坏性变更（计划 v2.38：E60 流重写）

- `cactor_streams` 按 Reactive Streams 重写：旧 API（`StreamSource` / `StreamFlow` / `StreamSink` / `SourceFactory` / `SinkFactory` / `StreamGraph`、管道 `via` / `to` / `keep`、`ReactiveSource` / `ReactiveSink` / `DemandBox` 等，以及子包 `cactor_streams.core` / `.dsl` / `.unified`）全部移除；改用 `Sources` / `Flows` / `Sinks` + `Source` / `Flow` / `Sink` / `RunnableGraph`，`run(system)` 物化。`cactor_streams.reactive` 只保留 `Publisher` / `Subscriber` / `Subscription` / `Processor` 接口。
- 删除示例 `dsl_pipeline`、`streams_advanced_demo`、`streams_backpressure_demo`、`streams_dsl_demo`、`streams_unified_demo`、`streams_integration_test`、`cangjie_streams_demo`、`streams_enhanced_dsl_demo`；`streams_demo` 改用新 API。

### ✨ 新增与修复（计划 v2.38：E60 / E61 / E52 / DA4 / DA8）

- **背压流（E60）**：融合岛（单个类型化 Actor 内执行）+ `.async()` 边界按批 request(n)（默认 16）；`mapAsync` / `mapAsyncUnordered` / `ask`（有界并行，物化带 `AskCancellationToken`，阶段结束撤销未回复的 ask）；`Sources.queue` / `actorRef` / `fromPublisher` / `asSubscriber` / `tick`；`Sinks.actorRefWithBackpressure` / `asPublisher` / `queue`；`KillSwitches`；流内监督 `withSupervision`（Stop / Resume / Restart）；`Materializer.create(ctx)`。慢消费者面对无限流时内存有界。
- **类型化流式回复 StreamRef（E61 / DA7）**：新包 `cactor_cluster.streamref`。`StreamRefs.sourceRef<T>` 返回 `(SourceRef<T>, StreamProducer<T>)`，`SourceRef` 可放进 ask 回复跨节点传递；累计需求信用背压（非阻塞 `offer` / `whenReady`）、序号校验（缺号/重复/超发大声失败）、取消传播到生产方、属主停止/节点终止传播、单次挂接、订阅与空闲超时；Reactive Streams 形状的 `StreamRefSubscription` 与 `subscribeActor`。新增示例 `llm_token_stream`。
- **可注入调度器（E52）**：`ActorScheduler` / `ScheduledTask` / `RealTimeScheduler`，`CActorRuntimeConfig.withScheduler`、`CActorSystem.scheduler()`、`ActorContext.scheduler()`；定时器、接收超时、退避重启延迟、`ctx.scheduleOnce`、Actor 内 ask 超时都经系统调度器（默认实现行为不变）；`ActorFailure.failedAt`、`BackoffPolicy.next(failedAt:)`（带默认值的命名参数，源码兼容），退避复位与 `withLimit` 窗口按调度器时钟计算。testkit：`ActorTestKit(name, ManualTime())` 让虚拟时钟驱动真实系统，`ManualTime` 线程安全，新增 `awaitPending`、`expectNoMessageFor`。
- **分片按分片租约（DA4）**：`ShardingSettings.withLease`（默认关闭）——移交确认收齐后还须取得分片租约才托管，后台获取并指数退避、不超时强推；失去租约立即停止该分片实体；先交还租约再回 STOPPED；region 停止交还全部租约。`ShardLeases` 在 region 与实体间共享，`leaseForEntity` 可包装 `FencedEventStore` / `FencedDurableStateStore`，写入自动带 fencing token。`/cluster/shards` 增加 `lease` 字段，新增 `/cluster/singletons`。`tools/lease_contend_demo.sh` 跨进程验证 FileLease + fencing（已接入 ci.sh）。
- **测试稳定性（DA8）**：四个负载敏感用例查清根因（自动再平衡合法迁移分片、固定时限过紧、栈溢出用例耗时、共享工作线程被占）并修复，不放宽被验证的语义。
- 进度 33.5/44 ≈ 76.14%、加权 79.5%，验收见实施计划第 40 批。

### ✨ 新增、修复与行为变更（计划 v2.37：分布式加固 DA3 / DA4 / DA5 / DA6，API 快照 E51）

- **修复（DA3）**：TCP 入站帧字节预算在读帧体抛异常（对端帧中途断开、停机关闭 socket）时不再泄漏；此前每次最多永久占用 16 MB，几次即可让节点拒收全部入站帧。新增 `TcpTransportLimits.inboundFrameReadTimeout`（默认 30 s，须为正）限制单帧帧体的总读取时间，慢速对端被关闭并归还预算；新增 `TcpTransport.getInboundFrameTimeouts()`。
- **修复（DA3）**：对端重启只回收**已隔离**启动标识的入站可靠通道；未握手/未隔离的旧代保留去重状态，重放不会再次投递。新增 `ReliableBudgetUsage` 与 `SimpleRemoteManager.reliableBudgetUsage()`（节点级计数与通道实际占用对照），`ReliableDeliveryStats.inboundBuffered`。
- **新增（DA4）**：`cactor_foundation.coordination.Lease`（对标 Akka Coordination Lease）：获取带丢失回调、心跳续约、单调本地有效期（ttl − clockSkewMargin）、每次获取严格递增的 fencing token；`LeaseSettings`、`InMemoryLease` / `InMemoryLeaseStore`。`cactor_persistence.FileLease`（共享目录，`link(2)` 原子编号即 token）；`FencingTokenStore`（Memory / File）与 `FencedEventStore` / `FencedDurableStateStore` 拒绝旧 token 写入（`StaleFencingTokenException`）。`ClusterSingletonSettings.withLease`：取得租约才启动、失败退避不超时强启、丢失立即停止、移交前释放；默认关闭，旧构造签名与行为不变。
- **修复（DA5）**：Replicator 一致性操作序号起点随机化，同址重建的新实例不再被旧实例迟到的确认造成过半写/读虚假成功（线格式不变）。`ReplicatorConsistencyException` 新增 `replied` / `needed` / `total`。新增成员变化一致性矩阵 10 例并写明一致性边界（过半基于操作开始时的视图；视图不一致时过半写与过半读可不相交）。
- **新增（DA6）**：握手线协议能力协商，扩展附在旧节点视为不透明的字段末尾，旧节点无需升级即可互通。`SimpleRemoteManager.configureWire(WireProtocolSettings)`、`peerWireProfile`、`onWireIncompatible`、`wireIncompatibleSent` / `wireIncompatibleReceived` / `wireProtocolRejected`；`withMinPeerProtocol(n)` 拒绝低版本握手。`MessageCodec` 新增默认方法 `requiredCapabilities` / `encodeFor`；`SerializationException` / `DeserializationException` 改为 `open`。
- **行为变更（DA6）**：发往不支持 codec v3 的对端的 kind 3 载荷（带协议标记的 Message）不再“发送成功、对端静默丢弃”，而是抛 `WireIncompatibleException`（tell 抛出、ask 失败、tellReliable 不入队）并计数；kind 1/2 对旧节点自动写 v2 版本头。尚未完成握手的对端按保守能力编码。接收侧未知种类/更高版本抛 `UnsupportedWireFormatException`，单独计数、不投递、连接保留。
- **新增（E51）**：`tools/api_snapshot.py` 公开 API 快照（`docs/api-snapshot/`），`tools/ci.sh` 比对；有意修改公开签名后执行 `python3 tools/api_snapshot.py --update`。
- **测试**：修正 `controlChannelHasItsOwnByteBudget` 的预算算术；`testTopLevelCreateStopDoesNotLeak` 等待注册表注销完成（注销发生在 postStop 之后）。进度 31/44 ≈ 70.45%、加权 70.5%，验收见实施计划第 39 批。

### ✨ 新增与行为变更（计划 v2.36：隔离状态数量预算）

- `withPeerLimit` 同时限制隔离表条目数，默认 4096；满额时拒绝需要增加隔离历史的重启准入，保留当前身份。禁止缩容到已有隔离记录数量以下。
- 必须新增隔离但表已满时关闭后续全部身份准入，既有记录不驱逐，故障处理仍继续；新增 `SimpleRemoteManager.isPeerAdmissionClosed()` 查询保护状态。该状态跨 stop/start 与扩容保留，须替换管理器实例恢复；已有业务连接不统一断开，目标实例仍按正常隔离流程断连。
- 新增两个回归覆盖满额重启、扩缩容及 50 个并发隔离请求下数量有界和保护状态保留。全节点字节预算与安全回收仍待实现，进度保持 69.32%、加权 69.5%，完整验证见实施计划第 38 批。

### ✨ 新增与行为变更（计划 v2.35：对端身份与握手挑战预算）

- 新增 `RemotingSettings.maxKnownPeers` / `withPeerLimit(limit)`，默认 4096，分别限制已知对端身份和出站握手挑战表的地址数；全部链式设置保留预算。已有地址可复用，满额拒绝新地址，不驱逐旧身份或隔离历史。
- 出站新挑战满额抛 `RemotePeerCapacityException`；入站身份满额拒绝握手，保持协议线格式。禁止缩容到已保留状态以下；stop/start 保留预算及历史，扩容后可继续准入。
- 新增三个回归覆盖并发准入、旧 UID 重放、配置保留、挑战表容量与重启/缩容。隔离表预算、安全回收和全节点字节预算仍开放，进度保持 69.32%、加权 69.5%；验证结果见实施计划第 37 批。

### ✨ 新增与行为变更（计划 v2.34：可靠通道数量预算）

- 新增 `RemotingSettings.withChannelLimits(outbound, inbound)` 与 `SimpleRemoteManager.reliableChannelStats()`；默认出站/入站各 4096（此前总通道数无上限），所有设置构造器保留预算。新出站通道满额丢弃并计数，入站满额不登记、不投递、不确认；既有通道继续工作，同前缀新代在满额时替换旧代，旧代重放被拒绝。
- 保留确认后的序号/去重状态，不按空闲或 ACK 回收；业务与系统可靠通道共享额度。新增六个回归覆盖并发准入、代替换/旧信封重放、真实 TCP 饱和及 stop/start 后缩容保护；定向 6/6，完整门禁通过（全量 784/784、8 进程集群两轮各 137/137、宏负例 14/14）。DA3 的对端/隔离表及全局字节预算仍开放，44 项进度保持 69.32%、加权 69.5%；完整门禁证据见实施计划。

### ✨ 新增与行为变更（计划 v2.33：TCP 实例级连接预算）

- 新增 `cactor_foundation.network.TcpTransportLimits`，可在两个 `TcpTransport` 构造方法中以 `limits:` 注入。出站关联/入站连接默认各 256，空闲期限默认 10 秒；所有配置必须为正。满额出站新目标丢帧并计入总丢帧及容量拒绝，已有目标仍可发；普通和优先帧共用预算。
- 入站连接在启动 TLS/协议握手协程前准入，未完成握手也占用名额，满额立即关闭。新增 `getOutboundCapacityRejections`、`getInboundCapacityRejections`、`getInboundConnectionCount`；计数按实例累计，stop 清空当前连接。
- **行为变更**：原先已连接关联可无限空闲；现在空闲满期限也关闭，先关 socket 再归还关联槽，下次发送重新连接。旧构造调用保持源码兼容，但高扇出应用需要显式设置预算，不能再依赖无限目标/连接。仍是 best effort 传输，不保证容量拒绝的业务自动重试。
- 新增 5 个真实 TCP/并发回归，并更新原关联回收用例验证重连。DA3 只部分完成：可靠通道数量、全节点字节预算等仍未落地；计划进度保持 69.32%、加权 69.5%，不重复计分。验收记录见实施计划第 35 批。

### ✨ 新增与修复（计划 v2.32：类型化远程 ask 预算与停机）

- `TypedAsk.askRemote` 经 `RemoteActorRef` 发送时与经典 ask 共用 `maxPendingAsks`，默认 1024。满额 `RemoteAskCapacityException` 且不发送；`pendingAskCount` 保持经典计数，新增 `pendingTypedAskCount`；`replyEntryCount` 包含类型化临时入口。保留 sender 的本地代理转发也接入；普通 tell、本地 fallback 与自定义引用保留。
- 成功、超时、取消、发送异常即时注销类型化回复入口，stop 在锁外完成所有在途失败。`PromiseActorRef.fail/onComplete` 支持远程生命周期；非正 askRemote 超时直接抛 IllegalArgumentException，Timer 创建异常完成失败并解除取消订阅。首次完成语义与远端副作用边界保持。
- 新增 7 个回归，覆盖混合容量/拒绝不发送/成功复用、取消/停机重入、迟到回复、错误类型、发送异常、本地 fallback/代理及 20 次停止竞争；验收结果见实施计划第 34 批。DA2 完成后总进度仍按 30.5/44=69.32%、加权 69.5% 计，不重复折算已有能力。

### 🛠️ 修复与 API（计划 v2.31：远程 ask 资源与停机保障）

- 经典 `SimpleRemoteManager.ask` 增加统一在途登记：成功、超时、计时器创建异常、发送异常均注销临时回复入口并归还容量。`stop()` 立即结束等待并在锁外完成 Promise；停止与回复竞争遵循首次完成，不撤回远端副作用。
- 新增 `RemotingSettings.maxPendingAsks`、`withAskLimit(limit)`、`SimpleRemoteManager.pendingAskCount()` 及 `RemoteAskCapacityException.limit`。**行为变更**：经典远程 ask 默认上限 1024，满额失败且不发送；非正超时在登记前抛 `IllegalArgumentException`。该上限不覆盖 `TypedAsk.askRemote` 或整个节点的传输内存。
- 新增容量拒绝/成功释放、超时/迟到回复隔离、停机/回调重入、20 次停止竞争及发送异常注入等 7 个回归；新增同址新管理器的旧网络帧重放，在旧实现确认旧 ask 回复与发送方回复均串入新入口。两类入口名加入节点启动标识，阻断跨实例撞名，消息线格式保持兼容；最终 `source tools/env.sh && cjHeapSize=2GB tools/ci.sh` 完整通过：全量 766/766、8 进程集群两轮各 137/137、宏负例 14/14、TLS/故障恢复/日志 kill -9/三进程提醒/DData/soak/库输出检查。一次 LLVM llc 139 退出与重跑证据见实施计划 §3.7。新增分布式边界审查 §3.11，保持总计划 30.5/44=69.32%、加权 69.5%，不因加固已完成条目重复计分。

### ✨ 新增（2026-11-11，计划 v2.30：持久化提醒）

- `cactor_persistence.reminders`：新增 `ReminderDefinition`、`ReminderDelivery`、`ReminderCommand` 与 `DurableReminders.behavior`，复用异步持久状态引擎，登记/替换/撤销/确认在存储成功后回复。未确认到期消息在每轮和恢复后重发，确认后删除一次性提醒或合并错过的周期并推进。
- 单调持久代次与到期时间拒绝旧确认；业务按 `(PersistenceId, name, generation, dueAtMillis)` 去重。只提供至少一次投递，取消不撤回已发消息。每个标识要求唯一活跃写者，文件存储不提供跨进程所有权、复制和自动故障转移；集群路由和共享存储由应用提供。
- 验证：`source tools/env.sh && cjHeapSize=2GB tools/ci.sh` 全部通过：全量 759/759、8 进程集群两轮各 137/137、宏负例 14/14、三进程提醒恢复、TLS/黑洞/宕机恢复、日志 kill -9、DData、soak 与库输出检查。修正 remember-entities 宕机恢复测试的前置条件：该专测关闭自动再平衡，精确恢复计数和启动次数断言继续保留，定向及额外 5 次重复验证通过。
- 进度复算：E61 六类能力中三类落地，按 🟡 半项计，折合 30.5/44=69.32%；加权 69.5%。纠正旧表阶段 5 将已完成权重 12 误记 13 的偏差（旧基线应为 67.5%，本批增加 2）。流式回复、HTTP/LLM 与编排仍待实现。
- 新增 4 个真实运行时/磁盘恢复测试、1 个真实双节点 TCP 幂等重发/业务确认测试及三进程 `tools/reminder_demo.sh` 自检并接入 CI。支持注入 UTC 时钟；内部定时器随行为生命周期清理。API 文档明确状态表扫描成本、墙钟回拨和网络协议边界。


### ✨ 新增（2026-11-11，计划 v2.29：异步退避重试）

- `cactor_core.typed.TypedRetry.run<T>(operation, maxAttempts, backoff, ...)`：操作返回 Promise，每次完成失败后才安排下一次；尝试编号从 1 计，总尝试上限包含首次。复用 `BackoffPolicy.delayFor` 的指数退避、封顶和抖动，仅采用延迟参数，不采用监督的重启次数/稳定期复位语义。
- 支持 `shouldRetry` 失败筛选、`cancellation` 共享令牌及 `schedule` 调度注入；预取消跳过操作，取消清理等待中的定时任务。同步工厂异常按普通失败处理，筛选/调度异常直接结束，`TypedAskCancelledException` 不重试，次数耗尽保留末次错误。正在等待的 ask 应传入同一令牌。
- 验证：新增 8 例（7 个本地/虚拟时间/Timer + 1 个真实 TCP 重试），全量 **754/754**。一次完整 `cjHeapSize=2GB tools/ci.sh` 通过：两轮 8 进程集群各 136/136，以及 TLS/重启/黑洞/日志 kill -9/ddata/稳定性/println 门禁；本轮未复验默认堆配置。
- 新增虚拟时间测试和真实 Timer/TCP 回归。重试不是 exactly-once；超时不证明远端未执行，写操作须采用稳定幂等键或业务去重。E61 仅取消与重试两类完成，整体折算百分比保持不变。


### ✨ 新增（2026-11-11，计划 v2.28：可取消分布式 ask）

- `AskCancellationToken`（`cancel` / `isCancellationRequested`）可控制一组本地或跨节点 ask；`TypedAsk.ask` / `askRemote` 增加可选命名参数 `cancellation`，取消以 `TypedAskCancelledException` 结束等待。旧调用不传令牌时行为不变。
- 预取消跳过工厂和发送，工厂内取消跳过发送；取消与回复/超时以最先完成的结果为准。完成解除订阅并取消定时器，远程回复引用经既有 terminated 清理回收。取消不撤回已发送请求、不停止目标或回滚副作用。
- E61 仅 ask 取消子项完成；流式回复、异步退避重试、持久 reminder、HTTP/LLM 与编排示例仍待实现，不按整项完成记账。
- 验证：新增 8 例，全量 746/746。默认 CI 在并行 CRDT 旧用例出现 OOM（load >150）；以 `cjHeapSize=2GB` 复验 8 进程两轮各 135/135，并续跑全部剩余示例/故障/println 检查通过。默认配置的单次完整 CI 未全绿，失败与复验日志已写入计划。


### ✨ 新增（2026-11-11，分布式协议期望，计划 v2.27）

- `TypedProps.withProtocolTypeName(name)`、`ServiceKey<T>(id, protocolTypeName: Some(name))` 和 `TypedPayloadCodecs.declareTypeName<T>` 提供可选稳定协议名；顶层/子 Actor 引用及集群发现引用保留名称。登记前拒绝引用与 key 的名称错配，接收端对明确错配返回 Unhandled、计数并限速告警；未知名称继续交给原有 `as T` 校验并记 unverified，默认未声明期望时不查注册表。名称仅用于诊断与兼容检查，不是鉴权，也不改变服务注册的 id 命名空间。
- `TypedTypeExpectations` 提供检查、错配与未验证统计。`TypedPayloadBoxCodec` v3 保留 Message 显式协议名；仍读取 v1/v2，新增 kind 3 需要通信两端升级。普通 enum 仍须注册编解码器。
- 新增三个 codec 测试及一个真实三节点服务发现/协议错配测试；全量 738/738，完整 `tools/ci.sh` 通过。单例迁移测试等待所有存活代理的成员视图收敛后再测迁移后的 tell，遵循至多一次契约。

### 💥 破坏性变更（2026-11-11，删除重复聚合模块）

- 删除 `cactor_distribution` 和 `cactor_integration` 及其 workspace 依赖。分布式功能直接使用 remote / persistence / cluster；重复日志、配置、监控实现删除，`Histogram` / `calculatePercentile` 迁至 `cactor_foundation.monitoring`，同步 `TestActorRef` 等辅助迁至 `cactor_testkit`，语义测试迁入 tests。
- 示例改用 `ActorTestKit` / `TestProbe`；原 `TestActorSystem` / `TestTimeController` 文档替换为真实 API。兼容 `TestActorContext` 的 watch、行为切换、选择与调度明确抛异常，避免虚假成功或立刻执行定时任务；需要真实能力时用运行时或 BehaviorTestKit。ManualTime 尚不驱动真实运行时（E52）。
- E48 仍为部分完成，streams 收敛待 E60；不提高整项完成比例。计划总进度 30/44≈68%，加权 68.5/100≈69%，既定分布式清单 32/32=100%。


### ✨ 新增（2026-11-10，跨节点类型化 ask）

- `TypedAsk.askRemote<Req, Res>(target, request, timeout)`：跨节点类型化请求-响应。请求经 `TypedPayloadBox` 过线，
  回复引用走**信封的 sender** 通道（远程层为它登记回复入口，回复沿网络直达发起方），因此请求消息里不需要携带任何引用，
  用户自定义的 codec 也不必认识 `TypedActorRef` 字段。
- 目标端照常回复即可：经典上下文 `context.reply(响应)`，类型化上下文 `context.toClassic().getOrThrow().reply(响应)`。
- 回复无法还原为 `Res` 时抛 `IllegalStateException`，不再静默等待到超时；超时抛 `TypedAskTimeoutException`，错误文案带目标路径。
- 测试：`unit/cluster/typed_remote_test.cj` 两个用例（回复到达、不回复时超时且对端确实处理了请求），全量 732 → **734**。
- E63 记满：`ChildFailed` 信号、`SpawnProtocol`、集群版 Receptionist、`rest_for_one`、`withMdc` 之后，跨节点类型化 ask 补上最后一块。

### ✨ 新增（2026-11-10，跨进程 barrier）

- `cactor_testkit.ProcessBarrier`：用共享目录的 `.ready` 文件做跨进程会合，`arrive` / `await` / `parties` / `isOpen` /
  `reset`，shell 侧 `touch <dir>/<name>.ready` 即可参与同一 barrier；崩溃不会留下半开的连接，缺谁直接列在 `parties()`。
- `awaitGossipRounds(nodes, rounds, timeout)`（测试 harness）：按 `gossipCount()` 的推进替代固定 `sleep`，
  8 处负向断言的睡眠已迁移，高负载下不再因为『消息还没传播完』偶发变红。
- 测试：`unit/testkit/process_barrier_test.cj` 4 例（含用真实 `/bin/sh` 子进程验证跨进程会合），全量 728 → **732**。

### ✨ 新增（2026-11-10，类型化协议的普通 enum 过线）

- `TypedPayloadCodecs`：为不是 `Message` 的类型（类型化协议最常见的普通 `enum`）注册命名编解码器，
  `register<T>(typeName:, encode:, decode:)` 在注册时记录 `v is T`，发送时按注册顺序找第一个匹配；
  类型名重复立刻抛 `IllegalStateException`（两端对同一字节串解出不同类型比崩溃更糟）。
- 类型化信封升到 codec 版本 2：首字节区分"嵌套消息帧"与"命名载荷"，版本 1 的旧帧仍可解码。
- `TypedPayloadCodecs.find / findFor / unregister / count / typeNames`：诊断两端配置是否一致。
- 测试：`typed_codec_test.cj` 6 例、`typed_remote_test.cj` 新增 `enumProtocolCrossesNodes`（两节点真实 TCP），
  全量 724 → **728**。


### 💥 破坏性变更（2026-11-09，分布式收尾）
- **删除 `cactor_cluster` 根包原型**：`cluster_support.cj`、`cluster_routing.cj`、`failover.cj`、`shard_region.cj`、`cluster_sharding.cj`
  共 1673 行，以及只演示它们的 `cactor_examples.cluster_sharding_demo`（414 行）。这些类型（`ClusterState`、`ShardCoordinator`、
  `ClusterRouter`、`ShardRegion` 等）用进程内结构模拟集群，`forwardToRemoteShard` 只打日志、路由恒选第一个目标、
  `isShardLocal` 硬编码，与真实成员/分片层无关。请改用 `cactor_cluster.membership`：`ClusterSharding`（分片）、
  `ClusterRouterPool`（集群感知路由）、`ClusterSingleton`、`DistributedPubSub`；示例见 `best09_cluster_sharding`、
  `tools/cluster_demo.sh`。
- **`xstate` 去 `cactor_cluster` 依赖**：`DistributedStateSyncManager.init` 由
  `init(conflictResolver, syncInterval, clusterSupport)` 变为 `init(conflictResolver, syncInterval)`；
  `clusterSupport` 原本只是保存下来、从没参与同步逻辑。模块现标注 experimental（完整状态同步尚未实现）。

### ✨ 新增（2026-11-10，类型化引用的跨进程能力）
- **`SimpleRemoteManager.typedRemoteActorOf<T>(address, actorName): TypedActorRef<T>`**：远程地址上的类型化引用，
  `tell` 的载荷以 `TypedPayloadBox` 过线；载荷必须是实现了 `Message` 的协议类型。
- **`TypedPayloadBoxCodec`**（`cactor_core.typed`）：把载荷按它自己的 codec 嵌成一帧完整消息，不为每个协议再注册
  "类型化版本"；载荷不是 `Message` 时在发送侧抛 `SerializationException`（而不是悄悄丢包）。
  `ensureTypedPayloadCodecRegistered()` 幂等，`SimpleRemoteManager.start()` 自动调用，已存在的同名 codec 不会被覆盖。
- **`ClusterReceptionist.registerTyped / deregisterTyped / findTyped / findAllTyped`**：用 `ServiceKey<T>` 保证
  "找到的引用协议正确"；本节点返回本地引用，其他节点返回远程类型化引用。一次性回复引用（`PromiseRef`）不能登记。
- 测试 719 → 724：`typed_remote_test.cj` 2 例（两节点类型化 tell、三节点 registerTyped/findTyped）、
  `typed_codec_test.cj` 3 例（往返、非 Message 载荷、重复注册幂等）。

### ✨ 新增（2026-11-10，进程级故障注入）
- **`BlackholeTransport` 移到 `cactor_foundation.network`**（`modules/foundation/src/network/blackhole.cj`）：进程级注入需要示例进程与运维端点直接构造它；
  `cactor_testkit.BlackholeTransport` 通过 `public import` 继续可用，另新增 `blackholedAddresses()` / `blackholedCount()`。
- **`attachFaultInjectionRoutes(mgmt, transport, allowAll!)`**（`cactor_cluster.ops`）：`/cluster/faults`（黑洞地址 + 丢帧数）、
  `/cluster/blackhole?addr=host:port`、`/cluster/heal[?addr=]`，用 GET 以便 `curl` 一行触发。
- **`ManagementServer.addQueryRoute(path, contentType, handler: (String) -> String)`**：需要查询串的端点不必再把参数塞进路径；
  处理器拿到含 `?query` 的原始请求目标，解析留在上层。
- **`tools/cluster_chaos.sh`**：3 个真进程组成集群，黑洞一条链路并断言"被判不可达的是收不到心跳的一端"、`droppedCount` 增长、
  heal 后重新收敛；已接入 `tools/ci.sh`。`cluster_node` 新增 `CACTOR_MGMT_PORT`、`CACTOR_NO_AUTODOWN` 与 `hold` 模式。

### 🐛 修复（2026-11-10，进程级故障注入）
- **传输被包装后加密连接数恒为 0**：`SimpleRemoteManager.securedConnections()` 把传输向下转型成 `TcpTransport`，
  一旦外面包了 `BlackholeTransport` 就永远读到 0（TLS 示例自检会报"TLS 连接 0 条"）。改为接口方法
  `NetworkTransport.securedConnectionCount()` / `rejectedConnectionCount()`（默认 0，`TcpTransport` 覆盖真实计数，
  包装型传输转发内层），远程层不再转型——观测能力不再随装饰器一起丢失。
- **`ShardingRebalanceTest.manualRebalanceMovesShard` 在 8 进程并行下偶发超时**：用例关掉自动再平衡（只测手动迁移），
  迁移方向按观察到的负载决定，只等"集群里确实有实体"再迁一个分片并断言源 -1 / 目标 +1。

### ✨ 新增（2026-11-10，分片获取可观测）
- **`AcquisitionHealth` + `ClusterSharding.acquisitionHealth()`**（`cactor_cluster.membership`）：分片获取是许可式两阶段移交，
  必须等每个可能托管者确认"已停止"，否则会出现双主——所以不设超时强推，而是暴露 `slow`（等待超过阈值的分片数）、
  `oldestMs`、`pendingMembers`（还在等谁，通常是没跑同类型区域的节点）与累计告警次数 `slowWarnings`。
- **`ShardingSettings.acquireWarnAfter`**（默认 10 s，`withAcquireWarnAfter` 可调）：删掉硬编码的 10 s 阈值；
  告警从每个分片只报一次改为每过一个窗口再报一次，长时间故障不会被旧 warn 淹没。
- **`/cluster/shards` 新增字段**：`slowAcquires`、`oldestAcquireMs`、`acquireSlowWarnings`、`pendingAcquireMembers`。

### ✨ 新增（2026-11-10，运维与测试可见性）
- **管理端点的自定义路由**（`cactor/api/management.cj`）：`ManagementRoute(path, contentType, handler)` +
  `ManagementServer.addRoute`。路径必须是绝对的，重复注册抛 `IllegalArgumentException`；处理器在连接协程里执行，
  抛异常回 500 并计入 `handlerErrors()`，不会打断服务。门面仍不认识集群类型（分层约束），集群视图靠注入。
- **集群视图端点**（`cactor_cluster.ops`，新包）：`attachClusterRoutes(mgmt, cluster, sharding: Some(region))`
  挂上 `/cluster/status`（自身地址与实例标识、成员状态、收敛、领导者、upCount、不可达、SBR 是否 down、gossip 轮次）
  与 `/cluster/shards`（分片数、托管/获取中/已完成获取、实体数、投递/转发/移交/缓冲/丢弃、钝化与恢复计数、
  最忙 5 个分片、按分片号排序的负载表）。两者只读本地快照，不向其他节点发消息。此前
  `docs/TROUBLESHOOTING.md` 让运维 `curl /cluster/status`，但服务端只有健康检查与指标，路径一律 404。
- **`TestProbe.watch` / `expectTerminated`**（`cactor_testkit`）：探针用内部 `ProbeWatch` 控制消息在自己的
  Actor 线程里建立 DeathWatch；已停止的目标立即返回，活着的目标超时抛 `TestKitAssertionException`（消息含
  "没有停止"），不会吞掉其他消息。
- **`LoggingTestKit`**（`cactor_testkit/logging_testkit.cj`）：`start(level)` 临时替换全局 sink 并调低级别，
  `expectMessage` / `expectNoMessage` / `clear` / `close`（幂等还原）。只匹配安装或 clear 之后的新记录，
  失败时带上最近 5 条日志。为此 `Logging` 新增 `currentSink()`。

### ✨ 新增（2026-11-09，再平衡策略）
- **`RebalancePolicy`（`cactor_cluster.membership`）**：再平衡的阈值不再写死在区域里。不可变 struct，
  命名参数默认 `minEntityDelta = 5`、`ratioPercent = 150`、`cooldown = 3.seconds`、`reportTtl = 5.seconds`，
  构造时校验；`withXxx` 返回新实例，`disabled()` 关闭自动迁移（手动 `rebalanceShard` 不受影响）。
  用法：`ShardingSettings.passivating(1.minutes).withRebalance(RebalancePolicy().withRatioPercent(200))`。
- **`decideRebalance(view, policy)` 纯函数 + `RebalanceDecision` 枚举**：决策从 `ShardRegionActor` 抽出，
  返回 `Move(ShardMove)` 或 `Skip(RebalanceSkipReason)`（Disabled / UnreachableMembers / NotEnoughMembers /
  MissingLoadReport / Balanced / Cooldown），日志与测试都能看到"为什么没搬"；新增理由时穷尽 `match` 会在编译期报错。
  同值按地址/分片号 tie-break，与 HashMap 迭代顺序无关。
- **负载上报按签名去重**：负载不变时不推报告，但每 `reportTtl / 2`（默认 2.5 s）强制刷新一次，保证领导者手里的报告不过期；
  领导者判定统一走新的 `leaderAddress`。
- **`ShardingSettings` 链式方法**：`withRebalance` / `withBufferSize` / `withPassivation`（原构造与静态工厂不变）。

### ✨ 新增（2026-11-09，分布式收尾）
- **分片按负载的跨节点自动再平衡**（`cluster/membership/sharding.cj`）：领导者聚合所有 Up 成员 5 s 内的
  `ShardLoadReport`（非 Up 成员的报告直接丢弃、已离开成员的旧报告清理），不平衡度超过 1.5 倍且实体数差值 > 5 时，
  把最忙节点上最重的分片迁到最空闲节点——源与目标都可以是**任意成员**（此前只有"领导者自己最忙"时搬自己的分片）。
  视图里有不可达成员时不动，全部成员都有报告才决策，两次迁移间隔 3 s，每 2 s tick 评估一次；`rebalance()` 立即触发一次评估。
- **`BlackholeTransport`（`cactor_testkit.network_harness`）**：包装任意 `NetworkTransport`，按地址双向丢弃出站帧
  （同时覆盖普通与优先通道），`blackhole` / `heal` / `healAll` / `droppedCount` 可随时开关，其余方法全部转发。
  用于验证"对端不可达 → 故障检测 → 恢复"的完整链路（`unit/cluster/cluster_blackhole_test.cj`）。

- **同址重启偶发 `Address already in use`**：`TcpTransport.start()` 的 `bind()` 改为有界重试（最多 7 次、约 280 ms
  退避）。旧监听器的 `close()` 与阻塞在 `accept()` 的读协程在不同线程，Darwin 上描述符要等 `accept()` 返回才释放，
  同一端口立刻重启会先撞上 `EADDRINUSE`；`SO_REUSEADDR` 对"仍在监听的 fd"无效。端口被别的进程占用时仍然直接失败。
- **钝化与自动再平衡的交互（测试暴露）**：节点负载 20/0 时领导者会把分片连同实体移交，移交停止的实体计入
  `handedOff` 而不是 `passivatedCount`，`ShardingPassivationTest` 因此偶发 18/20。钝化用例改为显式
  `withRebalance(RebalancePolicy.disabled())`（库默认仍开启），断言改为等待计数到位，并在超时时打印逐区域诊断。
- **再平衡决策的内联常量与不确定结果**：阈值/冷却/TTL 原是常量，比例比较用 `Int64 * 3 / 2`（极端值会触发溢出检查），
  最忙/最空闲节点用 HashMap 迭代顺序决定（同值时不同节点可能搬给对方）。现统一走 `decideRebalance`：`Float64` 比较、
  地址与分片号 tie-break，策略可配置。

### 🐛 修复（2026-11-09，再平衡策略）
- **Grain 不该被按负载搬走**：`Grains` 的宿主区域改用 `ShardingSettings.passivating(...).withRebalance(RebalancePolicy.disabled())`。
  Grain 激活是有状态的（非持久化 Grain 的内存状态随激活走），按负载迁移等于静默丢状态，也违反"同一标识固定宿主"；
  分布由 rendezvous 哈希保证，迁移只在节点离开/宕机时发生。策略关闭时区域也不再上报负载报告。

### 🐛 修复（2026-11-09，分布式收尾）
- **分片放置覆盖不是可靠广播**：迁移与再平衡的 `ShardRebalance` 覆盖原先用 `ref.tell` 发给其他成员（at-most-once），
  漏收的成员会按 rendezvous 自行获取分片，与覆盖记录冲突、反复取/让（实测出现某节点被"搬空"）。
  改为 `remote.tellReliable(ref, ...)`：覆盖必须每个成员都收到，否则各节点的归属判断不一致。
- **再平衡测试恒真**：旧 `manualRebalanceMovesShard` 的断言块直接写 `true`；现断言源节点少一个分片、目标多一个，
  并新增 `leaderRebalancesSkewedLoadAutomatically`（8 个分片全部指向 a 后，b 开始托管只能由自动再平衡造成）。

### 🔧 工具链（2026-11-09）
- **macOS 门禁环境**：`tools/env.sh` 探测仓颉安装位置并导出 `DYLD_LIBRARY_PATH` / `DYLD_FALLBACK_LIBRARY_PATH`
  （`unittest_bin` 以 `@rpath` 引用运行时而没有写 LC_RPATH，`install_name_tool -add_rpath` 因无 header pad 失败）。
- **`tools/cluster_parallel_soak.sh`**：`--parallel=N` 会重新 exec 自己，子进程丢掉 `DYLD_*`，导致 0 s 内
  `Abort trap: 6`（退出码 134，`dyld: Library not loaded: @rpath/libcangjie-runtime.dylib`）。现在在 Darwin 上用临时包装脚本
  把 `DYLD_LIBRARY_PATH` 写进子进程环境后再 exec 真实二进制。
- **`tools/ddata_stress.sh`**：每轮用 `env CACTOR_LOG_LEVEL=error $cfg ...` 转发，而 `env` 是 SIP 保护的可执行文件，dyld 会把
  `DYLD_*_LIBRARY_PATH` 从它的环境里剥掉（同一二进制直接跑没事、经 `env` 就是 0 s `Abort trap: 6`）。现在显式把两个 DYLD 变量
  再传给 `env`。另加一条诊断：失败输出含 `Out of memory` 时提示先看该轮堆大小与 `uptime` 的 load average（本机 load 57–84 时
  GC 线程抢不到 CPU，256 MB / 128 MB 堆的轮次会以 `OutOfMemoryError` 退出，不是崩溃）。

### 💥 破坏性变更（2026-11-06 补记，docs/plan/20261106.md E30）
升级前请逐条核对：
- **默认监督策略与 Akka 对齐（E27）**：`DefaultSupervisionStrategy.defaultDecider` 对
  `ActorInitializationException`（`onStarted` / 类型化 setup 抛出的异常被运行时包装成它）、
  `ActorKilledException`（Kill）、`DeathPactException`（未处理 Terminated）返回 **Stop**，其余异常仍为 Restart。
  此前三者都重启：Kill 不停止 Actor、DeathPact 不停止监视者，启动恒失败的顶层 Actor 会同栈递归到 StackOverflowError。
  仍需重启时显式配置策略（如 `withSupervisorStrategy(OneForOneStrategy(..., { _ => Restart }))`）。
  自定义决策函数看到的启动失败是 `ActorInitializationException`（原异常在 `cause`）；类型化监督规则仍按原异常匹配。
- **`SupervisionDirective` 新增 `RestartAfter(Duration)` 分支**（6183072）：对它做 `match` 且没有通配分支的代码需要补分支。
- **类型化消息载体**：`tell` / 定时器的载体由 `TypedEnvelope<T>` 改为非泛型 `TypedPayloadBox`（8f512b9）。
  经典 Actor 中按 `TypedEnvelope` 匹配的代码会变成 unhandled。
- **删除 `SimpleActorRegistryEntry`**（d7976f0）。
- **持久化**：`AbstractPersistentActor.persist` 失败改为抛 `PersistenceException`（此前只记日志）；
  `saveSnapshot` 改为泛型签名 `<T> where T <: ToString`（4bfecc0）。
- **`typedActorOf` 加 `@Frozen`**（8f512b9）：依赖方需要重新编译。
- **集群分片哈希**：`shardOf` / `shardOwner` 先经 `mixHash` 混合（4bfecc0，修复分布偏斜）。
  **新旧版本不能混部**：滚动升级期间同一实体可能在两个节点上同时激活；开启 remember-entities 时旧分片键中的实体无法清理。
  请整群停机升级。
- **路由 `ConsistentHashRoutingStrategy` 改为 Rendezvous 哈希（E25）**：同一键的落点与旧版本（`hashCode % n`）不同；
  新增 `ConsistentHashRoutingStrategy(keyOf)` 指定路由键。
- **`SimpleStream.runAsync()` 返回 `Future<StreamResult<R>>`（E29）**；旧的 `runAsync(CActorSystem)` 标记 @Deprecated。
- **`StreamSource.distinct()` / `FlowFactory.distinct<T>()` 要求 `T <: Hashable & Equatable<T>`（E29）**：
  此前对任意类型可用但结果错误（只剩第一个元素）。
- **删除 `cactor_core.typed` 的 `TypedRouter` 一族**（`typed_router.cj`，零引用，策略为假实现）。
- **退避监督统一为一套（E6′）**：
  - `BackoffSupervisorStrategy` 标记 @Deprecated，改为委托 `BackoffSupervisionStrategy`，返回 `RestartAfter(延迟)`
    （此前退避 ≥1 ms 时恒返回 Stop）；删除其附带的 `BackoffInfo` 与恒返回 0.5 的 `Math` 结构体；
  - 删除 `cactor_runtime.supervision.BackoffSupervisor`（由定时器线程直接 `restartChild`，与 Cell 线程并发）；
  - 删除 `TypedTimers.startBackoffTimer`（一次性预排全部重试、取消无效、抖动恒定），退避重试请用监督。
- **重启上限计数（E6′，R13/R14）**：`OneForOneStrategy` / `OneForAllStrategy` 的 `maxRetries = 0` 表示不重启
  （此前第一次失败总被放行）；`RestartAfter` 与 `Restart` 一样计入上限（此前绕过上限）。
- **类型化 setup 失败（E6′，与 Akka Typed 一致）**：不限次数的 `restart()` 遇到 setup 失败直接停止（否则必然无限重启）；
  `withLimit` 的 restart 与 `restartWithBackoff` 照常重试。`supervise` 的规则改为在展开内层行为之前登记，
  首次 setup 失败也按规则处理（此前规则表为空，一律停止）。
- **DeathWatch 与 Akka 对齐（E32）**：
  - `Terminated` 改为进入监视者的**用户邮箱**队尾（此前在系统消息处理中直接调用 `receive`）。
    被监视者死前发来的消息因此一定先于 `Terminated` 被处理；但 `Terminated` 也会排在此前已入队的普通消息之后。
  - 本地 Actor 的 `Terminated` 入队后、处理前调用了 `unwatch`，该 `Terminated` 会被丢弃。
    手工发送的、目标为本地 Actor 的 `Terminated` 同样被丢弃（与 Akka 相同）；远程 watch 的 `Terminated` 不受影响。
  - 重启时先 `unwatch` 再停止子 Actor（Akka 默认 `preRestart`）：新实例不再收到旧子 Actor 的 `Terminated`。
- **类型化 Actor 内阻塞等待 `TypedPromise.get` 直接报错（E36，T15）**：
  在亲和调度线程（Actor 的处理方法）里等待未完成的 promise 时抛 `IllegalStateException`。
  此前要等到超时才报错，被等的 Actor 若在同一线程上就是死锁。已完成的结果任何线程都可以取。
  请用 `ctx.ask` / `pipeToSelf` 在处理方法里接收结果。
- **`TypedStashBuffer.unstashAll` 只处理调用时已暂存的消息（E36，T10）**：处理中再次 stash 的消息排在末尾，本次不处理。
- **CoordinatedShutdown（E34）**：
  - 改由 `CActorRuntime.getCoordinatedShutdown()` 按实例持有，`CoordinatedShutdown.get(name)` 标为 @Deprecated。
  - 阶段改为 Akka 的名称与顺序（新增 service-requests-done、before-cluster-shutdown、cluster-leave、cluster-shutdown、
    before-actor-system-terminate）。
  - 用户 Actor 改在 `actor-system-terminate` 阶段停止：此前在所有阶段之前就停止了。
  - 同一阶段的任务并行执行，每阶段有超时。
- **断路器语义对齐 Akka（E45，P7）**：`BasicCircuitBreaker`
  - Closed 状态计**连续**失败，任何一次成功清零（此前从不清零，偶发失败也会累计到开启）；
  - HalfOpen 同一时刻只放行一个试探调用，其余抛 `CircuitBreakerOpenException`（此前全部放行）；
  - 配置中的 `timeout`（callTimeout）生效：调用耗时超过它按失败计，并抛 `CircuitBreakerTimeoutException`，为 0 时不检查；
  - 开启满 `resetTimeout` 后 `getState()` 即返回 HalfOpen（等价于 Akka 的定时转换），开启时长改用单调时钟；
  - `getStats()` 的 `failureCount` / `successCount` 改为累计次数（此前是会被清零的阈值计数）。
- **指标注册（E45，P12）**：`MetricRegistry.register` 遇到名字已被**另一个**实例占用时抛 `DuplicateMetricException`
  （此前静默覆盖，先注册方此后的写入全部丢失）；`GlobalMetricRegistry.registerCounter/Gauge/Timer` 改为"取或建"，
  同名同类型返回已有实例，类型不符时抛异常。
- **告警（E45，P11）**：`AlertManager.evaluateMetric` 对同一规则只在首次越线时告警，条件不再成立时自动解决并发送 RESOLVED 通知
  （此前每次评估都新建告警、重复通知，且从不自动解决）。
- **追踪 ID（E45，P11）**：trace ID 改为 128 位、span ID 改为 64 位随机数，W3C 小写十六进制（32 / 16 个字符）。
  此前为 `trace-<毫秒>` / `span-<毫秒>`。
- **Prometheus 每 Actor 指标默认最多覆盖 1000 个 Actor（E45，标签基数上限）**：超出时按路径排序取前 1000 个，
  并输出 `cactor_actor_series_omitted`；用 `metricsText(perActor, maxActorSeries)` 调整。
- **生命周期钩子与重启协议对齐 Akka（E33，R12）**：
  - `Actor.preRestart(reason)` 默认调用 `postStop()`（此前调用 `preStop()`，旧实例的 `postStop` 在重启时从不执行）；
    `postRestart(reason)` 默认调用 `preStart()`（此前调用从不被运行时调用的 `postStart()`）。
    覆写了 `preStart` 的 Actor 重启后会再执行一次 `preStart`；只想在首次启动时初始化的代码请覆写 `postRestart` 为空。
  - `preStart()` 改在 Actor 自己的线程上、`onStarted` 之前执行；抛出的异常按启动失败（`ActorInitializationException`）
    交给监督，默认停止。此前在 `actorOf` 调用方线程同步执行，异常直接从 `actorOf` 抛出——依赖这一点的代码需改为 watch 该 Actor。
  - 重启先停止全部子 Actor，**等它们全部终止**后才创建新实例；期间到达的消息由新实例处理。
    此前立即创建新实例，新实例在 `onStarted` 里同名重建子 Actor 时与尚未终止的旧子 Actor 冲突。
  - Actor 失败、等待监督决定期间（含退避延迟），它的整棵子树暂停处理用户消息，Resume 后继续（Akka 的 suspendChildren）。
    此前子孙照常运行。
- **路由（E37）**：删除 `LeastConnectionsRoutingStrategy`、`FastestResponseRoutingStrategy` 与 `ResponseTimeTracker`
  （前者退化为轮询、后者把全部流量钉在第一个目标上，且都没有真实的连接数 / 应答时间来源），
  `RouterType.LeastConnections` / `FastestResponse` 随之删除，新增 `RouterType.SmallestMailbox`。
- **`Receptionist.subscribe` 返回 `ReceptionistSubscription`（E37，T12）**：此前返回 Unit。
  以 Actor 订阅时，订阅者停止后自动退订；不需要句柄的调用方用 `|> ignore` 一类写法丢弃即可。
- **ask 入口收敛（E38，A9 / C10）**：
  - 删除 `cactor_patterns.ask` 的 `TypedAskPattern` 与 `TypedAsk`（恒以超时结束的模拟实现）；类型化 ask 请用
    `cactor_core.typed.TypedAsk.ask` / `ctx.ask`。`TypedResult` / `TypedAskFuture` 保留。
  - `cactor_patterns.ask.Ask` 与 `AskPatternManager` 标记 `@Deprecated(since 1.3.0)`，2.0.0 移除；
    经典 ask 改用 `AskInbox.ask(target, message).get(timeout)`，目标照常 `context.reply`，不再解包 `AskMessage`。
- **类型化监督按行为结构匹配（E36，T9，对标 Akka Typed）**：
  - 嵌套 `supervise` **由内向外**匹配（内层规则优先）；上一轮曾改为按登记先后（外层先匹配），本次纠正。
  - 规则随行为走：处理中返回的新行为仍在原监督层内；重启次数、退避状态按规则在行为中的位置累计，
    `setup` 里的 `supervise(...).withLimit(n)` 跨重启共用一份计数（此前每次重启登记一组新规则，规则表只增不减）。
  - `transformMessages` 里的 `supervise` 生效（此前没有登记，失败一律按默认停止）。
- **`MailboxOverflow.Block` 在 Actor 内发送时不再等待（E39）**：发送方在默认调度器的工作线程上（即 Actor 的处理方法里）时，
  邮箱满即按超时处理（计死信、WARN 一次），不占住工作线程；Actor 之外的生产者照常等待空位，改为条件变量唤醒
  （此前在发送线程上轮询 sleep，最长占住整个 Block 超时）。Actor 之间的背压请用 ask / 确认消息实现。
- **未处理消息通知死信订阅者（E36，T11）**：`subscribeDeadLetters` 的回调现在也会收到 `UnhandledMessage`
  （receive 返回 Unhandled、类型化行为返回 unhandled、类型化 Actor 收到协议之外的消息）。
  只关心投递失败的订阅者请过滤 `is UnhandledMessage`；`deadLetterCount()` 不计入它们。
- **死信日志改为限速 INFO（E36）**：每 5 分钟逐条记前 10 条死信 / 未处理消息，之后只计数并在下一窗口汇总
  （`CACTOR_LOG_DEAD_LETTERS` 调整条数，0 关闭）；此前死信只在 DEBUG 级别逐条记。
- **`TypedActorContext` 的子 Actor 创建改由 `actorOfWithHooks` 实现（E36，T16）**：自定义上下文实现
  （测试替身等）应覆写 `actorOfWithHooks(behavior, name: Option<String>, props)`，三个 `actorOf` 改用接口默认实现。
- **`@Message` 编解码器重名检测（E36，T13）**：`registerCodec()` 遇到**另一个**类已用同一名称注册时抛
  `IllegalStateException`（此前后注册者静默覆盖，不同包里的同名消息类互相顶掉）；用 `@Message[name = "包名.类名"]`
  指定唯一的线上名称。手写编解码器的两参 `MessageCodecRegistry.register` 仍是"显式替换"。
- **`system.stop` 与 Akka 一致：立即停止，名字保留到终止（E33）**：
  - 停止请求走系统消息：处理完**当前**消息即停，邮箱中剩余的消息计为死信。此前在已入队的消息之后才停。
    需要"处理完已排队的消息再停"请改用新增的 `system.gracefulStop(ref, timeout)`，或发 `PoisonPill`。
  - 名字与登记保留到 Actor 真正终止：终止前同名 `actorOf` 抛"名称已存在"，`lookup` 仍能查到它；
    `getAllUserActors` / `actorCount` / 注册表计数在终止时才减少。此前 stop 时立即释放名字，停止中的旧实例与同名新实例短暂共存。
    stop 之后要立即同名重建的代码请改用 `gracefulStop`（返回 true 时名字已释放），或 watch 等 `Terminated`。
  - 监督决定 Stop 的顶层 Actor 同样立即停止（此前要先把积压消息处理完）。
  - `system.stop` 对子 Actor 也生效（此前静默无效）。
- **远程可靠投递与远程 watch（E41，阶段 2）**：
  - `tellReliable` 的通道最多保存 `RemotingSettings.maxUnacked`（默认 20000）条未确认消息，满了**丢弃新消息**并计入
    `reliableDeliveryStats().overflowDropped`（此前无上限）。最老的未确认消息超过 `giveUpAfter`（默认 6 小时）仍未确认，
    放弃发往该节点的全部消息，并向监视该节点上 Actor 的本地 Actor 投递 `Terminated`。两个默认值同 Akka。
  - 接收端重启后，未确认的消息在新通道里重发：跨重启是**至少一次**（旧实例收到但没来得及确认的会再投一次）。
    此前新实例永远不投递（N2）。
  - 被监视节点重启（启动标识变化）时立即投递 `Terminated`，不再等心跳超时；节点在心跳容忍时间内重启也能识别（此前识别不了）。
  - 线上协议：`ACK` / `PONG` 末尾多一个字段（启动标识），WATCH / UNWATCH / DEATH 改以可靠信封发往 `$sys`。
    旧版本节点会忽略多出的字段，也能解开可靠信封，混部可以互通，但旧节点享受不到重启识别。
- **TCP 传输发送不再阻塞、不再因对端不可达抛异常（E40，N1，对标 Akka Artery）**：
  - `TcpTransport.send`（以及 `RemoteActorRef.tell`、`SimpleRemoteManager.sendMessage`）只把帧放进目标地址的出站队列：
    连接、重连、写入都在该地址的写线程里完成。对端不可达、连接失败、写失败时帧被**丢弃并计数**
    （`getDroppedFrames()` / `getSendFailures()`），**不再抛异常**；此前在调用方线程同步 connect（3 s 超时，失败再试一次），
    失败抛异常。依赖异常判断对端不可达的代码请改用远程 watch 或 `tellReliable`。传输未启动、帧超过 16 MB 仍抛异常。
  - 出站积压超过 64 MB（控制队列 4 MB）时**丢弃新帧**并限速告警；此前发送方阻塞等待。
  - 连接失败后按 100 ms 起翻倍、封顶 5 s 退避重连；没有连接且空闲 10 s 的关联退出。
- **远程连接握手与隔离（E7′，对标 Akka Artery handshake / quarantine）**：
  - `SimpleRemoteManager` 的每条 TCP 连接先交换身份（地址 + 启动标识）：连接方发 HELLO、接受方回 ACK，
    不通过就断开。**与未升级的节点不能互通**（旧节点不发握手，第一帧被拒绝），请整群升级。
  - 远程 watch 的心跳判定不可达、可靠消息超过 `giveUpAfter` 未确认时，对端当前实例被**隔离**：再也连不进来，
    必须重启（换新的启动标识）才能重新通信——误判不可撤回，与 Akka 相同。此前判定不可达后对端照样能继续收发消息。
  - 同一地址出现新的启动标识（对端重启）时，旧实例被隔离、监视者立即收到 `Terminated`。
  - `RemotingSettings.defaults()` 的心跳改为 1 s、φ 阈值 10、容忍停顿 10 s（同 Akka watch-failure-detector；
    此前 200 ms / 8 / 1 s——误判不可撤回之后，1 s 容忍一次 GC 停顿就可能把活着的节点永久隔离）。需要快速检测的请显式配置。
- **集群节点实例与种子加入（E7′ 的 N4 / N5，对标 Akka UniqueAddress / FirstSeedNodeProcess）**：
  - 成员 gossip 与 JOIN 的报文格式变了（成员带启动标识 uid、gossip 带墓碑），**与未升级的节点不能组成同一个集群**，请整群升级。
  - 同一地址重启的进程是新实例：加入时旧实例先被 down、由 Leader 移除，新实例再以 Joining 加入、分配新的 upNumber。
    此前新进程直接继承旧成员的 Up 状态与 upNumber。订阅者会依次收到旧实例的 `MemberRemoved` 与新实例的 `MemberJoined` / `MemberUp`。
  - 被移除的地址可以由**新实例**重新加入（此前同一地址永远被拒绝）；被移除的同一实例仍被拒绝。
  - 首个种子不再立即自己组成集群：先向其他种子发 JOIN，其他种子全部回 NACK（都是新启动的）立即组成；
    没有任何回应时等 `seedNodeTimeout`（默认 5 s）后组成；已有集群在运行时加入它。种子列表只有自己（或为空）时行为不变。
    此前首个种子重启后会另起一个只有自己的集群（两个集群、两个 Singleton）。
  - `Replicator` 的默认节点标识由远程地址改为"地址#启动标识"：同一地址重启后的 CRDT 写入不再被旧进程的分量吞掉
    （此前 PNCounter 的新增量在超过旧值前全部丢失，ORSet 的新 add 被当成已删除）。显式传入 `selfNode` 的不受影响。
  - `Cluster.selfMember()` 只返回本实例的记录（本地址的记录属于别的实例时为 None）。
- **持久化日志 v2（E8′，对标 Akka journal）**：
  - `FileEventStore` 的文件格式改为 v2（文件头 + 每条记录带序号、CRC32、tags）。v1 文件首次打开时**自动原子迁移**，
    迁移后旧版本程序读不了，请整体升级后再回滚需先备份 `*.journal`。
  - `FileEventStore` 默认 **fsync**：`append` / `saveEvent` 返回时事件已落盘，逐条写入约 1 千条/秒（本机），
    比此前（只写页缓存、不 fsync）慢；吞吐敏感处用 `persistAll` / 批量 `append`（一批一次 fsync），
    或 `FileEventStore(path, fsync: false)`（只防进程崩溃，不防断电）。
  - `EventSourcedActor.persist` 改用带期望序号的 `append`：同一持久化标识的第二个写者被拒绝（`JournalConflictException`，
    交给监督，重启后从日志恢复）；此前两个实例的事件交错写入日志、状态分叉。
  - 重放一律按序号：`EventSourcedActor` 恢复、`ReadJournal` 的 `sequenceNr`、`DefaultPersistencePlugin` 快照之后的重放
    （此前按时间戳，同一毫秒内的事件会重复或遗漏；没有快照时恢复出的最高序号恒为 1）。`deleteTo` 之后序号不再等于位置。
  - 日志尾部残缺（写到一半、断电后的零填充）自动截断；中间损坏而其后仍有完整记录时抛 `PersistenceCorruptionException`，
    不截断（此前长度为 0 的记录被当成合法记录，恢复报损坏、Actor 永远起不来；中间损坏不被发现）。
  - `FileSnapshotStore` / `FileDurableStateStore` 写入改为"临时文件 fsync → rename → 目录 fsync"，返回时已落盘。
- **`AbstractPersistentActor`（E8′ 的 P3 / P4 部分）**：
  - `preStart` 自动从存储恢复（重启后的新实例同样恢复）；已手动调用 `startRecovery()` 的不重复恢复。
    此前没有恢复钩子：重启后序号为 0，首次 persist 即与存储冲突，在默认监督下无限重启。
    依赖"启动时是空状态"的子类需要改写；重写 `preStart` 时请调用 `super.preStart()`。
  - 事件已落库后处理器抛异常：抛 `EventHandlerException`（`PersistenceException` 的子类，原来的 catch 仍能捕获），
    **序号不再回退**；默认监督重启后由恢复重放该事件。此前回退序号，之后每次 persist 都与存储冲突。
  - `persistAll` 消费存储返回的 Future：版本冲突抛 `PersistenceException`（此前静默吞掉、序号照样前进）。
  - 恢复失败抛 `PersistenceException`（此前只记日志、以残缺状态继续运行）；在 `preStart` 中即为启动失败，默认停止。
- **持久状态删除留下墓碑、修订号不回退（E44，P14，对标 Akka DurableStateStore）**：
  - `DurableStateStore.delete(id, revision)` 改为写入修订号 `revision + 1` 的墓碑：`get` 返回 None，
    新增的 `revisionOf(id)` 返回墓碑的修订号；之后的 `upsert` 必须用 `revisionOf + 1`（此前删除后修订号回到 0，
    从 1 重新开始）。持有删除前修订号的旧写者因此被拒绝——此前它能把已删除的状态以旧修订号写回来（ABA）。
  - 经典 `DurableStateActor` 删除后修订号同样接续；`persistenceIds()` 不含已删除的标识。
  - `FileDurableStateStore` 的墓碑是带第 4 个字段的记录，旧文件照常读取；**旧版本程序会把墓碑读成值为空串的状态**，回滚前请注意。
  - 自定义 `DurableStateStore` 不实现 `revisionOf` 时，默认实现基于 `get`（不认识墓碑，行为同此前）。
- **集群加入时检查配置兼容（E42，对标 Akka JoinConfigCompatChecker）**：JOIN 带上兼容键——内置分片哈希版本
  `cactor.sharding.hash` 与移交协议版本 `cactor.sharding.protocol`，以及 `ClusterSettings.withJoinCompatibility(k, v)`
  添加的用户键；键集合或取值不一致的节点收到 REJECT，永远不会成为成员（`Cluster.joinRejection()` 给出原因），
  被拒的首个种子也不会自己另起一个集群。**不带兼容信息的旧版本节点同样被拒绝**：本版本不能与旧版本混部，请整群升级。
  此前新旧哈希算法的节点可以混部，同一实体在两个节点上同时激活（N9）。
- **分片改为许可式移交（E13′）**：节点在征得其他每个 Up / Leaving / Exiting 成员确认"已停止该分片"后才托管它，
  视图建立后立即获取自己的全部分片。可观察的变化：
  - 获取分片要等每个成员的区域回应：**集群中每个节点都必须创建同类型的 `ClusterSharding`**（此前文档已要求，
    现在不满足时这些分片的消息一直缓冲，10 s 后告警点名没有回应的成员）；
  - 路由不再等成员视图收敛：只有拥有者不可达或正在获取的分片缓冲消息；
  - 缓冲区满时丢弃**新到的**消息（此前丢最旧的），节点离开集群后的消息与缓冲区里的消息交给死信；
  - 移交期间旧节点先处理完实体邮箱里的消息、实体终止后新节点才创建实体，移交耗时比此前略长。
- **Replicator 协议 v2（E64：delta 传播与 pruning）**：更新推送改为带（来源, 键）序号的增量 / 整值报文（`DL` / `DQ`），
  一致性读写与反熵报文附带 pruning 标记；新增内置兼容键 `cactor.ddata.protocol = 2`，与之前版本的节点不能混部（加入时被 REJECT），
  请整群升级。经 `ClusterReplicatorBinding`（分片、pubsub、Receptionist 都会调用）使用的 Replicator 自动启用 pruning：
  被移除节点的计数器分量并入其他节点，`ReplicatedValue` 的内部分量（`nodeComponents()`）随之变化，用户可见的值不变。
- **集群工具补齐（E12′）的可观察变化**：
  - **gossip 编码 v2**：墓碑带移除时间、成员表不再保留 Removed 记录；新增内置兼容键 `cactor.cluster.gossip = 2`，
    与 E42 之前及 E13′/E42 中间版本的节点不能混部（加入时被 REJECT），请整群升级；
  - 被移除的成员从 `Gossip` 删除、只留墓碑，墓碑 24 h 后清除（`ClusterSettings.withPruneTombstonesAfter`）；
    `MemberRemoved` 事件照常发出；
  - **被 down 后移除的成员按 uid 隔离**（对标 Akka）：被误判的僵尸进程再也连不进来，入站连接随之关闭，同址重启的新实例不受影响；
  - `DistributedPubSub.subscribe` **不再阻塞**（此前同步等待 Replicator，最长 10 s）；需要确认本地生效用 `subscribeAsync`；
  - `Grains.call` 在 Actor（包括 Grain）的处理方法里调用时**立即抛 `IllegalStateException`**（此前阻塞工作线程直到超时，
    被等的 Grain 恰好在同一线程上就是死锁）；Grain 之间互相调用请实现 `onCallAsync` 并用 `callAsync` 组合；
  - Grain 的应答入口名改为 `grain-replies-<kind>-<uid>`、`GrainCall.replyTo` 改为 "地址#uid"；
  - 远程回复入口名改为 `$ref<路径>#<序号>`（按实例登记，Actor 终止后注销），发给已终止实例的迟到回复计为不可投递。
- **Cluster Singleton 的 HandOver 握手（N16）与代理改道**：
  - 新的最老成员收齐其他 Up / Leaving / Exiting 成员的 HANDOVER_DONE 才启动实例，旧实例以 PoisonPill 停止
    （先处理完邮箱里的消息，此前 `system.stop` 直接停止、丢弃邮箱）；正常离开后被移除的成员最多再等 10 s
    （`SINGLETON_HANDOVER_GRACE_MS`），被 down 的成员不用等。移交比此前慢一个"旧实例停止"的时间；
  - 代理发往其他节点的消息改发该节点的管理器入口 `singleton-<名称>`（此前直接发给暴露的实例，
    新实例握手期间到达的消息会丢失）；实例本身不再暴露给远程层，控制消息走 `singleton-ctl-<名称>`；
  - 实例自己停止后，管理器在下一轮重新获取并启动（此前留着已死的引用，消息全进死信）。
- **构建引入 stdx（E65）**：根 `cjpm.toml` 以 `bin-dependencies` 静态链接 stdx，路径取环境变量 `CANGJIE_STDX_PATH`
  （stdx 的 `static/stdx` 目录）。直接 `cjpm build` 之前须设置它，或 `source tools/env.sh`（按本机安装位置给出默认值，
  `tools/ci.sh` 已自动执行）；未设置时 cjpm 只报 "imports package 'stdx...' but it is not added as a dependency"。
  可执行文件不新增运行时依赖（静态链接；OpenSSL 3 只在用到 TLS 时由 stdx 加载）。stdx 只允许 remote 及以上的模块使用，
  `tools/check_module_deps.py` 拦截底层模块的 stdx 导入。
- **`@actor` 生成的分派顺序（E36，T13）**：兜底类型（`Message` / `Any`）的处理方法总是排在最后；
  首参为 `ActorContext` 的 `onXxx`（如 `onStarted(context)`）与 static 方法不再被当作消息处理方法。

### 🐛 修复（2026-11-08，CI 第 19 批发现）
- **顶层 Actor 终止后同名重建偶发失败**：`lookup` 在 Actor 完全停止（`postStop` 之后）即看不到它，但名字要等终止回调
  释放 Guardian 登记、再注销运行时登记表之后才可重用——这个窗口里同名 `actorOf` 报"Actor名称已存在"
  （负载下 200 次"启动失败 → 同名重建"约 3 次失败，再加 24 个忙循环时 15 轮中 1 轮失败 18 次）。现在同名创建遇到已完全停止的旧实例时
  代它释放名字（清理恰好执行一次，迟到的终止回调不再重复）；`SimpleActorRegistry.unregister(ref)` 只注销这个实例
  （此前按名字删除，旧实例迟到的注销会删掉同名新实例的登记），同名替换不重复计数

### 🐛 修复（docs/plan/20261106.md 阶段 0，2026-11-06）
- **Random 路由死循环（T1）**：CAS 期望值误用掩码后的值，种子 > 2^31 时首次 `selectRoutee` 即 100% 空转，
  并使 `tools/ci.sh` 永久卡死。CAS 改用原值，种子折叠进 Lehmer 生成器的合法区间
- **顶层 Actor 启动失败无限递归（R1）**：重启经本 Cell 系统队列异步进行；默认策略对启动失败返回 Stop
- **顶层 Actor 延迟重启（R2）**：`RestartAfter` 由定时器线程直接 `doRestart`（与 Cell 线程并发、可让已停止的 Actor 复活），
  改为挂起后到期只投递 `SysRecreate`，由本 Cell 线程重启
- **父先于子停止（R3）**：父 Actor 停止期间子 Actor 的死亡通知不再参与合批计数，被监视的子不会被扣减两次
- **系统消息重复处理（R4）**：`pushSystem` 恢复 CAS 循环（load + swap 与无锁的消费端竞争会把已取走的节点挂回栈上）
- **工作线程被 Error 杀死（R5）**：`run()` 用 finally 释放调度位；用户代码中的 Error（如 StackOverflowError）
  包装成 `ActorErrorException` 交给监督；工作线程循环兜住 Error；监督策略本身抛异常时按 Stop 处理
- **同批 [SysTerminate, SysWatch] 漏发 Terminated（R20）**：已终止后收到的 watch 立即通知
- **路由（E25）**：SmallestMailbox / ConsistentHash / WeightedRoundRobin 跳过已终止目标（`ActorRef.isTerminated()`，默认 false）；
  WeightedRoundRobin 改为平滑加权轮询，routee 与权重数量不一致时不再永久返回 None；ContentBased 回退哈希不再出现负下标
- **门面假入口（E29）**：`CActorFactory.createSystem`、`CActorSystemBuilder.build`、`cactor.api.extensions` 的系统创建、
  `CActorRuntime.getActorSystem()` 均改为由真实运行时实现的 `RuntimeActorSystem`；`ActorSelection.ask` 返回目标的真实回复
- **流（E29）**：`distinct` 保序去重；空源交给 Sink 处理（fold 返回初值）；`runAsync` 在轻量线程上执行且 Sink 只执行一次
- **CQRS（E29）**：`CQRSBus.sendCommand` 使用真实序号与期望版本、等待写入结果，写入失败时不更新读模型；
  新增 `registerAggregateFactory`，处理器拿到重放后的当前聚合
- **CI 门禁（E26）**：`tools/ci.sh` 单用例超时（`CI_TEST_TIMEOUT`，默认 120 s）、示例/脚本超时、清除 `CACTOR_LOG_LEVEL`、
  `--bench` 与 `--soak` 可组合；`stress_matrix.sh` 不再依赖 zsh；`bench_gate.sh` 按退出码判定基准运行失败；
  两者在未 source 仓颉环境的 shell 中自动 source `envsetup.sh`（此前每个程序都因缺 `libcangjie-runtime.so` 以 127 退出）；
  `tools/ci.sh` 跑测试时加 `--no-progress`（仓颉 1.2.0 的 std.unittest 进度汇报线程偶发 NoneValueException，
  全部用例通过时也会让 `cjpm test` 判失败）
- 新增 `cactor_foundation.hashing`（稳定 FNV-1a、fmix64、Rendezvous 选择）
- **DeathWatch（E32）**：
  - 修复：被监视者死前发出的消息可能晚于 `Terminated` 到达（R6）；
  - 修复：类型化 `watchWith` 的映射随重启丢失，新实例收到未处理的 `Terminated` 后触发 DeathPact（T3）；
  - 修复：重启后同名重建的子 Actor 因按路径判重而没有被监视，旧子的 `Terminated` 反而投给了新实例（R18 的 watch 部分）；
  - 修复：有界邮箱已满时 `Terminated` 不再丢失。
- **协调关闭（E34，R17）**：
  - 修复：闭包挂在静态表中永不释放，同名系统会共享任务；
  - 修复：没有超时，卡住的任务让关闭永远结束不了；
  - 修复：重复调用会重复执行任务；
  - 修复：在 Actor 中调用 `terminate()` 时只发起关闭、不等待（否则就是等自己停止）。
- **停止协议（E33 部分）**：
  - `system.stop` 的停止请求不再受有界邮箱容量限制（R7）：此前邮箱满时（DropNew）停止请求被丢弃，Actor 已摘除登记却继续运行；
    Fail 策略下 `stop` 本身还会抛异常；
  - 顶层 Actor 的注销与 `actorOf` 的登记串行化（R11）：此前 Actor 在登记前就终止时会留下死条目，此后同名 `actorOf`
    永远报"名称已存在"。
- **类型化（E36 部分）**：
  - `unstash` 逐条出队（T10）：处理某条消息时抛异常，其余未处理的消息仍留在缓冲区（此前先清空再处理，Resume 后全部丢失）；
  - 类型化 Actor 的 PreRestart / PostStop 信号处理抛出的异常改为记 warn（A10）：此前静默吞掉。
- **经典 ask 超时（E38，T8）**：每个请求自带一次性定时器，按自己的时限准时超时，收到回复时取消定时器。
  此前只靠 30 s 一次的清理循环：1 s 的超时最多晚 30 s 才回调，未调用 `start()` 时永不超时。
- **阻塞调度器扩容（E39，R16）**：改为在没有空闲线程时扩容；`getBlockingWorkerCount()` 计入扩容出的线程。
  此前判断"排队数 > 线程总数"，所有线程都被阻塞后新任务无限期排队。
- **并发安全（E35）**：
  - lookup / Prometheus 抓取跨线程读父 Actor 的子表（R8）：子表改为"父线程写时持表锁、其他线程持表锁读"，
    首次分配经调度字的 `CHILDREN_BIT` 发布；此前无锁遍历一个正在被父 Actor 增删的 HashMap。
  - 释放调度位之后不再读只属于持有者的字段（R15）：`state`、unstash 游标、邮箱出队缓冲改为释放前快照，
    释放后只读原子量；子 Actor 读父的"合批停止"标志改为父调度字上的 `BATCH_BIT`（此前读父的多字 Option 字段，与父的首次写入并发）。
  - 每 Actor 计数器（R19）：已处理数为 64 位；死信 / 重启 / 未处理改为按需分配的独立 64 位计数（同样经调度字的位发布）。
    此前四个计数共用一个 64 位字、各 16 位，在 65535 饱和却仍按 counter 导出；多个发送方并发计死信时"先读后加"会丢数、
    进位污染重启计数。每 Actor 内存不变（删掉的合批标志字段与新增的计数引用相抵）。
  - 监督策略的重试窗口（v1 A3）：窗口起点用 CAS 只设置一次、过期也只由 CAS 成功的一方清零，改用单调时钟；
    `EscalationInfo` 的失败历史加锁。
- **基础件（E45）**：
  - `cactor_foundation.queue.ConcurrentHashMap`（P13）：改用互斥锁，键的 `hashCode` / `==` 抛异常时锁也会释放
    （此前自旋锁没有 finally，之后所有调用方永远空转）；`put` 返回旧值（此前恒为 None）。
  - `PersistentEvent.withMetadata`（P8）：返回的事件带上新元数据并保留原时间戳（此前构建了新表随后丢弃）。
  - `Tracer`：`getActiveSpanCount` / `getFinishedSpanCount` 返回真实数量（此前恒为 5 / 10）；已完成 span 有保留上限。
  - `AlertManager`：通知改在锁外发送，单个渠道抛异常只记日志（此前在锁内发送且没有 finally，渠道一抛异常锁就永不释放）；
    告警 ID 不再在同一毫秒内重复；`getRuleCount` / `getChannelCount` 返回真实数量（此前恒为 3 / 2）；历史有上限。
  - integration `Histogram`（P15）：大于全部上界的观测计入 +Inf 桶（此前被丢弃）；新增累积计数与 Prometheus 文本输出。
  - integration `calculatePercentile`（P15）：先排序再按最近秩取值（此前按插入顺序取下标）。
- **路由（E37，C12、T7）**：经路由器转发的消息保留原发送方（`RouterActorRef.tell(message, sender)`、
  `Router.route(message, sender)`）：目标 `context.reply` 直接回到原发送方，此前 sender 在路由层被丢弃，回复进死信。
- **Receptionist 订阅泄漏（E37，T12）**：订阅从不移除——订阅者 Actor 停止后仍持续收到（投递到死信的）通知，
  类型化组路由器停止后订阅也不释放。
- **退避计数在负载下被误清零（E6′ 遗留，第 5 批 CI 发现）**：`resetBackoffAfter` 的"重启后稳定运行时长"此前按
  "失败时刻 + 退避延迟"估算；重启定时器或调度晚到（负载下 40 ms 实测约 100 ms 才生效，或重启要等子 Actor 慢慢停止）时，
  估算的运行时长虚增、计数被清零，`maxRestarts` 永远到不了，Actor 无限重启。改为由运行时报告实际完成重启的时刻
  （`ActorFailure.lastRestartAt`），经典与类型化退避都以它为准。
- **Receptionist 内部监视者与用户 Actor 重名（T12）**：此前固定名为 "receptionist"，用户已有同名顶层 Actor 时
  `receptionist()` 直接抛"名称已存在"；改用 `$receptionist`，被占用时加序号。
- **类型化（E36，2026-11-07）**：
  - 监督规则表随重启只增不减（T9 / v1 A4）；`transformMessages` 里的 supervise 不生效；
  - 未处理消息与类型化协议之外的消息只加一个计数、静默消失（T11）；
  - `ctx.actorOf` 构造的桥接闭包没有传下去，子 Actor 全部走慢路径（T16）：以 `setup` 开头的子 Actor 现在与顶层
    `typedActorOf` 同样走用户包 hooks（`typed_spawn_bench` 实测由顶层的约 45% 提到约 84%）；
    `Behaviors` 门面中转发 @Frozen 工厂的方法补上 @Frozen，此前经门面调用会让泛型在 core 实例化；
  - `@actor` 把 `onStarted(context)` 当成"处理 ActorContext 类型消息"的方法（配合 `cases` 清单时直接编译失败），
    写在前面的兜底处理方法吞掉其后的具体类型处理方法（T13）。
- **断路器监督半开时的 RestartAfter（R14）**：补专门用例，确认半开状态下基础策略给出的退避重启同样按"再失败即停止"处理。
- **Replicator 读路径长时间占锁、压住写入（E43，N0 复核）**：`get()` 此前在锁内把整个 CRDT 编码一遍、锁外再解码出副本，
  `setElements` / `counterValue` / `registerValue` 每次读都如此；摘要、合并、推送编码也全部在锁内。4 个读线程就能让
  Replicator Actor 抢不到锁，并行跑集群套件时 `testConcurrentReadsDuringLocalUpdates` 50 轮中 22 轮超时。
  现在锁只保护"键 → 值引用"：`get()` 直接返回不可变值（O(1)，对标 Akka `GetSuccess`），modify、类型检查、merge、
  编码、摘要都在锁外；摘要缓存只在 Replicator Actor 线程访问、不再加锁；modify 返回其他类型时直接拒绝（此前为此做一次完整 merge）。
  当年把这些挪进锁内是为了"消除 SIGSEGV"，E43 复核否定了这个归因：值深度不可变、经锁发布，只读共享没有数据竞争，
  旧写法的独立压测与旧提交的集群套件在本机都复现不出崩溃（见计划 §3.7 第 13 批）。
- **集群测试在并行执行下的时序问题（E43）**：remember-entities 用例在区域登记实体后立即检查启动记录（`preStart`
  在实体线程上稍后执行），改为等待；"原端口重启"的用例改用临时端口段（32768–60999）之外的端口——宕机释放的临时端口
  会被并行的其他测试进程拿去当出站连接的源端口，重启时 "Address already in use"。
- **分片双活与可用性（E13′）**：
  - 成员变化时新拥有者按新视图直接创建实体，旧拥有者要等下一次刷新视图、再处理完实体邮箱里的消息才停止，
    二者重叠（B4）：成员持续变化的压测中 36–70 个实体同时有两个实例，现在为 0；
  - 任一节点不可达时视图不收敛，全集群分片的消息都被缓冲（N6）：现在只缓冲拥有者不可达的分片；
  - 转发过一次的消息在新视图下反复排队、且使其后每条消息都多等一个 50 ms 周期；节点离开集群后缓冲的消息永不处理（N8）。
- **集群工具（E12′）**：
  - 远程回复入口永远绑定第一次登记的引用：同一路径上重建的 Actor（例如分片实体钝化后重建）收不到回复，回复投给已死的旧实例（N7）；
    入口永不注销，1000 个短命 Actor 各问答一次就留下 1000 个入口（v1 B6）——现在按实例登记、终止后每秒清扫注销，
    10 万次远程回复后入口数回到 0；
  - 分片与单例丢弃 sender：实体 / 单例里 `context.reply` 无处可回（N16）——现在透传，跨节点回复直接回到发起方；
  - `Grains.call` 阻塞工作线程，A→B→A 的调用链必然超时；correlationId 进程内自增，重启后旧应答会串到新调用（N13）；
  - pubsub "最后一个订阅者离开"的删除在另一个线程异步执行，可能晚于紧随其后的订阅，把仍有订阅者的节点移出主题（N12）——
    现在由主题 Actor 串行发出增删；
  - 任意来源地址发来的心跳都会建一个故障检测器、永不删除；Removed 成员永久留在 gossip 里（N14）。
- **单例移交重叠（N16）**：最老成员离开时新实例立即启动、旧实例要到下一轮才停止，停止较慢的实例与新实例同时存活
  （实测 2 个），现在由 HandOver 握手保证任何时刻最多一个。
- **日志 kill -9 验证的损坏步骤（`tools/journal_crash_demo.sh`）**：写入进程在创建日志目录之前被 kill -9（负载高时启动慢）时，
  `journal_crash corrupt` 读不存在的目录抛 FSException，而脚本不检查它的退出码，这一轮在没注入故障的情况下照样判通过。
  现在目录不存在时 corrupt 按"无日志可损坏"正常返回，其他异常退出判红。
- **多进程宕机验证的"重复启动"误判（`cluster_crash_node`）**：driver 看到全部成员 Up 就开始发消息，此时其他节点的视图可能还没包含
  最后一个节点，分片先在存活节点上启动、视图一致后再移交过去（正常的单活移交）。旧判定按"第一次启动的节点"认定实体位置，
  kill 后这些实体在存活节点上重启被误算成"重复启动"（负载下约 1/12：宕机进程上算出 6 个 + 误判 4 个 = 实际的 10 个）。
  现在等启动事件静止 2 s 后记 kill 前基线，按最后所在节点认定，只统计 kill 之后新增的启动，并打印出问题实体的启动历史；
  CI 过滤也会显示任何判定为 false 的行。
- **多进程集群示例的"集群已形成"判定**：`cluster_node` / `cluster_crash_node` 改为订阅 MemberUp，
  曾看到预期数量的成员都 Up 即为形成（此前要求轮询那一刻本地视图恰好收敛，分片不再等收敛后 driver 可能先完成并离开）。
- **快照存储的"最新快照"按序号（E44）**：`MemorySnapshotStore` / `FileSnapshotStore` 此前按保存顺序取最后一个，
  保留份数也按保存顺序截取。快照异步保存时，重启前提交的旧快照可能比新实例的快照晚落盘，被当成"最新"；
  它之后的事件若已被保留策略删除，恢复会悄悄少掉一段事件。现在按序号有序保存（同一序号覆盖），最新 = 序号最大，
  保留的是序号最大的若干份。

### ✨ 新增（docs/plan/20261106.md 阶段 1，2026-11-06）
- **经典 `context.watchWith(ref, message)`（E32）**：目标停止时收到 message 而不是 `Terminated`；
  映射保存在 Cell 上，重启后仍然有效，`unwatch` 时一并取消。类型化 `ctx.watchWith` 改为基于它实现。
- **CoordinatedShutdown API（E34）**：
  - 阶段常量 `PhaseXxx`；
  - `addPhase(name, timeout)`；
  - `registerTask(phase, name, task)`；
  - `runAsync(): Future<Unit>`；
  - `phaseNames()`、`isRunning()`。
- **统一指数退避（E6′）**：`BackoffPolicy`（最小/最大退避、抖动 `randomFactor`、`maxRestarts`、`resetBackoffAfter`）
  为经典与类型化共用的纯函数算法，延迟 = min(max, min × 2^n) × (1 + [0, randomFactor))，与 Akka 相同；
  稳定运行 `resetBackoffAfter`（缺省 = minBackoff）后计数清零。
  - 经典：`BackoffSupervisionStrategy(BackoffPolicy(...)[, decider])`；默认决策 Kill / DeathPact 停止、其余（含启动失败）退避重启。
    旧的三参数构造保持原语义（无抖动、不自动复位）。
  - 类型化：`TypedSupervisorStrategy.restartWithBackoff(minBackoff, maxBackoff, randomFactor)`，
    可接 `.withMaxRestarts(n)` / `.withLimit(n, _)` / `.withResetBackoffAfter(d)`；退避期间 Actor 挂起，期间到达的消息由新实例处理。
- **基础件 API（E45）**：
  - 断路器：`BasicCircuitBreaker(config, clock)`（注入单调毫秒时钟，测试可确定性地驱动开启 → 半开）、
    `succeed()` / `fail()`（对标 Akka，手动记录结果）、`CircuitBreakerTimeoutException`；
  - 指标：`MetricRegistry.counter/gauge/timer(name)`（取或建）、`unregister(name)`、`DuplicateMetricException`；
  - 追踪：`generateTraceId()` / `generateSpanId()` 公开，`Tracer(serviceName, maxFinishedSpans)`、`getDroppedSpanCount()`；
  - 告警：`AlertManager(maxHistory)`、`getHistorySize()`；
  - `ConcurrentHashMap.putIfAbsent`；`PersistentEvent` 全字段构造；
  - integration `Histogram.getCumulativeCount(i)` / `toPrometheus(name)`；
  - `CActorSystem.metricsText(perActor, maxActorSeries)` / `CActorRuntime.prometheusMetrics(perActor, maxActorSeries)`。
- **路由器 Actor（E37，对标 Akka `Routers.pool` / `Routers.group`）**：
  - 经典：`Routers.pool(n, props[, strategy])` / `Routers.group(refs[, strategy])` 返回 `Props<Actor>`，
    路由器本身是 Actor（`RouterActor`）：转发保留发送方；watch 全部目标，目标终止后摘除、流量分给存活目标，
    池目标全部终止后路由器停止；管理消息 `Broadcast`、`GetRoutees` → `Routees`、`AddRoutee`、`RemoveRoutee`、`AdjustPoolSize`；
    选目标复用全部 `RoutingStrategy`（轮询、随机、最小邮箱、一致性哈希、加权轮询、广播）。
  - 类型化：`TypedRouters.pool<T>(n, behavior[, routing])` / `TypedRouters.group<T>(serviceKey[, routing])`，
    路由器就是一个 `TypedBehavior<T>`；`TypedRouting<T>()` 不可变配置：`.roundRobin()` / `.random()` /
    `.consistentHashing(keyOf)` / `.withBroadcastPredicate(p)` / `.withRouteeProps(props)`。
    组路由器跟随 Receptionist 列表（实例停止后自动注销即刻生效），停止时退订。
  - `forwardTo(routee, message, sender)`：保留发送方转发的工具函数。
- **Receptionist 退订（E37，T12）**：`unsubscribe(subscription): Bool`、`subscriberCount(key)`。
- **`ActorFailure.lastRestartAt: Option<MonoTime>`**：该 Actor 最近一次实际完成重启的时刻（运行时填写；命名参数，
  已有的三参数构造不变）；`BackoffPolicy.next(..., restartedAt!)` 据此计算稳定运行时长。
- **经典 ask 唯一入口 `AskInbox.ask(target, message)`（E38，对标 Akka `Patterns.ask`）**：以新收件箱为发送方投递，
  返回收件箱，之后 `get(timeout)` / `tryGet(timeout)`；目标不需要任何配合。
- **未处理消息（E36，T11，对标 Akka `UnhandledMessage`）**：`cactor_core.actor.UnhandledMessage(message, recipient)`
  交给死信订阅者；`CActorSystem.unhandledCount()` / `CActorRuntime.unhandledCount()`；
  `cactor_foundation.logging.LogRateLimiter(limit, window)`（无锁限速日志闸门）。
- **`TypedPromise` 组合子（E36）**：`flatMap` / `recover` / `recoverWith` / `tryComplete`；
  `TypedPromises.successful` / `failed` / `sequence`（按输入顺序给结果，任一失败即失败）。全部非阻塞。
- **`TypedSpawn.hooksFor<U>()`（E36，T16）**：@Frozen，在调用方包里构造类型化 Actor 的热路径 hooks；
  `TypedActorContext.actorOfWithHooks`。示例 `typed_spawn_bench` 对比各创建路径的吞吐。
- **`@Message[name = "..."]`（E36，T13）**：指定消息的线上名称；`MessageCodecRegistry.register(name, codec, owner)`。
- **`CActorSystem.gracefulStop(ref, timeout): Bool`（E33，对标 Akka `Patterns.gracefulStop`）**：发 PoisonPill，
  处理完已入队的消息后停止并等待终止；返回 true 时名字已释放。等待由终止方唤醒（只在有等待者时加锁），只能在 Actor 之外调用。
- **《Akka 语义对照表》**：`docs/zh/akka-semantics.md`，逐条列出与 Akka 一致 / 不同之处及验证用例。
- **失败用例先行（E31）**：N2（可靠投递的接收端重启后不再投递）、P3（persist 处理器异常回退已落库序号）
  先以 `@Skip` 入库；随 E41 / E8′ 修复后转正，见 `unit/remote_semantics/system_message_reliability_test.cj`、
  `unit/persistence/persist_failure_semantics_test.cj`。
- **远程鉴权与隔离 API（E7′）**：
  - `RemotingSettings.withSecureCookie(cookie)` / 八参构造的 `secureCookie`：握手带 HMAC-SHA256、双向校验，
    时间戳窗口 ±5 分钟，ACK 回显 HELLO 的 mac 防止重放旧应答；不知道密钥的节点连不上，伪造的 DEATH / JOIN / HB 进不来。
  - `SimpleRemoteManager.quarantine(address, reason)` / `isQuarantined(address)` / `rejectedHandshakes()` / `quarantinedCount()`。
  - 传输层：`TransportHandshake` 接口；`NetworkTransport.setHandshake` / `closeInboundFrom` / `resetOutbound`（默认实现为空）；
    `TcpTransport.getHandshakeFailures()` / `getRejectedHandshakes()`。握手帧限 4 KB，接受方等第一帧限时 5 s。
  - `cactor_foundation.hashing`：纯仓颉的 `sha256`、`hmacSha256`、`constantTimeEquals`、`toHex` / `fromHex`
    （对照 FIPS 180-4 与 RFC 4231 标准向量，不依赖 OpenSSL）。
- **持久化日志 v2 API（E8′）**：`EventStore.append(persistenceId, events, expectedSeq): Int64`、
  `replay(persistenceId, fromSeq, toSeq): Array<EventEnvelope>`、`highestSequenceNr(persistenceId)`、`deleteTo(persistenceId, toSeq)`
  （接口默认实现基于旧方法，只实现旧接口的自定义存储照常编译）；`JournalConflictException`（`PersistenceException` 子类）；
  `Event.getTags()`、`BasicEvent.withTags(tags)` / 六参构造，`ReadJournal.currentEventsByTag(tag)`；
  `EventSourcedActor.persist(type, data, tags)` / `persistAll(events[, tags])`（一批一次 fsync）/ `deleteEventsTo(seq)`；
  `FileEventStore(path, fsync:, maxOpenFiles:)`（常驻追加句柄，超过上限按 LRU 关闭）、`openFileCount()`；
  `persistenceFsyncCount()`；`cactor_foundation.hashing.crc32` / `crc32Update`（与 zlib 一致）。
  组提交：并发写同一日志的写者合并 fsync（本机 8 线程 1000 次追加 164–215 次 fsync，吞吐约为逐条的 5 倍）。
  新增 `cactor_examples.journal_crash` 与 `tools/journal_crash_demo.sh`（随机 kill -9 + 伪造残缺尾部，已接入 `tools/ci.sh`）。
- **弃用（E8′ / B9）**：持久化的 Enhanced 族（`EnhancedEventStore`、`AbstractPersistentActor`、`PersistentActor`、`PersistentEvent` 等）、
  CQRS（`CQRSBus` 等）、Plugin / Manager（`PersistencePlugin`、`PersistentActorManager` 等）标记 `@Deprecated`，2.0 删除；
  事件溯源只保留 `EventSourcedActor` + `EventStore` 一套。
- **集群节点实例 API（E7′，N4 / N5）**：`Member.uid`、`Member.uniqueAddress()`、四参构造 `Member(address, status, upNumber, uid)`
  （三参构造保留，uid 为 0）、`uniqueAddressOf(address, uid)`；`Cluster.selfUid` / `selfUniqueAddress()`；
  `SimpleRemoteManager.selfUid()`；`ClusterSettings.seedNodeTimeout`（六参构造、`withSeedNodeTimeout(d)`、配置键
  `cluster.seed-node-timeout`）。新实例从入站握手出现时，远程层重建发往该地址的出站关联，不等旧连接的重连退避（最长 5 s）。
  多进程宕机验证（`tools/cluster_crash_demo.sh`）增加"原端口重启后 10 s 内以新实例重新加入、实体 ID 记录不丢"。
- **ManagementServer 加固（E7′，T14）**：`ManagementServer(system, host, port)` 指定绑定地址；
  `withMaxConnections(n)`（默认 64，超出直接回 503）；`withRequestTimeout(d)`（默认 10 s，从接受连接起的总时限，
  慢速客户端到时被断开；此前只有单次读超时 5 s）；`rejectedConnections()`。
- **传输优先通道（E40）**：`NetworkTransport.sendPriority(address, data)`（接口默认实现等同 `send`，自定义传输不受影响）；
  TCP 实现中控制帧先于已积压的普通帧写出（普通数据按帧边界分段写，段间插入控制帧）。
  `SimpleRemoteSender.sendControl` / `SimpleRemoteManager.sendControl`：远程层 `$sys`（PING / PONG / ACK / WATCH / DEATH）
  与集群成员协议（JOIN / GOSSIP / HB / HBR）改走优先通道。`TcpTransport.getDroppedFrames()` / `getConnectFailures()` /
  `getAssociationCount()`。
- **远程可靠投递参数与统计（E41）**：`RemotingSettings` 新增七参构造（`maxUnacked`、`giveUpAfter`）与
  `withLimits(maxUnacked, giveUpAfter)`；`SimpleRemoteManager.reliableDeliveryStats(): ReliableDeliveryStats`
  （重发、去重、未确认、溢出丢弃、放弃、对端重启次数）。原有五参构造与 `reliableStats()` 不变。
- **持久化（E8′ 部分）**：`EventHandlerException(message, persistedSequenceNr, cause)`；`PersistenceException` 改为 open；
  `AbstractPersistentActor.receiveCommand` / `onRecoveryCompleted` / `preStart` 改为 open
  （此前 `receiveCommand` 不能重写，子类处理不了自己的命令）。
- **默认调度器饥饿检测（E39）**：工作线程卡在同一次 Actor 运行里超过 `CACTOR_STARVATION_WARN_MS`（默认 1000，0 关闭）
  时告警一次并点名该 Actor；`cactor_runtime.DispatcherDiagnostics.starvationWarnings()` / `lastStarvationPath()`，
  Prometheus 指标 `cactor_dispatcher_starvation_warnings_total`。
- **并行与 GC 压测工具（E43）**：`tools/cluster_parallel_soak.sh [轮数] [进程数]` 以 `--parallel` 连跑集群测试套件，
  崩溃（运行时打印 "unhandled SIGSEGV" / 框架报 "Crashed with exit code"）与断言失败分开统计；
  `cactor_examples.ddata_stress` + `tools/ddata_stress.sh [轮数] [秒数]`：3 节点回环 TCP 上并发读写 Replicator，
  轮流切换四种 GC 配置（默认、`cjGCInterval=1ms cjGCThreshold=4MB`、128 MB 小堆、4 KB region），停止后校验收敛。
  `tools/ci.sh` 新增"集群套件 8 进程并行 2 轮"与"ddata 压测 4 轮"两步（此前集群套件只串行跑）。
- **类型化持久化（E44，对标 Akka Typed `EventSourcedBehavior` / `DurableStateBehavior`）**，新包 `cactor_persistence.typed`：
  - `EventSourcedBehavior<C, E, S>(persistenceId, emptyState:, commandHandler:, eventHandler:, eventAdapter:, journal:)`，
    链式 `withSnapshots(store, codec)`、`withRetention(RetentionCriteria)`、`snapshotWhen((S, E, seqNr) -> Bool)`、
    `receiveSignal((S, TypedSignal) -> Unit)`、`withStash(capacity, overflow:)`，`behavior()` 得到可 `actorOf` 的行为；
  - `DurableStateBehavior<C, S>(persistenceId, emptyState:, commandHandler:, codec:, store:)`：整体保存状态，删除不回退修订号；
  - 命令处理器 `(state, cmd, effect) -> Effect`：`effect.persist / persistAll / none / unhandled / stop / stash / unstashAll / reply`
    （持久状态为 `persist(newState) / delete()`），副作用 `thenRun / thenReply / thenStop / thenUnstashAll`
    在事件**落盘并应用之后**执行、拿到新状态；
  - 恢复与写入期间到达的命令自动暂存，按到达顺序逐条处理；暂存满时 `StashOverflowStrategy.DropCommand`
    （默认，被丢弃的命令作为未处理消息交给死信订阅者）或 `FailActor`；
  - 日志读写在独立的持久化 IO 线程池上执行（`PersistenceIo`，`CACTOR_PERSISTENCE_IO_THREADS`，默认 8），
    结果以信号送回 Actor；监督重启后旧实例在途 IO 的完成信号按实例 token 丢弃；`PersistenceIo.awaitIdle(timeout)`
    等在途 IO 全部完成（系统关闭不等它们）；
  - `PersistenceId.of(entityTypeHint, entityId)`（`"类型|标识"`）/ `ofUniqueId`；`EventAdapter<E>`（manifest、
    `fromJournal` 返回 0..n 个事件以支持 schema 演进、tags）与 `FunctionEventAdapter`；`StateCodec<S>` 与 `FunctionStateCodec`；
  - `RetentionCriteria.snapshotEvery(n, keep).withDeleteEventsOnSnapshot()`：快照后删除序号 ≤ 当前 − keep × n 的快照与事件；
  - 信号 `RecoveryCompleted` / `RecoveryFailed` / `SnapshotCompleted` / `SnapshotFailed` / `DeleteSnapshotsCompleted` /
    `DeleteEventsCompleted` / `DeleteFailed`；恢复失败（日志损坏、事件处理器抛异常、序号缺口）与写入失败让 Actor 失败
    （默认停止，`supervise(...).onFailure(restartWithBackoff)` 可重启并重新恢复）；
  - 示例 `cactor_examples.typed_persistence_demo`（购物车 + 用户资料，三个系统先后恢复并自检），已接入 `tools/ci.sh`。
- **分片移交诊断（E13′）**：`ClusterSharding.shardStats()` = (本节点托管的分片数, 正在获取的分片数, 已完成的获取次数)；
  移交控制消息 `ShardControl`（BEGIN / STOPPED，带协议版本 `SHARDING_PROTOCOL_VERSION`）；
  `SHARDING_HASH_VERSION` 常量。
- **加入兼容检查（E42）**：`ClusterSettings.withJoinCompatibility(key, value)`、`Cluster.joinRejection()`、
  `joinIncompatibility(mine, theirs)`（逐键给出不兼容原因）。
- **集群工具补齐（E12′）**：
  - 分片：`ClusterSharding.tell(entityId, message, sender)`、`ask(entityId, message, timeout): TypedPromise<Message>`；
  - 单例：`ClusterSingleton.tell(message, sender)`、`ask(message, timeout)`；
  - Grains：`Grains.callAsync(id, request, timeout): TypedPromise<Message>`；`Grain.onCallAsync`（默认调用 `onCall`，
    结果未完成期间排队后续调用）、`Grain.reentrant()`（对标 Orleans `[Reentrant]`）；`Grain.onCall` 改为有默认实现（二选一实现）；
  - pubsub：`DistributedPubSub.subscribeAsync(topic, ref): TypedPromise<Unit>`（对标 Akka SubscribeAck）；
    `Replicator.afterPendingUpdates(callback)`（更新屏障）；
  - 成员：`Cluster.membershipStats()` = (gossip 成员记录数, 墓碑数, 故障检测器数)；
    `ClusterSettings.pruneTombstonesAfter` / `withPruneTombstonesAfter`；`CLUSTER_GOSSIP_VERSION`；
  - 远程：`SimpleRemoteManager.quarantine(address, uid, reason)`（只隔离指定实例）、`replyEntryCount()`；
    `SimpleRemoteReceiver.handlerCount()`；`REPLY_ENTRY_IDLE_TTL_MS`（AskInbox 这类无生命周期引用的入口空闲回收时长）；
    `PromiseActorRef.isTerminated()` = Promise 已完成。
- **分布式补齐（计划 §3.10）**：
  - 集群版 Receptionist（E63 的分布式部分，对标 Akka Cluster Receptionist）：`ClusterReceptionist(cluster, system, remote, replicator)`
    的 `register` / `deregister` / `find`（可达成员上的实例）/ `findAll` / `subscribe` / `localRegistrationCount`；
    实例停止自动注销，节点被 down / 移除后登记被删除；
  - Replicator 一致性级别（E64 的一部分，对标 Akka）：`WriteConsistency`（`WriteLocal` / `WriteMajority(t)` / `WriteAll(t)`）、
    `ReadConsistency`（`ReadLocal` / `ReadMajority(t)` / `ReadAll(t)`）、`updateWith` / `getWith` / `addToSetWith` /
    `incrementCounterWith`（返回 `TypedPromise`），`ReplicatorConsistencyException`；
  - 单例：`ClusterSingleton.handOverCount()`、`SINGLETON_HANDOVER_GRACE_MS`。
  - **Replicator delta 传播（E64，对标 Akka delta-CRDT）**：本地更新只推送变化部分——计数器只带变化的分量，
    ORSet 只带新增的点与删除的点（`ORSetDelta`），不能表示为增量时（键第一次出现、更新里混入了别的节点的点）推送整值。
    增量按（来源, 键）编序号，接收方只应用恰好是下一个序号的增量；断档时不应用（否则版本向量会越过没收到的点），
    向来源请求整值补齐，请求或回应丢失时每秒重试（最多 5 次，之后交给反熵）。300 个元素的集合上再加 50 个元素，
    推送字节数约为整值推送的 1/134。`Replicator.setDeltaPropagation(enabled)`（默认开启，关闭后每次推送整值）、
    `deltaStats()`（`ReplicatorDeltaStats`：增量 / 整值推送数、已应用增量数、断档数）；`GCounter` / `GSet` / `ORSet.deltaFrom`、
    `ORSet.applyDelta`。
  - **移除节点的 pruning（E64，对标 Akka RemovedNodePruning）**：节点被移除后，它在计数器里的分量并入执行者的分量、
    ORSet 里它的点改记到执行者，值不变、按节点区分的状态不再随重启次数增长。两阶段：Leader（视图收敛时）在节点移除满
    `maxDissemination` 后为含它数据的键发起标记，每个成员登记并以整值推送（手里该节点的数据随之回到发起者），发起者看到全部成员
    都已登记才执行；已执行标记保留 `markerTimeToLive`（默认 6 h），期间所有合并先剥掉该节点的数据，陈旧副本重新连上不会重复计数，
    被移除节点迟到的增量被丢弃。发起者失去 Leader 身份仍会执行，离开集群时由现任 Leader 接管；Leader 还会扫描数据中不在集群的节点
    （同址重启后新实例成为 Leader 时收不到旧实例的移除事件）。`ClusterReplicatorBinding` 自动接上：`Replicator.setMembership`、
    `nodeRemoved`、`configurePruning(interval:, maxDissemination:, markerTimeToLive:)`、`pruningStats()`；
    `ReplicatedValue.modifiedByNodes()` / `needsPruning(node)` / `nodeComponents()`，CRDT 的 `prune` / `cleanup` / `modifiedByNodes`。
    与 Akka 相同，pruning 视同执行者重新添加元素：与它并发的删除按 add-wins 处理。
  - `ClusterReplicatorBinding`：成员被移除或 down 时，若同一地址上已有新实例（同址重启），不再停止向该地址推送。
  - **远程传输 TLS 1.3 / mTLS（E65，对标 Akka Artery TLS）**：`RemoteTls.fromPem(证书链, PKCS#8 私钥, 信任的 CA)` /
    `fromPemFiles`，`RemotingSettings.withTls(tls)` / `withTransportSecurity(s)`（start 之前配置）。默认双向认证、只用 TLS 1.3、
    对端证书只按配置的 CA 校验（不信任系统根证书）、不校验主机名（同 Akka 默认）；构造时校验本节点证书由信任的 CA 签发且在有效期内。
    TCP 连接建立后先完成 TLS 握手，远程层握手（身份、共享密钥）与全部消息都在加密流上；明文连接、他 CA 证书、不带证书的客户端都被断开，
    TLS 节点也不会向明文节点降级。集群的 gossip、心跳、分片、ddata、Receptionist 经同一个远程层，配置一次即全部加密。
    `RemoteTls.withoutMutualAuthentication()`、`rejectedPeerCertificates()`；`SimpleRemoteManager.securedConnections()` /
    `rejectedConnections()`。基础层新增 `TransportSecurity` 接口（不依赖 stdx）与 `NetworkTransport.setSecurity`
    （默认抛 `UnsupportedException`：UDP 等不支持加密的传输不会悄悄以明文运行）、`TcpTransport.getSecuredConnections()`。
    测试证书与生成脚本：`tools/gen_test_tls_certs.sh`、`tools/tls-test-certs/`（仅供测试）；`tools/cluster_demo.sh --tls`
    以 3 个进程验证整个集群走 TLS。
- **`CActorSystem.publishDeadLetter(message, recipient)`**：自己做路由的组件把投递不出去的消息交给死信
  （计入 `deadLetterCount`、通知 `subscribeDeadLetters` 的订阅者，对标 Akka `system.deadLetters`）。
- **`TypedActorContext.pipeSignalToSelf(future, map)`（E44）**：Future 完成后结果以 `TypedSignal` 送回本 Actor、
  在 `onSignal` 中处理（不占命令协议 `T`，经过 setup / supervise 包装同样送达）；接口默认实现抛 `UnsupportedContextException`。
- **存储接口（E44）**：`SnapshotStore.deleteSnapshotsTo(id, maxSequenceNr)`（内存与文件实现；接口默认抛 `UnsupportedException`）；
  `DurableStateStore.revisionOf(id)`。

### ✨ 新增（此前已实现但未记录，2026-11-06 补记 E5）
- **集群 gossip 成员协议**：Joining → Up → Leaving → Exiting → Removed 状态机、种子节点引导、
  成员事件订阅、Phi Accrual 故障检测接入、gossip 收敛与可达性表传播（`cluster/membership/membership.cj`）
- **脑裂处理（SBR）**：keep-majority 策略，不可达视图稳定后按多数派决定自我下行（`cluster/membership/sbr.cj`）
- **Cluster Singleton**：主节点上唯一实例、主切换后迁移（`cluster/membership/singleton.cj`）
- **分布式发布订阅（DistributedPubSub）**：按 topic 的集群范围发布，经 gossip 成员视图路由（`cluster/membership/pubsub.cj`）
- **远程 ask / 远程 watch**：`RemoteRemoting.ask`（超时 Promise）、WATCH/DEATH 控制消息（连接断开投递 Terminated）、
  序号 + ACK 基础（`remote/remoting.cj`）
- **行为拦截器（Typed Behaviors）**：`TypedBehaviors.intercept` / `monitor` / `logMessages`（`core/typed/typed_interceptor.cj`）
- **序列化注册与 schema 演进（基础版）**：远程 codec 注册表 + 版本号，旧版本数据可读、更新版本明确拒绝（`schema_evolution_test.cj`）

### ⚡ 性能（与 protoactor-go 对比，详见 docs/plan/20261031-protoactor-performance.md）

> 以下是 [2026-10-31 历史报告](docs/plan/20261031-protoactor-performance.md)的观察：同容器、cgroup 4 核限额、交替运行三轮；报告区分最好值、中位数与进程 CPU 时间，并说明部分后续修改尚未计入。33.6M / 36.2M 等数字只适用于对应场景，不能外推为当前版本或所有消息模式的吞吐。

- **多线程吞吐不再随线程数下降**：`invoke` 不再对每条消息做 `message is AutoReceivedMessage`，改为直接匹配 5 种控制消息类。
  仓颉判断"别的包里的类是否实现某接口"在多线程并发时互相阻塞（4 线程每次约 1µs），用户消息总是命中这条慢路径。
  多对 ping-pong（场景 6）从 1.2M 升到 33.6M msg/s，同一时间窗口内约为 protoactor-go（36.2M）的 93%，进程 CPU 基本相同
- **类型化路径**：core 的泛型代码以用户类型实例化后，`is C<T>` 和分配 `C<T>` 都要在运行时查找类型信息，多线程时互相阻塞
  - `TypedBehaviors.isSame / isUnhandled / isStopped / isDeferred`、`start()` 与适配器改为非泛型基类 + 种类标记，
    监督行为与信号 / 转换 / 过滤 / 拦截行为也带标记，普通行为只做一次非泛型类检查
  - `TypedBehaviors.receive / receiveMessage / setup / same / unhandled / stopped / ignore / empty / receivePartial` 标为 `@Frozen`：
    `-O2` 下在调用方的包里实例化，`same()` 4 线程时每次从约 0.4µs 降到约 20ns。这些函数的实现会编译进调用方，修改后依赖方需重新编译
- **运行时不再启动从不使用的 `DispatcherPool`**：它原来随每个运行时启动 4 个旧调度器，线程以 10–100µs 间隔空转轮询，
  空闲时占约 11% CPU 并干扰 Actor 工作线程；去掉后空闲 CPU 约为原来的 1/8
- **邮箱改为双缓冲数组**（与 protoactor-go 的 goring 同类）：稳态零分配，单线程每条 CPU 约 214ns → 161ns，GC 次数降为 0
- **调度**：runNext（被本线程唤醒的 Actor 紧接着在本线程运行，往来频繁的 Actor 留在同一线程）、
  空闲线程举手 + 忙线程捐赠 Cell 的负载均衡；默认工作线程数不超过 cgroup CPU 限额（与 Go 1.25 的 GOMAXPROCS 规则一致）；
  查当前工作线程用 `Thread.currentThread.id`，不用 `ThreadLocal`（仓颉 `ThreadLocal.get` 多线程并发时每次阻塞数微秒）；
  runNext 槽位可被空闲线程窃取（与 Go 同理）：某个线程卡在同一次运行里超过 100µs 时（例如 receive 里同步等待刚唤醒的 Actor），
  空闲线程取走它 runNext 里的 Cell，两者不会互相等待
- **基准**：`real_benchmark` 新增场景 6（多对 ping-pong，protoactor 官方写法）与场景 7（类型化多对互发），
  输出测量窗口内每条消息的 CPU 时间；新增对照程序 `bench/protoactor`


### ✨ 新增（Actor DSL：docs/plan/dsl20261030.md P1–P5 全部落地）
- **`ctx.same()` 零分配路径（P2-20）**：`TypedActorContext.same()` 返回每 Actor 缓存的
  同一 `SameBehavior<T>` 实例，消除类型化路径每消息的第三次堆分配（泛型类静态单例在仓颉
  不可行，改为每 Actor 惰性缓存）；`TypedBehaviors.same<T>()` 保持不变，现有代码零改动
- **`@actor` 属性宏 + 协议清单**：`@actor[cases = [A, B]]` / `@actor[protocol = E]` 在编译期核对
  漏处理、协议漂移、重复处理器；签名支持 `onXxx(m)` / `onXxx(m, ctx)` / 返回 `Unit` 或 `MessageResult`；
  不带方括号的 `@actor` / `@actorReceive` 旧写法由同名非属性宏承接，继续可用
- **`@Message` 增强**：支持 `Option<T>`、`HashMap<String, T>` 字段；字段级错误用 `diagReport` 定位（带行列）
- **`@StateMachine` 宏**：状态枚举 + 转换表生成状态机骨架；漏状态（穷尽 match）、写错状态名、
  非法转换均在编译期报错；宏生成的转换表可直接对接 xstate `StateMachineBuilder`；
  每个转换生成 `guard<From><To>` / `onTransition<From><To>` 骨架，同名覆写即生效；
  支持命名守卫（`transitions = [(A, B, 守卫方法名)]`，方法缺失编译期报错）与
  子状态机（`submachines = [(状态, 子机类型)]`，进入复合状态自动复位子机）；
  `canTransition` / `transitionTo` 展开为 `(from, to)` 枚举 match（无分配、无字符串比较）
- **经典路径 ask**：`askInbox()` 零侵入临时收件箱（`cactor_core.dsl`）——请求消息自带 `replyTo`，
  目标 Actor 无需解包包装消息；超时抛 `TypedAskTimeoutException`（与 typed ask 共用），`tryGet` 返回 `Option`
- **行为层门面**：`cactor_core.dsl.Behaviors`（receive/setup/supervise/withTimers/withStash/intercept/…）
  与 `messageResultToBehavior` 打通宏路径与 lambda 路径语义（`Restart` / `Escalate` 抛
  `SupervisionRequestedException` 交给监督者，与经典运行时一致）
- **流管道 API**：`source |> via(flow) |> to(sink)`（`cactor_streams.dsl`，`to(sink)` 走 `runWith`，与链式写法语义等价）
- **用户文档**：`docs/dsl.md`（三档写法选型、"一句英语"自检表、经典/DSL 对照）

### 🔧 变更
- **ask 收敛**：`cactor_patterns.ask.TypedAskPattern` / `TypedAsk`（早期模拟实现）标记 `@Deprecated`，
  唯一推荐入口为 `cactor_core.typed.TypedAsk.ask`（进程外）与 `TypedActorContext.ask`（Actor 内）
- **公开面**：`cactor` 门面 re-export `cactor_core.dsl.*`；宏包公开宏收敛为 4 个
  （`@actor`、`@actorReceive`（兼容别名）、`@Message`、`@StateMachine`），无占位宏
- **CI 增强**：`tools/ci.sh` 新增宏负例编译测试（`tools/negative_tests.sh`，11 用例）、
  宏公开面检查（`tools/check_macro_surface.sh`）、DSL 行数对照（`tools/dsl_line_count.sh`，代码行降幅 43%）
- **示例**：`hello_world_demo` 改用 `@actor`；新增 `dsl_showcase`（经典/DSL 三组对照）、
  `dsl_agent`（AI Agent 协议一屏 DSL，G5）、
  `dsl_state_machine`（@Message + @StateMachine + @actor + xstate 对接）、`dsl_pipeline`（流管道）、
  `dsl_bench`（宏 vs 手写零开销基准，最好成绩比值 97.5%）

### ⚡ 性能

> 下列内存、吞吐与微基准数字记录不同优化步骤的历史对比，主要依据 [2026-11-01 性能记录](docs/plan/perf20261101.md)。1017 / 1072 / 1120 / 1170 / 1182 / 1280 B 等值来自不同阶段、Actor 层级与规模；存活堆不等于 RSS，也不是对当前版本的统一内存保证。未给出独立复现信息的局部倍数仅作当时观察，发布指标须重新测量并记录环境。

- **每 Actor 存活堆突破 1 KB：3371 → 1017 B（-70%）**；5M 子 Actor 规模 **1072 B/Actor**（详见 perf20261101.md §7.13）
  - **`SysStarted` 系统消息改为 `schedState` 的 START_BIT**：`run()` 进入消息循环前先启动，
    每条 tell 省掉系统栈上的节点分配
  - **系统消息队列迁入邮箱并按需分配**：`CellMailbox.pushSystem` 在邮箱锁内推送（与 close 互斥），
    首条系统消息才分配队列对象并以 `SYS_BIT` 发布；从未收到系统消息的 Actor 不再占那个原子对象
  - 邮箱 6 个 `Option<Array<T>>` 改共享空数组哨兵（该字段类型占 32 B，普通引用 8 B）
  - receive-timeout 的 4 个字段收进按需分配的 `ReceiveTimeoutState`
- **P1-18（CellMailbox 改 struct 内联）实测否决**：值类型省约 16 B 对象头，但 mut 方法按值复制
  整个邮箱结构，交替 A/B 实测 [1] 13M → 4.3M、[6] 62M → 30M msg/s，吞吐损失过半，已回退为
  class 并留档（perf20261101.md §7.14）
- **每 Actor 存活堆降到 1120–1151 B（基线 3371 B，-66~67%）**：`Cell` 的 4 个 receive-timeout 字段
  （约 32 B）收进按需分配的 `ReceiveTimeoutState`，只有调用过 `setReceiveTimeout` 的 Actor 才分配；
  `invoke` 热路径改为 Option 判空（无超时的 Actor 更便宜）
- **扇出"反缩放"根因修正**：`real_benchmark` 新增 `CACTOR_BENCH_SENDERS` 开关。实测扇出吞吐随发送方
  数量线性增长（单发送方约 4.6M/s），T14 相对 T4 的下降约等于"8 发送方 + 14 worker = 22 线程跑在
  14 核"的超额订阅比例；worker 驱动的多对 ping-pong/类型化在 T14 仍正扩展。因此该指标不构成
  "运行时反缩放"的证据（详见 docs/plan/perf20261101.md §7.10）
- **邮箱自旋锁对象内联**：`CellMailbox` 不再持有独立的 `SpinLock` 对象，改为 `AtomicBool` 字段 +
  手写 lock/unlock（语义不变），每 Actor 再省约 35 B（`bench_gate` 实测 **1170 B**）
- **每 Actor 存活堆继续下降：1280–1387 → 1169–1212 B（基线 3371 B，累计 -64%）**；
  5M 子 Actor 规模 **1182 B/Actor**，创建 824k/s、drain 1871 ms、停止 9086 ms（详见 perf20261101.md §7.5）
  - 邮箱的 6 个 `Option<Array<T>>` 改为普通数组 + 共享只读空数组哨兵：仓颉 `Option<Array<T>>` 字段占 32 B，
    普通数组引用只占 8 B，这一项每 Actor 省约 90 B（`in/dr × msg/sender/senderRef`）
- **每条 tell 少一次跨核原子 RMW**：邮箱 incoming 计数从 `schedState` 的每条 `fetchAdd`
  改为锁内普通自增；锁外"是否还有消息"改读同一状态字的 `INCOMING_BIT`（每批首条置位、换批清除）。
  交替 A/B 均值：单 Actor +19%、扇出 **+42%**、多对 ping-pong +7%、类型化 +4.5%（perf20261101.md §7.5.2）
- **每 Actor 存活堆大幅下降**：子 Actor **3371 → 1280–1387 B（-59~62%）**，顶层 Actor
  **3423 → 1645–1880 B（-49%）**
  - Cell 不再常驻完整路径字符串，只保留 `name`（子 Actor 与父 `childMap` 键共用同一个 String 实例），
    完整路径由 `fullPath()` 按"父链 + 名字"惰性拼出；`hashCode()` 改为创建时混合父链哈希
  - 新增 `Props.creator()` / `ActorFactory.rawCreator()`，Cell 的创建闭包不再经
    "闭包 → Props 副本 → SimpleActorFactory" 这条链（每组省约 130–140 B）
  - 顶层路径恒为 `/user/<名字>`，因此删掉三张冗余注册表：`cellsByPath`（与 `userActors` 同构）、
    `SimpleActorRegistry` 的 `pathToActor`+`nameToPath`（合并为 `nameToActor`）、
    `ActorLifecycleManager` 以完整路径为键（改为名字）
  - `cellsByPath` 由 `ConcurrentHashMap`（每条目约 631 B）改为普通 `HashMap` + 既有锁（每条目约 134 B）
- **顶层创建吞吐 203k → 551k/s**（Akka 2.6.21 同口径 415k/s、protoactor-go 707k/s）
- **`@Message` 编码**：`MessageFieldWriter` 由 `ArrayList<UInt8>` 逐字节追加改为数组缓冲 + 下标写入，
  编码一条 283 字节的消息快约 1.5 倍；线上字节格式不变（`testWireFormatIsStable` 逐字节锁定）
- **`HashMap` 字段解码**：先按每条最小字节数核对剩余数据，再按条目数预分配容量（快 1.4–3 倍）
- **`@StateMachine`**：`canTransition` / `transitionTo` 改为 `(from, to)` 枚举 match，比查数组 + 字符串比较快 1.8–2.0 倍

### 🐛 修复
- **集群分片的 rendezvous 哈希缺陷（真实分配 bug）**：节点地址共享前缀、仅末位不同（如顺序分配的
  `127.0.0.1:4916{8,9,0}`）时，直接比较 FNV-1a 值取最大会让某个节点一个分片都分不到
  （实测 64 分片全部落在 2 个节点）。新增 `mixHash`（MurmurHash3 fmix64）做雪崩混合后再取最大；
  `shardOf` 同样先混合再取模。`stableHash` 作为公开的稳定哈希未改动
  （`testStableHashKnownVectors` 继续通过）。修复后集群包连跑 6 次全通过
- **分片区域成员视图访问器**：新增 `ClusterSharding.ownerCount()` / `Grains.ownerCount()`
  （0 = 视图未收敛），供测试等待"区域已看到全部成员"后再断言分布
- **CI 测试改为串行执行**：集群/分片用例会起真实 TCP 集群，并行跑多个时故障检测器在宿主高负载下
  偶发误判节点不可达，导致与代码无关的分布断言失败（`distinct.size==2`）。`tools/ci.sh` 与
  `tools/stress_matrix.sh` 改用 `cjpm test --parallel 1`，421 例连续通过，墙钟时间几乎不变
- **分片路由在成员视图更新前使用旧视图**：`ShardRegionActor.refreshOwners` 只在 50ms 的
  RegionTick 上刷新成员快照，成员变化后的窗口内会把实体路由到错误节点（测试表现为某节点
  `localEntityCount()==0` / 实体只分布在 2 个节点）。现在 `route` 前按"快照对象引用是否变化"
  即时刷新；新增 `Cluster.stateAndTerminated()` 一次加锁读取快照与终止状态。
  隔离复跑：`testGrainsSpreadAndStayPut` / `testEachEntityLivesOnExactlyOneNode` 均 10/10
- **测试受害者选择**：`testEventSourcedEntityKeepsStateAcrossCrashAndHandoff` 固定宕机 1 号节点，
  第三节点加入后有约 7% 概率该节点已无实体；改为选实体较多的初始节点
- **遗留分片 API 的两处硬编码占位**（`modules/cluster/src/cluster_sharding.cj`）：
  `EntityId.extractShardId` 恒返回 `shard-0`（所有实体挤在一个分片），改为按实体 id 的稳定哈希取模；
  `ShardInfo.getEntityCount()` 恒返回 42，改为返回真实条目数；`ShardId.hashCode()` 恒返回 42，改为稳定哈希
- **新增 CRDT 并发回归测试**：`modules/tests/src/unit/cluster/crdt_concurrency_test.cj`
  覆盖"多线程并发 merge/遍历/编解码不相交 ORSet"与"并发读 Replicator + 持续本地更新"，
  把 P0-4 的崩溃路径固化为常驻用例（连续 5/5 通过）
- **CRDT 合并偶发 SIGSEGV（集群测试约 5/10）**：两处修复——
  ① `ORSet.merge` 里 `entries.get(k) ?? HashSet<S>()` 的空集临时对象在多线程下被并发使用，
  崩溃于 `HashMap.contains`（addr 0x8）；改为显式 `Option` 分支，缺失键按空集合处理、不再分配临时空集
  （修复后集群套件 **20/20 无崩溃**）；
  ② `Replicator.get(key)` 原在锁内取出 `store.values` 的引用后直接返回，调用方（`setElements` /
  `counterValue` / `registerValue`）在锁外遍历 CRDT 内部 HashMap，与 Replicator Actor 线程的
  `merge` / `digestsNow` 并发；现在 `get()` 在锁内 `encode()`、锁外 `decode()` 返回独立副本
  （不在热路径）。全量测试 ERROR 归零
- **分片的记忆实体在 receive 里同步等待 Replicator**：`ShardRegionActor` 启动 / 停止实体时调用阻塞的 `addToSet` / `removeFromSet`，
  Replicator 与它排在同一工作线程时要等 10 秒超时，实体启动失败。改用新增的
  `Replicator.updateAsync / addToSetAsync / removeFromSetAsync`（不等待本地生效，`modify` 的异常记录为警告）；
  `update` 的文档注明不要在 receive 里调用
- **`askInbox().get` 在 Actor 处理方法内阻塞会死锁**：默认亲和调度没有工作窃取，被等的 Actor 与调用方
  分在同一工作线程时互相卡住（`dsl_agent` 示例用 `CACTOR_DISPATCHER_THREADS=1` 可稳定复现）。
  现在亲和工作线程上带超时的 `get` / `tryGet` 立即抛 `IllegalStateException`；`blocking` 调度器不受限制。
  运行时新增 `cactor_core.actor.isAffinityWorkerThread()` 标记
- **`dsl_agent` 示例**改为非阻塞写法：工具结果回到 Agent 邮箱（`replyTo = ctx.self`），按 `requestId` 续上对话；
  自检并发 16 轮对话，单工作线程也能通过
- **消息解码更严格**：`Option` 标记只接受 0 / 1；`HashMap` 字段的条目数先校验再分配，重复键视为数据损坏
- **`within`** 改用单调时钟计时（墙钟调整时不再误报或漏报慢处理）
- **`dsl_bench`** 新增不经调度器的直接分派基准作为 A3 判定依据（端到端 ping-pong 噪声过大，仅作参考）

### 计划中
- HTTP集成和Web服务器
- 序列化优化和多格式支持
- 可视化监控工具
- 云原生支持

## [1.2.1] - 2026-11-10

### 🚀 发布

- 12 个库模块（`cactor_foundation` / `cactor_macros` / `cactor_core` / `cactor_runtime` / `cactor` /
  `cactor_patterns` / `cactor_testkit` / `cactor_streams` / `cactor_remote` / `cactor_persistence` /
  `cactor_cluster` / `cactor_xstate`）发布到[仓颉中心仓](https://pkg.cangjie-lang.cn)，统一 1.2.1。
- 每个模块补齐 `README.md`、`license = ["MIT"]` 与 `repository` 元数据（artifact 根目录必须有 README，否则 bundle 直接失败）。

### 🐛 修复

- **发布产物不再携带 `[replace]` 绝对路径**：cjpm 会把 `cjpm.toml` 打包进 artifact，早先为了让构建用本地源码
  注入的 `[replace] x = { path = "/发布机/绝对/路径" }` 会让其他机器构建直接失败
  （`the path '/...' does not exist which is listed in 'replace' field`）。`tools/publish.sh` 改为
  path 依赖→中心仓版本号、不带 `[replace]`，并加守卫拒绝含 `[replace]`/绝对路径的暂存 manifest。
- `cactor_remote` / `cactor_cluster` 在模块自己的 `cjpm.toml` 里声明 stdx 的 `[target.*.bin-dependencies]`
  `path-option`，脱离 workspace 后不再报 `imports package 'stdx.crypto.common' ... but it is not added as a dependency`。

### 🛠 工具

- `tools/publish.sh`：按依赖顺序发布、等待中心仓索引生成（索引异步、cjpm 会缓存旧索引且不回源）、
  发布前清理本机索引缓存、bundle 失败自动重试；默认 `--skip-lint`（`LINT=1` 严格模式）。

## [1.2.0] - 2026-10-28

### 🎉 全面模块化大版本

本版本将 CActor 从单包工程全面重构为多模块 workspace，并统一项目版本号、仓颉工具链版本和全部文档。

### 💥 破坏性变更

#### 多模块工程重构
- **单包拆分为 workspace**: 原 `src/` 下的单一 `cactor` 包拆分为 `modules/` 下 12 个独立仓颉模块，根 `cjpm.toml` 改为 `[workspace]`
- **模块划分**（依赖自底向上，无环）:
  - `cactor_foundation` - 基础设施：队列、内存、序列化、网络、配置、DI、监控、断路器
  - `cactor_core` - 核心抽象：Actor/ActorRef/ActorSystem/Message/Supervision/Typed
  - `cactor_patterns` - 通用模式：Ask、路由、背压
  - `cactor_runtime` - 运行时：mailbox、dispatcher、guardian、registry、lifecycle
  - `cactor` - 门面模块：统一对外 API（`cactor` / `cactor.api` / `cactor.config`）
  - `cactor_macros` - Actor DSL 宏
  - `cactor_streams` - 流处理 DSL
  - `cactor_distribution` - 分布式：cluster / remote / persistence / streaming（实验性）
  - `cactor_xstate` - XState 风格状态机
  - `cactor_integration` - 日志、指标、配置与集成测试程序
  - `cactor_tests` - 单元测试与集成测试
  - `cactor_examples` - 示例程序
- **包名变更**: 内部包由 `cactor.<层>.*` 改为 `cactor_<模块>.*`，例如 `cactor.core.actor` → `cactor_core.actor`、`cactor.runtime` → `cactor_runtime`
- **断路器迁移**: `cactor.patterns.circuit_breaker` 迁至 `cactor_foundation.circuit_breaker`，由 `cactor_patterns` 重导出
- **产物类型**: 库模块输出由 `dynamic` 改为 `static`

### 🔢 版本统一
- **项目版本号统一为 1.2.0**: 全部 12 个模块的 `version` 统一为 `1.2.0`；文档及源码注释中的项目版本标识一律统一为 1.2.0，版本线承接 1.1.0

### 🔧 工具链
- **仓颉版本升级**: `cjc-version` 由 `0.60.5` 升级到 `1.2.0`，现有 std API 无需迁移

### 📚 文档
- README、CONTRIBUTING、`docs/`（中英文）、示例 README、`xstate.md` 统一到 1.2.0：项目版本、仓颉版本要求（原 0.53.4 / 0.60.5 / 0.7.0）及安装/部署示例
- 文档中的包名同步为模块化后的新包名（`cactor_core.*`、`cactor_runtime.*` 等）
- 架构文档版本历史新增 1.2.0 记录

### 🧹 项目清理
- 移除空的 `source/CangjieCorpus` 子模块引用
- 移除过时的过程文档：`task0.20.md`、`CURRENT_PROGRESS_REPORT.md`、`PROJECT_STATUS.md`、`docs/DOCUMENTATION_COMPLETION_REPORT.md`
- 修正 `docs/DOCUMENTATION_STRUCTURE.md` 中的失效链接

### 📦 迁移指南
- 只依赖门面 API（`import cactor.*`、`cactor.api.*`、`cactor.config.*`）的代码无需修改
- 直接引用内部包的代码需按上述规则替换包名，并在 `cjpm.toml` 中通过 `path` 依赖对应模块

## [1.1.0] - 2025-07-03

> 历史条目说明（文档审计补记）：本节 Ask 的 657,894 req/s、1,520 ns/req、100 万 Actor、流处理 500 万元素/秒及“纳秒级 / 微秒级”等宣传缺少关联的源码提交、机器配置、原始输出和完整测量方法，尚未核实。保留原文作历史记录，不作为真实消息投递、分片规模、当前 Streams 或当前版本的性能保证；能力现状请参照当前 API 与测试。

### 🎉 重大版本更新

这是 CActor 的重大版本更新，完成了所有核心企业级功能的实现。

### ✨ 新增功能

#### Ask模式完善
- **生产级实现**: 完整的请求-响应模式支持
- **高性能**: 657,894 req/s，1,520 ns/req
- **异步支持**: 完整的异步回调和批量处理
- **超时管理**: 完善的超时处理机制
- **类型安全**: 编译时类型检查

#### 集群管理增强
- **集群分片系统**: 完整的分片协调和管理
- **多种路由策略**: 轮询、一致性哈希、随机、广播等
- **负载均衡**: 智能负载均衡和健康检查
- **故障转移**: 自动故障检测和恢复
- **支持规模**: 100万Actor，8个工作线程

#### 持久化系统完善
- **事件溯源**: 完整的事件存储和重放机制
- **CQRS架构**: 命令查询责任分离
- **快照管理**: 内存和文件快照存储
- **多存储后端**: 支持内存和文件存储
- **版本控制**: 事件版本管理和冲突检测

#### 测试框架建设
- **TestKit框架**: 完整的测试工具集
- **同步测试**: TestActorRef和同步消息处理
- **消息验证**: TestProbe和期望检查
- **性能测试**: 吞吐量和延迟测试支持
- **集成测试**: 与CActor系统完美集成

#### CActor Streams流处理系统
- **高性能**: 500万元素/秒吞吐量
- **背压控制**: 完整的流量控制机制
- **丰富操作符**: map、filter、fold、scan、distinct等
- **DSL支持**: 声明式流处理管道
- **高内聚低耦合**: 企业级架构设计

#### XState状态机系统
- **强类型**: 基于仓颉枚举的类型安全状态
- **模式匹配**: 高性能的状态转换引擎
- **CActor集成**: 状态机即Actor，天然并发
- **企业级特性**: 事件溯源、状态快照
- **DSL支持**: 声明式状态机定义

### 🏗️ 架构改进

#### 7层架构完善
- **Foundation Layer**: 并发原语、序列化、网络通信
- **Core Layer**: Actor、消息、系统核心抽象
- **Runtime Layer**: 调度器、邮箱、定时器
- **Patterns Layer**: Ask、路由、断路器
- **Distribution Layer**: 远程、集群、持久化
- **Integration Layer**: 监控、日志、配置
- **API Layer**: 统一对外接口

#### 仓颉0.60.5完全适配
- **语法更新**: 完全适配最新仓颉语法
- **API升级**: HashMap.add()、ArrayList.add()等
- **类型系统**: 充分利用仓颉强类型特性
- **并发原语**: 使用仓颉原生并发支持

### 📊 性能提升

#### 核心性能指标
- **Ask模式**: 657,894 req/s，1,520 ns/req
- **集群分片**: 支持100万Actor
- **流处理**: 500万元素/秒
- **状态机**: 纳秒级状态转换
- **整体延迟**: 保持微秒级延迟

### 🧪 测试覆盖

#### 完整测试体系
- **单元测试**: 所有核心组件100%覆盖
- **集成测试**: 端到端功能验证
- **性能测试**: 全面的性能基准测试
- **示例验证**: 所有示例程序正常运行
- **编译验证**: 无错误，仅Unicode字符警告

### 🔧 开发体验

#### 完善的示例系统
- **Hello World**: 入门示例
- **Ask模式**: 请求-响应示例
- **集群分片**: 分布式示例
- **持久化**: 事件溯源示例
- **流处理**: 流处理管道示例
- **状态机**: XState状态机示例

## [1.0.0] - 2025-06-22

> 历史条目说明（文档审计补记）：本节吞吐、排名、“所有场景最优”及最高 170000+ 倍的延迟差异未附足够的提交、配置、原始输出与同等工作量证据，尚未核实；这些数字不能用于当前框架对比。同步函数调用、提交消息和等待 Actor 实际处理完成是不同测量口径，须先核对，再引用。

### 🎉 首个正式版本

这是 CActor 的首个正式版本，提供了完整的 Actor 系统实现和企业级特性。

### ✨ 新增功能

#### 核心 Actor 系统
- **完整的 Actor 模型**: 实现了标准的 Actor 模型，包括消息传递、状态封装和位置透明性
- **高性能消息传递**: 优化的消息传递机制，支持高吞吐量和低延迟
- **Actor 生命周期管理**: 完整的 Actor 创建、启动、停止和销毁流程
- **监督策略**: 实现了 OneForOne、AllForOne 等监督策略
- **Actor 层次结构**: 支持父子 Actor 关系和监督树

#### 性能优化
- **微秒级延迟**: 所有测试场景下都保持微秒级延迟
- **I/O 密集型优化**: 在 I/O 密集型任务中达到 166,667 msg/s
- **复杂计算优化**: 在中等到重量级计算任务中性能领先
- **内存优化**: 高效的对象池和内存管理
- **批处理支持**: 消息批处理以提高吞吐量

#### 企业级特性
- **Ask 模式**: 完整的请求-响应模式支持
- **路由器**: 多种路由策略（轮询、随机、一致性哈希等）
- **断路器**: 故障隔离和自动恢复机制
- **背压控制**: 流量控制和过载保护
- **配置管理**: 灵活的配置系统

#### 运行时系统
- **多种调度器**: 支持不同的调度策略
- **多种邮箱**: 有界、无界、优先级邮箱
- **定时器支持**: 延迟消息和周期性任务
- **线程池管理**: 高效的线程池实现
- **NUMA 感知**: 针对 NUMA 架构的优化

#### 监控和诊断
- **性能指标**: 详细的性能监控指标
- **健康检查**: Actor 系统健康状态监控
- **调试工具**: 性能分析和诊断工具
- **日志集成**: 结构化日志支持

### 📊 性能基准

基于与主流 Actor 框架的全面对比测试：

| 测试类型 | CActor | ProtoActor-Go | Actix | 排名 |
|----------|--------|---------------|-------|------|
| 轻量级 | 454,545 msg/s | 3,333,333 msg/s | 96,154 msg/s | 🥈 |
| 中等强度 | **54,348 msg/s** | 47,619 msg/s | 47,170 msg/s | 🥇 |
| 重量级 | **35,714 msg/s** | 9,615 msg/s | 9,434 msg/s | 🥇 |
| CPU密集型 | 11,364 msg/s | 17,391 msg/s | 16,393 msg/s | 🥉 |
| I/O密集型 | **166,667 msg/s** | 4,951 msg/s | 4,902 msg/s | 🥇 |
| 内存密集型 | 177 msg/s | 894 msg/s | 971 msg/s | 🥉 |
| 混合负载 | 386 msg/s | 4,975 msg/s | 4,000 msg/s | 🥉 |

#### 延迟性能（所有场景 CActor 最优）
- **轻量级**: 0.0019ms（比其他框架快 1.4-850倍）
- **I/O密集型**: 0.0019ms（比其他框架快 14000-24000倍）
- **内存密集型**: 0.0030ms（比其他框架快 170000+倍）

### 🏗️ 架构设计

实现了完整的 6 层模块化架构：

```
API Layer (API层)           ← 统一对外接口
Integration Layer (集成层)    ← 监控、日志、配置
Distribution Layer (分布式层) ← 远程、集群、持久化
Patterns Layer (模式层)      ← Ask、路由、断路器
Runtime Layer (运行时层)     ← 调度器、邮箱、定时器
Core Layer (核心层)         ← Actor、消息、系统
Foundation Layer (基础设施层) ← 并发、序列化、网络
```

### 📚 文档

- **完整的 API 文档**: 详细的 API 参考文档
- **架构设计文档**: 深入的架构设计说明
- **性能优化指南**: 性能调优最佳实践
- **示例代码**: 丰富的示例和教程
- **基准测试报告**: 详细的性能测试结果

### 🧪 测试覆盖

- **单元测试**: 核心功能的单元测试
- **集成测试**: 系统集成测试
- **性能测试**: 全面的性能基准测试
- **压力测试**: 高负载场景测试
- **示例验证**: 所有示例代码的验证

### 🔧 开发工具

- **构建系统**: 基于 cjpm 的构建配置
- **代码规范**: 统一的代码风格和规范
- **CI/CD**: 持续集成和部署流程
- **性能分析**: 性能分析和优化工具

## [0.9.0] - 2025-06-15

### ✨ 新增功能
- 实现基础 Actor 系统
- 添加消息传递机制
- 实现简单的调度器

### 🐛 修复问题
- 修复内存泄漏问题
- 优化消息处理性能

## [0.8.0] - 2025-06-10

### ✨ 新增功能
- 项目初始化
- 基础架构设计
- 核心接口定义

---

## 版本说明

### 版本号格式
- **主版本号**: 不兼容的 API 修改
- **次版本号**: 向下兼容的功能性新增
- **修订号**: 向下兼容的问题修正

### 变更类型
- **新增功能** (Added): 新功能
- **修改** (Changed): 对现有功能的变更
- **弃用** (Deprecated): 即将移除的功能
- **移除** (Removed): 已移除的功能
- **修复** (Fixed): 任何 bug 修复
- **安全** (Security): 安全相关的修复

---

*发行步骤与兼容性要求见 [发布指南](docs/RELEASE.md)。*
