# CActor 路线图

> 修订：2026-10-28，基于提交 `d652d59` 的全量代码核对。
> 本文是项目唯一的路线图；`docs/zh/roadmap.md`、`docs/en/roadmap.md` 只保留指向本文的链接。
> 设计细节和调研数据见 [docs/plan/20261028.md](docs/plan/20261028.md)（差距分析，R1–R8）、[docs/plan/20261029.md](docs/plan/20261029.md)（HTTP 与 API 重构，P0–P11）、[docs/plan/20261028-performance.md](docs/plan/20261028-performance.md)（性能对比）。本文把这两条计划合并成一条按版本推进的主线，并按代码现状修正了其中已经过时的描述。

---

## 1. 目标与原则

**目标**：成为仓颉上可以用于生产的 Actor 框架。先把单机 Actor 做扎实、把代码收敛到一套实现，再提供 HTTP 服务能力和可靠的分布式能力。

**原则**：

1. **一条主路径**：每种能力只保留一个实现。新增功能之前先删除同类的旧实现。
2. **以测试为准**：只有在主消息路径上生效、并有测试覆盖的能力才算完成；README 中列出的每一项都要能对应到测试。
3. **以实测为准**：性能结论只来自同机、同等 CPU 的实测（测量规则见 20261029 §9.1），不再引用宣传数字。
4. **性能不倒退**：每个版本都跑 `real_benchmark`，点对点场景相差不超过 ±3%。
5. **兼容有节奏**：1.x 期间新旧 API 并存，旧 API 转调新实现并标记废弃；2.0.0 删除旧 API 并冻结公开面。

---

## 2. 现状（2026-11-06 核对，上一轮核对 2026-10-28）

### 2.1 已经可用的能力

| 领域 | 状态 | 依据 |
|---|---|---|
| 运行时 | ✅ 每个 Actor 一个双缓冲数组邮箱（稳态零分配）；亲和调度 + 侵入式就绪队列 + runNext + 空闲线程捐赠；线程数不超过 cgroup 限额；独立系统消息队列；阻塞调度器；吞吐参数可配置；每 Actor 存活堆 1.0–1.2 KB（5M 规模实测 1072–1182 B） | `runtime/guardian/actor_cell.cj`、`docs/plan/perf20261101.md` |
| 经典 API 语义（R1） | ✅ 监督树（父停子、Escalate、OneForAll）、watch / watchWith / Terminated（Terminated 入用户邮箱，先于它发出的消息先到，与 Akka 一致）、协调关闭（Akka 阶段）、become、定时器、Stash、接收超时、PoisonPill / Kill、Props 生效、按路径查找、死信订阅；失败处理与 Akka 对齐（启动失败 / Kill / DeathPact 默认停止、延迟重启挂起、Error 交给监督）；经典与类型化共用的指数退避（E6′）；重启协议与钩子对齐 Akka（preRestart 默认 postStop、postRestart 默认 preStart、preStart 在 Actor 线程执行、等旧子 Actor 全部终止再建新实例、失败待决期间整棵子树暂停，E33）；`system.stop` 立即停止、剩余消息进死信、名字保留到终止，`gracefulStop` 处理完排队消息再停并等待终止（E33）；与 Akka 的逐条对照见 `docs/zh/akka-semantics.md`；路由器 Actor（`Routers.pool` / `group`：保留发送方、摘除已终止目标、管理消息，E37） | `semantics_test.cj` 34 个用例、`failure_semantics/` 13 个、`death_watch/` 7 个、`lifecycle/` 8 个、`patterns/router_actor_test.cj` 6 个用例、`stop_protocol/` 6 个用例 |
| 类型化 API（R2） | ✅ 运行在经典 Cell 上；setup / receive / supervise / withTimers / withStash / ask / messageAdapter / pipeToSelf / Receptionist（单机，可退订，订阅者停止后自动退订）；类型化路由器（`TypedRouters.pool` / `group`，组路由器跟随 Receptionist，E37）；行为拦截器（`intercept` / `monitor` / `logMessages`）；监督按行为结构由内向外匹配、计数跨重启累计（E36）；未处理 / 协议之外的消息以 `UnhandledMessage` 进死信并限速记日志；`TypedPromise` 组合子（flatMap / recover / sequence）；以 `setup` 开头的子 Actor 走用户包快路径 | `typed_semantics_test.cj`、`typed_interceptor.cj`、`typed_routing/typed_router_test.cj` |
| DSL | ✅ `@actor`（协议清单编译期核对，`@actorReceive` 为兼容别名）、`@Message`（编解码 + 注册）、`@StateMachine`（转换表 + 穷尽检查）；`Behaviors` 门面、`askInbox`、流管道 `\|>`；`<<` 运算符、时间字面量；占位宏已删除 | `macros/src/`、`core/src/dsl/`、`docs/dsl.md` |
| 远程 | ✅ 基于 std.net 的 TCP，按目标地址的出站关联：连接与重连在写线程里（指数退避），发送只入队、不阻塞，队列有界、满了丢弃计数，控制消息走优先通道（E40，黑洞节点不拖累健康成员）；写线程合并写、批量解析；至多一次投递；远程 ask（`remoting.cj` ask）、远程 watch（WATCH/UNWATCH/DEATH 走可靠通道，被监视节点重启凭启动标识立即投递 `Terminated`）；可靠投递（按序去重、接收端重启后在新通道重发、未确认上限与超时放弃，E41）；序列化注册与 schema 演进（`remote_semantics` 测试）。连接握手交换身份（地址 + 启动标识），可选共享密钥 HMAC 鉴权；隔离（quarantine）：心跳不可达、超时放弃、对端重启时隔离旧实例，误判不可撤回（E7′）；成员与 CRDT 按"地址#启动标识"区分实例（N4）、种子加入对标 FirstSeedNodeProcess（N5）。TLS 1.3 双向认证（`RemoteTls`，基于 stdx，E65）：明文连接与他 CA 证书被拒绝，集群全部流量经同一远程层加密 | `foundation/network/transport.cj`、`remote/remoting.cj` |
| 持久化 | ✅ 日志 v2（E8′）：记录带序号 / CRC32 / tags，`append(expectedSeq)` 单写者校验、`replay` / `deleteTo`，FFI fsync + 组提交，残缺尾部截断、中间损坏拒绝恢复，v1 日志自动迁移；随机 kill -9 验证已确认事件不丢（`tools/journal_crash_demo.sh`，接入 CI）；快照与持久状态原子写并落盘；`EventSourcedActor` 为经典主路径，Enhanced / CQRS / Plugin 三套已 `@Deprecated`；✅ 类型化 `EventSourcedBehavior` / `DurableStateBehavior`（E44，`cactor_persistence.typed`：Effect 链、恢复与写入期间自动暂存、独立持久化 IO 线程池、RetentionCriteria、EventAdapter schema 演进，购物车示例三次重启恢复自检）；持久状态删除留墓碑、修订号不回退（P14）。🟡 实时 `eventsByTag`；快照与持久状态是每个标识一个文件，fsync 不能跨实体合并 | `persistence/` |
| 集群 | ✅ gossip 成员协议（Phi Accrual 故障检测）、SBR 脑裂处理（keep-majority）、Cluster Singleton、分布式发布订阅（`DistributedPubSub`）、CRDT + Replicator、分片（rendezvous 哈希 + 许可式两阶段移交：征得每个成员确认已停止后才托管，单活不依赖视图收敛，成员不可达时只缓冲它拥有的分片，E13′）；加入时比对兼容键（分片哈希与移交协议版本、用户键），不兼容的节点被拒绝（E42）；成员按"地址#启动标识"区分实例（同址重启以新实例重新加入）、首个种子先加入其他种子、CRDT 节点键含启动标识（E7′）；成员协议经远程握手可选共享密钥鉴权（B2）；Replicator 读取返回不可变快照，merge / 编码 / 摘要在锁外，集群套件恢复并行执行（E43）；分片 / 单例透传 sender 并提供 `ask`，`Grains.callAsync` 与可重入 Grain、pubsub `subscribeAsync` 与主题 Actor 串行写入，远程回复入口按 Actor 生命周期注册与注销，Removed 成员只留带时间的墓碑（24 h 裁剪）、被 down 的成员按 uid 隔离（E12′）；单例 HandOver 握手（N16）、集群版 Receptionist（E63，v2.23 补类型化入口：`ServiceKey<T>` + `typedRemoteActorOf` + `TypedPayloadBoxCodec`，跨节点类型化 ask 已在 v2.26 实现；v2.27 增加可选稳定协议名、登记/接收端错配校验与计数，codec v3 显式 Message 标签要求两端升级）、Replicator 一致性级别 WriteMajority / ReadMajority / All、delta 传播（按来源序号、断档向来源补齐）与移除节点的 pruning（两阶段标记，分量并入执行者）（E64）；全部经远程层的 TLS 1.3 / mTLS（E65）；按负载的跨节点自动再平衡（领导者聚合 Up 成员负载，任意成员为源，阈值 1.5 倍 + 差值 > 5、冷却 3 s，覆盖经可靠通道广播；策略可配——`RebalancePolicy` 支持 `disabled()` 与链式 `withXxx`，决策为纯函数 `decideRebalance`，放弃理由用枚举返回）；集群原型（根包 `cluster_sharding` 等 1673 行 + demo 414 行）已删除 | `cluster/membership/`、`cluster/ddata/` |
| 流 / 状态机 | 🟠 Streams 有背压策略（Block 策略仍抛异常，见 C7）；XState 有基本状态机，`features` / `enterprise` 是占位 | `streams/core/` |
| 可观测性 | ✅ 分级日志（`foundation.logging`，默认 WARN，支持结构化字段 / MDC）；Prometheus 文本导出（每 Actor 计数 64 位，每 Actor 序列默认上限 1000）；Timer 指标平均值/时间戳已修复为真实值；同名指标注册报错。🟡 追踪只有 `foundation.monitoring.Tracer`（W3C 格式 ID），还没有接入消息投递与 ask | `runtime/prometheus_metrics.cj`、`foundation/logging/` |
| 工程 | ✅ `tools/ci.sh`（构建、分层检查、全部测试、`println` 检查、`--bench` 门禁接入 bench_gate）；测试 698 / 698（2026-11-09 第 20 批，`tools/ci.sh` 单用例超时 120 s；另跑集群套件 8 进程并行 2 轮与 ddata GC 压测 4 轮）；`cactor_testkit`（TestProbe、BehaviorTestKit、ManualTime、`BlackholeTransport` 故障注入）；Akka 对比基准 | `tools/`、`bench/` |
| HTTP | ✅ 最小可用（ManagementServer：健康检查 + Prometheus 指标，经真实 HTTP；绑定地址、并发上限、请求总时限可配置）。🟡 Ktor 风格插件体系未开始 | `cactor/api/` |

### 2.2 性能（同机实测，32 核 Xeon 8582C，详见 20261028-performance.md）

| 场景 | CActor | Akka 2.6.21 | 结论 |
|---|---|---|---|
| 单 Actor 吞吐 | 5.6–7.2 M/s | 2.7–3.4 M/s | CActor 约快 2 倍 |
| 扇出（8 发送方 × 100 Actor） | 2.5–4.5 M/s | 9.1–11.9 M/s | **Akka 约快 3 倍** |
| ping-pong P50 / P99 | 1.9–2.2 / 2.8–4.3 µs | 3.0 / 5.9–6.7 µs | CActor 更好 |
| Ask P50 / P99 | 5.0–6.0 / 11–13 µs | 8.7 / 10–16 µs | 持平或更好 |
| 远程 TCP（回环） | 146–194 k/s（合并写后观察到 405 k/s，未复测） | 135–308 k/s | **需要在空闲机器上复测** |

R1 之后的点对点回归对比、R4 两项优化的收益，都因开发机负载过高（load 30–80）没有得到可信数字。
2026-11-07 在负载 8.5–9 时与 `6183072` 交替各跑 5 次 `real_benchmark`：工作区单 Actor 中位 3.16 M/s、扇出 6.70 M/s，HEAD 分别为 3.06、6.21 M/s，没有回退；扇出单次波动 4.4–17 M/s，同日 E40（出站关联）之后再测一轮（负载 7.9–9.7）：远程 TCP 中位 36.7 万条/秒（HEAD 33.8 万），扇出中位 9.55 M/s（HEAD 6.78 M/s）；负载 4.8 时远程 TCP 42–50 万条/秒。上表数字待空闲机器与 Akka 同机复测。

### 2.3 主要问题

按对后续工作的阻碍程度排序：

1. **重复实现和死代码太多，API 面过大。**
   - 库代码（不含 tests / examples）约 64,500 行，其中 `cactor_runtime` 约 17,400 行、1,142 个公开声明，但消息主路径只经过 `guardian/actor_cell.cj`（`CellMailbox` / `PriorityCellMailbox` / `ActorCellDispatcher`）。`CActorRuntime` 仍在构造 `HighPerformanceActorSystem`、`SimpleActorSystem` 以及邮箱 / 调度器工厂等旧组件，它们不参与消息投递（2026-10-31 起不再构造 `DispatcherPool`：它从不接收任务，却一直轮询，空闲时占约 11% CPU）。
   - 调度器有 8 个子包（work_stealing、adaptive、optimized、dynamic、advanced、batch_processing……），邮箱有 6 个子包，加起来 30 多个类，都没有接入主路径；`runtime.message`、`runtime.execution`、`runtime.scheduler`、`dispatcher.batch_processing`、`mailbox.ringbuffer`、`mailbox.queue` 在仓库中没有任何引用。
   - 13 个 `placeholder.cj`；库代码中约 390 处“简化”标注。
   - 库模块合计约 3,300 个公开声明（统计口径见 20261029 §2），目标是 2.0.0 时降到 1,000 以下。
2. **同类基础设施各有多套。**
   - 日志 3 套：`foundation.logging`（72 个文件在用）、`foundation.log`（新加，无人使用）、`integration.logging`。
   - 配置 7 个包：`foundation.config`、`foundation.simple_config`、`core.config`、`cactor.config`、`cactor.config.file`、`cactor.api.config`、`integration.configuration`；dispatcher 参数只能用环境变量设置。
   - ask 已收敛为三个入口（20261106 计划 E38）：`TypedAsk.ask`（Actor 外）、`ctx.ask`（Actor 内）、`AskInbox.ask`（经典 ActorRef，目标照常 `context.reply`）；`AskPatternManager` / `patterns.ask.Ask` 已 @Deprecated（2.0.0 移除），`TypedAskPattern` 与 patterns 的 `TypedAsk` 已删除。剩余：经典 `ActorSystem.ask` 接口方法随死代码清理删除。
   - 监控 2 套：`foundation.monitoring` 与 `integration.monitoring`。
   - `foundation.plugin`、`foundation.attributes` 已经写好（为 HTTP 计划准备），但还没有任何使用方。
3. **存在假实现。** `JsonSerializer` 的反序列化固定返回 `"parsed_from_json"`；集群路由、CQRS、streams 中有多处“返回第一个目标 / 虚拟 ActorRef”式的占位逻辑。
4. **测试仍依赖时序。** 测试代码中有 135 处 `sleep`；没有覆盖率统计、长时间稳定性测试和故障注入；编译仍带 `-Woff unused -Woff parser`。
5. **示例过多且大多过时。** `cactor_examples` 有 47 个可执行示例、14,500 行，很多演示的是将要删除的实现（对象池、旧宏、旧邮箱、旧调度器）。
6. **文档与代码不一致。** 旧 `docs/*/roadmap.md` 写着“20,000,000 msg/s”“NUMA 感知”等未经实测或不存在的内容；README 的“当前限制”中“日志无法关闭”已经过时；20261029 §2 中“ActorContext 只有 `sender()`”“经典 API 没有定时器”在 R1 完成后也已过时。

---

## 3. 版本规划总览

两条主线在“收敛”之后并行推进：

- **A 线（API 与 HTTP）**：新 API 入口、HTTP 服务端与 Actor 融合、Native 引擎、生产插件。对应 20261029 的 P0–P11。
- **B 线（分布式）**：远程可靠化、集群成员、Singleton、分片、持久化插件。对应 20261028 的 R6。

| 版本 | 主题 | 主要内容 | 预计（人周） | 状态 |
|---|---|---|---|---|
| **1.3.0** | 收敛 | 删除死代码与重复实现；统一日志、配置、ask、Scheduler；真实 JSON；API 快照；测试去 `sleep`；基准复测 | 6–7 | 下一步 |
| **1.4.0** | 新 API + HTTP 最小可用版本 | `actorSystem {}` / `spawn` / `ask` 新入口；`@Actor` / `@Message` 宏；Ktor 风格 HTTP 服务端（stdx 引擎）；`call.ask` 融合；`testApplication` | 7–8 | 规划 |
| **1.5.0** | 性能与可靠远程 | Native HTTP/1.1 引擎达到验收线；扇出差距缩到 1.5 倍以内；远程 ask / watch、序列化注册与版本、可靠投递、TLS | 8–10 | 规划 |
| **1.6.0** | 集群与生产化 | gossip 成员协议、Cluster Singleton、分布式发布订阅、脑裂处理；HTTP 客户端、WebSocket / SSE、生产插件；扩展插件化；可观测性 | 10–12 | 规划 |
| **2.0.0** | API 冻结与发布 | 分片与再平衡、持久化插件接口与 Durable State；删除废弃 API；公开声明 ≤ 1,000；发布到仓颉中心仓 | 6–8 | 规划 |
| 2.x | 按需推进 | Reactive Streams 重写（R7）、Replicator、OpenTelemetry、虚拟 Actor、Persistence Query、HTTP/2 默认开启 | — | 远期 |

单人顺序执行约 40–45 人周；两人分别负责 A、B 线时，1.4.0 之后可以并行，总工期约 25–28 周。

---

## 4. 1.3.0：收敛（下一步）

**目标**：在加新功能之前，把代码收敛到一套实现，让后续的 HTTP 和分布式工作建立在稳定、可测量的基础上。本版本不新增用户可见的大功能。

### 4.1 删除与合并

- [ ] 计划 v2.35（DA3，部分）：已实现已知对端身份与出站握手挑战表预算，默认分别 4096，withPeerLimit 配置；并发准入、不驱逐旧身份、满额旧地址重启/旧 UID 拒绝、stop/start 保留及同锁缩容保护。验证见实施计划第 37 批。隔离表预算、安全回收、全节点字节及故障压力矩阵仍待完成；进度保持 69.32%、加权 69.5%。
- [ ] 计划 v2.34（DA3，部分）：可靠出站/入站通道默认各 4096，可配置；同锁准入、拒绝诊断、不确认被拒信封、满额同前缀新代替换与旧代拒绝已实现，新增六个回归（含 stop/start 后保留状态的缩容保护）通过，完整门禁通过（784/784、集群两轮各 137/137）。不按空闲/ACK 删除去重状态；对端/隔离表及全局字节预算仍待实现，总进度保持 69.32%、加权 69.5%。
- [ ] 计划 v2.33（DA3，部分）：TCP 实例级入站/出站预算、握手前准入、空闲连接回收与拒绝诊断已实现，新增 5 个真实网络回归；完整门禁通过（778/778、集群两轮各 137/137）。可靠通道总数与全节点字节预算仍待实现，总进度保持 69.32%、加权 69.5%。

- [x] 计划 v2.32（DA2）：类型化远程 ask 与经典 ask 共用容量，新增 pendingTypedAskCount；回复、超时、取消、失败即时回收，停机完成等待，本地代理保留 sender 的转发也受管理。新增 7 个回归，最终门禁见实施计划第 34 批；仍为 30.5/44=69.32%、加权 69.5%，DA3 全节点预算留待下一批。

- [x] 计划 v2.31（分布式复核与远程 ask）：经典 `SimpleRemoteManager.ask` 统一终态清理、停机即时完成、锁外回调；新增 `withAskLimit`（默认 1024）、`pendingAskCount` 与 `RemoteAskCapacityException`。补齐 7 个回归：原 6 个定向及第一轮 CI 通过；新增同址重放在旧实现先红，修复两类回复入口加入节点启动标识，防止重启撞名。覆盖真实 TCP、迟到回复、容量释放、20 次停止竞争与发送异常注入；最终 `cjHeapSize=2GB tools/ci.sh` 完整通过，全量 766/766、8 进程集群两轮各 137/137、宏负例 14/14 及全部故障/持久化/soak 门禁；日志与一次 LLVM llc 139 失败的重跑记录见实施计划 §3.7。§3.11 单列分布式剩余问题，类型化 ask/全局预算/所有权/兼容/背压仍需推进。加固已计分条目不重复加分，仍为 30.5/44=69.32%、加权 69.5%。

- [x] 2026-11-11（计划 v2.30，E61 reminder 子项）：`DurableReminders` 复用异步持久状态，支持登记/替换/撤销、到期重发、确认后推进、恢复与代次隔离；新增 5 个用例通过（含真实双节点 TCP 去重/确认），三进程恢复示例通过；`cjHeapSize=2GB tools/ci.sh` 全部门禁通过，全量 759/759、集群两轮各 137/137，日志见实施计划。业务必须去重；部署要求单写者及可访问的持久存储，不提供集群复制/选主。E61 三类子能力完成，按 🟡 半项折算，全计划 30.5/44=69.32%、工作量 69.5%（逐行复算修正旧阶段 5 多记的 1 个单位）；流式回复、HTTP/LLM 与编排仍未实现。

- [x] 2026-11-11（计划 v2.29，E61 子项）：`TypedRetry.run` 实现异步串行退避、总尝试上限、失败筛选、共享取消、定时清理及异常传播；复用 BackoffPolicy 延迟算法，支持虚拟时间调度注入。跨节点重试须保证幂等，取消不撤回已发送操作。E61 仍余流式回复、持久 reminder、HTTP/LLM 与编排示例，暂不提高整体折算比例。
- [x] v2.29 验证：新增 8 例，全量 754/754；`cjHeapSize=2GB tools/ci.sh` 单次完整通过，集群 8 进程两轮各 136/136，全部示例与故障检查通过。本轮未复验默认堆配置。
- [x] 2026-11-11（计划 v2.28，E61 子项）：本地与跨节点 ask 的 `AskCancellationToken`、主动取消异常、预取消跳过发送、竞态一次完成及订阅/超时资源清理已实现；取消不撤回远端操作。E61 其余能力仍待实现，本批不提高整体折算百分比。

- [x] 2026-11-11（计划 v2.27）：删除 distribution / integration；直方图迁 foundation、同步测试辅助迁 testkit，示例及中英文 API/包结构文档迁移。E48 仍部分完成，streams 收敛依赖 E60。
- [x] v2.27 门禁：全量 738/738、集群 8 进程并行两轮各 134/134、进程故障/TLS/日志 kill -9/ddata 压测及完整 `tools/ci.sh` 通过。全计划折合 30/44≈68%，加权 68.5/100≈69%；分布式既定清单 32/32=100%，不代表全部计划完成。

- [ ] **runtime 瘦身**：先解除 `CActorRuntime` 对旧组件（`HighPerformanceActorSystem`、`SimpleActorSystem`、`AdvancedMailboxFactory`、`AdvancedDispatcherFactory`；`DispatcherPool` 已解除）的依赖；再删除未接入主路径的调度器（work_stealing、adaptive、optimized、dynamic、advanced、batch_processing、monitoring 中的重复实现）和邮箱（lockfree、ringbuffer、queue、advanced、batching、factory），只保留 `ActorCellDispatcher`、阻塞调度器和 `CellMailbox` 的 unbounded / bounded / priority 三种；删除无引用的 `runtime.message`、`runtime.execution`、`runtime.scheduler`。删除前逐个确认 tests / examples 中的引用，有对应语义测试的保留测试、改到主路径上。
- [ ] **ActorSystem 收敛**：只保留 `CActorRuntime` + `CActorSystem`；`SimpleActorSystem`、`HighPerformanceActorSystem`、`CActorSystemBuilder`、`ActorSystemExtensions.create*` 转调或删除。
- [ ] **删除 13 个 `placeholder.cj`**，以及没有真实逻辑的“简化”实现：要么实现，要么删除，不再保留只返回固定值的代码。库代码中“简化”标注降到 50 处以下。
- [ ] **日志只留一套**：以 `foundation.logging` 为准（已被 72 个文件使用），把 `foundation.log` 中的后端抽象和 `LoggerFactory` 合并进去后删除 `foundation.log`；`integration.logging` 改为在其之上的适配器或删除。补结构化字段 / MDC。
- [ ] **监控只留一套**：合并 `integration.monitoring` 到 `foundation.monitoring`。
- [ ] **示例收敛**：`cactor_examples` 只保留约 10 个能说明当前 API 的示例（hello world、计数器、请求-应答、监督、类型化、定时器、远程、持久化、状态机、基准）；其余删除。
- **验收**：库代码行数（不含 tests / examples）减少 ≥ 35%；全部测试通过；`tools/check_module_deps.py` 通过。

### 4.2 统一基础设施（对应 20261029 的 P0、P1）

- [ ] **Scheduler**（`foundation.scheduler`）：时间轮 + 可注入的 `VirtualClock`；Actor 定时器、typed `scheduleOnce`、接收超时都改用它，不再各自创建 `std.sync.Timer`。验收：定时器精度 ±2 ms；`ManualTime` 下的测试不需要真实等待。
- [ ] **统一 ask 内核**：一个 Completion 内核；经典 Actor 用 `Request<R>`（不再需要处理 `AskMessage` 信封），类型化 Actor 用 `replyTo`；删除 `AskPatternManager` 等重复实现，`patterns.ask` 只做转调。验收：ask P50 ≤ 6 µs，不倒退。
  - 进展（20261106 计划 E38）：入口已收敛——经典 `AskInbox.ask` 以收件箱为发送方，不再需要 `AskMessage` 信封；旧入口 @Deprecated / 删除。Completion 内核与 P50 验收未做。
- [ ] **统一配置**：一套 `Config`，TOML 格式，覆盖顺序为默认值 → 文件 → 环境变量 → `-P` 参数；dispatcher、邮箱、远程参数都能从配置读取；合并现有 7 个配置包。
- [ ] **邮箱过载策略**：有界邮箱满时可选丢弃（计入死信）、返回失败（`tryTell`）、阻塞发送方（带超时）。
  - 进展：`MailboxOverflow.DropNew / Fail / Block(timeout)` 已有；Block 改为条件变量唤醒，Actor 内发送不等待、按超时计死信（20261106 计划 E39）；默认调度器新增饥饿检测（`CACTOR_STARVATION_WARN_MS`）。`tryTell` 未做。
- [x] **CoordinatedShutdown 增强**：阶段超时；每个系统一个实例（不再放在静态注册表中）；Akka 阶段顺序，用户 Actor 在 actor-system-terminate 阶段停止（20261106 计划 E34，`lifecycle/coordinated_shutdown_phase_test.cj`）。
- [ ] **真实 JSON**：新增 `cactor_json`（基于 stdx `json.stream` 或自写流式解析器），删除假的 `JsonSerializer`。验收：通过 JSONTestSuite。此项需要先完成 stdx 接入（`CANGJIE_STDX_PATH`），stdx 只允许 json / http / integration 模块依赖。
- [ ] **插件内核落地**：`foundation.plugin` 至少有一个真实使用方（例如 Logging 或 Metrics 作为 SystemPlugin），否则推迟到 1.4.0 再合入。

### 4.3 工程质量

- [ ] **API 快照**：`tools/api_dump.py` 生成各模块公开声明清单并提交到仓库，CI 中比较差异；实现包改为 `protected package`（先验证 20261029 §12 中的假设 5、6）。
- [ ] **测试去时序依赖**：135 处 `sleep` 改为 `awaitCondition` / `TestProbe` / `ManualTime`；`cactor_testkit` 补 `BehaviorTestKit` 和 `ManualTime`。
- [ ] **告警清零**：逐模块去掉 `-Woff unused`、`-Woff parser`。
- [ ] **CI 门禁扩展**：cjlint；逐模块构建；覆盖率统计（核心模块 ≥ 70%）；基准回归检查（在空闲机器或固定 CPU 配额下运行）。
- [ ] **稳定性测试**：1 小时持续收发内存不增长；随机异常 / 随机停止的故障注入。
- [ ] **基准复测**：在空闲机器上按 20261029 §9.1 的规则重测 `real_benchmark` 和 Akka 对比，确认 R1 之后点对点没有回退，并记录侵入式就绪队列和远程合并写的实际收益。

### 4.4 文档

- [ ] README 的“当前限制”和“路线图”与本文同步；删除旧 roadmap 中未经实测的数字。
- [ ] 20261029 §2 中已过时的现状描述（ActorContext、经典定时器）加注说明。
- [ ] 每个保留的示例对应一篇简短指南。

---

## 5. 1.4.0：新 API 与 HTTP 最小可用版本

对应 20261029 的 P2–P4，以及 R2、R5 的剩余项。

### 5.1 新 API 入口（P2）

- [ ] 单一入口 `actorSystem("name") { s => s.install(...) }`；旧入口（`CActor.system`、各种 Factory / Builder）转调新 API 并标记废弃。
- [ ] 创建与通信收敛为 `spawn` / `props` / `<<` / `ask`；`TypedBehavior` 构建器。
- [x] `@actor` 宏（在 `@actorReceive` 基础上，同一消息类型多个处理方法、协议清单漏处理/漂移时编译期报错）；`@Message` 宏（生成 `messageType`、编解码器并自动注册）。删除其余原样返回输入的宏。
- [ ] `testActorSystem`（`cactor_testkit`）。
- [x] 类型化 API 补齐：行为拦截器（`TypedBehaviors.intercept` / `monitor` / `logMessages`，`typed_interceptor.cj`）；Akka Typed 聊天室示例（`examples.chat_room`）。
- **验收**：hello world、计数器、请求-应答示例的代码行数比 1.2.0 减少 ≥ 40%；文档给出“经典写法 / DSL 写法”对照。

### 5.2 HTTP 服务端核心（P3）

- [ ] 新模块 `cactor_http`（模型）、`cactor_http_server`（Application、路由树、插件适配）、`cactor_http_test`（`testApplication`）；引擎接口 + stdx 引擎。
- [ ] Ktor 风格 API：`install(Plugin)`、路由树、`call.receive` / `call.respond`，一个请求一个协程。
- [ ] 安全默认值（20261029 §7.9）：各类上限与超时；408 / 413 / 414 / 431 / 503；`stop(grace, deadline)`。
- [ ] 首批插件：ContentNegotiation、StatusPages、CallLogging、CallId、DefaultHeaders、AutoHeadResponse、BodyLimit、RequestTimeout、RequestValidation。
- **验收**：每个插件 ≥ 5 个测试；上限、超时、停机、请求走私四类稳定性测试通过；插件开销 ≤ 15%。

### 5.3 HTTP 与 Actor 融合（P4）

- [ ] `cactor_http_actor`：`call.ask`（两种写法；过载和超时返回 503；请求取消时取消 ask）；`install(HttpServer)`；停机与 `/ready` 联动；`testActorApplication`；REST + Actor 示例。
- **验收**：每请求一次 ask 的场景有与 Akka HTTP、actix-web 的同等 CPU 对比数据；Actor 过载时快速返回 503；停机时在途请求零丢失。

---

## 6. 1.5.0：性能与可靠远程

### 6.1 性能（R4 剩余项 + P5）

- [x] **多线程负扩展**（2026-10-31，[20261031-protoactor-performance.md](docs/plan/20261031-protoactor-performance.md)）：控制消息按具体类匹配、类型化特殊行为用种类标记、运行时不再启动 `DispatcherPool`、双缓冲邮箱、runNext 与捐赠。多对 ping-pong 约为 protoactor-go 的 93%。
- [ ] **扇出**：发送方批量提交、减少每条消息的原子操作；空闲线程工作窃取。目标：与 Akka 的差距从约 3 倍缩小到 1.5 倍以内，负载不均时吞吐不下降。外部线程扇出目前约为 protoactor-go 的 1/3：每次运行只处理 3–4 条，调度与跨核缓存传递占大头。
- [ ] **外部线程发送**：单发送方每次 tell 约 235ns（protoactor-go 约 120ns）；单线程同样步骤只要约 37ns，多出的部分是消费方频繁交换缓冲区造成的跨核缓存传递。考虑把 incoming 计数与调度标志合并为一个状态字，减少双方共同写的缓存行。
- [ ] **类型化路径**：每条消息在 core 泛型代码里分配一次 `TypedEnvelope<T>`（多线程时争用）。评估协议类型是 `Message` 时免信封，或提供按具体类型实例化的发送入口。
- [ ] **规模**：100 万个 Actor 的创建速度和每个 Actor 的内存，目标 < 1 KB / Actor。
- [ ] **编译**：评估 `--lto=thin` 和 PGO。
- [ ] **Native HTTP/1.1 引擎**（P5）：先做 stdx 瓶颈分析，再实现零拷贝解析、接入层反压、对象池、Date 缓存。验收线：同等 CPU 下 plaintext / JSON / 路由 / ask 场景 ≥ Akka HTTP；挑战线：每核 ≥ actix-web 的 50%；模糊测试连续 1 小时无崩溃。达到验收线之前不切换默认引擎。

### 6.2 远程（R6 第一部分）

- [ ] 序列化注册表与版本号（与 `@Message` 宏打通），支持 schema 演进。✅ 基础版已落地（`remote` 模块 codec 注册 + 版本拒绝，`schema_evolution_test.cj`）。
- [x] 远程 `ActorRef` 与本地统一：远程 ask（`remoting.cj` `ask`）；远程 watch（WATCH/DEATH，连接断开时投递 `Terminated`）。
- [x] 可靠投递：序号 + 确认 + 重发（对应 Akka Reliable Delivery）。`tellReliable` 有序号、累计 ACK、重发与去重；接收端重启后未确认消息在新通道重发（跨重启至少一次）；未确认上限 `maxUnacked` 与超时放弃 `giveUpAfter`（E41，`unit/remote_semantics/system_message_reliability_test.cj`）。
- [x] 基于 stdx 的 TLS（E65）：`RemoteTls` TLS 1.3 双向认证、只信任配置的 CA；明文连接被拒绝、线上抓包看不到消息内容（`remote_tls_test.cj`），3 进程集群全程 TLS（`tools/cluster_demo.sh --tls`）。
- **验收**：远程吞吐在空闲机器上追平 Akka Artery TCP；随机断连测试下可靠投递不丢消息、不乱序。

---

## 7. 1.6.0：集群与生产化

### 7.1 集群（R6 第二部分）

- [x] gossip 成员协议（Joining → Up → Leaving → Exiting → Removed），种子节点引导，成员事件订阅；接入已有的 Phi Accrual 故障检测器（`cluster/membership/membership.cj`）。
- [x] Cluster Singleton（`membership/singleton.cj`）、分布式发布订阅（`membership/pubsub.cj`）。
- [x] Receptionist 集群版（`membership/receptionist.cj`，20261106 计划 E63；测试 `unit/cluster/cluster_distribution_test.cj`）。
- [x] Distributed Data 增强（20261106 计划 E64）：一致性级别 WriteMajority / ReadMajority / All（`cluster_distribution_test.cj`）；delta 传播（按来源序号、断档向来源补齐）与移除节点的 pruning（两阶段标记，分量并入执行者，陈旧副本不重复计数）（`unit/cluster/ddata_delta_pruning_test.cj`）。
- [x] 脑裂处理（keep-majority，`membership/sbr.cj`）。
- [x] 删除或重写现有原型（2026-11-09）：`cluster` 根包 5 个原型文件（`cluster_support.cj`、`cluster_routing.cj`、`failover.cj`、`shard_region.cj`、`cluster_sharding.cj`，1673 行）与 `cluster_sharding_demo`（414 行）已删除，`xstate` 去掉 `cactor_cluster` 依赖并标 experimental；分片语义只保留 `cluster/membership/ClusterSharding`（示例 `best09_cluster_sharding`、`tools/cluster_demo.sh`）。
- [x] 分片按负载再平衡（2026-11-09）：领导者聚合全部 Up 成员 5 s 内的负载报告，可在任意成员之间搬迁最重分片（阈值 1.5 倍且差值 > 5、冷却 3 s、覆盖经可靠通道广播）；测试 `unit/cluster/sharding_rebalance_test.cj` 覆盖手动与自动两条路径。策略与决策已抽出（2026-11-09 续）：`RebalancePolicy` + 纯函数 `decideRebalance`，12 例纯函数用例在 `unit/cluster/rebalance_policy_test.cj`；负载上报按签名去重、每 TTL/2 刷新。
- [x] 单机黑洞注入：testkit `BlackholeTransport`（`modules/testkit/src/network_harness.cj`），测试 `unit/cluster/cluster_blackhole_test.cj`。
- [x] 集群视图管理端点（2026-11-10）：`cactor/api/management.cj` 的通用 `ManagementRoute` + `addRoute`，`cactor_cluster.ops.attachClusterRoutes` 挂上 `/cluster/status`、`/cluster/shards`；测试 `unit/cluster/cluster_management_test.cj`。此前 `docs/TROUBLESHOOTING.md` 让运维访问这两个路径，服务端一直 404。
- [x] 跨进程 barrier 与等待迁移（2026-11-10，E62 收口）：`cactor_testkit.ProcessBarrier` 用共享目录会合（真进程用例验证，shell 也能参与）；`awaitGossipRounds` 替掉 8 处负向断言的固定 sleep；E62 记满。
- [x] 分片获取可观测（2026-11-10）：`ShardingSettings.acquireWarnAfter`（默认 10 s，可调）+ `ClusterSharding.acquisitionHealth()`（slow / oldestMs / pendingMembers / slowWarnings）+ `/cluster/shards` 字段；告警按窗口重复而不是只报一次；测试 `unit/cluster/sharding_acquisition_test.cj`（只在一侧建区域 → 托管为 0、报出在等的节点 → 另一侧建区域后收敛）。
- [x] testkit 断言工具（2026-11-10，E62 的四项）：`TestProbe.watch` / `expectTerminated`、`LoggingTestKit`、`ProcessBarrier` 跨进程会合、进程级黑洞注入（`/cluster/blackhole` + `tools/cluster_chaos.sh`）；E62 记满。
- [x] 跨节点类型化 ask（2026-11-10，E63 收口）：`TypedAsk.askRemote` 让回复引用走信封 sender，配合 v2.23/v2.24 的类型化引用过线，E63 记满；测试 `unit/cluster/typed_remote_test.cj`。
- [x] 成员协议鉴权与 incarnation（20261106 计划 E7′，见上表"集群"一行）。
- **验收**：本机 3 节点集群测试：加入、离开、杀掉一个节点后 Singleton 切换（已有测试覆盖）。

### 7.2 HTTP 生产化（P6–P9）

- [ ] HttpClient（按 host 分池、补 SNI、代理、重试与退避）、MockEngine、`pipeTo`。
- [ ] 流式请求体、chunked、multipart；WebSocket（协程风格 + `webSocketActor`）；SSE。
- [ ] 生产插件：TLS / mTLS、Compression、CORS、Authentication、Sessions、缓存与条件请求、静态文件、ForwardedHeaders、HSTS、RateLimit、CircuitBreaker；HTTP/2 默认关闭。
- [ ] 可观测性：Prometheus 指标插件（以路由模板为标签）；CallId / traceparent 透传到 ask 和客户端；访问日志。

### 7.3 扩展插件化（P10）

- [ ] Logging、Metrics、Remote、Cluster、Persistence 统一做成 SystemPlugin，通过 `install` 获得；ready 探针与集群状态联动；停机顺序正确。

---

## 8. 2.0.0：API 冻结与发布

- [ ] 分片与再平衡（基于实体 ID），与 `EventSourcedActor` 结合，迁移后自动恢复状态（基础能力与按负载再平衡已在 1.6.0 落地，这里指与持久化实体的整合与状态恢复）。
- [ ] 持久化插件接口（文件实现之外至少一个数据库实现，视仓颉生态而定）；Durable State。
- [ ] 删除所有废弃 API；库模块公开声明 ≤ 1,000；API 快照的每处变化都写入 CHANGELOG。
- [ ] Resources（类型安全路由）与 OpenAPI 生成；迁移文档（从 Akka、Akka HTTP、Ktor、CActor 1.x 迁移）。
- [ ] 发布到仓颉中心仓，语义化版本，API 兼容性检查。
- **验收**：3 节点集群测试中杀掉节点后分片迁移、状态恢复；全部稳定性测试（20261029 §9.2）进入 CI，长时间项每晚运行。

---

## 9. 远期（2.x，按需推进）

- **流处理重写（R7）**：Reactive Streams 语义的 Source / Flow / Sink、基于需求的背压、异步边界、物化值；`mapAsync`、`groupBy`、`merge`、`zip`、`throttle` 等操作符；与 Actor、TCP、HTTP 的 Channel 互通。完成前现有 streams 模块标记为实验性。
- **分布式数据**：Replicator，在节点间同步已有的 CRDT。
- **持久化查询**：按 tag 读事件流。
- **追踪**：OpenTelemetry 兼容的上下文随消息传播。
- **虚拟 Actor**：Orleans 风格的按需激活与空闲回收。
- **状态机**：XState 模块与 Actor、DSL 整合（`@StateMachine` 宏已可用：枚举状态 + 转换表 + guard/action 骨架，转换表可喂给 xstate `StateMachineBuilder`），清理 `features`、`enterprise` 中的占位。

---

## 10. 明确不做（或无限期推迟）

旧路线图中的以下条目在可见的版本内不做，原因是投入大、与核心目标无关，或依赖的仓颉生态尚不存在：

- 多语言绑定（Java / Python / Go / Rust）、与 Akka 协议兼容。
- Kubernetes Operator、Helm Charts、服务网格集成。
- IDE 插件、可视化管理界面、代码生成器。
- 多种数据库后端同时支持（MySQL、MongoDB、Cassandra、InfluxDB、HDFS、S3 等）；2.0.0 只要求一个数据库实现。
- 以 GitHub Star、企业用户数作为目标的社区指标。

---

## 11. 风险

| 风险 | 对策 |
|---|---|
| 收敛阶段删除代码破坏现有用户 | 只依赖门面 API（`cactor.*`）的代码保持可用；删除的公开类型写入 CHANGELOG 和迁移指南 |
| 开发机负载高，性能数字不可信 | 按 20261029 §9.1 测量；正式数字在空闲机器上测；基准程序只在 workspace 根目录 `cjpm build` 后运行（`cjpm build -m` 不带 `-O2`） |
| stdx 的缺陷与二进制依赖 | stdx 只在 json / http / integration 中使用；CI 固定版本；h2 默认关闭 |
| 仓颉语言能力假设不成立（20261029 §12） | 1.3.0 中先用实验工程验证；每项都有退路 |
| 两条主线争夺人力 | 1.3.0 必须先完成；之后 A 线优先保证 1.4.0 的最小可用版本，B 线的远程可靠化不晚于 1.5.0 |

---

## 12. 维护方式

- 每个版本发布时更新第 2 节“现状”和第 3 节的状态列，勾选对应条目。
- 只有在主路径上生效、并有测试覆盖的条目才勾选；部分完成的条目保持未勾选并注明还差什么。
- 计划有实质调整时，在 `docs/plan/` 下新增带日期的设计文档，并在本文中引用。
