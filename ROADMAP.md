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

## 2. 现状（2026-10-28 核对）

### 2.1 已经可用的能力

| 领域 | 状态 | 依据 |
|---|---|---|
| 运行时 | ✅ 每个 Actor 一个侵入式 MPSC 邮箱；亲和调度 + 侵入式就绪队列；独立系统消息队列；阻塞调度器；吞吐参数可配置 | `runtime/guardian/actor_cell.cj` |
| 经典 API 语义（R1） | ✅ 监督树（父停子、Escalate、OneForAll）、watch / Terminated、become、定时器、Stash、接收超时、PoisonPill / Kill、Props 生效、按路径查找、死信订阅 | `semantics_test.cj` 33 个用例 |
| 类型化 API（R2） | ✅ 运行在经典 Cell 上；setup / receive / supervise / withTimers / withStash / ask / messageAdapter / pipeToSelf / Receptionist（单机） | `typed_semantics_test.cj` 28 个用例 |
| DSL | 🟡 `@actorReceive` 宏、`<<` 运算符、时间字面量可用；其余 20 个宏基本原样返回输入 | `macros/src/macros.cj` |
| 远程 | 🟡 基于 std.net 的 TCP，写线程合并写、批量解析；至多一次投递；没有远程 ask / watch、序列化注册和版本 | `foundation/network/transport.cj`、`remote/simple_remote.cj` |
| 持久化 | 🟡 文件事件日志、快照、`EventSourcedActor`；没有存储插件接口；CQRS 部分是简化实现 | `persistence/` |
| 集群 | 🟠 Phi Accrual 故障检测器和 5 种 CRDT 真实可用；成员协议、Singleton 都没有；分片和集群路由是原型（如轮询“总是选择第一个目标”） | `cluster/detector`、`cluster/ddata`、`cluster_routing.cj` |
| 流 / 状态机 | 🟠 Streams 是对内存数组的同步处理，没有背压和异步边界；XState 有基本状态机，`features` / `enterprise` 是占位 | `streams/core/simple_stream_api.cj` |
| 可观测性 | 🟡 分级日志（`foundation.logging`，默认 WARN）；Prometheus 文本导出；没有追踪 | `runtime/prometheus_metrics.cj` |
| 工程 | 🟡 `tools/ci.sh`（构建、分层检查、全部测试、`println` 检查），2026-10-28 在 `d652d59` 上全部通过，测试 250 / 250；`cactor_testkit`（TestProbe 等）；Akka 对比基准 | `tools/`、`bench/` |
| HTTP | ❌ 没有任何 HTTP 代码，也没有引入 stdx | — |

### 2.2 性能（同机实测，32 核 Xeon 8582C，详见 20261028-performance.md）

| 场景 | CActor | Akka 2.6.21 | 结论 |
|---|---|---|---|
| 单 Actor 吞吐 | 5.6–7.2 M/s | 2.7–3.4 M/s | CActor 约快 2 倍 |
| 扇出（8 发送方 × 100 Actor） | 2.5–4.5 M/s | 9.1–11.9 M/s | **Akka 约快 3 倍** |
| ping-pong P50 / P99 | 1.9–2.2 / 2.8–4.3 µs | 3.0 / 5.9–6.7 µs | CActor 更好 |
| Ask P50 / P99 | 5.0–6.0 / 11–13 µs | 8.7 / 10–16 µs | 持平或更好 |
| 远程 TCP（回环） | 146–194 k/s（合并写后观察到 405 k/s，未复测） | 135–308 k/s | **需要在空闲机器上复测** |

R1 之后的点对点回归对比、R4 两项优化的收益，都因开发机负载过高（load 30–80）没有得到可信数字。

### 2.3 主要问题

按对后续工作的阻碍程度排序：

1. **重复实现和死代码太多，API 面过大。**
   - 库代码（不含 tests / examples）约 64,500 行，其中 `cactor_runtime` 约 17,400 行、1,142 个公开声明，但消息主路径只经过 `guardian/actor_cell.cj`（`CellMailbox` / `PriorityCellMailbox` / `ActorCellDispatcher`）。`CActorRuntime` 仍在构造 `HighPerformanceActorSystem`、`SimpleActorSystem`、`DispatcherPool` 以及邮箱 / 调度器工厂等旧组件，它们不参与消息投递。
   - 调度器有 8 个子包（work_stealing、adaptive、optimized、dynamic、advanced、batch_processing……），邮箱有 6 个子包，加起来 30 多个类，都没有接入主路径；`runtime.message`、`runtime.execution`、`runtime.scheduler`、`dispatcher.batch_processing`、`mailbox.ringbuffer`、`mailbox.queue` 在仓库中没有任何引用。
   - 13 个 `placeholder.cj`；库代码中约 390 处“简化”标注。
   - 库模块合计约 3,300 个公开声明（统计口径见 20261029 §2），目标是 2.0.0 时降到 1,000 以下。
2. **同类基础设施各有多套。**
   - 日志 3 套：`foundation.logging`（72 个文件在用）、`foundation.log`（新加，无人使用）、`integration.logging`。
   - 配置 7 个包：`foundation.config`、`foundation.simple_config`、`core.config`、`cactor.config`、`cactor.config.file`、`cactor.api.config`、`integration.configuration`；dispatcher 参数只能用环境变量设置。
   - ask 多套：`AskPatternManager`、`patterns.ask`、`TypedAskPattern`、`TypedAsk`；经典 ask 仍要求被询问的 Actor 处理 `AskMessage` 信封。
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

- [ ] **runtime 瘦身**：先解除 `CActorRuntime` 对旧组件（`HighPerformanceActorSystem`、`SimpleActorSystem`、`DispatcherPool`、`AdvancedMailboxFactory`、`AdvancedDispatcherFactory`）的依赖；再删除未接入主路径的调度器（work_stealing、adaptive、optimized、dynamic、advanced、batch_processing、monitoring 中的重复实现）和邮箱（lockfree、ringbuffer、queue、advanced、batching、factory），只保留 `ActorCellDispatcher`、阻塞调度器和 `CellMailbox` 的 unbounded / bounded / priority 三种；删除无引用的 `runtime.message`、`runtime.execution`、`runtime.scheduler`。删除前逐个确认 tests / examples 中的引用，有对应语义测试的保留测试、改到主路径上。
- [ ] **ActorSystem 收敛**：只保留 `CActorRuntime` + `CActorSystem`；`SimpleActorSystem`、`HighPerformanceActorSystem`、`CActorSystemBuilder`、`ActorSystemExtensions.create*` 转调或删除。
- [ ] **删除 13 个 `placeholder.cj`**，以及没有真实逻辑的“简化”实现：要么实现，要么删除，不再保留只返回固定值的代码。库代码中“简化”标注降到 50 处以下。
- [ ] **日志只留一套**：以 `foundation.logging` 为准（已被 72 个文件使用），把 `foundation.log` 中的后端抽象和 `LoggerFactory` 合并进去后删除 `foundation.log`；`integration.logging` 改为在其之上的适配器或删除。补结构化字段 / MDC。
- [ ] **监控只留一套**：合并 `integration.monitoring` 到 `foundation.monitoring`。
- [ ] **示例收敛**：`cactor_examples` 只保留约 10 个能说明当前 API 的示例（hello world、计数器、请求-应答、监督、类型化、定时器、远程、持久化、状态机、基准）；其余删除。
- **验收**：库代码行数（不含 tests / examples）减少 ≥ 35%；全部测试通过；`tools/check_module_deps.py` 通过。

### 4.2 统一基础设施（对应 20261029 的 P0、P1）

- [ ] **Scheduler**（`foundation.scheduler`）：时间轮 + 可注入的 `VirtualClock`；Actor 定时器、typed `scheduleOnce`、接收超时都改用它，不再各自创建 `std.sync.Timer`。验收：定时器精度 ±2 ms；`ManualTime` 下的测试不需要真实等待。
- [ ] **统一 ask 内核**：一个 Completion 内核；经典 Actor 用 `Request<R>`（不再需要处理 `AskMessage` 信封），类型化 Actor 用 `replyTo`；删除 `AskPatternManager` 等重复实现，`patterns.ask` 只做转调。验收：ask P50 ≤ 6 µs，不倒退。
- [ ] **统一配置**：一套 `Config`，TOML 格式，覆盖顺序为默认值 → 文件 → 环境变量 → `-P` 参数；dispatcher、邮箱、远程参数都能从配置读取；合并现有 7 个配置包。
- [ ] **邮箱过载策略**：有界邮箱满时可选丢弃（计入死信）、返回失败（`tryTell`）、阻塞发送方（带超时）。
- [ ] **CoordinatedShutdown 增强**：阶段超时；每个系统一个实例（不再放在静态注册表中）。
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
- [ ] `@Actor` 宏（在 `@actorReceive` 基础上，同一消息类型多个处理方法时编译期报错）；`@Message` 宏（生成 `messageType`、编解码器并自动注册）。删除其余原样返回输入的宏。
- [ ] `testActorSystem`（`cactor_testkit`）。
- [ ] 类型化 API 补齐：行为拦截器（Interceptor）；Akka Typed 聊天室示例。
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

- [ ] **扇出**：发送方批量提交、减少每条消息的原子操作；空闲线程工作窃取。目标：与 Akka 的差距从约 3 倍缩小到 1.5 倍以内，负载不均时吞吐不下降。
- [ ] **规模**：100 万个 Actor 的创建速度和每个 Actor 的内存，目标 < 1 KB / Actor。
- [ ] **编译**：评估 `--lto=thin` 和 PGO。
- [ ] **Native HTTP/1.1 引擎**（P5）：先做 stdx 瓶颈分析，再实现零拷贝解析、接入层反压、对象池、Date 缓存。验收线：同等 CPU 下 plaintext / JSON / 路由 / ask 场景 ≥ Akka HTTP；挑战线：每核 ≥ actix-web 的 50%；模糊测试连续 1 小时无崩溃。达到验收线之前不切换默认引擎。

### 6.2 远程（R6 第一部分）

- [ ] 序列化注册表与版本号（与 `@Message` 宏打通），支持 schema 演进。
- [ ] 远程 `ActorRef` 与本地统一：远程 ask；远程 watch（连接断开时投递 `Terminated`）。
- [ ] 可靠投递：序号 + 确认 + 重发（对应 Akka Reliable Delivery）。
- [ ] 基于 stdx 的 TLS。
- **验收**：远程吞吐在空闲机器上追平 Akka Artery TCP；随机断连测试下可靠投递不丢消息、不乱序。

---

## 7. 1.6.0：集群与生产化

### 7.1 集群（R6 第二部分）

- [ ] gossip 成员协议（Joining → Up → Leaving → Exiting → Removed），种子节点引导，成员事件订阅；接入已有的 Phi Accrual 故障检测器。
- [ ] Cluster Singleton、分布式发布订阅、Receptionist 集群版。
- [ ] 脑裂处理（先实现 keep-majority）。
- [ ] 删除或重写现有原型：`cluster_routing.cj`、`failover.cj` 中的占位逻辑。
- **验收**：本机 3 节点集群测试：加入、离开、杀掉一个节点后 Singleton 切换。

### 7.2 HTTP 生产化（P6–P9）

- [ ] HttpClient（按 host 分池、补 SNI、代理、重试与退避）、MockEngine、`pipeTo`。
- [ ] 流式请求体、chunked、multipart；WebSocket（协程风格 + `webSocketActor`）；SSE。
- [ ] 生产插件：TLS / mTLS、Compression、CORS、Authentication、Sessions、缓存与条件请求、静态文件、ForwardedHeaders、HSTS、RateLimit、CircuitBreaker；HTTP/2 默认关闭。
- [ ] 可观测性：Prometheus 指标插件（以路由模板为标签）；CallId / traceparent 透传到 ask 和客户端；访问日志。

### 7.3 扩展插件化（P10）

- [ ] Logging、Metrics、Remote、Cluster、Persistence 统一做成 SystemPlugin，通过 `install` 获得；ready 探针与集群状态联动；停机顺序正确。

---

## 8. 2.0.0：API 冻结与发布

- [ ] 分片与再平衡（基于实体 ID），与 `EventSourcedActor` 结合，迁移后自动恢复状态。
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
- **状态机**：XState 模块与 Actor、DSL 整合（`@StateMachine` 或枚举状态 + 转换表），清理 `features`、`enterprise` 中的占位。

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
