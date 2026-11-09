# CActor 与 Akka 语义对照表

> 对应 docs/plan/20261106.md 阶段 1（M1）的验收项《Akka 语义对照表》。
> 每一行都给出验证它的测试文件（路径相对 `modules/tests/src/unit/`，`core:` 前缀表示 `modules/core/src/` 内的包内测试）。
> "一致"指可观察行为与 Akka（classic / Typed 2.6+）相同；"差异"一列写明不同之处及原因。

## 1. 生命周期与重启

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| `preStart` 执行线程 | Actor 自己的线程，第一条消息之前 | 相同；之后才执行 `onStarted(context)` | `onStarted` 是 CActor 扩展（可用完整上下文） | `runtime/lifecycle/restart_protocol_test.cj` |
| `preStart` 抛异常 | `ActorInitializationException`，默认策略停止 | 相同；不会从 `actorOf` 抛出 | — | `runtime/failure_semantics/failure_semantics_test.cj`、`restart_protocol_test.cj` |
| `preRestart` 默认行为 | 停止全部子 Actor，调用 `postStop` | 相同 | — | `restart_protocol_test.cj` |
| `postRestart` 默认行为 | 调用 `preStart` | 相同 | — | `restart_protocol_test.cj` |
| 重启时机 | 等旧子 Actor 全部终止后才创建新实例（Recreating） | 相同；等待期间到达的消息由新实例处理 | — | `restart_protocol_test.cj` |
| 失败时子树挂起 | 失败 Actor 的整棵子树暂停，直到监督决定 | 相同（调度字 `PAUSE_BIT`） | — | `runtime/lifecycle/subtree_suspension_test.cj` |
| 用户代码抛 Error | JVM Error 通常致命 | 包装成 `ActorErrorException` 交给监督，工作线程存活 | 仓颉的 Error（如栈溢出）不应杀死调度线程 | `failure_semantics_test.cj` |

## 2. 停止

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| `system.stop(ref)` / `context.stop(child)` | 系统消息：处理完当前消息即停，邮箱中剩余消息进死信 | 相同（E33）；对子 Actor 同样生效 | — | `runtime/stop_protocol/stop_semantics_test.cj`、`stop_protocol_test.cj` |
| 停止后名字何时可复用 | 终止（`Terminated`）之后 | 相同：终止前同名 `actorOf` 报"名称已存在" | — | `stop_semantics_test.cj`（nameIsReservedUntilTerminated） |
| 处理完已排队消息再停 | `PoisonPill`（普通消息） | 相同；`gracefulStop(ref, timeout)` 发 PoisonPill 并等待终止 | — | `stop_semantics_test.cj`（gracefulStopProcessesQueuedMessagesThenReleasesName）、`runtime/semantics/semantics_test.cj` |
| `Kill` | 以 `ActorKilledException` 失败，默认停止 | 相同 | — | `semantics_test.cj`（killStopsActorByDefault、killRestartsWhenStrategySaysSo） |
| 邮箱满时停止 | 系统消息不受邮箱容量限制 | 相同 | — | `stop_protocol_test.cj`（systemStopWorksWhenBoundedMailboxIsFull） |
| 父停止 | 先停全部子 Actor，子的 `postStop` 先于父 | 相同 | — | `failure_semantics_test.cj`、`runtime/stop_race/stop_race_test.cj` |
| 协调关闭阶段 | CoordinatedShutdown 各阶段，用户 Actor 在 actor-system-terminate 阶段停止 | 相同；每阶段超时、幂等 | — | `runtime/lifecycle/coordinated_shutdown_phase_test.cj` |

## 3. 监督

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 经典默认策略 | 启动失败 / Kill / DeathPact → Stop，其余 → Restart | 相同 | — | `failure_semantics_test.cj`、`semantics_test.cj` |
| 重启上限 | `maxNrOfRetries` 计入所有重启（含退避） | 相同；`maxRetries = 0` 表示不重启 | — | `core:supervision/backoff_test.cj` |
| 指数退避 | `BackoffSupervisor` / `restartWithBackoff`：min×2ⁿ 封顶、抖动、稳定后复位 | 相同算法，经典与类型化共用 `BackoffPolicy`；以实际重启时刻计稳定时长 | 退避期间 Actor 挂起，期间消息由新实例处理 | `core:supervision/backoff_test.cj`、`runtime/failure_semantics/backoff_semantics_test.cj` |
| 类型化默认 | 未监督的失败 → 停止 | 相同 | — | `runtime/typed_semantics/typed_semantics_test.cj` |
| 类型化嵌套 supervise | 由内向外匹配，内层规则优先 | 相同（E36） | — | `runtime/typed_alignment/supervision_layers_test.cj` |
| 类型化重启计数 | 跟随 Supervisor 拦截器，跨重启累计 | 相同：按规则在行为中的位置累计 | — | `supervision_layers_test.cj` |
| setup 失败 + 不限次数 restart | 停止（否则无限重启） | 相同 | — | `backoff_semantics_test.cj` |
| 断路器 | 无内置监督版本 | `CircuitBreakerSupervisionStrategy`：半开时再失败（含退避重启）即停止 | CActor 扩展 | `core:supervision/backoff_test.cj` |

## 4. DeathWatch

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| `Terminated` 的投递 | 进入监视者用户邮箱队尾 | 相同：被监视者死前发来的消息先于 `Terminated` | — | `runtime/death_watch/death_watch_test.cj` |
| 未处理 `Terminated` | `DeathPactException` | 相同 | — | `semantics_test.cj`（unhandledTerminatedIsDeathPact） |
| `unwatch` 后 | 已入队未处理的 `Terminated` 被丢弃 | 相同 | — | `death_watch_test.cj` |
| `watchWith(ref, msg)` | 收到自定义消息，跨重启有效 | 相同（经典与类型化） | — | `death_watch_test.cj` |
| watch 已死的 Actor | 立即收到 `Terminated` | 相同 | — | `failure_semantics_test.cj`（R20） |
| 重启时 | 先 unwatch 再停子 Actor | 相同 | — | `death_watch_test.cj` |

## 5. 消息与死信

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 投递失败 | `DeadLetter` 进 eventStream | `subscribeDeadLetters` 回调；`deadLetterCount()` | 回调在发送方线程执行 | `semantics_test.cj`、`runtime/backpressure/backpressure_test.cj` |
| 未处理消息 | `UnhandledMessage`，属于 AllDeadLetters | 相同：以 `UnhandledMessage` 交给死信订阅者；`unhandledCount()` 单独计数 | — | `supervision_layers_test.cj`（unhandledMessagesReachDeadLetterSubscribers） |
| 死信日志 | `log-dead-letters = 10`，之后暂停 | 相同：每 5 分钟前 10 条（`CACTOR_LOG_DEAD_LETTERS`） | — | （日志限速，无专门用例） |
| 类型化协议之外的消息 | 编译期不可能（`ActorRef[T]`） | 运行期限速 WARN + `UnhandledMessage` | 仓颉没有 Scala 的路径相关类型，`childAs` 是未检查转换 | `supervision_layers_test.cj` |
| 有界邮箱满 | 丢弃进死信（BoundedMailbox 可阻塞） | `DropNew` 计死信 / `Fail` 抛异常 / `Block(timeout)` 外部生产者等待 | Actor 内发送不阻塞（直接按超时计死信），避免占住亲和线程 | `runtime/scheduling/starvation_and_block_test.cj`、`backpressure_test.cj` |
| 运行时生成的消息 | `Terminated` 等不受容量限制 | 相同 | — | `death_watch_test.cj` |

## 6. ask

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 类型化，Actor 外 | `ref.ask(replyTo => Req(replyTo))` → Future | `TypedAsk.ask<Req, Res>(ref, factory, timeout)` → `TypedPromise` | — | `runtime/typed_semantics/typed_semantics_test.cj` |
| 类型化，Actor 内 | `context.ask(target, factory)(mapResponse)` | `ctx.ask(target, factory, timeout) { outcome => ... }` | — | `typed_semantics_test.cj` |
| 经典 | `Patterns.ask`：临时 Actor 作发送方，目标照常回复 | `AskInbox.ask(target, message)`：收件箱作发送方 | — | `dsl/ask_inbox_test.cj` |
| 超时 | 定时器准时失败 | 相同：`TypedAskTimeoutException` | — | `patterns/ask_timeout_test.cj` |
| Future 组合 | map / flatMap / recover / sequence | `TypedPromise.map/flatMap/recover/recoverWith`、`TypedPromises.sequence` | — | `runtime/typed_alignment/promise_combinators_test.cj` |
| Actor 内阻塞等待 | 不推荐（`Await` 会占线程） | 直接抛 `IllegalStateException` | 默认调度器是亲和调度、没有工作窃取，阻塞就可能死锁，因此禁止而不是警告 | `typed_alignment_test.cj`、`ask_inbox_test.cj` |

## 7. 路由与发现

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 池 / 组路由器 | `Routers.pool` / `Routers.group`，路由器本身是 Actor | 相同（经典与类型化） | 没有 Resizer，只有手动 `AdjustPoolSize` | `patterns/router_actor_test.cj`、`runtime/typed_routing/typed_router_test.cj` |
| 转发保留发送方 | 是 | 是 | — | `router_actor_test.cj` |
| 目标终止 | 自动摘除；池空时路由器停止 | 相同 | — | `router_actor_test.cj`、`typed_router_test.cj` |
| 一致性哈希 | 哈希环 + 虚拟节点 | Rendezvous（HRW）哈希 | 不需要虚拟节点即均匀，减员只迁移被删目标的键 | `patterns/routing_strategy_test.cj` |
| Receptionist | 注册 / 订阅，订阅者死亡自动摘除 | 相同（单机） | 集群版未实现（计划 E63） | `typed_router_test.cj` |

## 8. 调度

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 阻塞操作 | 放到独立的 blocking dispatcher | 相同：`blocking` 调度器按空闲线程数扩容 | — | `runtime/scheduling/blocking_pool_test.cj` |
| 饥饿检测 | `akka.actor.starvation-detector`（告警） | 相同：超过 `CACTOR_STARVATION_WARN_MS` 告警并点名 Actor | 只告警，不迁移（没有工作窃取） | `starvation_and_block_test.cj` |

## 9. 远程（E41）

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 可靠投递 | Reliable Delivery：序号、确认、重发，按序去重 | `tellReliable`：序号 + 累计 ACK + 定时重发，接收端去重、乱序缓存 | 不需要显式的 ProducerController / ConsumerController | `remote_semantics/remoting_test.cj`（30% 丢包 2000 条恰好一次、按序） |
| 消费端重启 | 重发未确认的消息（跨重启至少一次） | 相同：ACK 带接收端启动标识，变化后未确认消息在新通道从 1 重新编号重发 | — | `remote_semantics/system_message_reliability_test.cj`（reliableDeliveryContinuesAfterReceiverRestart） |
| 系统消息的可靠性 | watch / unwatch / DeathWatchNotification 走可靠的系统消息通道 | 相同：WATCH / UNWATCH / DEATH 走可靠通道 | — | `system_message_reliability_test.cj`（remoteWatchSurvivesPacketLoss：30% 丢包下 20 个 watch 全部收到 `Terminated`） |
| 对端重启 | 新 uid 握手后隔离旧 uid，旧实例上被监视的 Actor 收到 `Terminated` | 相同（E7′）：握手、心跳回应、ACK 都带启动标识，变化即隔离旧实例并投递 `Terminated` | — | `system_message_reliability_test.cj`（remoteWatchDetectsNodeRestartWithinHeartbeatPause） |
| 缓冲上限 | `system-message-buffer-size = 20000` | `maxUnacked = 20000`：满了丢弃新消息并计数；接收端乱序缓冲同一上限 | Akka 系统消息缓冲溢出会隔离对端；CActor 只丢新消息 | `system_message_reliability_test.cj`（fullChannelDropsNewMessagesAndCounts） |
| 超时放弃 | `give-up-system-message-after = 6h` 后隔离对端 | 相同：`giveUpAfter = 6h`，放弃发往该节点的全部消息并隔离，监视者收到 `Terminated` | — | `system_message_reliability_test.cj`（reliableChannelGivesUpAndNotifiesWatchers） |
| 节点不可达 | Phi Accrual 心跳，不可达时监视者收到 `Terminated`，对端被隔离 | 相同（E7′ 起隔离）；默认心跳 1 s、容忍 10 s、φ 阈值 10 | — | `remoting_test.cj`（testRemoteWatchNodeFailure） |
| 隔离（quarantine） | 被隔离的 uid 永远不能再通信，误判不可撤回，对端须重启 | 相同：握手拒绝该 uid、断开其入站连接、出站握手拒绝其应答；`quarantine(address)` 可手动隔离 | — | `remote_semantics/remote_security_test.cj`（quarantinedPeerIsCutOffUntilItRestarts、misjudgedNodeStaysQuarantined） |
| 鉴权 | 依赖 TLS（早期 remoting 有 secure-cookie） | 握手 HMAC-SHA256 共享密钥（`withSecureCookie`），双向校验，时间戳窗口 ±5 分钟 | 可与 TLS（E65）叠加；只用共享密钥时，能截获流量的攻击者可在窗口内重放 HELLO | `remote_security_test.cj`（wrongOrMissingCookieCannotReachTheNode、forgedDeathIsRejected）、`unit/cluster/transport_isolation_test.cj`（forgedJoinWithoutCookieIsRejected） |
| 传输加密（TLS / mTLS） | Artery `tls-tcp` 传输：`ConfigSSLEngineProvider` 读 key-store / trust-store，`require-mutual-authentication = on`，`hostname-verification = off` | 相同（E65）：`RemoteTls`（stdx.net.tls，TLS 1.3）默认双向认证、只信任配置的 CA、不校验主机名；TCP 连上后先 TLS 握手，远程层握手与消息都在加密流上，明文连接与他 CA 证书被断开 | 证书以 PEM 配置（不是 JKS / PKCS#12）；只支持 TLS 1.3；没有证书热更新与 `RotatingKeysSSLEngineProvider` | `remote_semantics/remote_tls_test.cj`（plaintextConnectionsAreRejected、certificatesFromAnotherCaAreRejected、trafficOnTheWireIsEncrypted 等 6 例）、`unit/cluster/cluster_tls_test.cj`、`tools/cluster_demo.sh --tls` |
| 发送是否阻塞 | `tell` 只入出站队列；连接由 Association 的出站流建立，失败后退避重连 | 相同（E40）：每个目标地址一个关联，写线程负责连接与写入 | — | `foundation/network/association_test.cj`（sendToBlackHoleNeverBlocksTheCaller：发往黑洞地址的 send P99 < 50 ms） |
| 出站队列满 | 丢弃并记日志（`outbound-message-queue-size`） | 相同：普通 64 MB、控制 4 MB，满了丢新帧并计数 | 按字节而不是按条数限 | `association_test.cj` |
| 控制消息通道 | 心跳与系统消息走独立的 control 流，不排在大消息后面 | 优先通道：控制帧插在普通数据的段间写出 | 与普通数据共用一条 TCP 连接（Akka 是独立的 control 流） | `association_test.cj`（priorityFramesOvertakeNormalBacklog）、`unit/cluster/transport_isolation_test.cj`（黑洞地址的伪造心跳不让健康成员被误判） |

## 10. 集群成员、分片与分布式数据（E7′、E12′、E13′、E42、E43）

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 节点实例身份 | `UniqueAddress` = 地址 + longUid，成员按实例区分 | 相同：成员记录带启动标识（`Member.uid` / `uniqueAddress()`），可达性表按实例区分，旧实例的行不能压过新实例 | 心跳报文不带 uid（远程层握手已按 uid 隔离旧实例） | `cluster: membership/gossip_merge_test.cj`（reachabilityRowsFollowTheCurrentInstance） |
| 同一地址重启后加入 | 新实例 Join 时旧实例被 down，移除后新实例才被接纳 | 相同：旧实例 down → Leader 移除 → 新实例 Joining → Up（新 upNumber）；被替换的旧实例记入墓碑，迟到的旧视图不能复活它 | 无（E12′ 起墓碑带移除时间，`pruneTombstonesAfter` 默认 24 h 后裁剪，同 Akka） | `unit/cluster/incarnation_test.cj`（restartedMemberRejoinsAsNewIncarnation）、`gossip_merge_test.cj`（staleViewCannotResurrectReplacedInstance） |
| 被移除的成员重新加入 | 同一 `UniqueAddress` 不允许 | 相同；同一地址的新实例可以 | — | `incarnation_test.cj`（restartedFirstSeedRejoinsAfterItWasRemoved） |
| 首个种子启动 | 先向其他种子发 InitJoin，全部 Nack 或 `seed-node-timeout`（5 s）内无回应才自己组成集群 | 相同：JOIN / NACK / WAIT，`seedNodeTimeout` 默认 5 s；首个种子重启后加入原集群 | 没有单独的 InitJoin 报文，JOIN 兼作探测 | `incarnation_test.cj`（freshSeedsFormOneClusterWithoutWaitingForTimeout、firstSeedFormsAfterTimeoutWhenOtherSeedsAreDown、restartedFirstSeedWaitsForOldInstanceInsteadOfFormingIsland） |
| CRDT 的节点键 | `UniqueAddress`；移除节点的分量由 pruning 并入其他节点 | 默认"地址#启动标识"：重启后的写入不被旧分量吞掉；移除后的分量由 pruning 并入（E64，见下两行） | — | `incarnation_test.cj`（crdtWritesAfterSameAddressRestartAreKept） |
| 加入时的配置兼容检查 | `JoinConfigCompatChecker`：加入时比对配置，不兼容的节点被拒绝 | 相同：JOIN 携带兼容键（内置分片哈希、移交协议版本 + `withJoinCompatibility`），不一致回 REJECT；不带兼容信息的旧版本同样被拒 | 比对的是显式声明的键，不是整份配置 | `unit/cluster/sharding_handoff_test.cj`（incompatibleNodeIsRejectedAndNeverHostsEntities、legacyJoinWithoutCompatibilityIsRejected、incompatibilityIsReportedPerKey） |
| 分片移交 | 协调者发 BeginHandOff → 各区域确认 → 旧区域停止实体 → ShardStopped → 分配给新区域 | 无协调者的许可式移交：新拥有者向每个可能托管的成员发 BEGIN，对方实体全部终止后回 STOPPED，收齐才创建实体；任何时刻最多一个实例 | 分片归属由 rendezvous 哈希决定（不需要协调者单例与其状态存储），没有按负载的再平衡策略 | `sharding_handoff_test.cj`（singleActivationWhileMembersJoinAndLeave、shardsAreHostedOnlyAfterAcquisition） |
| 成员不可达时的分片 | 只影响分配在不可达区域上的分片 | 相同：只缓冲拥有者不可达的分片 | — | `sharding_handoff_test.cj`（blackholedNodeDoesNotStallOtherShards） |
| 无法投递的分片消息 | 区域缓冲满时丢弃并记死信 | 相同；节点离开集群后的消息也进死信 | — | `sharding_handoff_test.cj`（messagesAfterLeavingGoToDeadLetters） |
| 读取本地值 | `Get` 得到 `GetSuccess`，携带的数据不可变，可交给任意线程；`Update` 的 modify 在 Replicator 内执行 | 相同：`Replicator.get` 直接返回存储里的不可变值（O(1)），之后的更新生成新实例，不影响已取得的值；modify 在 Replicator Actor 线程、锁外执行，返回类型不同则拒绝 | 只有 WriteLocal / ReadLocal 一致性 | `unit/cluster/crdt_concurrency_test.cj`（testGetReturnsImmutableSnapshot、testConcurrentReadsDuringLocalUpdates） |
| 被移除成员的记录 | Removed 成员从 gossip 删除，只留墓碑；墓碑 `prune-gossip-tombstones-after`（24 h）后裁剪 | 相同（E12′）：记录删除、只留带移除时间的墓碑，合并取较大时间，各节点按同一时间裁剪；故障检测器只为当前成员建立 | — | `unit/cluster/cluster_tools_test.cj`（removedMemberIsPurgedAndTombstonePruned、heartbeatsFromNonMembersCreateNoDetectors）、`gossip_merge_test.cj`（removedRecordsAreNotKeptAndTombstoneTimesMergeByMax） |
| 移除被 down 的成员 | 隔离其 `UniqueAddress`，僵尸进程不能再通信 | 相同（E12′）：按 uid 隔离，入站连接关闭；同址新实例不受影响 | — | `cluster_tools_test.cj`（downedMemberIsQuarantinedOnRemoval） |
| 经分片 / 单例的 ask | `EntityRef.ask`、单例 proxy 透传 sender，实体 / 单例回复到发起方 | 相同（E12′）：信封携带 sender，跨节点时编码为远程回复地址，回复直接回到发起方（多跳也不经中间节点）；`ask` 返回 `TypedPromise` | — | `cluster_tools_test.cj`（askThroughShardingRoundTrips、askThroughSingletonRoundTrips） |
| 远程回复地址 | 回复按 actor path + uid 解析，发给已死实例的回复进死信 | 本地 Actor 作为远程发送方时登记 `$ref<路径>#<序号>` 入口，按实例区分、终止后注销 | 用登记表代替 path 解析；AskInbox 这类无生命周期的引用按空闲时间回收 | `cluster_tools_test.cj`（repliesReachActorRecreatedAtSamePath、replyEntriesDoNotGrowAfter100kReplies） |
| 单例移交 | 新的最老成员发 HandOverToMe，旧的停止实例（terminationMessage 默认 PoisonPill）后回 HandOverDone，之后才启动 | 相同（N16）：向其他每个 Up / Leaving / Exiting 成员发 HANDOVER_TO_ME，实例处理完邮箱、终止后回 DONE；已 down 的成员不用等，正常离开后移除的成员最多等 10 s | 没有协调者状态，许可式（同 E13′） | `unit/cluster/cluster_distribution_test.cj`（handOverWaitsForPreviousInstanceToStop、crashedOldestDoesNotBlockHandOver） |
| 集群 Receptionist | 登记表经 Distributed Data 复制；`Listing.serviceInstances` 只含可达节点，`allServiceInstances` 含全部；实例停止、节点移除自动注销 | 相同：登记表是 ORSet（"地址#uid\|暴露名"），`find` / `findAll` 对应两种 Listing；监视者 Actor 注销停止的实例，节点被 down / 移除后各节点删除它的登记 | 经典 `ActorRef` 接口（未提供 ServiceKey&lt;T&gt; 的类型化远程引用） | `cluster_distribution_test.cj`（registerFindAndCallAcrossNodes、stoppedOrDeregisteredInstancesDisappear、crashedNodeRegistrationsAreRemovedAndSubscribersNotified） |
| 分布式数据 delta 传播 | delta-CRDT：更新只传变化部分，按（来源, 键）序号因果有序；缺序号时回退为整值 | 相同（E64）：计数器传变化分量、ORSet 传新增 / 删除的点；只应用恰好是下一个序号的增量，断档时向来源请求整值，每秒重试最多 5 次，之后由摘要反熵补齐 | Akka 的增量经 gossip 批量发送、断档时回 DeltaNack；这里每次更新立即推送、请求带重试 | `ddata_delta_pruning_test.cj`（concurrentUpdatesConvergeThroughDeltasAlone、deltasAreFarSmallerThanFullState、sequenceGapsAreRepairedFromTheOrigin） |
| 移除节点的 pruning | Leader 发起 PruningInitialized，全部成员 seen 后 owner 执行（分量并入 owner），PruningPerformed 保留 `pruning-marker-time-to-live`（6 h），期间合并先清理；`max-pruning-dissemination` 后才发起；Leader 从数据中收集已移除节点 | 相同（E64）：标记随整值传播、合并（同一发起者合并 seen，不同发起者取较小者，已执行胜出）；发起者不要求仍是 Leader，离开后由 Leader 接管；被移除节点迟到的增量丢弃；Leader 只在视图收敛时发起 | 只对 "地址#uid" 形式的节点标识扫描；没有持久化键的单独 TTL | `ddata_delta_pruning_test.cj`（removedNodeDataIsCollapsedWithoutChangingValues、pruningWaitsUntilEveryMemberHasSeenTheMarker、staleReplicaRejoiningDoesNotResurrectPrunedData、lateUpdatesFromAPrunedNodeAreIgnored、leaderFindsNodesThatAreNoLongerMembersInTheData、clusterBindingPrunesANodeThatLeft） |
| 分布式数据一致性级别 | WriteLocal / WriteMajority / WriteAll，ReadLocal / ReadMajority / ReadAll；超时失败但本地已写入 | 相同：过半 = ⌊n/2⌋+1（n = 本节点 + 对等节点），读到的合并值写回本地；返回 `TypedPromise` | 没有 `minCap`、`WriteTo(n)` | `cluster_distribution_test.cj`（majorityWriteThenMajorityReadSeesValue、majorityFailsOnMinoritySide） |
| 订阅确认 | `Subscribe` 后收到 `SubscribeAck`，订阅表由 mediator 串行维护 | 相同（E12′）：`subscribeAsync` 在本地生效后完成；节点集合的增删由主题 Actor 串行发出 | 节点集合经 Replicator（ORSet）同步，而不是 mediator 之间的 gossip | `cluster_tools_test.cj`（resubscribeRightAfterLastUnsubscribeKeepsNode） |
| 虚拟 Actor 的可重入（Orleans 语义） | Orleans：默认非可重入，A→B→A 死锁至超时；`[Reentrant]` 允许交错 | 相同（E12′）：`Grain.reentrant()`；`onCallAsync` 的结果未完成期间不占线程（不可重入时后续调用排队） | Akka 无对应概念 | `cluster_tools_test.cj`（reentrantCallChainCompletes、callAsyncIssuesManyCallsWithoutBlocking、blockingCallInsideGrainFailsFast） |

## 11. 持久化（E8′）

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 单写者 | 日志按 `persistenceId` 单写者；写者状态过期时写入失败 | `append(expectedSeq)`：期望序号与日志不符即拒绝（`JournalConflictException`），Actor 交给监督重启后恢复 | Akka 依赖集群分片保证单写者、日志插件一般不校验；CActor 在日志层再校验一次 | `integration/persistence/journal_semantics_test.cj`（secondWriterForSameIdIsRejectedAndRecovers）、`persistence: journal_v2_test.cj`（appendRejectsStaleWriter） |
| 序号与重放 | 每个事件带 `sequenceNr`，恢复按序号重放快照之后的事件 | 相同：序号写进记录，`replay(from, to)` 闭区间 | — | `journal_v2_test.cj`（deleteToKeepsHighestSequenceNr）、`journal_semantics_test.cj`（pluginReplaysBySequenceNotTimestamp） |
| 删除事件 | `deleteMessages(toSequenceNr)`，最高序号保留 | 相同：`deleteTo` / `deleteEventsTo` | — | `journal_semantics_test.cj`（persistAllAndDeleteToSurviveRestart） |
| 持久化确认 | 回调在日志确认写入之后执行（LevelDB 插件默认 fsync） | 相同：`append` 返回时已 fsync；批量与并发写入合并 fsync | 同步调用，不是异步回调 | `journal_v2_test.cj`（batchIsOneFsyncAndConcurrentWritersShareFsyncs）、`tools/journal_crash_demo.sh` |
| 残缺与损坏 | 依插件而定 | 尾部残缺（含零填充）截断；中间损坏拒绝恢复 | — | `journal_v2_test.cj`（zeroFilledTailIsTruncated、midFileCorruptionIsRefusedNotTruncated） |
| 标签 | `Tagged` + `eventsByTag` | `withTags` + `currentEventsByTag`（有限流） | 没有实时的 eventsByTag | `journal_v2_test.cj`（tagsSurviveAndCanBeQueried） |
| 启动 / 重启时恢复 | 先重放快照与事件，再处理命令 | `AbstractPersistentActor` 在 `preStart` 恢复，重启后的新实例同样恢复 | — | `unit/persistence/persist_failure_semantics_test.cj`（handlerFailureRestartsAndReplaysPersistedEvents） |
| 事件处理器抛异常 | 事件已落库；异常交给监督（经典默认重启，重启后重放） | 相同：抛 `EventHandlerException`，序号不回退 | — | `persist_failure_semantics_test.cj`（handlerFailureDoesNotRollBackPersistedSequence） |
| 写入失败（版本冲突） | `onPersistFailure`，Actor 停止 | 抛 `PersistenceException`，序号回退，交给监督 | 不强制停止 | `persist_failure_semantics_test.cj`（storeConflictRollsBackAndIsNotAHandlerFailure、persistAllReportsStoreConflict） |
| 恢复失败 | `onRecoveryFailure`，Actor 停止 | 抛 `PersistenceException`；在 `preStart` 中即启动失败，默认停止 | — | （无专门用例） |
| 恢复期间的命令 | 暂存，恢复完成后处理 | 恢复在 `preStart` 中同步完成，期间不处理命令 | — | 同上 |

## 12. 类型化持久化（E44）

测试路径相对 `modules/tests/src/`；`typed:` 指 `integration/persistence/typed_persistence_test.cj`。

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 命令 → 效果 | 命令处理器返回 `Effect`，`thenRun` / `thenReply` 在事件落盘并应用之后执行 | 相同：`EffectFactory` 创建效果，副作用拿到新状态 | 效果工厂是命令处理器的第三个参数（仓颉不能从返回位置推断泛型） | `typed:`（thenReplyRunsAfterEventIsPersisted） |
| 写入期间的命令 | 暂存，写入确认后按顺序处理 | 相同；每条命令看到之前全部事件落盘后的状态 | — | `typed:`（commandsDuringPersistAreProcessedInArrivalOrder） |
| 恢复 | 快照 + 之后的事件，完成后 `RecoveryCompleted`，恢复期间命令暂存 | 相同；日志读在持久化 IO 线程（对标 plugin-dispatcher），事件处理器在 Actor 线程 | 另校验序号连续，缺口即恢复失败 | `typed:`（counterRecoversAcrossSystemRestart、journalGapWithoutCoveringSnapshotFailsRecovery） |
| 恢复失败 | `RecoveryFailed` 信号后停止 | 相同（日志损坏、事件处理器抛异常） | — | `typed:`（corruptJournalFailsRecoveryAndStopsActor、eventHandlerFailureDuringReplayFailsRecovery） |
| 写入失败 | 停止；`onPersistFailure(backoff)` 重启 | Actor 以异常失败，默认停止；`supervise(...).onFailure(restartWithBackoff)` 重启并重新恢复 | 通过普通监督配置，不是专门的 `onPersistFailure` | `typed:`（journalConflictFailsActorAndRestartRecovers） |
| 暂存溢出 | `StashOverflowStrategy` 默认丢弃（发布 `Dropped`），可配置失败 | 默认丢弃，作为未处理消息交给死信订阅者；`FailActor` 失败 | 不是专门的 `Dropped` 事件 | `typed:`（stashOverflowDropsCommandsAsUnhandled、stashOverflowCanFailActor） |
| 用户 stash | `Effect.stash()` / `thenUnstashAll()` | 相同 | — | `typed:`（userStashAndUnstashAllKeepOrder） |
| 保留策略 | `RetentionCriteria.snapshotEvery(n, keep)`、`withDeleteEventsOnSnapshot` | 相同；删除上界 = 当前序号 − keep × n | 在途期间到达的快照点只保留最新一个 | `typed:`（retentionKeepsRecentSnapshotsAndDeletesOldEvents） |
| 事件适配 | `EventAdapter` / `EventSeq`（0..n 个事件） | `EventAdapter.fromJournal` 返回数组 | — | `typed:`（eventAdapterEvolvesOldSchema） |
| 重启后旧的 IO 结果 | 旧实例的回复不影响新实例 | 实例 token 过滤旧实例的完成信号 | — | `typed:`（staleIoCompletionFromPreviousIncarnationIsIgnored） |
| 持久状态删除 | `DeleteRevision`：修订号继续递增 | 相同：墓碑修订号 +1，旧写者被拒绝 | — | `typed:`（durableStatePersistsDeletesAndRecovers、deletedStateRejectsStaleWriter） |

## 13. 已知差异汇总

- **类型安全的边界**：类型化引用的协议类型在 `childAs` / lookup 处无法检查（仓颉没有路径相关类型），用错时运行期告警而不是编译期报错。
- **子 Actor 快路径**：`ctx.actorOf` 是接口虚调用，子 Actor 的初始行为以 `setup` 开头才走用户包的快路径（性能差异，不影响语义）。
- **未实现**：ChildFailed 信号、SpawnProtocol、Resizer、withMdc（计划 E63）；按负载的分片再平衡；实时的 `eventsByTag`；类型化持久化的 `onPersistFailure` 专门配置与 `Dropped` 事件（用普通监督与未处理消息代替）。
