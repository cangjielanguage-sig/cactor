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
| 对端重启 | 新 uid 握手后隔离旧 uid，旧实例上被监视的 Actor 收到 `Terminated` | 心跳回应带启动标识，变化后立即投递 `Terminated` | 没有握手与隔离（quarantine）：不拒绝旧 uid 的迟到消息（计划 E7′） | `system_message_reliability_test.cj`（remoteWatchDetectsNodeRestartWithinHeartbeatPause） |
| 缓冲上限 | `system-message-buffer-size = 20000` | `maxUnacked = 20000`：满了丢弃新消息并计数；接收端乱序缓冲同一上限 | Akka 系统消息缓冲溢出会隔离对端；CActor 只丢新消息 | `system_message_reliability_test.cj`（fullChannelDropsNewMessagesAndCounts） |
| 超时放弃 | `give-up-system-message-after = 6h` 后隔离对端 | `giveUpAfter = 6h`：放弃发往该节点的全部消息，监视者收到 `Terminated` | 放弃后不隔离，之后的消息建新通道（隔离归 E7′） | `system_message_reliability_test.cj`（reliableChannelGivesUpAndNotifiesWatchers） |
| 节点不可达 | Phi Accrual 心跳，不可达时监视者收到 `Terminated` | 相同 | — | `remoting_test.cj`（testRemoteWatchNodeFailure） |
| 发送是否阻塞 | `tell` 只入出站队列；连接由 Association 的出站流建立，失败后退避重连 | 相同（E40）：每个目标地址一个关联，写线程负责连接与写入 | — | `foundation/network/association_test.cj`（sendToBlackHoleNeverBlocksTheCaller：发往黑洞地址的 send P99 < 50 ms） |
| 出站队列满 | 丢弃并记日志（`outbound-message-queue-size`） | 相同：普通 64 MB、控制 4 MB，满了丢新帧并计数 | 按字节而不是按条数限 | `association_test.cj` |
| 控制消息通道 | 心跳与系统消息走独立的 control 流，不排在大消息后面 | 优先通道：控制帧插在普通数据的段间写出 | 与普通数据共用一条 TCP 连接（Akka 是独立的 control 流） | `association_test.cj`（priorityFramesOvertakeNormalBacklog）、`unit/cluster/transport_isolation_test.cj`（黑洞地址的伪造心跳不让健康成员被误判） |

## 10. 持久化（E8′ 部分）

| 语义 | Akka | CActor | 差异 | 测试 |
|---|---|---|---|---|
| 启动 / 重启时恢复 | 先重放快照与事件，再处理命令 | `AbstractPersistentActor` 在 `preStart` 恢复，重启后的新实例同样恢复 | — | `unit/persistence/persist_failure_semantics_test.cj`（handlerFailureRestartsAndReplaysPersistedEvents） |
| 事件处理器抛异常 | 事件已落库；异常交给监督（经典默认重启，重启后重放） | 相同：抛 `EventHandlerException`，序号不回退 | — | `persist_failure_semantics_test.cj`（handlerFailureDoesNotRollBackPersistedSequence） |
| 写入失败（版本冲突） | `onPersistFailure`，Actor 停止 | 抛 `PersistenceException`，序号回退，交给监督 | 不强制停止 | `persist_failure_semantics_test.cj`（storeConflictRollsBackAndIsNotAHandlerFailure、persistAllReportsStoreConflict） |
| 恢复失败 | `onRecoveryFailure`，Actor 停止 | 抛 `PersistenceException`；在 `preStart` 中即启动失败，默认停止 | — | （无专门用例） |
| 恢复期间的命令 | 暂存，恢复完成后处理 | 恢复在 `preStart` 中同步完成，期间不处理命令 | — | 同上 |

## 11. 已知差异汇总

- **类型安全的边界**：类型化引用的协议类型在 `childAs` / lookup 处无法检查（仓颉没有路径相关类型），用错时运行期告警而不是编译期报错。
- **子 Actor 快路径**：`ctx.actorOf` 是接口虚调用，子 Actor 的初始行为以 `setup` 开头才走用户包的快路径（性能差异，不影响语义）。
- **未实现**：ChildFailed 信号、SpawnProtocol、集群版 Receptionist、Resizer、withMdc（计划 E63）；远程握手与隔离（quarantine）、TLS（计划 E7′、E65）；类型化持久化 `EventSourcedBehavior`（计划 E44）。
