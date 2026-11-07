# 📝 CActor 更新日志

本文档记录了 CActor 项目的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

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
- **`AbstractPersistentActor`（E8′ 的 P3 / P4 部分）**：
  - `preStart` 自动从存储恢复（重启后的新实例同样恢复）；已手动调用 `startRecovery()` 的不重复恢复。
    此前没有恢复钩子：重启后序号为 0，首次 persist 即与存储冲突，在默认监督下无限重启。
    依赖"启动时是空状态"的子类需要改写；重写 `preStart` 时请调用 `super.preStart()`。
  - 事件已落库后处理器抛异常：抛 `EventHandlerException`（`PersistenceException` 的子类，原来的 catch 仍能捕获），
    **序号不再回退**；默认监督重启后由恢复重放该事件。此前回退序号，之后每次 persist 都与存储冲突。
  - `persistAll` 消费存储返回的 Future：版本冲突抛 `PersistenceException`（此前静默吞掉、序号照样前进）。
  - 恢复失败抛 `PersistenceException`（此前只记日志、以残缺状态继续运行）；在 `preStart` 中即为启动失败，默认停止。
- **`@actor` 生成的分派顺序（E36，T13）**：兜底类型（`Message` / `Any`）的处理方法总是排在最后；
  首参为 `ActorContext` 的 `onXxx`（如 `onStarted(context)`）与 static 方法不再被当作消息处理方法。

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

*更多详细信息请查看 [GitHub Releases](../../releases)*
