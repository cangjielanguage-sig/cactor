/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - User Guardian实现
 * 用户级Guardian，管理用户创建的Actor
 * 从Core层移动到Runtime层
 */
protected package cactor_runtime.guardian

import std.sync.*
import std.collection.*
import std.collection.concurrent.*
import std.time.*
import cactor_core.actor.{Actor, ActorRef, Props, ActorPath, ActorSystemView, UnhandledMessage}
import cactor_foundation.logging.{Logging, LogRateLimiter}
import cactor_core.message.{Message, MessageResult, AutoReceivedMessage}
import cactor_core.context.{ActorContext}
import cactor_core.supervision.{SupervisionStrategy, DefaultSupervisionStrategy, SupervisionDirective, ActorFailure}
import cactor_core.system.{ActorSystem}
import cactor_core.typed.{TypedPayloadBox}
import cactor_core.guardian.{GuardianType}

// gracefulStop 等待终止的分段长度：正常由终止方唤醒，分段只是兜底
let TERMINATION_RECHECK: Duration = Duration.millisecond * 50

/**
 * Runtime层的用户Guardian实现
 * 管理用户创建的Actor
 */
public class RuntimeUserGuardian <: BaseGuardianActor {
    private let actorSystem: ActorSystem
    private let userActors: HashMap<String, ActorRef>
    private let defaultSupervisionStrategy: SupervisionStrategy
    private let actorPathPrefix: String
    // 顶层 Actor 只由 userActors（名字 → 引用）承载，不再额外维护"完整路径 → Cell"的第二张表：
    // 顶层路径恒为 "${actorPathPrefix}/<名字>"，因此 lookup 的首段、liveCells 的顶层集合、
    // 停止注销都可以直接由 userActors 推出。原先的第二张表在百万级顶层 Actor 时
    // 每个 Actor 多占一份完整路径 String + 一个表项（实测合计约 500–900 B）。
    // 子 Actor 由父的 childMap 承载，lookup 继续按段遍历。
    // 死信订阅者列表的写锁（读走不可变快照，无需加锁）
    private let deadLetterLock: Mutex = Mutex()
    // 存活 Cell 总数（含子 Actor），O(1) 读取，替代遍历
    private let liveCellTotal: AtomicInt64 = AtomicInt64(0)
    // 死信监听器（写时复制）
    private var deadLetterListeners: Array<(Message, String) -> Unit> = Array<(Message, String) -> Unit>()
    private let deadLetterTotal: AtomicInt64 = AtomicInt64(0)
    // 未处理消息（Unhandled / 类型化协议之外的消息）的累计数：与死信分开计——死信是"没有送达"，
    // 这里是"送达了但没处理"，监控与测试对两者的语义要求不同
    private let unhandledTotal: AtomicInt64 = AtomicInt64(0)
    // 死信 / 未处理消息的 INFO 日志限速（对标 Akka log-dead-letters = 10，暂停 5 分钟）：
    // 此前死信只在 DEBUG 逐条记、未处理消息完全不记，默认日志级别下误投与协议错误静默消失。
    // CACTOR_LOG_DEAD_LETTERS 设每个窗口记几条，0 关闭
    private let letterLog: LogRateLimiter = LogRateLimiter(envInt("CACTOR_LOG_DEAD_LETTERS", 10, 0), Duration.minute * 5)
    // 顶层 Actor 自行停止（PoisonPill、MessageResult.Stop、监督 Stop 等）时通知运行时清理注册信息
    private var topLevelTerminatedHook: Option<(ActorRef) -> Unit> = None<(ActorRef) -> Unit>
    private let log = Logging.logger("cactor.guardian")
    // Actor 中 context.system() 返回的视图，由运行时在启动时设置
    private var systemViewValue: Option<ActorSystemView> = None<ActorSystemView>
    // gracefulStop 等待目标终止：Cell 终止时只在有等待者时才加锁唤醒（百万级子 Actor 批量停止不付这份开销）。
    // 协议与 BoundedSlots 相同：等待者先 waiters+1 再检查，终止方先改状态再读 waiters
    private let terminationLock: Mutex = Mutex()
    private let terminationCond: Condition
    private let terminationWaiters: AtomicInt64 = AtomicInt64(0)

    public init(system: ActorSystem) {
        super(GuardianType.User, None<ActorRef>)  // 用户Guardian通常没有父Guardian
        this.actorSystem = system
        this.userActors = HashMap<String, ActorRef>()
        // 与 Akka 经典 API 相同：默认不限次数重启（旧默认值一分钟 3 次后停止，长期运行的 Actor 会因偶发故障被永久停止）
        this.defaultSupervisionStrategy = DefaultSupervisionStrategy.restartAlways()
        this.actorPathPrefix = "/user"
        this.terminationCond = synchronized(terminationLock) { terminationLock.condition() }
    }

    /** 顶层 Actor 的路径前缀（Cell 用它惰性拼出自己的完整路径） */
    func topLevelPathPrefix(): String {
        actorPathPrefix
    }

    /**
     * 创建用户Actor
     */
    public func createUserActor(props: Props<Actor>, name: String): ActorRef {
        // 验证用户Actor名称
        if (!isValidUserActorName(name)) {
            throw Exception("非法的用户Actor名称: ${name}")
        }
        // 名称检查、构造与登记保持在同一把锁内（避免并发同名 Actor）。
        // 试过"锁外构造、锁内登记"的并行化：仓颉的分配与 GC 不随线程数扩展
        // （见 docs/plan/20261031-protoactor-performance.md），并行构造只增加分配争用，
        // 实测顶层创建从 174k/s 掉到 93–124k/s，已回退。顶层创建 ≥200k/s 的目标
        // 需要降低每 Cell 的分配成本（结构化改造），不是锁的问题。
        synchronized(guardianLock) {
            if (userActors.contains(name)) {
                throw Exception("Actor名称已存在: ${name}")
            }
            let userActor = doCreateChild(props, name)
            userActors.add(name, userActor)
            childCounter.fetchAdd(1)
            // 只登记显式指定的策略；查找时缺省回退到 defaultSupervisionStrategy，避免每个 Actor 多一个映射条目
            if (let Some(strategy) <- props.getSupervisorStrategy()) {
                supervise(userActor, strategy)
            }
            return userActor
        }
    }

    /**
     * 停止 Actor（对标 Akka `system.stop` / 监督 Stop，E33）：经系统消息投递，处理完当前消息即停，
     * 邮箱中剩余的消息计为死信；名字与登记保留到真正终止（onCellTerminated 注销），终止前同名 actorOf
     * 报"名称已存在"。顶层与子 Actor 都适用。重复调用无害。
     * 此前先摘除登记、再在已入队的消息之后停止：停止期间可以同名重建（新旧实例短暂共存），
     * 被监督停止的失败 Actor 还要先把积压消息处理完；对子 Actor 调用则静默无效。
     * @return 该 Actor 此刻是否登记为本 Guardian 下的顶层 Actor
     */
    public func removeUserActor(child: ActorRef): Bool {
        let managed = synchronized(guardianLock) {
            let path = child.path.getPath()
            let key = match (path.lastIndexOf("/")) {
                case Some(i) => path[(i + 1)..]
                case None => path
            }
            match (userActors.get(key)) {
                case Some(r) => r.equals(child)
                case None => false
            }
        }
        match (child) {
            // 系统消息不受有界邮箱容量限制（R7）
            case cell: SimpleUserActorRef => cell.sendSystem(SysTerminate())
            case _ =>
                if (managed) {
                    child.tell(StopMessage())
                }
        }
        managed
    }

    /**
     * 等待 Cell 终止并释放名字（gracefulStop 用）：只能在 Actor 之外调用。
     * 顶层 Actor 以"登记已注销"为准，子 Actor 以 Cell 终止为准
     */
    public func awaitTerminated(cell: SimpleUserActorRef, timeout: Duration): Bool {
        let deadline = MonoTime.now() + timeout
        terminationWaiters.fetchAdd(1)
        try {
            synchronized(terminationLock) {
                while (!terminatedAndReleased(cell)) {
                    let remaining = deadline - MonoTime.now()
                    if (remaining <= Duration.Zero) {
                        return false
                    }
                    // 分段等待只是兜底：正常由 onCellTerminated 唤醒
                    terminationCond.wait(timeout: if (remaining < TERMINATION_RECHECK) { remaining } else { TERMINATION_RECHECK })
                }
                return true
            }
        } finally {
            terminationWaiters.fetchAdd(-1)
        }
    }

    private func terminatedAndReleased(cell: SimpleUserActorRef): Bool {
        if (!cell.isTerminated()) {
            return false
        }
        if (cell.parentCell.isSome()) {
            return true
        }
        synchronized(guardianLock) {
            match (userActors.get(cell.getName())) {
                case Some(r) => !r.equals(cell)
                case None => true
            }
        }
    }

    private func notifyTerminationWaiters(): Unit {
        if (terminationWaiters.load() > 0) {
            synchronized(terminationLock) {
                terminationCond.notifyAll()
            }
        }
    }
    
    /**
     * 获取用户Actor
     */
    public func getUserActor(name: String): Option<ActorRef> {
        synchronized(guardianLock) {
            userActors.get(name)
        }
    }
    
    /**
     * 获取所有用户Actor
     */
    public func getAllUserActors(): Array<ActorRef> {
        synchronized(guardianLock) {
            let actorList = ArrayList<ActorRef>()
            for ((_, actorRef) in userActors) {
                actorList.add(actorRef)
            }
            return actorList.toArray()
        }
    }
    
    /**
     * 处理Actor失败
     */
    public func handleActorFailure(failure: ActorFailure): Unit {
        failureCounter.fetchAdd(1)
        let childPath = failure.actor.path.toString()
        let strategy = synchronized(guardianLock) {
            supervisionStrategies.get(childPath) ?? defaultSupervisionStrategy
        }
        let directive = strategy.decide(failure)
        executeSupervisionDirective(failure.actor, directive, failure.cause)
    }
    
    /**
     * 在 Actor Cell 内处理失败（与该 Actor 的消息处理串行）
     * Restart / Stop 立即生效，邮箱中尚未处理的消息由新实例处理或被丢弃
     */
    func handleFailureInCell(cell: SimpleUserActorRef, cause: Exception): Unit {
        failureCounter.fetchAdd(1)
        let key = cell.path.toString()
        let failure = cell.failureInfo(cause)
        let strategy = synchronized(guardianLock) {
            supervisionStrategies.get(key) ?? defaultSupervisionStrategy
        }
        // 决策执行用户代码（自定义 decider），异常按 Stop 处理，不能逃出 Cell 的消息处理
        let directive = try {
            strategy.decide(failure)
        } catch (e: Exception) {
            log.error("Actor ${cell.pathString} 的监督策略执行异常，停止: ${e}")
            SupervisionDirective.Stop
        }
        // 与 Akka 一致：每次失败都以 ERROR 级别记录（否则一直失败并重启的 Actor 完全看不出问题）
        log.error("Actor ${cell.pathString} 失败：${cause}，监督指令：${directiveLabel(directive)}")
        match (directive) {
            case SupervisionDirective.Resume => ()
            case SupervisionDirective.Restart =>
                restartCounter.fetchAdd(1)
                // E27：挂起后经本 Cell 的系统队列重启（不在失败调用栈里同步重启）
                cell.restartInCell(cause, Duration.Zero)
            case SupervisionDirective.RestartAfter(delay) =>
                // E27：Backoff 延迟重启——挂起（延迟期间不处理用户消息），到期只投递 SysRecreate。
                // 此前由定时器线程直接 doRestart，与 Cell 线程并发改写状态，且会让已停止的 Actor 复活（R2）
                restartCounter.fetchAdd(1)
                cell.restartInCell(cause, delay)
            case SupervisionDirective.Stop =>
                // 先停止（包括子 Actor），停止完成后由 onCellTerminated 清理登记信息
                cell.terminateInCell()
            case SupervisionDirective.Escalate =>
                // 顶层 Actor 没有可以上报的父 Actor：停止它
                log.error("顶层 Actor ${cell.pathString} 上报失败（${cause}），已停止")
                cell.terminateInCell()
        }
    }

    private static func directiveLabel(d: SupervisionDirective): String {
        match (d) {
            case SupervisionDirective.Resume => "Resume"
            case SupervisionDirective.Restart => "Restart"
            case SupervisionDirective.RestartAfter(delay) => "RestartAfter(${delay.toMilliseconds()}ms)"
            case SupervisionDirective.Stop => "Stop"
            case SupervisionDirective.Escalate => "Escalate"
        }
    }

    /**
     * 执行监督指令
     */
    private func executeSupervisionDirective(actor: ActorRef, directive: SupervisionDirective, cause: Exception): Unit {
        match (directive) {
            case SupervisionDirective.Resume =>
                // 保留 Actor 状态，继续处理后续消息
                ()

            case SupervisionDirective.Restart =>
                // 退避重启统一由策略返回 RestartAfter（E6′），不再由定时器线程直接 restartChild
                restartChild(actor, cause)

            case SupervisionDirective.RestartAfter(delay) =>
                // E6（2026-11-06）：Backoff 监督的真实延迟重启——延迟后重建实例
                restartCounter.fetchAdd(1)
                match (actor) {
                    // 不在目标 Cell 线程上：只投递系统消息，由目标自己的线程重启
                    case cell: SimpleUserActorRef => cell.requestRestart(cause, delay)
                    case _ => restartChild(actor, cause)
                }

            case SupervisionDirective.Stop =>
                removeUserActor(actor)

            case SupervisionDirective.Escalate =>
                escalateFailure(actor, cause)
        }
    }

    /**
     * 升级失败到系统Guardian
     */
    private func escalateFailure(actor: ActorRef, cause: Exception): Unit {
        log.error("顶层 Actor ${actor.path.toString()} 上报失败（${cause}），已停止")
        removeUserActor(actor)
    }
    
    /**
     * 验证用户Actor名称
     */
    private func isValidUserActorName(name: String): Bool {
        // 用户Actor名称不能以system-开头
        !name.startsWith("system-") && 
        !name.isEmpty() && 
        name.size <= 100 &&
        isValidActorNameCharacters(name)
    }
    
    /**
     * 验证Actor名称字符
     */
    private func isValidActorNameCharacters(name: String): Bool {
        // 简化实现：只检查长度和基本字符
        return name.size > 0 && name.size <= 100
    }
    
    // 实现抽象方法
    protected func doCreateChild(props: Props<Actor>, name: String): ActorRef {
        createCell(props, name, None<SimpleUserActorRef>)
    }

    /**
     * 创建 Cell：调用工厂创建实例并执行 preStart，然后登记路径。
     * preStart 抛出异常时创建失败，异常传给调用方。
     *
     * 路径不再作为参数传入：Cell 只保存名字，完整路径按需由"父链 + 名字"拼出（P1-11），
     * 顶层 Cell 则在这里按需拼一次用于登记全局路径表。
     */
    func createCell(props: Props<Actor>, name: String, parent: Option<SimpleUserActorRef>): SimpleUserActorRef {
        let actor = props.create()
        // preStart 改在 Cell 线程、onStarted 之前执行（E33）
        let cell = SimpleUserActorRef(name, actor, props.creator(), this, parent, props)
        // 置启动位并调度：保证 onStarted 先于任何用户消息执行（P1-17，替代 SysStarted 系统消息）
        cell.scheduleStart()
        if (parent.isSome()) {
            // 子 Actor 不进全局路径表：百万级子 Actor 时每个表项（路径 String + 条目）约 150 字节，
            // 且创建要串行过全局注册表；子 Actor 由父的 childMap 承载，lookup 按段遍历
            liveCellTotal.fetchAdd(1)
        } else {
            // 顶层 Actor 已由 createUserActor 写入 userActors，这里无需再登记路径
            liveCellTotal.fetchAdd(1)
        }
        cell
    }

    /** 默认监督策略（父 Actor 未指定时使用）：OneForOne，异常时重启 */
    public func defaultStrategy(): SupervisionStrategy {
        defaultSupervisionStrategy
    }

    /** 按路径查找存活的 Actor，例如 "/user/parent/child" */
    public func lookup(path: String): Option<ActorRef> {
        match (lookupCell(path)) {
            case Some(c) => Some<ActorRef>(c)
            case None => None<ActorRef>
        }
    }

    func lookupCell(path: String): Option<SimpleUserActorRef> {
        // 子 Actor：按路径分段从顶层向下遍历（父的 childMap 只含存活子 Actor）
        let prefix = "${actorPathPrefix}/"
        if (!path.startsWith(prefix)) {
            return None<SimpleUserActorRef>
        }
        let segs = path[(prefix.size)..].split("/", removeEmpty: true)
        if (segs.size == 0) {
            return None<SimpleUserActorRef>
        }
        // 顶层段直接查 userActors（顶层路径恒为 "/user/<名字>"，不再维护第二张路径表）
        var cur: SimpleUserActorRef
        let top = synchronized(guardianLock) { userActors.get(segs[0]) }
        match (top) {
            case Some(r) =>
                match (r as SimpleUserActorRef) {
                    case Some(c) => cur = c
                    case None => return None<SimpleUserActorRef>
                }
            case None => return None<SimpleUserActorRef>
        }
        for (i in 1..segs.size) {
            match (cur.childInternal(segs[i])) {
                case Some(r) =>
                    match (r as SimpleUserActorRef) {
                        case Some(c) => cur = c
                        case None => return None<SimpleUserActorRef>
                    }
                case None => return None<SimpleUserActorRef>
            }
        }
        if (cur.isTerminated()) {
            return None<SimpleUserActorRef>
        }
        Some(cur)
    }

    /** 当前存活的全部 Actor（含子 Actor）的快照（诊断用，递归遍历） */
    public func liveCells(): Array<SimpleUserActorRef> {
        let result = ArrayList<SimpleUserActorRef>()
        func collect(cell: SimpleUserActorRef): Unit {
            if (!cell.isTerminated()) {
                result.add(cell)
            }
            for (r in cell.childrenInternal()) {
                match (r as SimpleUserActorRef) {
                    case Some(c) => collect(c)
                    case None => ()
                }
            }
        }
        let tops = synchronized(guardianLock) { userActors.values().toArray() }
        for (r in tops) {
            match (r as SimpleUserActorRef) {
                case Some(c) => collect(c)
                case None => ()
            }
        }
        result.toArray()
    }

    /** 当前存活的 Actor 总数（含子 Actor） */
    public func liveCellCount(): Int64 {
        liveCellTotal.load()
    }

    /** Cell 完全停止后调用：注销路径；顶层 Actor 还要从 Guardian 中移除并通知运行时 */
    /**
     * 系统终止：立即停止全部顶层 Actor（各自先停止子 Actor 再执行 postStop），
     * 最多等待 timeout 让全部 Cell 终止；返回仍未终止的 Cell 数
     */
    public func terminateAll(timeout: Duration): Int64 {
        let tops = synchronized(guardianLock) {
            let l = ArrayList<ActorRef>()
            for ((_, r) in userActors) {
                l.add(r)
            }
            l.toArray()
        }
        for (r in tops) {
            if (let Some(cell) <- (r as SimpleUserActorRef)) {
                cell.sendSystem(SysTerminate())
            }
        }
        if (isActorThread()) {
            // 在 Actor 中调用 terminate：等待会等到自己，只发出停止请求
            return liveCellTotal.load()
        }
        let deadline = MonoTime.now() + timeout
        while (true) {
            // 顶层 Cell 终止前必先等全部子 Actor 终止，所以顶层计数归零即全部停止
            let alive = liveCellTotal.load()
            if (alive == 0) {
                return 0
            }
            if (MonoTime.now() > deadline) {
                log.warn("系统终止：${alive} 个 Actor 在 ${timeout.toMilliseconds()}ms 内没有停止")
                return alive
            }
            sleep(Duration.millisecond)
        }
        0
    }

    func onCellTerminated(cell: SimpleUserActorRef): Unit {
        if (cell.parentCell.isSome()) {
            // 子 Actor 不在全局路径表（与 createCell 对应）：停止时只递减计数，
            // 不碰全局注册表锁——百万级子 Actor 批量停止时，全部工作线程抢同一把锁曾是主要瓶颈
            liveCellTotal.fetchAdd(-1)
            notifyTerminationWaiters()
            return
        }
        liveCellTotal.fetchAdd(-1)
        let removed = synchronized(guardianLock) {
            match (userActors.get(cell.getName())) {
                case Some(r) where r.equals(cell) =>
                    userActors.remove(cell.getName())
                    supervisionStrategies.remove(cell.path.toString())
                    stopCounter.fetchAdd(1)
                    true
                case _ => false
            }
        }
        if (removed) {
            if (let Some(hook) <- topLevelTerminatedHook) {
                hook(cell)
            }
        }
        notifyTerminationWaiters()
    }

    /** 运行时注册：顶层 Actor 自行停止时回调，用于清理注册表和计数 */
    public func setSystemView(view: ActorSystemView): Unit {
        systemViewValue = Some(view)
    }

    public func systemView(): Option<ActorSystemView> {
        systemViewValue
    }

    public func setTopLevelTerminatedHook(hook: (ActorRef) -> Unit): Unit {
        topLevelTerminatedHook = Some(hook)
    }

    // ---------------- 死信 ----------------

    /** 订阅死信：参数为消息和原目标路径。回调在发送方线程上执行，应尽快返回 */
    public func subscribeDeadLetters(listener: (Message, String) -> Unit): Unit {
        synchronized(deadLetterLock) {
            let next = ArrayList<(Message, String) -> Unit>()
            next.add(all: deadLetterListeners)
            next.add(listener)
            deadLetterListeners = next.toArray()
        }
    }

    public func deadLetterCount(): Int64 {
        deadLetterTotal.load()
    }

    public func publishDeadLetter(message: Message, path: String): Unit {
        deadLetterTotal.fetchAdd(1)
        let listeners = deadLetterListeners
        for (l in listeners) {
            try {
                l(message, path)
            } catch (e: Exception) {
                log.warn("死信监听器异常: ${e}")
            }
        }
        letterLog.tryLog({ s => log.info(s) }, { => "死信: ${describeLetter(message)} -> ${path}（目标不存在或已停止）" })
    }

    /** 系统内累计的未处理消息数（不含死信） */
    public func unhandledCount(): Int64 {
        unhandledTotal.load()
    }

    /**
     * Actor 没有处理的消息（对标 Akka UnhandledMessage）：包装成 UnhandledMessage 交给死信订阅者
     * （Akka 中二者同属 AllDeadLetters），并按限速记 INFO。不计入 deadLetterCount
     */
    func publishUnhandled(message: Message, path: String): Unit {
        unhandledTotal.fetchAdd(1)
        let listeners = deadLetterListeners
        if (listeners.size > 0) {
            let wrapped = UnhandledMessage(message, path)
            for (l in listeners) {
                try {
                    l(wrapped, path)
                } catch (e: Exception) {
                    log.warn("死信监听器异常: ${e}")
                }
            }
        }
        letterLog.tryLog({ s => log.info(s) }, { => "未处理消息: ${describeLetter(message)} -> ${path}" })
    }
    
    protected func doStopChild(child: ActorRef): Unit {
        removeUserActor(child)
    }
    
    protected func doRestartChild(child: ActorRef, reason: Exception): Unit {
        // 重启通过邮箱串行执行：在 Actor Cell 中 preRestart → 重新创建实例 → postRestart
        child.tell(RestartMessage(reason))
    }
    
    // 重写Actor属性
    public prop name: String {
        get() { "user" }
    }
    
    public prop description: String {
        get() { "Runtime User Guardian - manages user-created actors" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case userMsg: CreateUserActorMessage =>
                try {
                    createUserActor(userMsg.getProps(), userMsg.getName())
                    MessageResult.Handled
                } catch (e: Exception) {
                    log.error("创建用户Actor失败: ${e}")
                    MessageResult.Unhandled
                }
            case failureMsg: ActorFailureMessage =>
                try {
                    handleActorFailure(failureMsg.getFailure())
                    MessageResult.Handled
                } catch (e: Exception) {
                    log.error("处理Actor失败异常: ${e}")
                    MessageResult.Unhandled
                }
            case _ => 
                // 委托给基类处理
                super.receive(message, context)
        }
    }
}

/**
 * 创建用户Actor的消息
 */
public class CreateUserActorMessage <: Message {
    private let props: Props<Actor>
    private let actorName: String

    public init(props: Props<Actor>, name: String) {
        this.props = props
        this.actorName = name
    }

    public func getProps(): Props<Actor> { props }
    public func getName(): String { actorName }

    public func getType(): String { "CreateUserActor" }
    public func getPayload(): String { "CreateUserActor(${actorName})" }
    public func messageType(): String { "CreateUserActor" }
}

/**
 * Actor失败消息
 */
public class ActorFailureMessage <: Message {
    private let failure: ActorFailure

    public init(failure: ActorFailure) {
        this.failure = failure
    }

    public func getFailure(): ActorFailure { failure }

    public func getType(): String { "ActorFailure" }
    public func getPayload(): String { "ActorFailure(${failure.actor.path.toString()})" }
    public func messageType(): String { "ActorFailure" }
}

/**
 * 停止消息
 */
public class StopMessage <: AutoReceivedMessage {
    // 与普通消息同一优先级：在已入队的消息之后停止
    public func priority(): Int32 { 0 }
    public func getType(): String { "Stop" }
    public func getPayload(): String { "Stop" }
    public func messageType(): String { "Stop" }
}

/**
 * 重启消息
 */
public class RestartMessage <: AutoReceivedMessage {
    private let reason: Exception

    public func priority(): Int32 { 0 }

    public init(reason: Exception) {
        this.reason = reason
    }

    public func getReason(): Exception { reason }

    public func getType(): String { "Restart" }
    public func getPayload(): String { "Restart(${reason})" }
    public func messageType(): String { "Restart" }
}

/**
 * 日志里的消息描述：类型化载体取出载荷再描述（否则只看到 "TypedPayloadBox"）。
 * 只解 TypedPayloadBox——AdaptedEnvelope 取载荷要重新执行适配函数，不能为了打日志调用
 */
func describeLetter(message: Message): String {
    match (message) {
        case b: TypedPayloadBox => "类型化消息 ${describePayload(b.payload)}"
        case _ => message.messageType()
    }
}

func describePayload(payload: Any): String {
    match (payload) {
        case m: Message => m.messageType()
        case t: ToString =>
            let runes = t.toString().toRuneArray()
            // 按字符截断：按字节切片可能切在多字节字符中间
            if (runes.size > 120) {
                "${String(runes[..120])}…"
            } else {
                String(runes)
            }
        case _ => "（无 toString 的载荷）"
    }
}
