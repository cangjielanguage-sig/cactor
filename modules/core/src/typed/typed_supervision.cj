/*
 * CActor Core.Typed - 监督（对标 Akka Typed Behaviors.supervise）
 *
 * 与 Akka Typed 一致：未指定监督策略的类型化 Actor 失败时停止。
 * supervise(behavior).onFailure(strategy) 可以按异常类型指定 restart / resume / stop / restartWithBackoff，
 * restart 可以限制时间窗口内的最大重启次数，超过后停止。
 * setup 中抛出的异常：不限次数的 restart 不重试（必然死循环，停止）；限次 restart 与 restartWithBackoff 照常重试。
 *
 * 实现：每个类型化 Actor 创建时在经典 Props 上安装一个 TypedDeciderStrategy，
 * 它查询该 Actor 当前行为登记的规则（行为启动时登记，内层规则优先）。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.actor.{SupervisionStrategy, SupervisionDirective, ActorFailure, ActorInitializationException}
import cactor_core.supervision.{BackoffPolicy, BackoffState, BackoffDecision}

/**
 * 失败时的处理方式
 */
public class TypedSupervisorStrategy {
    let directive: SupervisionDirective
    let maxRestarts: Int64
    let within: Duration
    let backoff: Option<BackoffPolicy>

    init(directive: SupervisionDirective, maxRestarts: Int64, within: Duration) {
        this(directive, maxRestarts, within, None)
    }

    init(directive: SupervisionDirective, maxRestarts: Int64, within: Duration, backoff: Option<BackoffPolicy>) {
        this.directive = directive
        this.maxRestarts = maxRestarts
        this.within = within
        this.backoff = backoff
    }

    /** 重启：丢弃状态，重新执行初始行为（含 setup），子 Actor 被停止 */
    public static func restart(): TypedSupervisorStrategy {
        TypedSupervisorStrategy(SupervisionDirective.Restart, -1, Duration.Zero)
    }

    /** 恢复：忽略失败的消息，保持当前行为和状态继续处理 */
    public static func resume(): TypedSupervisorStrategy {
        TypedSupervisorStrategy(SupervisionDirective.Resume, -1, Duration.Zero)
    }

    /** 停止 */
    public static func stop(): TypedSupervisorStrategy {
        TypedSupervisorStrategy(SupervisionDirective.Stop, -1, Duration.Zero)
    }

    /**
     * 指数退避重启（对标 Akka SupervisorStrategy.restartWithBackoff）：
     * 第 n 次重启前等待 min(maxBackoff, minBackoff × 2^n) × (1 + [0, randomFactor) 的随机数)；
     * 等待期间 Actor 挂起，不处理消息；重启后稳定运行 minBackoff（可用 withResetBackoffAfter 修改）计数清零。
     * 与经典 BackoffSupervisionStrategy 共用同一套算法。
     */
    public static func restartWithBackoff(minBackoff: Duration, maxBackoff: Duration, randomFactor: Float64): TypedSupervisorStrategy {
        TypedSupervisorStrategy(SupervisionDirective.Restart, -1, Duration.Zero,
            BackoffPolicy(minBackoff, maxBackoff, randomFactor: randomFactor))
    }

    /**
     * 限制重启次数：within 时间窗口内超过 maxNrOfRetries 次后停止（within 为 0 表示不限时间窗口）。
     * 对 restartWithBackoff，等同于 withMaxRestarts(maxNrOfRetries)，时间窗口由 withResetBackoffAfter 决定。
     */
    public func withLimit(maxNrOfRetries: Int64, withinTimeRange: Duration): TypedSupervisorStrategy {
        match (backoff) {
            case Some(p) => TypedSupervisorStrategy(directive, -1, Duration.Zero, p.withMaxRestarts(maxNrOfRetries))
            case None => TypedSupervisorStrategy(directive, maxNrOfRetries, withinTimeRange)
        }
    }

    /** 最多重启 n 次（不限时间窗口；对退避策略则是自上次复位以来的次数） */
    public func withMaxRestarts(n: Int64): TypedSupervisorStrategy {
        withLimit(n, Duration.Zero)
    }

    /** 仅对 restartWithBackoff 有意义：重启后稳定运行 d 未再失败，退避计数清零 */
    public func withResetBackoffAfter(d: Duration): TypedSupervisorStrategy {
        match (backoff) {
            case Some(p) => TypedSupervisorStrategy(directive, maxRestarts, within, p.withResetBackoffAfter(d))
            case None => throw IllegalArgumentException("withResetBackoffAfter 只能用于 restartWithBackoff")
        }
    }

    public func toString(): String {
        if (let Some(p) <- backoff) {
            return "restart-with-${p}"
        }
        let name = match (directive) {
            case SupervisionDirective.Restart => "restart"
            case SupervisionDirective.Resume => "resume"
            case SupervisionDirective.Stop => "stop"
            case SupervisionDirective.Escalate => "escalate"
            case SupervisionDirective.RestartAfter(d) => "restart-after(${d.toMilliseconds()}ms)"
        }
        if (maxRestarts >= 0) {
            "${name}(limit=${maxRestarts}, within=${within})"
        } else {
            name
        }
    }
}

/** 一条监督规则 */
class SupervisionRule {
    let matches: (Exception) -> Bool
    let strategy: TypedSupervisorStrategy

    init(matches: (Exception) -> Bool, strategy: TypedSupervisorStrategy) {
        this.matches = matches
        this.strategy = strategy
    }
}

/**
 * 带监督策略的行为
 */
public class SupervisedBehavior<T> <: SpecialBehavior & TypedBehavior<T> {
    let inner: TypedBehavior<T>
    let rules: Array<SupervisionRule>

    init(inner: TypedBehavior<T>) {
        this(inner, Array<SupervisionRule>())
    }

    init(inner: TypedBehavior<T>, rules: Array<SupervisionRule>) {
        super(SpecialBehavior.SUPERVISED)
        this.inner = inner
        this.rules = rules
    }

    /** supervise(setup(...)) 透出内层 setup 携带的调用方包 hooks（T16） */
    public override func carriedHooks(): Option<TypedHooks> {
        carriedHooksOf(inner)
    }

    /** 任意异常都使用 strategy */
    public func onFailure(strategy: TypedSupervisorStrategy): SupervisedBehavior<T> {
        onFailure({ _ => true }, strategy)
    }

    /** 满足 predicate 的异常使用 strategy；按添加顺序匹配 */
    public func onFailure(predicate: (Exception) -> Bool, strategy: TypedSupervisorStrategy): SupervisedBehavior<T> {
        let list = ArrayList<SupervisionRule>(rules)
        list.add(SupervisionRule(predicate, strategy))
        SupervisedBehavior<T>(inner, list.toArray())
    }

    /** 类型为 E（或其子类）的异常使用 strategy */
    public func onFailureOf<E>(strategy: TypedSupervisorStrategy): SupervisedBehavior<T> where E <: Exception {
        onFailure({ e => e is E }, strategy)
    }

    /**
     * 启动：展开内层，并让本层作为**持续存在的包裹层**留在行为链里（对标 Akka 的监督拦截器，T9）。
     *
     * - 展开内层之前把本层压入"启动中"栈：内层 setup 抛出的异常也按本层规则处理（首次 setup 失败同样受监督）；
     * - 返回 ActiveSupervision 包裹层：内层切换行为时包裹层随之重新包裹，规则一直有效；
     *   失败时运行时沿行为链收集包裹层，**按嵌套层级由内向外**匹配（此前按登记先后匹配，规则表只增不减）。
     */
    func startIn(ctx: TypedActorContext<T>): TypedBehavior<T> {
        let host = supervisionHostOf(ctx)
        if (let Some(h) <- host) {
            h.pushStarting(rules)
        }
        // 内层启动抛异常时不弹栈：由适配器在捕获时连同行为链一起取走（见 TypedActorAdapter.captureFailure）
        let started = TypedBehaviors.start(inner, ctx)
        if (let Some(h) <- host) {
            h.popStarting()
        }
        if (TypedBehaviors.isStopped(started)) {
            started
        } else {
            ActiveSupervision<T>(rules, started)
        }
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        inner.onMessage(message, context)
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        inner.onSignal(signal, context)
    }

    public func getDescription(): String {
        "SupervisedBehavior(${inner.getDescription()})"
    }
}

/**
 * 运行中的监督包裹层（SupervisedBehavior 启动后的形态）。内层每次切换行为，包裹层随之重新包裹，
 * 所以 supervise 一旦生效就覆盖它内部后续的所有行为（与 Akka 的监督拦截器相同）。
 * 用种类标记（ACTIVE_SUPERVISION）识别，热路径上不做泛型类型检查
 */
internal class ActiveSupervision<T> <: SpecialBehavior & TypedBehavior<T> {
    let rules: Array<SupervisionRule>
    private let inner: TypedBehavior<T>

    init(rules: Array<SupervisionRule>, inner: TypedBehavior<T>) {
        super(SpecialBehavior.ACTIVE_SUPERVISION)
        this.rules = rules
        this.inner = inner
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        rewrap(inner.onMessage(message, context), context)
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        rewrap(inner.onSignal(signal, context), context)
    }

    private func rewrap(next: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        let kind = specialKindOf(next)
        // same / unhandled：适配器保留当前行为（即本包裹层）；stopped 原样交出
        if (kind == SpecialBehavior.SAME || kind == SpecialBehavior.UNHANDLED || kind == SpecialBehavior.STOPPED) {
            return next
        }
        let started = TypedBehaviors.start(next, ctx)
        if (TypedBehaviors.isStopped(started)) {
            return started
        }
        // 内层自己又带着同一组规则（例如返回了同一个 supervise 行为）：不重复包裹
        if (specialKindOf(started) == SpecialBehavior.ACTIVE_SUPERVISION) {
            if (let Some(a) <- (started as ActiveSupervision<T>)) {
                if (sameRules(a.rules, rules)) {
                    return started
                }
            }
        }
        ActiveSupervision<T>(rules, started)
    }

    public override func wrappedBehavior(): Option<Any> {
        Some<Any>(inner)
    }

    public override func supervisionLayer(): Option<Any> {
        Some<Any>(rules)
    }

    public func getDescription(): String {
        "Supervised(${inner.getDescription()})"
    }
}

/** 规则数组按引用判同（同一个 supervise 行为值里的规则） */
func sameRules(a: Array<SupervisionRule>, b: Array<SupervisionRule>): Bool {
    if (a.size != b.size) {
        return false
    }
    for (i in 0..a.size) {
        if (!refEq(a[i], b[i])) {
            return false
        }
    }
    true
}

/** 承载监督规则表的上下文（经典适配上下文，以及转发给它的 transformMessages 内层上下文） */
internal interface SupervisionHost {
    func pushStarting(rules: Array<SupervisionRule>): Unit
    func popStarting(): Unit
}

func supervisionHostOf(ctx: Any): Option<SupervisionHost> {
    ctx as SupervisionHost
}

/**
 * 沿行为链（类型擦除）由外向内收集监督包裹层的规则
 */
func collectSupervisionLayers(behavior: Any, out: ArrayList<Array<SupervisionRule>>): Unit {
    var cur: Option<Any> = Some(behavior)
    var guard = 0
    while (let Some(b) <- cur) {
        match (b as SpecialBehavior) {
            case Some(sb) =>
                if (let Some(layerAny) <- sb.supervisionLayer()) {
                    if (let Some(layer) <- (layerAny as Array<SupervisionRule>)) {
                        out.add(layer)
                    }
                }
                cur = sb.wrappedBehavior()
            case None => cur = None<Any>
        }
        guard++
        if (guard > 10_000) {
            break
        }
    }
}

/** 一条规则的进度：按结构位置（由外向内的层号、层内序号）识别，跨重启保留 */
class RuleProgress {
    let key: String
    let history = ArrayList<MonoTime>()
    var backoff: Option<BackoffState> = None<BackoffState>

    init(key: String) {
        this.key = key
    }
}

/**
 * 每个类型化 Actor 一份的监督状态（T9）。
 *
 * - 失败时由适配器在 Actor 自己的线程上快照当前生效的监督层（行为链上的包裹层 + 正在启动的层），
 *   父 Actor 决策时按快照**由内向外**匹配；
 * - 重启计数 / 退避进度按结构位置保存：setup 里每次新建的 supervise 在重启后处于同一位置，计数延续
 *   （此前按对象登记：setup 里的 supervise 每次重启新增一组空记录，withLimit 永远到不了，无限重启；
 *   规则表也只增不减）；不在当前快照里的进度随即丢弃，表的大小以当前行为结构为界。
 */
class SupervisionRules <: SupervisionHost {
    private let lock = Mutex()
    // 正在启动的监督层（只在本 Actor 线程读写）
    private let starting = ArrayList<Array<SupervisionRule>>()
    // 失败时的快照：由外向内（子线程写、父线程读，锁内）
    private var snapshot: Option<Array<Array<SupervisionRule>>> = None<Array<Array<SupervisionRule>>>
    private let progress = ArrayList<RuleProgress>()

    public func pushStarting(rules: Array<SupervisionRule>): Unit {
        starting.add(rules)
    }

    public func popStarting(): Unit {
        if (starting.size > 0) {
            starting.remove(at: starting.size - 1)
        }
    }

    /** 本 Actor 线程：失败（异常逃出处理方法 / 启动）时快照生效的监督层 */
    func captureFailure(behavior: Any): Unit {
        let layers = ArrayList<Array<SupervisionRule>>()
        collectSupervisionLayers(behavior, layers)
        // 正在启动的层比当前行为链更内层（例如内层返回了一个新的 supervise(setup(...)) 且 setup 失败）
        layers.add(all: starting)
        starting.clear()
        synchronized(lock) {
            snapshot = Some(layers.toArray())
        }
    }

    /** 当前进度条目数（测试用：验证不随重启增长） */
    func progressCount(): Int64 {
        synchronized(lock) {
            progress.size
        }
    }

    /** duringStart：失败发生在 setup / 启动阶段 */
    func decide(cause: Exception, duringStart: Bool, restartedAt: Option<MonoTime>,
        failedAt!: Option<MonoTime> = None): SupervisionDirective {
        synchronized(lock) {
            let layers = snapshot ?? Array<Array<SupervisionRule>>()
            snapshot = None<Array<Array<SupervisionRule>>>
            prune(layers)
            // 由内向外：最内层的 supervise 先匹配，层内按 onFailure 的添加顺序
            var depth = layers.size - 1
            while (depth >= 0) {
                let layer = layers[depth]
                for (j in 0..layer.size) {
                    let rule = layer[j]
                    if (rule.matches(cause)) {
                        return apply(rule.strategy, progressFor(keyOf(depth, j)), duringStart, restartedAt, failedAt)
                    }
                }
                depth--
            }
            // 与 Akka Typed 一致：没有匹配的监督规则时停止
            SupervisionDirective.Stop
        }
    }

    private static func keyOf(depth: Int64, index: Int64): String {
        "${depth}/${index}"
    }

    private func progressFor(key: String): RuleProgress {
        for (p in progress) {
            if (p.key == key) {
                return p
            }
        }
        let p = RuleProgress(key)
        progress.add(p)
        p
    }

    /** 丢弃不在当前生效结构里的进度 */
    private func prune(layers: Array<Array<SupervisionRule>>): Unit {
        progress.removeIf({ p =>
            var live = false
            for (d in 0..layers.size) {
                for (j in 0..layers[d].size) {
                    if (keyOf(d, j) == p.key) {
                        live = true
                    }
                }
            }
            !live
        })
    }

    private func apply(strategy: TypedSupervisorStrategy, state: RuleProgress, duringStart: Bool,
        restartedAt: Option<MonoTime>, failedAt: Option<MonoTime>): SupervisionDirective {
        if (let Some(policy) <- strategy.backoff) {
            return match (policy.next(state.backoff, restartedAt: restartedAt, failedAt: failedAt)) {
                case BackoffDecision.RestartIn(delay, next) =>
                    state.backoff = Some(next)
                    SupervisionDirective.RestartAfter(delay)
                case BackoffDecision.GiveUp =>
                    state.backoff = None<BackoffState>
                    SupervisionDirective.Stop
            }
        }
        let restarts = state.history
        match (strategy.directive) {
            case SupervisionDirective.Restart =>
                if (strategy.maxRestarts < 0) {
                    // 与 Akka Typed 一致：不限次数的 restart 遇到 setup 失败直接停止，否则就是无限重启
                    return if (duringStart) { SupervisionDirective.Stop } else { SupervisionDirective.Restart }
                }
                // 失败时刻按系统调度器的时钟（E52），未提供时用真实时钟
                let now = failedAt ?? MonoTime.now()
                if (strategy.within > Duration.Zero) {
                    // 丢弃时间窗口之外的记录
                    let recent = ArrayList<MonoTime>()
                    for (t in restarts) {
                        if (now - t <= strategy.within) {
                            recent.add(t)
                        }
                    }
                    restarts.clear()
                    restarts.add(all: recent)
                }
                if (restarts.size >= strategy.maxRestarts) {
                    SupervisionDirective.Stop
                } else {
                    restarts.add(now)
                    SupervisionDirective.Restart
                }
            case d => d
        }
    }
}

/**
 * 安装在经典 Props 上的策略：委托给类型化 Actor 登记的规则
 */
class TypedDeciderStrategy <: SupervisionStrategy {
    let rules: SupervisionRules

    init(rules: SupervisionRules) {
        this.rules = rules
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        // setup 失败由运行时包成 ActorInitializationException；规则按原异常匹配，
        // 这样 onFailureOf<E> 对 setup 里抛出的 E 同样生效（与 Akka Typed 一致）
        match (failure.cause) {
            case ie: ActorInitializationException => rules.decide(ie.cause, true, failure.lastRestartAt,
                failedAt: failure.failedAt)
            case other => rules.decide(other, false, failure.lastRestartAt, failedAt: failure.failedAt)
        }
    }
}
