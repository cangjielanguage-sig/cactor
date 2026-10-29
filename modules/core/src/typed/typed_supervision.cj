/*
 * CActor Core.Typed - 监督（对标 Akka Typed Behaviors.supervise）
 *
 * 与 Akka Typed 一致：未指定监督策略的类型化 Actor 失败时停止。
 * supervise(behavior).onFailure(strategy) 可以按异常类型指定 restart / resume / stop，
 * restart 可以限制时间窗口内的最大重启次数，超过后停止。
 *
 * 实现：每个类型化 Actor 创建时在经典 Props 上安装一个 TypedDeciderStrategy，
 * 它查询该 Actor 当前行为登记的规则（行为启动时登记，内层规则优先）。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.actor.{SupervisionStrategy, SupervisionDirective, ActorFailure}

/**
 * 失败时的处理方式
 */
public class TypedSupervisorStrategy {
    let directive: SupervisionDirective
    let maxRestarts: Int64
    let within: Duration

    init(directive: SupervisionDirective, maxRestarts: Int64, within: Duration) {
        this.directive = directive
        this.maxRestarts = maxRestarts
        this.within = within
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

    /** 限制重启次数：within 时间窗口内超过 maxNrOfRetries 次后停止（within 为 0 表示不限时间窗口） */
    public func withLimit(maxNrOfRetries: Int64, withinTimeRange: Duration): TypedSupervisorStrategy {
        TypedSupervisorStrategy(directive, maxNrOfRetries, withinTimeRange)
    }

    public func toString(): String {
        let name = match (directive) {
            case SupervisionDirective.Restart => "restart"
            case SupervisionDirective.Resume => "resume"
            case SupervisionDirective.Stop => "stop"
            case SupervisionDirective.Escalate => "escalate"
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
public class SupervisedBehavior<T> <: TypedBehavior<T> {
    let inner: TypedBehavior<T>
    let rules: Array<SupervisionRule>

    init(inner: TypedBehavior<T>) {
        this(inner, Array<SupervisionRule>())
    }

    init(inner: TypedBehavior<T>, rules: Array<SupervisionRule>) {
        this.inner = inner
        this.rules = rules
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

    /** 启动：先展开内层（内层规则先登记，优先匹配），再登记本层规则 */
    func startIn(ctx: TypedActorContext<T>): TypedBehavior<T> {
        let started = TypedBehaviors.start(inner, ctx)
        if (let Some(c) <- (ctx as ClassicTypedContext<T>)) {
            c.supervision.install(this, rules)
        }
        started
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
 * 每个类型化 Actor 一份的监督规则表（父 Actor 线程读取、子 Actor 线程登记，加锁）
 */
class SupervisionRules {
    private let lock = Mutex()
    private let owners = ArrayList<Object>()
    private let rules = ArrayList<SupervisionRule>()
    // 每条规则的重启时间记录
    private let history = ArrayList<ArrayList<MonoTime>>()

    func install(owner: Object, list: Array<SupervisionRule>): Unit {
        synchronized(lock) {
            for (o in owners) {
                if (refEq(o, owner)) {
                    return
                }
            }
            owners.add(owner)
            for (r in list) {
                rules.add(r)
                history.add(ArrayList<MonoTime>())
            }
        }
    }

    func ruleCount(): Int64 {
        synchronized(lock) {
            rules.size
        }
    }

    func decide(cause: Exception): SupervisionDirective {
        synchronized(lock) {
            var i: Int64 = 0
            while (i < rules.size) {
                let rule = rules[i]
                if (rule.matches(cause)) {
                    return apply(rule.strategy, history[i])
                }
                i++
            }
            // 与 Akka Typed 一致：默认停止
            SupervisionDirective.Stop
        }
    }

    private func apply(strategy: TypedSupervisorStrategy, restarts: ArrayList<MonoTime>): SupervisionDirective {
        match (strategy.directive) {
            case SupervisionDirective.Restart =>
                if (strategy.maxRestarts < 0) {
                    return SupervisionDirective.Restart
                }
                let now = MonoTime.now()
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
        rules.decide(failure.cause)
    }
}
