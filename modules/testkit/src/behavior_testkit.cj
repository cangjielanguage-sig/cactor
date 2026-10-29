/*
 * Copyright (c) 2024. All rights reserved.
 * CActor TestKit - 类型化行为的同步测试（对标 Akka 的 BehaviorTestKit / TestInbox / ManualTime）
 *
 * 不启动 ActorSystem、不创建线程：在测试线程中直接用行为处理消息，并把行为对上下文的操作
 * （创建子 Actor、监视、定时器、ask 等）记录为 Effect，供测试断言。
 * 时间是虚拟的：定时器、receive timeout、ask 超时只在 timePasses 推进虚拟时间时触发，
 * 因此与时间有关的逻辑可以确定性地、瞬间完成测试。
 */
package cactor_testkit

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.actor.{TimerScheduler, ActorContext, DeathPactException, InvalidActorNameException}
import cactor_core.message.{Message}
import cactor_core.typed.*

// ======================= Effect =======================

/** 行为对上下文执行的、测试可以观察到的操作 */
public enum Effect <: Equatable<Effect> & ToString {
    /** 创建了具名子 Actor */
    | Spawned(String)
    /** 创建了自动命名的子 Actor（参数为生成的名称） */
    | SpawnedAnonymous(String)
    /** 停止了子 Actor（参数为子 Actor 名称） */
    | Stopped(String)
    /** 监视了某个 Actor（参数为路径） */
    | Watched(String)
    | WatchedWith(String)
    | Unwatched(String)
    /** 启动了定时器：键、延迟/间隔、是否周期性 */
    | TimerScheduled(String, Duration, Bool)
    | TimerCancelled(String)
    /** scheduleOnce：延迟、目标路径 */
    | Scheduled(Duration, String)
    | ReceiveTimeoutSet(Duration)
    | ReceiveTimeoutCancelled
    /** ask：目标路径、超时 */
    | AskInitiated(String, Duration)
    | MessageAdapterCreated

    public func toString(): String {
        match (this) {
            case Spawned(n) => "Spawned(${n})"
            case SpawnedAnonymous(n) => "SpawnedAnonymous(${n})"
            case Stopped(n) => "Stopped(${n})"
            case Watched(p) => "Watched(${p})"
            case WatchedWith(p) => "WatchedWith(${p})"
            case Unwatched(p) => "Unwatched(${p})"
            case TimerScheduled(k, d, r) => "TimerScheduled(${k}, ${d.toMilliseconds()}ms, ${r})"
            case TimerCancelled(k) => "TimerCancelled(${k})"
            case Scheduled(d, p) => "Scheduled(${d.toMilliseconds()}ms, ${p})"
            case ReceiveTimeoutSet(d) => "ReceiveTimeoutSet(${d.toMilliseconds()}ms)"
            case ReceiveTimeoutCancelled => "ReceiveTimeoutCancelled"
            case AskInitiated(p, d) => "AskInitiated(${p}, ${d.toMilliseconds()}ms)"
            case MessageAdapterCreated => "MessageAdapterCreated"
        }
    }

    public operator func ==(other: Effect): Bool {
        this.toString() == other.toString()
    }

    public operator func !=(other: Effect): Bool {
        !(this == other)
    }
}

// ======================= TestInbox =======================

/** 只收集消息的类型化引用：作为被测行为的 self、子 Actor 或任意协作方 */
public class TestInbox<T> <: TypedActorRef<T> {
    private let path: String
    private let lock = Mutex()
    private let queue = ArrayList<T>()

    public init(name: String) {
        this.path = if (name.startsWith("/")) { name } else { "/user/${name}" }
    }

    public func tell(message: T): Unit {
        synchronized(lock) {
            queue.add(message)
        }
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<T>): Bool {
        path == other.pathString()
    }

    public func hasMessages(): Bool {
        synchronized(lock) {
            !queue.isEmpty()
        }
    }

    public func size(): Int64 {
        synchronized(lock) {
            queue.size
        }
    }

    /** 取出最早的一条消息；没有消息时抛出 TestKitAssertionException */
    public func receiveMessage(): T {
        synchronized(lock) {
            if (queue.isEmpty()) {
                throw TestKitAssertionException("${path} 中没有消息")
            }
            queue.remove(at: 0)
        }
    }

    /** 取出全部消息 */
    public func receiveAll(): Array<T> {
        synchronized(lock) {
            let all = queue.toArray()
            queue.clear()
            all
        }
    }

    /** 取出最早的一条消息并检查 */
    public func expectMessage(description: String, pred: (T) -> Bool): T {
        let m = receiveMessage()
        if (!pred(m)) {
            throw TestKitAssertionException("${path} 收到的消息不满足：${description}")
        }
        m
    }

    public func expectNoMessage(): Unit {
        if (hasMessages()) {
            throw TestKitAssertionException("${path} 中还有 ${size()} 条消息")
        }
    }
}

extend<T> TestInbox<T> where T <: Equatable<T> {
    /** 取出最早的一条消息，并断言它等于 expected */
    public func expectMessage(expected: T): Unit {
        let m = receiveMessage()
        if (m != expected) {
            throw TestKitAssertionException("${pathString()} 收到的消息与期望不符")
        }
    }
}

/** 收到消息时调用函数的引用（messageAdapter、ask 的 replyTo） */
class FunctionRef<T> <: TypedActorRef<T> {
    let path: String
    let f: (T) -> Unit

    init(path: String, f: (T) -> Unit) {
        this.path = path
        this.f = f
    }

    public func tell(message: T): Unit {
        f(message)
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<T>): Bool {
        path == other.pathString()
    }
}

// ======================= ManualTime =======================

/** 虚拟时钟上的任务 */
public class ManualTask <: TypedCancellable {
    let id: Int64
    var due: Duration
    let interval: Option<Duration>
    let action: () -> Unit
    var cancelled = false

    init(id: Int64, due: Duration, interval: Option<Duration>, action: () -> Unit) {
        this.id = id
        this.due = due
        this.interval = interval
        this.action = action
    }

    public func cancel(): Bool {
        if (cancelled) {
            return false
        }
        cancelled = true
        true
    }

    public func isCancelled(): Bool {
        cancelled
    }
}

/**
 * 虚拟时钟（对标 Akka ManualTime）：只有调用 timePasses 时时间才前进，
 * 到期的任务按到期时间先后（相同时按登记先后）依次执行；周期任务会在同一次推进中多次执行。
 */
public class ManualTime {
    private var nowValue = Duration.Zero
    private var nextId: Int64 = 0
    private let tasks = ArrayList<ManualTask>()

    public init() {}

    /** 从创建开始经过的虚拟时间 */
    public func now(): Duration {
        nowValue
    }

    public func schedule(delay: Duration, action: () -> Unit): ManualTask {
        add(delay, None<Duration>, action)
    }

    public func scheduleRepeated(interval: Duration, action: () -> Unit): ManualTask {
        add(interval, Some(interval), action)
    }

    private func add(delay: Duration, interval: Option<Duration>, action: () -> Unit): ManualTask {
        nextId++
        let d = if (delay < Duration.Zero) { Duration.Zero } else { delay }
        let t = ManualTask(nextId, nowValue + d, interval, action)
        tasks.add(t)
        t
    }

    /** 尚未执行（且未取消）的任务数 */
    public func pendingCount(): Int64 {
        var n: Int64 = 0
        for (t in tasks) {
            if (!t.cancelled) {
                n++
            }
        }
        n
    }

    /** 推进虚拟时间，期间到期的任务依次执行 */
    public func timePasses(amount: Duration): Unit {
        let target = nowValue + amount
        while (let Some(t) <- nextDue(target)) {
            nowValue = t.due
            match (t.interval) {
                case Some(i) where i > Duration.Zero => t.due = t.due + i
                case _ => t.cancelled = true
            }
            t.action()
        }
        nowValue = target
        tasks.removeIf({ t => t.cancelled })
    }

    private func nextDue(target: Duration): Option<ManualTask> {
        var best: Option<ManualTask> = None
        for (t in tasks) {
            if (t.cancelled || t.due > target) {
                continue
            }
            match (best) {
                case Some(b) where b.due < t.due || (b.due == t.due && b.id < t.id) => ()
                case _ => best = Some(t)
            }
        }
        best
    }
}

// ======================= 捕获的日志 =======================

public class CapturedLogEvent {
    public let level: String
    public let message: String

    public init(level: String, message: String) {
        this.level = level
        this.message = message
    }

    public func toString(): String {
        "[${level}] ${message}"
    }
}

class CapturingLogger <: TypedLogger {
    let events = ArrayList<CapturedLogEvent>()

    public func debug(message: String): Unit {
        events.add(CapturedLogEvent("DEBUG", message))
    }

    public func info(message: String): Unit {
        events.add(CapturedLogEvent("INFO", message))
    }

    public func warn(message: String): Unit {
        events.add(CapturedLogEvent("WARN", message))
    }

    public func error(message: String): Unit {
        events.add(CapturedLogEvent("ERROR", message))
    }
}

// ======================= 定时器 =======================

class ManualTimerScheduler <: TimerScheduler {
    let time: ManualTime
    let effects: ArrayList<Effect>
    let deliver: (Message) -> Unit
    let timers = HashMap<String, ManualTask>()

    init(time: ManualTime, effects: ArrayList<Effect>, deliver: (Message) -> Unit) {
        this.time = time
        this.effects = effects
        this.deliver = deliver
    }

    private func start(key: String, message: Message, delay: Duration, repeated: Bool): Unit {
        if (let Some(old) <- timers.remove(key)) {
            old.cancel()
        }
        let me = this
        let task = if (repeated) {
            time.scheduleRepeated(delay, { => me.deliver(message) })
        } else {
            time.schedule(delay, { =>
                me.timers.remove(key)
                me.deliver(message)
            })
        }
        timers[key] = task
        effects.add(Effect.TimerScheduled(key, delay, repeated))
    }

    public func startSingleTimer(key: String, message: Message, delay: Duration): Unit {
        start(key, message, delay, false)
    }

    public func startTimerWithFixedDelay(key: String, message: Message, delay: Duration): Unit {
        start(key, message, delay, true)
    }

    public func startTimerAtFixedRate(key: String, message: Message, interval: Duration): Unit {
        start(key, message, interval, true)
    }

    public func isTimerActive(key: String): Bool {
        timers.contains(key)
    }

    public func cancel(key: String): Unit {
        if (let Some(t) <- timers.remove(key)) {
            t.cancel()
            effects.add(Effect.TimerCancelled(key))
        }
    }

    public func cancelAll(): Unit {
        for ((_, t) in timers) {
            t.cancel()
        }
        timers.clear()
    }
}

// ======================= 上下文 =======================

class ChildEntry {
    let inbox: Any
    let anyRef: TypedActorRef<Any>
    let behavior: Any
    var kit: Option<Any> = None

    init(inbox: Any, anyRef: TypedActorRef<Any>, behavior: Any) {
        this.inbox = inbox
        this.anyRef = anyRef
        this.behavior = behavior
    }
}

class AskState {
    var done = false
    var task: Option<ManualTask> = None
}

class EffectfulContext<T> <: TypedActorContext<T> {
    let inbox: TestInbox<T>
    let time: ManualTime
    let effects = ArrayList<Effect>()
    let childEntries = HashMap<String, ChildEntry>()
    let logger = CapturingLogger()
    var behavior: TypedBehavior<T>
    var returned: TypedBehavior<T>
    var alive = true
    var stopRequested = false
    var unhandled: Int64 = 0
    var anonCounter: Int64 = 0
    var askCounter: Int64 = 0
    var timersValue: Option<TypedTimerScheduler<T>> = None
    var manualTimers: Option<ManualTimerScheduler> = None
    var receiveTimeout: Option<(Duration, T)> = None
    var receiveTimeoutTask: Option<ManualTask> = None

    init(behavior: TypedBehavior<T>, inbox: TestInbox<T>, time: ManualTime) {
        this.behavior = behavior
        this.returned = behavior
        this.inbox = inbox
        this.time = time
    }

    func startBehavior(): Unit {
        let started = TypedBehaviors.start(behavior, this)
        if (TypedBehaviors.isSame(started) || TypedBehaviors.isUnhandled(started)) {
            throw IllegalStateException("初始行为不能是 same 或 unhandled")
        }
        behavior = started
        returned = started
        if (TypedBehaviors.isStopped(started)) {
            stopNow(TypedBehaviors.stoppedCallback(started))
        } else if (stopRequested) {
            stopNow(None<() -> Unit>)
        }
    }

    func run(message: T): Unit {
        if (!alive) {
            throw IllegalStateException("${inbox.pathString()} 已停止，不能再处理消息")
        }
        armReceiveTimeout()
        apply(behavior.onMessage(message, this))
    }

    func signal(sig: TypedSignal): Unit {
        if (!alive) {
            throw IllegalStateException("${inbox.pathString()} 已停止，不能再处理信号")
        }
        let next = behavior.onSignal(sig, this)
        if (TypedBehaviors.isUnhandled(next)) {
            if (let Some(t) <- (sig as TypedTerminated)) {
                throw DeathPactException(t.pathString())
            }
        }
        apply(next)
    }

    private func apply(next: TypedBehavior<T>): Unit {
        returned = next
        if (TypedBehaviors.isUnhandled(next)) {
            unhandled++
        } else if (!TypedBehaviors.isSame(next)) {
            let started = TypedBehaviors.start(next, this)
            if (TypedBehaviors.isStopped(started)) {
                stopNow(TypedBehaviors.stoppedCallback(started))
                return
            }
            behavior = started
        }
        if (stopRequested) {
            stopNow(None<() -> Unit>)
        }
    }

    private func stopNow(callback: Option<() -> Unit>): Unit {
        alive = false
        match (callback) {
            case Some(cb) => cb()
            case None =>
                behavior.onSignal(PostStop(), this)
                ()
        }
        if (let Some(t) <- manualTimers) {
            t.cancelAll()
        }
        if (let Some(t) <- receiveTimeoutTask) {
            t.cancel()
        }
        childEntries.clear()
    }

    private func armReceiveTimeout(): Unit {
        if (let Some(t) <- receiveTimeoutTask) {
            t.cancel()
        }
        if (let Some((d, m)) <- receiveTimeout) {
            let me = this
            receiveTimeoutTask = Some(time.schedule(d, { =>
                if (me.alive) {
                    me.run(m)
                }
            }))
        }
    }

    // ---------- TypedActorContext ----------

    public prop self: TypedActorRef<T> {
        get() {
            inbox
        }
    }

    public func stopSelf(): Unit {
        stopRequested = true
    }

    public func log(): TypedLogger {
        logger
    }

    public func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        let ref = spawnChild<U>(behavior, name)
        effects.add(Effect.Spawned(name))
        ref
    }

    public func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> {
        actorOf<U>(behavior, name)
    }

    public func actorOf<U>(behavior: TypedBehavior<U>): TypedActorRef<U> {
        anonCounter++
        let name = "$${anonCounter}"
        let ref = spawnChild<U>(behavior, name)
        effects.add(Effect.SpawnedAnonymous(name))
        ref
    }

    private func spawnChild<U>(behavior: TypedBehavior<U>, name: String): TestInbox<U> {
        if (name.isEmpty() || name.contains("/")) {
            throw InvalidActorNameException("非法的 Actor 名称: '${name}'")
        }
        if (childEntries.contains(name)) {
            throw InvalidActorNameException("子 Actor 名称 ${name} 已被使用")
        }
        let child = TestInbox<U>("${inbox.pathString()}/${name}")
        let anyRef = FunctionRef<Any>(child.pathString(), { a: Any =>
            match (a as U) {
                case Some(u) => child.tell(u)
                case None => throw IllegalArgumentException("消息类型与 ${child.pathString()} 的协议不符")
            }
        })
        childEntries[name] = ChildEntry(child, anyRef, behavior)
        child
    }

    public func stop<U>(child: TypedActorRef<U>): Unit {
        let path = child.pathString()
        if (path == inbox.pathString()) {
            stopRequested = true
            return
        }
        let name = match (path.lastIndexOf("/")) {
            case Some(i) => path[(i + 1)..]
            case None => path
        }
        if (childEntries.remove(name).isNone()) {
            throw IllegalArgumentException("只能停止自身或子 Actor：${path}")
        }
        effects.add(Effect.Stopped(name))
    }

    public func childAs<U>(name: String): Option<TypedActorRef<U>> {
        match (childEntries.get(name)) {
            case Some(e) => match (e.inbox as TestInbox<U>) {
                case Some(i) => Some<TypedActorRef<U>>(i)
                case None => None<TypedActorRef<U>>
            }
            case None => None<TypedActorRef<U>>
        }
    }

    public func child(name: String): Option<TypedActorRef<Any>> {
        match (childEntries.get(name)) {
            case Some(e) => Some(e.anyRef)
            case None => None<TypedActorRef<Any>>
        }
    }

    public func children(): Array<TypedActorRef<Any>> {
        let result = ArrayList<TypedActorRef<Any>>()
        for ((_, e) in childEntries) {
            result.add(e.anyRef)
        }
        result.toArray()
    }

    public func watch<U>(other: TypedActorRef<U>): Unit {
        effects.add(Effect.Watched(other.pathString()))
    }

    public func watchWith<U>(other: TypedActorRef<U>, message: T): Unit {
        effects.add(Effect.WatchedWith(other.pathString()))
    }

    public func unwatch<U>(other: TypedActorRef<U>): Unit {
        effects.add(Effect.Unwatched(other.pathString()))
    }

    public func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req, timeout: Duration,
        mapResponse: (AskOutcome<Res>) -> T): Unit {
        askCounter++
        let state = AskState()
        let selfInbox = inbox
        let replyTo = FunctionRef<Res>("${inbox.pathString()}/$ask-${askCounter}", { r: Res =>
            if (!state.done) {
                state.done = true
                if (let Some(t) <- state.task) {
                    t.cancel()
                }
                selfInbox.tell(mapResponse(AskOutcome<Res>.Replied(r)))
            }
        })
        let targetPath = target.pathString()
        state.task = Some(time.schedule(timeout, { =>
            if (!state.done) {
                state.done = true
                selfInbox.tell(mapResponse(AskOutcome<Res>.Failed(
                    TypedAskTimeoutException("ask ${targetPath} 超时（${timeout.toMilliseconds()}ms）"))))
            }
        }))
        effects.add(Effect.AskInitiated(targetPath, timeout))
        target.tell(createRequest(replyTo))
    }

    public func pipeToSelf<V>(future: TypedFuture<V>, map: (AskOutcome<V>) -> T): Unit {
        let selfInbox = inbox
        future.onComplete({ o => selfInbox.tell(map(o)) })
    }

    public func messageAdapter<U>(f: (U) -> T): TypedActorRef<U> {
        effects.add(Effect.MessageAdapterCreated)
        let selfInbox = inbox
        FunctionRef<U>("${inbox.pathString()}/$adapter", { u: U => selfInbox.tell(f(u)) })
    }

    public prop timers: TypedTimerScheduler<T> {
        get() {
            match (timersValue) {
                case Some(t) => t
                case None =>
                    let me = this
                    let mt = ManualTimerScheduler(time, effects, { m: Message =>
                        if (me.alive) {
                            match (m as TypedEnvelope<T>) {
                                case Some(e) => me.run(e.payload)
                                case None => ()
                            }
                        }
                    })
                    manualTimers = Some(mt)
                    let t = TypedTimerScheduler<T>(mt)
                    timersValue = Some(t)
                    t
            }
        }
    }

    public func setReceiveTimeout(timeout: Duration, message: T): Unit {
        receiveTimeout = Some((timeout, message))
        effects.add(Effect.ReceiveTimeoutSet(timeout))
        armReceiveTimeout()
    }

    public func cancelReceiveTimeout(): Unit {
        receiveTimeout = None
        if (let Some(t) <- receiveTimeoutTask) {
            t.cancel()
        }
        receiveTimeoutTask = None
        effects.add(Effect.ReceiveTimeoutCancelled)
    }

    public func scheduleOnce<U>(delay: Duration, target: TypedActorRef<U>, message: U): TypedCancellable {
        effects.add(Effect.Scheduled(delay, target.pathString()))
        time.schedule(delay, { => target.tell(message) })
    }
}

// ======================= BehaviorTestKit =======================

/**
 * 同步行为测试工具
 *
 * ```
 * let kit = BehaviorTestKit<Cmd>(counter(0))
 * kit.run(Cmd.Increment)
 * kit.expectEffect(Effect.Spawned("worker"))
 * kit.timePasses(Duration.second)       // 触发到期的定时器
 * kit.selfInbox().expectMessage(...)
 * ```
 */
public class BehaviorTestKit<T> {
    private let ctx: EffectfulContext<T>

    public init(behavior: TypedBehavior<T>) {
        this(behavior, "testkit")
    }

    public init(behavior: TypedBehavior<T>, name: String) {
        this(behavior, TestInbox<T>(name), ManualTime())
    }

    init(behavior: TypedBehavior<T>, inbox: TestInbox<T>, time: ManualTime) {
        ctx = EffectfulContext<T>(behavior, inbox, time)
        ctx.startBehavior()
    }

    /** 用当前行为处理一条消息 */
    public func run(message: T): Unit {
        ctx.run(message)
    }

    /** 取出 self 收件箱中最早的一条消息（ask 响应、messageAdapter、pipeToSelf 都投递到这里）并处理 */
    public func runOne(): Unit {
        ctx.run(ctx.inbox.receiveMessage())
    }

    /** 处理 self 收件箱中的全部消息（包括处理过程中新产生的），返回处理条数 */
    public func runAll(): Int64 {
        var n: Int64 = 0
        while (ctx.alive && ctx.inbox.hasMessages()) {
            runOne()
            n++
        }
        n
    }

    /** 发送生命周期信号；未处理的 TypedTerminated 抛出 DeathPactException */
    public func signal(sig: TypedSignal): Unit {
        ctx.signal(sig)
    }

    public func selfInbox(): TestInbox<T> {
        ctx.inbox
    }

    public func isAlive(): Bool {
        ctx.alive
    }

    /** 当前生效的行为 */
    public func currentBehavior(): TypedBehavior<T> {
        ctx.behavior
    }

    /** 最近一次处理返回的原始行为（可用 TypedBehaviors.isSame / isUnhandled / isStopped 判断） */
    public func returnedBehavior(): TypedBehavior<T> {
        ctx.returned
    }

    /** 返回 unhandled 的消息数 */
    public func unhandledCount(): Int64 {
        ctx.unhandled
    }

    public func hasEffects(): Bool {
        !ctx.effects.isEmpty()
    }

    /** 取出全部 Effect */
    public func retrieveAllEffects(): Array<Effect> {
        let all = ctx.effects.toArray()
        ctx.effects.clear()
        all
    }

    /** 取出最早的一个 Effect；没有时抛出 TestKitAssertionException */
    public func retrieveEffect(): Effect {
        if (ctx.effects.isEmpty()) {
            throw TestKitAssertionException("没有 Effect")
        }
        ctx.effects.remove(at: 0)
    }

    /** 取出最早的一个 Effect 并断言它等于 expected */
    public func expectEffect(expected: Effect): Unit {
        if (ctx.effects.isEmpty()) {
            throw TestKitAssertionException("期望 ${expected}，但没有 Effect")
        }
        let actual = ctx.effects.remove(at: 0)
        if (actual != expected) {
            throw TestKitAssertionException("期望 ${expected}，实际 ${actual}")
        }
    }

    /** 子 Actor 的收件箱；名称不存在或类型不符时抛出 TestKitAssertionException */
    public func childInbox<U>(name: String): TestInbox<U> {
        match (ctx.childEntries.get(name)) {
            case Some(e) => match (e.inbox as TestInbox<U>) {
                case Some(i) => i
                case None => throw TestKitAssertionException("子 Actor ${name} 的协议类型与请求的类型不符")
            }
            case None => throw TestKitAssertionException("没有名为 ${name} 的子 Actor")
        }
    }

    /** 子 Actor 的同步测试工具（与父共享虚拟时钟，self 就是 childInbox(name)） */
    public func childTestKit<U>(name: String): BehaviorTestKit<U> {
        let e = match (ctx.childEntries.get(name)) {
            case Some(e) => e
            case None => throw TestKitAssertionException("没有名为 ${name} 的子 Actor")
        }
        if (let Some(k) <- e.kit) {
            if (let Some(kit) <- (k as BehaviorTestKit<U>)) {
                return kit
            }
        }
        match ((e.behavior as TypedBehavior<U>, e.inbox as TestInbox<U>)) {
            case (Some(b), Some(i)) =>
                let kit = BehaviorTestKit<U>(b, i, ctx.time)
                e.kit = Some(kit)
                kit
            case _ => throw TestKitAssertionException("子 Actor ${name} 的协议类型与请求的类型不符")
        }
    }

    /** ctx.log() 记录的日志 */
    public func logEntries(): Array<CapturedLogEvent> {
        ctx.logger.events.toArray()
    }

    public func clearLog(): Unit {
        ctx.logger.events.clear()
    }

    /** 虚拟时钟 */
    public func manualTime(): ManualTime {
        ctx.time
    }

    /** 推进虚拟时间：到期的定时器、receive timeout 依次处理；ask 超时与 scheduleOnce 投递到目标 */
    public func timePasses(amount: Duration): Unit {
        ctx.time.timePasses(amount)
    }
}
