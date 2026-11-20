/*
 * CActor Platform.Lifecycle - Actor 睡眠/唤醒（P2 / F1）
 *
 * 目标：空闲会话不再占一个存活 Actor（Cell + 邮箱 + 线程），状态落盘；下一条消息到达时
 * 按 key 重新拉起并恢复状态，且"唤醒期间到达的消息不丢、不重复"。
 *
 * 结构（每个 SleepRegistry 一个协调 Actor）：
 *   SleepableHandle --HandleMessage--> SleepManagerActor --转发--> 会话 Actor（SleepableActor 子类）
 *   Idle/显式 sleep 时：向会话 Actor 要状态（CaptureSleep → SleepStateMsg）→ 写入 DurableStateStore →
 *   停止会话 Actor（存活数 −1，状态不再常驻内存）。
 *   下一条消息：按 key 读回状态 → 创建新实例（构造时传入状态，preStart 中 restore）→ 投递该消息。
 *
 * 顺序：管理器是普通 Actor，receive 串行；"创建 + 投递第一条消息"在同一 receive 内完成，
 * 其后的消息按邮箱顺序转发，因此唤醒期间不会丢消息也不会乱序。
 *
 * 状态写入是同步 IO：管理器使用 DispatcherMode.ThreadPerActor，阻塞只占自己的线程。
 */
package cactor_platform.lifecycle

import std.collection.*
import std.time.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Props, PropsFactory, DispatcherMode}
import cactor_core.message.{Message, MessageResult}
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}

/** 睡眠策略（对标 F1） */
public enum SleepPolicy {
    /** 空闲这么久没有消息就睡 */
    | Idle(Duration)
    /** 只有显式调用 SleepableHandle.sleep() 才睡 */
    | Explicit
}

/**
 * 可睡眠 Actor 的基类。
 *
 * 子类实现 state()/restore()/onMessage()；平台消息（CaptureSleep）由本类处理。
 * 不要覆写 preStart——唤醒时的状态恢复在那里执行（需要的话在 onMessage 处理自己的初始化消息）。
 */
public abstract class SleepableActor <: Actor {
    private var restoredState: Option<String>

    /** restored 非空时，preStart 会调用 restore(restored) */
    public init(restored: Option<String>) {
        this.restoredState = restored
    }

    public init() {
        this(None<String>)
    }

    /** 序列化当前状态（在 Actor 线程上调用） */
    public func state(): String

    /** 从状态恢复（Actor 线程上、首条用户消息之前调用） */
    public func restore(state: String): Unit

    /** 子类的消息处理（平台控制消息已被基类截获） */
    public func onMessage(message: Message, context: ActorContext): MessageResult

    public func preStart(): Unit {
        match (restoredState) {
            case Some(s) =>
                // 恢复后立即释放状态字符串：睡眠期间状态只应存在于存储里
                restoredState = None<String>
                restore(s)
            case None => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case c: CaptureSleep =>
                c.replyTo.tell(SleepStateMsg(c.replyKey, c.generation, state()))
                MessageResult.Handled
            case _ => onMessage(message, context)
        }
    }
}

/** 从管理器发给会话 Actor：请交出状态 */
class CaptureSleep <: Message {
    let replyTo: ActorRef
    let replyKey: String
    let generation: Int64

    init(replyTo: ActorRef, replyKey: String, generation: Int64) {
        this.replyTo = replyTo
        this.replyKey = replyKey
        this.generation = generation
    }

    public func messageType(): String {
        "SleepCapture"
    }
}

/** 会话 Actor 回复状态 */
class SleepStateMsg <: Message {
    let key: String
    let generation: Int64
    let state: String

    init(key: String, generation: Int64, state: String) {
        this.key = key
        this.generation = generation
        this.state = state
    }

    public func messageType(): String {
        "SleepState"
    }
}

/** 句柄 → 管理器：把消息投递给会话（必要时先唤醒） */
class HandleMessage <: Message {
    let key: String
    let factory: (Option<String>) -> Actor
    let payload: Message
    let sender: Option<ActorRef>

    init(key: String, factory: (Option<String>) -> Actor, payload: Message, sender: Option<ActorRef>) {
        this.key = key
        this.factory = factory
        this.payload = payload
        this.sender = sender
    }

    public func messageType(): String {
        "SleepHandle"
    }
}

/** 空闲到期 */
class IdleTick <: Message {
    let key: String

    init(key: String) {
        this.key = key
    }

    public func messageType(): String {
        "SleepIdleTick"
    }
}

/** 显式睡眠请求（句柄调用） */
class SleepNow <: Message {
    let key: String

    init(key: String) {
        this.key = key
    }

    public func messageType(): String {
        "SleepNow"
    }
}

/** 会话 Actor 已停止（正常睡眠或监督停止） */
class ChildGone <: Message {
    let key: String
    let generation: Int64

    init(key: String, generation: Int64) {
        this.key = key
        this.generation = generation
    }

    public func messageType(): String {
        "SleepChildGone"
    }
}

/** 每个会话的管理条目：只保留"当前是否有活实例"与实例代数，状态不常驻内存 */
class SessionEntry {
    var active: Option<ActorRef> = None<ActorRef>
    var generation: Int64 = 0
    // 正在等待状态回执的代数；-1 表示没有进行中的睡眠
    var pendingSleepGeneration: Int64 = -1
    // 空闲定时器 key（每会话唯一，避免不同会话互相覆盖）
    var timerKey: String = ""

    init(timerKey: String) {
        this.timerKey = timerKey
    }
}

/**
 * 睡眠管理器：每个 SleepRegistry 一个。
 * 负责唤醒（按 key 读状态、创建实例、投递消息）、空闲/显式睡眠（要状态、落盘、停止实例）。
 */
class SleepManagerActor <: Actor {
    private let registryName: String
    private let store: DurableStateStore
    private let policy: SleepPolicy
    private let sessions = HashMap<String, SessionEntry>()
    private var entrySeq: Int64 = 0
    private var childSeq: Int64 = 0

    init(registryName: String, store: DurableStateStore, policy: SleepPolicy) {
        this.registryName = registryName
        this.store = store
        this.policy = policy
    }

    public prop name: String {
        get() { "sleep-manager-${registryName}" }
    }

    public prop description: String {
        get() { "Sleepable session manager" }
    }

    private func persistenceId(key: String): String {
        "sleepable/${registryName}/${key}"
    }

    private func entryOf(key: String): SessionEntry {
        match (sessions.get(key)) {
            case Some(e) => e
            case None =>
                entrySeq++
                let e = SessionEntry("sleep-idle-${entrySeq}")
                sessions[key] = e
                e
        }
    }

    private func loadState(key: String): Option<String> {
        match (store.get(persistenceId(key))) {
            case Some(r) => Some(r.state)
            case None => None<String>
        }
    }

    private func saveState(key: String, state: String): Unit {
        let pid = persistenceId(key)
        let revision = store.revisionOf(pid) + 1
        store.upsert(pid, revision, state)
    }

    private func armIdleTimer(context: ActorContext, entry: SessionEntry, key: String): Unit {
        match (policy) {
            case Idle(d) => context.timers.startSingleTimer(entry.timerKey, IdleTick(key), d)
            case Explicit => ()
        }
    }

    private func cancelIdleTimer(context: ActorContext, entry: SessionEntry): Unit {
        context.timers.cancel(entry.timerKey)
    }

    /** 唤醒（若无活跃实例）：读回状态、创建实例、投递第一条消息 */
    private func deliver(context: ActorContext, key: String, factory: (Option<String>) -> Actor,
        payload: Message, sender: Option<ActorRef>): Unit {
        let entry = entryOf(key)
        entry.pendingSleepGeneration = -1
        match (entry.active) {
            case Some(child) =>
                match (sender) {
                    case Some(s) => child.tell(payload, s)
                    case None => child.tell(payload)
                }
            case None =>
                let restored = loadState(key)
                childSeq++
                let gen = childSeq
                let name = "sleep-${gen}"
                let props = PropsFactory.create<Actor>({ => factory(restored) })
                    .withDispatcherMode(DispatcherMode.ThreadPerActor)
                let child = context.actorOf(props, name)
                context.watchWith(child, ChildGone(key, gen))
                entry.active = Some(child)
                entry.generation = gen
                match (sender) {
                    case Some(s) => child.tell(payload, s)
                    case None => child.tell(payload)
                }
        }
        armIdleTimer(context, entry, key)
    }

    private func beginSleep(context: ActorContext, key: String): Unit {
        match (sessions.get(key)) {
            case Some(entry) =>
                match (entry.active) {
                    case Some(child) =>
                        if (entry.pendingSleepGeneration < 0) {
                            entry.pendingSleepGeneration = entry.generation
                            child.tell(CaptureSleep(context.self, key, entry.generation))
                        }
                    case None => ()
                }
            case None => ()
        }
    }

    private func finishSleep(context: ActorContext, msg: SleepStateMsg): Unit {
        match (sessions.get(msg.key)) {
            case Some(entry) =>
                // 等待期间又来消息：本次睡眠已被取消（消息已经交给旧实例处理）
                if (entry.pendingSleepGeneration != msg.generation) {
                    return
                }
                entry.pendingSleepGeneration = -1
                saveState(msg.key, msg.state)
                cancelIdleTimer(context, entry)
                match (entry.active) {
                    case Some(child) =>
                        entry.active = None<ActorRef>
                        context.stop(child)
                    case None => ()
                }
            case None => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case h: HandleMessage =>
                deliver(context, h.key, h.factory, h.payload, h.sender)
                MessageResult.Handled
            case t: IdleTick =>
                beginSleep(context, t.key)
                MessageResult.Handled
            case s: SleepNow =>
                beginSleep(context, s.key)
                MessageResult.Handled
            case r: SleepStateMsg =>
                finishSleep(context, r)
                MessageResult.Handled
            case g: ChildGone =>
                match (sessions.get(g.key)) {
                    case Some(entry) =>
                        // 只清理"仍是这一代"的实例；正常睡眠时 active 已先置空
                        match (entry.active) {
                            case Some(child) =>
                                if (entry.generation == g.generation) {
                                    entry.active = None<ActorRef>
                                    entry.pendingSleepGeneration = -1
                                }
                            case None => ()
                        }
                    case None => ()
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/**
 * 可睡眠会话的句柄：持有管理器引用与 key，tell 时若实例在睡则先唤醒。
 *
 * 句柄不是 ActorRef——它不占一个 Actor；睡眠中的会话只有"句柄 + 存储里的状态"。
 */
public class SleepableHandle {
    private let manager: ActorRef
    private let key: String
    private let factory: (Option<String>) -> Actor
    private let path: String

    init(manager: ActorRef, key: String, factory: (Option<String>) -> Actor, path: String) {
        this.manager = manager
        this.key = key
        this.factory = factory
        this.path = path
    }

    /** 投递消息（睡着的会话会先被唤醒，消息只处理一次） */
    public func tell(message: Message): Unit {
        manager.tell(HandleMessage(key, factory, message, None<ActorRef>))
    }

    /** 带上发送方投递（会话可以用 context.reply 回复） */
    public func tell(message: Message, sender: ActorRef): Unit {
        manager.tell(HandleMessage(key, factory, message, Some<ActorRef>(sender)))
    }

    /** 显式睡眠（SleepPolicy.Explicit 或提前释放） */
    public func sleep(): Unit {
        manager.tell(SleepNow(key))
    }

    /** 会话的稳定标识（诊断用；不是 Actor 路径） */
    public func pathString(): String {
        path
    }

    public func toString(): String {
        "SleepableHandle(${path})"
    }
}

/**
 * 睡眠注册表：一个协调 Actor + 一组按 key 的会话。
 *
 * @param store 状态存储；默认内存实现（进程内），生产传 FileDurableStateStore 等
 * @param policy Idle(d) 自动睡 / Explicit 显式睡
 */
public class SleepRegistry {
    private let system: CActorSystem
    private let manager: ActorRef
    private let name: String
    private let policy: SleepPolicy

    public init(system: CActorSystem, name: String,
        store!: DurableStateStore = MemoryDurableStateStore(),
        policy!: SleepPolicy = SleepPolicy.Explicit) {
        this.system = system
        this.name = name
        this.policy = policy
        let props = PropsFactory.create<Actor>({ => SleepManagerActor(name, store, policy) })
            .withDispatcherMode(DispatcherMode.ThreadPerActor)
        this.manager = system.actorOf(props, name)
    }

    /** 创建（或取得）某个 key 的会话句柄；factory 在每次唤醒时调用，参数是上次睡眠时的状态 */
    public func handle(key: String, factory: (Option<String>) -> Actor): SleepableHandle {
        SleepableHandle(manager, key, factory, "/sleepable/${name}/${key}")
    }

    /** 注册表使用的睡眠策略 */
    public func sleepPolicy(): SleepPolicy {
        policy
    }

    /** 停止管理器（会话实例随之停止；状态保留在存储里） */
    public func stop(): Unit {
        system.stop(manager)
    }
}
