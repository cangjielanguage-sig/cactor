/*
 * Copyright (c) 2024. All rights reserved.
 * CActor TestKit
 *
 * 对标 Akka ActorTestKit / TestProbe：
 * - TestProbe：一个真实的探针 Actor，把收到的消息放进阻塞队列，测试线程用 expectMsg 等方法断言
 * - EventRecorder：线程安全的事件记录，用于断言生命周期回调的顺序
 * - awaitCondition：带超时的条件轮询，替代固定 sleep
 * - ActorTestKit：持有一个测试用的 ActorSystem，统一创建探针和关闭
 */
package cactor_testkit

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.actor.{Actor, ActorRef, ActorContext, Props, Terminated}
import cactor_core.message.{Message, MessageResult}
import cactor_core.typed.{TypedActorRef, ClassicTypedRef, TypedPayloadBox, TypedBehavior}
import cactor_core.typed.{TypedActorRef, ClassicTypedRef}
import cactor.api.{CActorSystem, CActorSystemFactory}

/** 断言失败 */
public class TestKitAssertionException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 轮询等待条件成立（每 1ms 检查一次），超时返回 false
 */
public func awaitCondition(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond)
    }
    cond()
}

/** 条件不成立时抛出 TestKitAssertionException */
public func awaitAssert(timeout: Duration, description: String, cond: () -> Bool): Unit {
    if (!awaitCondition(timeout, cond)) {
        throw TestKitAssertionException("等待超时（${timeout}）: ${description}")
    }
}

/**
 * 线程安全的事件记录器：Actor 在回调中 record，测试线程断言顺序与次数
 */
public class EventRecorder {
    private let lock = Mutex()
    private let events = ArrayList<String>()

    public init() {}

    public func record(event: String): Unit {
        synchronized(lock) {
            events.add(event)
        }
    }

    public func snapshot(): Array<String> {
        synchronized(lock) {
            events.toArray()
        }
    }

    public func size(): Int64 {
        synchronized(lock) {
            events.size
        }
    }

    public func count(event: String): Int64 {
        var n: Int64 = 0
        for (e in snapshot()) {
            if (e == event) {
                n++
            }
        }
        n
    }

    public func contains(event: String): Bool {
        count(event) > 0
    }

    /** event 第一次出现的位置，不存在返回 -1 */
    public func indexOf(event: String): Int64 {
        let all = snapshot()
        var i: Int64 = 0
        while (i < all.size) {
            if (all[i] == event) {
                return i
            }
            i++
        }
        -1
    }

    /** 等待 event 出现 */
    public func await(event: String, timeout: Duration): Bool {
        awaitCondition(timeout, { => this.contains(event) })
    }

    /** 等待记录总数达到 n */
    public func awaitSize(n: Int64, timeout: Duration): Bool {
        awaitCondition(timeout, { => this.size() >= n })
    }

    public func clear(): Unit {
        synchronized(lock) {
            events.clear()
        }
    }

    public func toString(): String {
        let all = snapshot()
        var s = "["
        var i: Int64 = 0
        while (i < all.size) {
            if (i > 0) {
                s += ", "
            }
            s += all[i]
            i++
        }
        s + "]"
    }
}

/** 探针收到的一条消息 */
public class ReceivedMessage {
    public let message: Message
    public let sender: Option<ActorRef>

    public init(message: Message, sender: Option<ActorRef>) {
        this.message = message
        this.sender = sender
    }
}

class ProbeQueue {
    let lock = Mutex()
    let cond: Condition
    let items = ArrayList<ReceivedMessage>()
    var head: Int64 = 0

    init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    func put(item: ReceivedMessage): Unit {
        synchronized(lock) {
            items.add(item)
            cond.notifyAll()
        }
    }

    func poll(timeout: Duration): Option<ReceivedMessage> {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (head >= items.size) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return None<ReceivedMessage>
                }
                cond.wait(timeout: left)
            }
            let item = items[head]
            head++
            Some(item)
        }
    }

    func pendingCount(): Int64 {
        synchronized(lock) {
            items.size - head
        }
    }
}

class ProbeActor <: Actor {
    let queue: ProbeQueue
    let autoReply: Option<(Message) -> Option<Message>>

    init(queue: ProbeQueue, autoReply: Option<(Message) -> Option<Message>>) {
        this.queue = queue
        this.autoReply = autoReply
    }

    public prop name: String {
        get() { "TestProbe" }
    }

    public prop description: String {
        get() { "测试探针" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        // 控制消息：在探针自己的上下文里建立监视（watch 只能在 Actor 线程里调用）
        if (let Some(w) <- (message as ProbeWatch)) {
            context.watch(w.target)
            return MessageResult.Handled
        }
        queue.put(ReceivedMessage(message, context.senderRef()))
        if (let Some(f) <- autoReply) {
            if (let Some(r) <- f(message)) {
                context.reply(r)
            }
        }
        MessageResult.Handled
    }
}

/** 让探针监视某个 Actor（内部消息，不进入被测逻辑） */
class ProbeWatch <: Message {
    let target: ActorRef

    init(target: ActorRef) {
        this.target = target
    }

    public func messageType(): String {
        "ProbeWatch"
    }
}

/**
 * 测试探针：一个真实的 Actor。把 probe.ref 交给被测 Actor，然后在测试线程中断言收到的消息
 */
public class TestProbe {
    private let queue: ProbeQueue
    private let probeRef: ActorRef
    private var lastReceived: Option<ReceivedMessage> = None<ReceivedMessage>
    public static let DEFAULT_TIMEOUT: Duration = Duration.second * 3

    public init(system: CActorSystem, name: String) {
        this(system, name, None<(Message) -> Option<Message>>)
    }

    /** autoReply：收到消息时自动回复（返回 None 则不回复） */
    public init(system: CActorSystem, name: String, autoReply: Option<(Message) -> Option<Message>>) {
        let q = ProbeQueue()
        this.queue = q
        this.probeRef = system.actorOf({ => ProbeActor(q, autoReply) }, name)
    }

    /** 探针的引用 */
    public prop ref: ActorRef {
        get() { probeRef }
    }

    /** 等待下一条消息，超时抛出 TestKitAssertionException */
    public func expectMsg(timeout: Duration): Message {
        match (queue.poll(timeout)) {
            case Some(item) =>
                lastReceived = Some(item)
                item.message
            case None => throw TestKitAssertionException("${timeout} 内没有收到消息")
        }
    }

    public func expectMsg(): Message {
        expectMsg(DEFAULT_TIMEOUT)
    }

    /** 等待下一条消息并要求其类型为 T */
    public func expectMsgType<T>(timeout: Duration): T where T <: Message {
        let m = expectMsg(timeout)
        match (m as T) {
            case Some(t) => t
            case None => throw TestKitAssertionException("收到的消息类型不符: ${m.messageType()}")
        }
    }

    public func expectMsgType<T>(): T where T <: Message {
        expectMsgType<T>(DEFAULT_TIMEOUT)
    }

    /** 在 duration 内不应收到任何消息 */
    public func expectNoMsg(duration: Duration): Unit {
        match (queue.poll(duration)) {
            case Some(item) => throw TestKitAssertionException("不应收到消息，但收到了: ${item.message.messageType()}")
            case None => ()
        }
    }

    /**
     * 让探针监视 target；target 停止后探针会收到 Terminated（作为普通消息入队）。
     * 对标 Akka TestProbe.watch。
     */
    public func watch(target: ActorRef): Unit {
        probeRef.tell(ProbeWatch(target))
    }

    /**
     * 等待 target 停止（E62，对标 Akka expectTerminated）。
     * 已经停止的目标会立即观察到（watch 已停止的目标会马上投递 Terminated）；
     * 期间收到的其他消息被丢弃（与 Akka 一致，需要保留请先自行收完）。
     */
    public func expectTerminated(target: ActorRef, timeout: Duration): Terminated {
        watch(target)
        let deadline = MonoTime.now() + timeout
        while (true) {
            let left = deadline - MonoTime.now()
            if (left <= Duration.Zero) {
                throw TestKitAssertionException("${timeout} 内被监视的 Actor 没有停止")
            }
            let m = try {
                expectMsg(left)
            } catch (_: TestKitAssertionException) {
                throw TestKitAssertionException("${timeout} 内被监视的 Actor 没有停止")
            }
            if (let Some(t) <- (m as Terminated)) {
                if (t.actor.equals(target)) {
                    return t
                }
            }
        }
        throw TestKitAssertionException("不可达")
    }

    /** 等待 target 停止，使用默认超时 */
    public func expectTerminated(target: ActorRef): Terminated {
        expectTerminated(target, DEFAULT_TIMEOUT)
    }

    /** 连续接收 n 条消息 */
    public func receiveN(n: Int64, timeout: Duration): Array<Message> {
        let deadline = MonoTime.now() + timeout
        let result = ArrayList<Message>()
        while (result.size < n) {
            let left = deadline - MonoTime.now()
            match (queue.poll(if (left > Duration.Zero) { left } else { Duration.Zero })) {
                case Some(item) =>
                    lastReceived = Some(item)
                    result.add(item.message)
                case None => throw TestKitAssertionException("${timeout} 内只收到 ${result.size}/${n} 条消息")
            }
        }
        result.toArray()
    }

    /** 最近一条消息的发送方 */
    public func lastSender(): Option<ActorRef> {
        match (lastReceived) {
            case Some(item) => item.sender
            case None => None<ActorRef>
        }
    }

    /** 以探针身份回复最近一条消息的发送方 */
    public func reply(message: Message): Unit {
        match (lastSender()) {
            case Some(s) => s.tell(message, probeRef)
            case None => throw TestKitAssertionException("最近一条消息没有发送方，无法回复")
        }
    }

    /** 以探针身份发送消息（对方可以 reply 给探针） */
    public func send(target: ActorRef, message: Message): Unit {
        target.tell(message, probeRef)
    }

    /** 尚未取出的消息数 */
    public func pending(): Int64 {
        queue.pendingCount()
    }
}

/**
 * 测试套件辅助：创建一个独立的 ActorSystem，统一创建探针，测试结束时 shutdown
 */
public class ActorTestKit {
    public let system: CActorSystem
    private var probeSeq: Int64 = 0

    public init(name: String) {
        this.system = CActorSystemFactory.create(name)
    }

    public func createTestProbe(): TestProbe {
        probeSeq++
        TestProbe(system, "testProbe-${probeSeq}")
    }

    public func createTestProbe(name: String): TestProbe {
        TestProbe(system, name)
    }

    public func actorOf(creator: () -> Actor, name: String): ActorRef {
        system.actorOf(creator, name)
    }

    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        system.actorOf(props, name)
    }

    /** 类型化生成（E62 的一部分） */
    public func spawnTyped<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        system.typed().actorOf(behavior, name)
    }

    public func shutdown(): Unit {
        system.terminate()
    }
}


/**
 * 类型化测试探针：对 TypedActorRef<T> 的消息做断言（E62 的一部分）。
 * T 需要是 Message 的子类型。
 */
public class TypedTestProbe<T> where T <: Message {
    private let probe: TestProbe
    public let ref: TypedActorRef<T>

    public init(system: CActorSystem, name: String) {
        this.probe = TestProbe(system, name)
        this.ref = ClassicTypedRef<T>(probe.ref)
    }

    public func expectMsg(timeout: Duration): T {
        let m = probe.expectMsg(timeout)
        match (m as T) {
            case Some(t) => t
            case None => throw TestKitAssertionException("收到的消息类型不符: ${m.messageType()}")
        }
    }

    public func expectMsg(): T {
        expectMsg(TestProbe.DEFAULT_TIMEOUT)
    }

    public func expectNoMsg(duration: Duration): Unit {
        probe.expectNoMsg(duration)
    }

    /** 等待一条满足 predicate 的消息（fishForMessage），最多 timeout */
    public func fishForMessage(timeout: Duration, predicate: (T) -> Bool): T {
        let deadline = MonoTime.now() + timeout
        while (true) {
            let left = deadline - MonoTime.now()
            if (left <= Duration.Zero) {
                throw TestKitAssertionException("fishForMessage 超时")
            }
            let m = probe.expectMsg(left)
            match (m as T) {
                case Some(t) =>
                    if (predicate(t)) {
                        return t
                    }
                case None => ()
            }
        }
        throw TestKitAssertionException("fishForMessage 未找到匹配消息")
    }


    public func send(target: TypedActorRef<T>, message: T): Unit {
        let cr = (ref as ClassicTypedRef<T>).getOrThrow()
        cr.classic.tell(TypedPayloadBox(message), probe.ref)
    }
}
