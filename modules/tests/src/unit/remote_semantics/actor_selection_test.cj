/*
 * CActor - 远程 ActorSelection（差距 4.1：按路径/通配寻址，不要求事先持有引用）
 *
 * 覆盖：
 *   - 通配投递：`worker-*` 命中全部 worker，未命中的 actor 收不到；
 *   - 只匹配业务名称：`$sys`、回复入口等内部处理器永远不参与（`*` 也不会打到内部协议）；
 *   - 解析：resolveNames 排序返回、resolveOne 要求恰好一个；
 *   - 无匹配计入对端死信；
 *   - 路径形态（完整 / `/user/` / 裸模式）与通配语义（`*`、`?`）。
 */
package cactor_tests.unit.remote_semantics

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_testkit.*

let SEL_WAIT: Duration = Duration.second * 5

/** 记录收到的字符串（用实例直接断言，不依赖 ActorSystem 内部状态） */
class SelectionSink <: Actor {
    private let lock = Mutex()
    private let items = ArrayList<String>()

    public prop name: String { get() { "selection-sink" } }
    public prop description: String { get() { "selection sink" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            synchronized(lock) {
                items.add(s.getContent())
            }
        }
        MessageResult.Handled
    }

    func count(): Int64 {
        synchronized(lock) {
            items.size
        }
    }

    func snapshot(): Array<String> {
        synchronized(lock) {
            items.toArray()
        }
    }
}

func selectionNode(): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configure(FAST)
    m.start()
    m
}

@Test
public class ActorSelectionTest {
    @TestCase
    public func tellFansOutToMatchesOnly(): Unit {
        let kit = ActorTestKit("sel-tell")
        let a = selectionNode()
        let b = selectionNode()
        let w1 = SelectionSink()
        let w2 = SelectionSink()
        let counter = SelectionSink()
        try {
            b.exposeActor("worker-1", kit.actorOf({ => w1 }, "sel-w1"))
            b.exposeActor("worker-2", kit.actorOf({ => w2 }, "sel-w2"))
            b.exposeActor("counter", kit.actorOf({ => counter }, "sel-counter"))

            let sel = a.actorSelection(b.getLocalAddress(), "worker-*")
            @Expect(sel.path(), "cactor://${b.getLocalAddress().toString()}/user/worker-*")
            sel.tell(StringMessage("hi"))

            @Expect(awaitCondition(SEL_WAIT, { => w1.count() == 1 && w2.count() == 1 }), true)
            @Expect(w1.snapshot()[0], "hi")
            @Expect(w2.snapshot()[0], "hi")
            @Expect(counter.count(), 0)
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func resolutionListsExposedNamesAndExcludesInternals(): Unit {
        let kit = ActorTestKit("sel-resolve")
        let a = selectionNode()
        let b = selectionNode()
        try {
            b.exposeActor("worker-1", kit.actorOf({ => SelectionSink() }, "sel-r1"))
            b.exposeActor("worker-2", kit.actorOf({ => SelectionSink() }, "sel-r2"))
            b.exposeActor("counter", kit.actorOf({ => SelectionSink() }, "sel-r3"))

            let workers = a.actorSelection(b.getLocalAddress(), "/user/worker-*").resolveNames()
            @Expect(workers.size, 2)
            @Expect(workers[0], "worker-1")
            @Expect(workers[1], "worker-2")

            // `*` 只返回业务名称：内部入口（$sys、ask/回复临时入口）不参与，且结果已排序
            let all = a.actorSelection(b.getLocalAddress(), "*").resolveNames()
            @Expect(all.size, 3)
            @Expect(all[0], "counter")
            @Expect(all[1], "worker-1")
            @Expect(all[2], "worker-2")
            for (n in all) {
                @Expect(n.startsWith("$"), false)
            }
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func resolveOneRequiresExactlyOneMatch(): Unit {
        let kit = ActorTestKit("sel-one")
        let a = selectionNode()
        let b = selectionNode()
        let counter = SelectionSink()
        try {
            b.exposeActor("worker-1", kit.actorOf({ => SelectionSink() }, "sel-o1"))
            b.exposeActor("worker-2", kit.actorOf({ => SelectionSink() }, "sel-o2"))
            b.exposeActor("counter", kit.actorOf({ => counter }, "sel-o3"))

            let one = a.actorSelection(b.getLocalAddress(), "counter").resolveOne()
            one.tell(StringMessage("direct"))
            @Expect(awaitCondition(SEL_WAIT, { => counter.count() == 1 }), true)
            @Expect(counter.snapshot()[0], "direct")

            // 多个匹配与零匹配都必须失败，避免"悄悄只发一个"的歧义
            @ExpectThrows[IllegalStateException](
                a.actorSelection(b.getLocalAddress(), "worker-*").resolveOne())
            @ExpectThrows[IllegalStateException](
                a.actorSelection(b.getLocalAddress(), "nobody-*").resolveOne())
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func unmatchedSelectionCountsAsUndeliverable(): Unit {
        let kit = ActorTestKit("sel-miss")
        let a = selectionNode()
        let b = selectionNode()
        try {
            b.exposeActor("worker-1", kit.actorOf({ => SelectionSink() }, "sel-m1"))
            let before = b.getReceiver().getUndeliverableCount()
            a.actorSelection(b.getLocalAddress(), "nobody-*").tell(StringMessage("x"))
            @Expect(awaitCondition(SEL_WAIT, { =>
                b.getReceiver().getUndeliverableCount() > before }), true)
            @Expect(a.actorSelection(b.getLocalAddress(), "nobody-*").resolveNames().size, 0)
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func pathFormsAndGlobSemantics(): Unit {
        // 通配语义：`*` 任意长度、`?` 单字节，其余按字面
        @Expect(selectionGlobMatch("worker-*", "worker-1"), true)
        @Expect(selectionGlobMatch("worker-*", "worker-"), true)
        @Expect(selectionGlobMatch("worker-?", "worker-1"), true)
        @Expect(selectionGlobMatch("worker-?", "worker-12"), false)
        @Expect(selectionGlobMatch("*-1", "worker-1"), true)
        @Expect(selectionGlobMatch("w*r-2", "worker-2"), true)
        @Expect(selectionGlobMatch("counter", "counter"), true)
        @Expect(selectionGlobMatch("counter", "counter-1"), false)
        @Expect(selectionGlobMatch("*", "anything"), true)

        let a = selectionNode()
        try {
            let addr = NetworkAddress("127.0.0.1", 9000)
            // 三种写法都应解析出同一个模式
            @Expect(a.actorSelection(addr, "cactor://127.0.0.1:9000/user/worker-*").selectionPattern,
                "worker-*")
            @Expect(a.actorSelection(addr, "/user/worker-*").selectionPattern, "worker-*")
            @Expect(a.actorSelection(addr, "worker-*").selectionPattern, "worker-*")
            // 非法输入：缺 /user/ 段、空模式、控制协议分隔符
            @ExpectThrows[IllegalArgumentException](a.actorSelection(addr, "cactor://127.0.0.1:9000"))
            @ExpectThrows[IllegalArgumentException](a.actorSelection(addr, "/user/"))
            @ExpectThrows[IllegalArgumentException](a.actorSelection(addr, "worker|*"))
        } finally {
            a.stop()
        }
    }
}
