/*
 * CActor - 经典路径 ask 临时收件箱测试（对照 docs/plan/dsl20261030.md §7.1「Ask 语法糖」）
 *
 * 验证：
 * 1. 收件箱作为普通 ActorRef 参与 `replyTo << 回复`，get 阻塞拿到回复；
 * 2. 超时抛 TypedAskTimeoutException（与 TypedAsk / ctx.ask 共用同一个超时异常）；
 * 3. 真实 Actor 调度路径：@actor 服务收到 Query(replyTo=inbox) 回 Answer，收件箱取到；
 * 4. 每个收件箱路径唯一（equals 按路径判等，连续创建的收件箱不能互相"相等"）。
 */
package cactor_tests.unit.dsl

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef, PropsFactory}
import cactor_core.message.{Message, MessageResult}
import cactor_core.dsl.*
import cactor_core.typed.TypedAskTimeoutException
import cactor_macros.*
import cactor_testkit.*

/** 请求消息（自带 replyTo，零侵入：目标无需解包任何包装消息） */
class InboxQuery <: Message {
    let text: String
    let replyTo: ActorRef

    init(text: String, replyTo: ActorRef) {
        this.text = text
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "InboxQuery"
    }
}

/** 应答消息 */
class InboxAnswer <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "InboxAnswer"
    }
}

@actor[cases = [InboxQuery]]
class InboxService <: Actor {
    func onInboxQuery(m: InboxQuery, ctx: ActorContext): MessageResult {
        m.replyTo << InboxAnswer("答复：${m.text}")
        MessageResult.Handled
    }
}

/** 在处理方法里阻塞等待收件箱：必须立即报错，而不是占住亲和调度线程直到超时 */
class BlockingAsker <: Message {
    let rec: EventRecorder
    let timeout: Duration

    init(rec: EventRecorder, timeout: Duration) {
        this.rec = rec
        this.timeout = timeout
    }

    public func messageType(): String {
        "BlockingAsker"
    }
}

@actor[cases = [BlockingAsker]]
class InboxMisuseActor <: Actor {
    func onBlockingAsker(m: BlockingAsker) {
        let inbox = askInbox()
        // 零超时只是查看，不会阻塞，Actor 内也允许
        m.rec.record("peek ${inbox.tryGet(Duration.Zero).isNone()}")
        try {
            inbox.get(m.timeout)
            m.rec.record("replied")
        } catch (e: IllegalStateException) {
            m.rec.record("rejected")
        } catch (e: TypedAskTimeoutException) {
            m.rec.record("timed-out")
        }
    }
}

@Test
public class AskInboxTest {
    @TestCase
    public func testInboxReceivesReply(): Unit {
        let inbox = askInbox()
        @Expect(inbox.received(), 0)
        // 模拟回复方：replyTo << 消息
        inbox << InboxAnswer("hello")
        @Expect(inbox.received(), 1)
        let m = inbox.get(1.seconds)
        @Expect((m as InboxAnswer).getOrThrow().text, "hello")
    }

    @TestCase
    public func testInboxTimesOut(): Unit {
        let inbox = askInbox()
        var timeout = false
        try {
            inbox.get(50.millis)
        } catch (e: TypedAskTimeoutException) {
            timeout = true
        }
        @Expect(timeout, true)
        // tryGet 不抛异常
        @Expect(inbox.tryGet(10.millis).isNone(), true)
    }

    @TestCase
    public func testInboxKeepsOrderUnderBacklog(): Unit {
        // 流式回复会在收件箱里积压：按到达顺序逐条取出
        let inbox = askInbox()
        for (i in 0..1000) {
            inbox << InboxAnswer("chunk-${i}")
        }
        @Expect(inbox.received(), 1000)
        var inOrder = true
        for (i in 0..1000) {
            let m = (inbox.get(Duration.Zero) as InboxAnswer).getOrThrow()
            if (m.text != "chunk-${i}") {
                inOrder = false
            }
        }
        @Expect(inOrder, true)
        @Expect(inbox.received(), 0)
    }

    @TestCase
    public func testInboxPathsAreUnique(): Unit {
        // 同一时钟刻度内连续创建：按时间戳取路径时这里会撞路径而判等
        let a = askInbox()
        let b = askInbox()
        @Expect(a.path.getPath() != b.path.getPath(), true)
        @Expect(a.equals(b), false)
        @Expect(a.equals(a), true)
    }

    @TestCase
    public func testBlockingGetInsideActorIsRejected(): Unit {
        let kit = ActorTestKit("ask-inbox-misuse")
        let rec = EventRecorder()
        let a = kit.actorOf({ => InboxMisuseActor() }, "misuse")
        a << BlockingAsker(rec, 1.seconds)
        // 立即被拒（远早于 1 秒的超时），而不是阻塞后超时
        @Expect(rec.await("rejected", 900.millis), true)
        @Expect(rec.contains("peek true"), true)
        @Expect(rec.contains("timed-out"), false)
        kit.shutdown()
    }

    @TestCase
    public func testBlockingDispatcherMayWait(): Unit {
        // blocking 调度器本来就允许阻塞：不拒绝，按正常超时结束
        let kit = ActorTestKit("ask-inbox-blocking")
        let rec = EventRecorder()
        let props = PropsFactory.create<Actor>({ => InboxMisuseActor() }).withDispatcher("blocking")
        let a = kit.actorOf(props, "blocking-asker")
        a << BlockingAsker(rec, 50.millis)
        @Expect(rec.await("timed-out", 3.seconds), true)
        @Expect(rec.contains("rejected"), false)
        kit.shutdown()
    }

    @TestCase
    public func testInboxAskOverRealActorPath(): Unit {
        let kit = ActorTestKit("ask-inbox")
        let service = kit.actorOf({ => InboxService() }, "service")
        // 一行式经典 ask：收件箱填进 replyTo，阻塞等回信
        let inbox = askInbox()
        service << InboxQuery("今天天气如何", inbox)
        let answer = (inbox.get(3.seconds) as InboxAnswer).getOrThrow()
        @Expect(answer.text, "答复：今天天气如何")
        kit.shutdown()
    }
}
