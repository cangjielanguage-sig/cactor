/*
 * CActor - 经典路径 ask 临时收件箱测试（对照 docs/plan/dsl20261030.md §7.1「Ask 语法糖」）
 *
 * 验证：
 * 1. 收件箱作为普通 ActorRef 参与 `replyTo << 回复`，get 阻塞拿到回复；
 * 2. 超时抛 AskTimeoutException；
 * 3. 真实 Actor 调度路径：@actor 服务收到 Query(replyTo=inbox) 回 Answer，收件箱取到。
 */
package cactor_tests.unit.dsl

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor_core.dsl.*
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
        } catch (e: AskTimeoutException) {
            timeout = true
        }
        @Expect(timeout, true)
        // tryGet 不抛异常
        @Expect(inbox.tryGet(10.millis).isNone(), true)
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
