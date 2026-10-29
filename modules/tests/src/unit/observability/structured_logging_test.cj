/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R3 测试：结构化日志字段、MDC（含 Actor 中的作用域恢复）、JSON Lines 输出
 */
package cactor_tests.unit.observability

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.*
import cactor_testkit.*

/** 每条消息在 MDC 作用域内记录日志，作用域外再记录一条 */
class RequestActor <: Actor {
    public prop name: String { get() { "req" } }
    public prop description: String { get() { "req" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            Mdc.scoped([("requestId", s.getContent())], { =>
                context.log().withField("step", "handle").warn("handling")
            })
            context.log().warn("after")
        }
        MessageResult.Handled
    }
}

@Test
public class StructuredLoggingTest {
    @TestCase
    public func testLoggerFieldsAreImmutableAndFormatted(): Unit {
        let sink = MemorySink()
        Logging.setSink(sink)
        let base = Logging.logger("orders")
        let withUser = base.withField("user", "u1")
        let withBoth = withUser.withFields([("order", "o-9"), ("amount", "12.5")])
        withBoth.warn("placed")
        base.warn("plain")
        Logging.setSink(StderrSink())
        let rs = sink.snapshot()
        @Expect(rs.size, 2)
        @Expect(rs[0].field("user"), Some("u1"))
        @Expect(rs[0].field("amount"), Some("12.5"))
        @Expect(rs[0].format().endsWith("[orders] placed user=u1 order=o-9 amount=12.5"), true)
        // 原记录器不受影响
        @Expect(rs[1].fields.size, 0)
        @Expect(base.fields.size, 0)
        @Expect(withUser.fields.size, 1)
    }

    @TestCase
    public func testMdcPutRemoveAndScopedRestore(): Unit {
        let sink = MemorySink()
        Logging.setSink(sink)
        let log = Logging.logger("mdc")
        Mdc.put("tenant", "t1")
        log.warn("one")
        let r = Mdc.scoped([("tenant", "t2"), ("req", "r7")], { =>
            log.warn("two")
            42
        })
        @Expect(r, 42)
        // 作用域结束：恢复为 t1，req 被移除
        @Expect(Mdc.get("tenant"), Some("t1"))
        @Expect(Mdc.get("req"), None<String>)
        // 异常时同样恢复
        try {
            Mdc.scoped<Unit>([("req", "boom")], { =>
                throw Exception("x")
            })
        } catch (_: Exception) {}
        @Expect(Mdc.get("req"), None<String>)
        Mdc.remove("tenant")
        log.warn("three")
        Mdc.clear()
        Logging.setSink(StderrSink())
        let rs = sink.snapshot()
        @Expect(rs[0].field("tenant"), Some("t1"))
        @Expect(rs[1].field("tenant"), Some("t2"))
        @Expect(rs[1].field("req"), Some("r7"))
        @Expect(rs[2].fields.size, 0)
    }

    @TestCase
    public func testMdcIsPerThread(): Unit {
        Mdc.put("who", "main")
        let other = spawn {
            Mdc.get("who")
        }
        @Expect(other.get(), None<String>)
        @Expect(Mdc.get("who"), Some("main"))
        Mdc.clear()
    }

    @TestCase
    public func testMdcInActorDoesNotLeakToNextMessage(): Unit {
        let sink = MemorySink()
        Logging.setSink(sink)
        let kit = ActorTestKit("mdc-actor")
        let r = kit.actorOf({ => RequestActor() }, "req")
        r << StringMessage("r1") << StringMessage("r2")
        @Expect(awaitCondition(Duration.second * 3, { => sink.snapshot().size == 4 }), true)
        Logging.setSink(StderrSink())
        let rs = sink.snapshot()
        @Expect(rs[0].field("requestId"), Some("r1"))
        @Expect(rs[0].field("step"), Some("handle"))
        @Expect(rs[0].source, "/user/req")
        @Expect(rs[1].message, "after")
        @Expect(rs[1].field("requestId"), None<String>)
        @Expect(rs[2].field("requestId"), Some("r2"))
        @Expect(rs[3].field("requestId"), None<String>)
        kit.shutdown()
    }

    @TestCase
    public func testJsonLinesEscaping(): Unit {
        let lines = ArrayList<String>()
        Logging.setSink(JsonLinesSink({ line => lines.add(line) }))
        Logging.logger("json\"src").withField("path", "C:\\tmp\n\"x\"").warn("引号\" 反斜杠\\ 制表\t 控制\u{1}")
        Logging.setSink(StderrSink())
        @Expect(lines.size, 1)
        let line = lines[0]
        @Expect(line.contains("\n"), false)
        @Expect(line.startsWith("{\"ts\":"), true)
        @Expect(line.endsWith("}"), true)
        @Expect(line.contains(",\"level\":\"WARN\""), true)
        @Expect(line.contains(",\"source\":\"json\\\"src\""), true)
        // 引号、反斜杠、制表符、控制字符都被转义，中文原样保留
        @Expect(line.contains(",\"message\":\"引号\\\" 反斜杠\\\\ 制表\\t 控制\\u0001\""), true)
        @Expect(line.contains(",\"path\":\"C:\\\\tmp\\n\\\"x\\\"\""), true)
    }
}
