/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R8 测试：Prometheus 指标导出
 */
package cactor_tests.unit.observability

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_runtime.{escapePrometheusLabel}
import cactor_testkit.*

class Sink <: Actor {
    public prop name: String { get() { "sink" } }
    public prop description: String { get() { "sink" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage where s.getContent() == "boom" => throw Exception("boom")
            case _ => MessageResult.Handled
        }
    }
}

func lineValue(text: String, prefix: String): Option<String> {
    for (line in text.split("\n")) {
        if (line.startsWith(prefix + " ")) {
            return Some(line[(prefix.size + 1)..])
        }
    }
    None<String>
}

@Test
public class PrometheusTest {
    @TestCase
    public func testRuntimeAndPerActorMetrics(): Unit {
        let kit = ActorTestKit("prom")
        let a = kit.actorOf({ => Sink() }, "sink")
        for (_ in 0..5) {
            a << StringMessage("x")
        }
        let key = "cactor_actor_processed_total{system=\"prom\",path=\"/user/sink\"}"
        @Expect(awaitCondition(Duration.second * 3, { => lineValue(kit.system.metricsText(true), key) == Some("5") }), true)
        let text = kit.system.metricsText(true)
        @Expect(text.contains("# TYPE cactor_actors_live gauge"), true)
        @Expect(text.contains("# TYPE cactor_dead_letters_total counter"), true)
        @Expect(lineValue(text, "cactor_actor_processed_total{system=\"prom\",path=\"/user/sink\"}") ?? "", "5")
        @Expect(lineValue(text, "cactor_actor_mailbox_size{system=\"prom\",path=\"/user/sink\"}") ?? "", "0")
        // 每个样本行都符合 name{labels} value 或 name value 格式
        for (line in text.split("\n")) {
            if (line.isEmpty() || line.startsWith("#")) {
                continue
            }
            @Expect(line.split(" ").size >= 2, true)
        }
        let compact = kit.system.metricsText(false)
        @Expect(compact.contains("cactor_actor_processed_total"), false)
        kit.shutdown()
    }

    @TestCase
    public func testLabelEscaping(): Unit {
        @Expect(escapePrometheusLabel("a\"b\\c\nd"), "a\\\"b\\\\c\\nd")
    }
}
