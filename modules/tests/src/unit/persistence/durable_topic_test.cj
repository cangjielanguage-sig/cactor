/*
 * CActor - 差距 4.8：持久化主题（Durable Subscriber）
 *
 * 覆盖：
 *   - 订阅者离线期间发布的消息，重连后按序重放；
 *   - 游标落 DurableStateStore：新主题实例（同一存储）继续从断点续读；
 *   - 在线实时投递与历史互补，不重不漏；
 *   - retention 只保留最近 N 条；
 *   - handler 抛异常时不推进游标，重连重放（至少一次）；
 *   - 同名订阅者重复订阅明确失败。
 */
package cactor_tests.unit.persistence

import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.*

@Test
public class DurableTopicTest {
    @TestCase
    public func offlineSubscriberReplaysInOrder(): Unit {
        let store = MemoryDurableStateStore()
        let offsets = MemoryDurableStateStore()
        let topic = DurableTopic("orders", store)
        for (i in 1..6) {
            topic.publish("evt-${i}")
        }
        @Expect(topic.highestSequenceNr(), 5)

        let got = ArrayList<String>()
        let sub = topic.subscribe("audit", offsets, { seq, payload => got.add("${seq}:${payload}") })
        @Expect(got.size, 5)
        @Expect(got[0], "1:evt-1")
        @Expect(got[4], "5:evt-5")

        // 在线实时
        topic.publish("evt-6")
        @Expect(got.size, 6)
        @Expect(got[5], "6:evt-6")
        sub.unsubscribe()

        // 离线期间的消息：重连后补齐
        topic.publish("evt-7")
        topic.publish("evt-8")
        let resumed = ArrayList<String>()
        let sub2 = topic.subscribe("audit", offsets, { seq, payload => resumed.add("${seq}:${payload}") })
        @Expect(String.join(resumed.toArray(), delimiter: ","), "7:evt-7,8:evt-8")
        @Expect(topic.offsetOf("audit", offsets), 8)
        sub2.unsubscribe()
    }

    @TestCase
    public func cursorSurvivesNewTopicInstance(): Unit {
        let store = MemoryDurableStateStore()
        let offsets = MemoryDurableStateStore()
        let first = DurableTopic("payments", store)
        first.publish("p1")
        first.publish("p2")
        let seen = ArrayList<String>()
        first.subscribe("risk", offsets, { _, payload => seen.add(payload) }).unsubscribe()
        first.publish("p3")

        // 重开同一存储：游标仍是 2，续读 p3
        let reopened = DurableTopic("payments", store)
        let resumed = ArrayList<String>()
        let sub = reopened.subscribe("risk", offsets, { _, payload => resumed.add(payload) })
        @Expect(String.join(resumed.toArray(), delimiter: ","), "p3")
        reopened.publish("p4")
        @Expect(String.join(resumed.toArray(), delimiter: ","), "p3,p4")
        sub.unsubscribe()
    }

    @TestCase
    public func handlerFailureDoesNotAdvanceCursor(): Unit {
        let store = MemoryDurableStateStore()
        let offsets = MemoryDurableStateStore()
        let topic = DurableTopic("billing", store)
        topic.publish("b1")
        topic.publish("b2")
        let fail = AtomicBool(true)
        let seen = ArrayList<String>()
        let sub = topic.subscribe("worker", offsets, { seq, payload =>
            if (fail.load() && seq == 2) {
                throw IllegalStateException("下游暂时失败")
            }
            seen.add(payload)
        })
        // 回放中第 2 条失败：停止在该条之前
        @Expect(String.join(seen.toArray(), delimiter: ","), "b1")
        @Expect(topic.offsetOf("worker", offsets), 1)
        sub.unsubscribe()

        // 故障恢复后重连：从 b2 继续
        fail.store(false)
        let resumed = ArrayList<String>()
        let sub2 = topic.subscribe("worker", offsets, { _, payload => resumed.add(payload) })
        @Expect(String.join(resumed.toArray(), delimiter: ","), "b2")
        @Expect(topic.offsetOf("worker", offsets), 2)
        sub2.unsubscribe()
    }

    @TestCase
    public func retentionKeepsOnlyRecentMessages(): Unit {
        let store = MemoryDurableStateStore()
        let topic = DurableTopic("logs", store, retention: 3)
        for (i in 1..6) {
            topic.publish("l${i}")
        }
        let kept = topic.history(1)
        @Expect(kept.size, 3)
        @Expect(kept[0][0], 3)
        @Expect(kept[2][0], 5)
        @Expect(kept[2][1], "l5")
        let offsets = MemoryDurableStateStore()
        let first = topic.subscribe("dup", offsets, { _, _ => () })
        @ExpectThrows[IllegalStateException](topic.subscribe("dup", offsets, { _, _ => () }))
        first.unsubscribe()
    }
}
