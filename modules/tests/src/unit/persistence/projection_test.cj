/*
 * CActor - 差距 4.12：Projection（事件 → 读模型 + 持久化偏移）
 *
 * 覆盖：
 *   - 首次运行应用 tag 下全部事件，读模型与事件一致；
 *   - 重复运行幂等（不重复应用）；
 *   - 追加新事件后只应用增量；
 *   - reset 后从头重放（读模型由调用方重建）；
 *   - 偏移量持久化：新的 Projection 实例（同一偏移存储）从断点续跑。
 */
package cactor_tests.unit.persistence

import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.*

func appendTagged(store: MemoryEventStore, pid: String, amount: Int64): Unit {
    let seq = store.highestSequenceNr(pid)
    let e = BasicEvent("e-${pid}-${seq + 1}", pid, "AmountAdded", amount.toString()).withTags(["cart"])
    store.append(pid, [e], seq)
}

@Test
public class ProjectionTest {
    @TestCase
    public func projectionRebuildsReadModelAndIsIdempotent(): Unit {
        let store = MemoryEventStore()
        for (_ in 0..12) {
            appendTagged(store, "cart-a", 1)
        }
        for (_ in 0..8) {
            appendTagged(store, "cart-b", 2)
        }
        let journal = ReadJournal(store)
        let offsets = MemoryDurableStateStore()
        let model = HashMap<String, Int64>()
        let proj = Projection("totals", journal, offsets, { env =>
            let amount = Int64.parse(env.event.getEventData())
            model[env.persistenceId] = (model.get(env.persistenceId) ?? 0) + amount
        })

        @Expect(proj.runOnce("cart"), 20)
        @Expect(model.get("cart-a"), Some(12))
        @Expect(model.get("cart-b"), Some(16))
        @Expect(proj.offsetOf("cart-a"), 12)
        @Expect(proj.offsetOf("cart-b"), 8)

        // 幂等：再跑一次不重复应用
        @Expect(proj.runOnce("cart"), 0)
        @Expect(model.get("cart-a"), Some(12))

        // 追加新事件后只应用增量
        appendTagged(store, "cart-a", 5)
        appendTagged(store, "cart-b", 3)
        @Expect(proj.runOnce("cart"), 2)
        @Expect(model.get("cart-a"), Some(17))
        @Expect(model.get("cart-b"), Some(19))

        // 新实例共享同一偏移存储：从断点续跑，不重复
        let model2 = HashMap<String, Int64>()
        let resumed = Projection("totals", journal, offsets, { env =>
            let amount = Int64.parse(env.event.getEventData())
            model2[env.persistenceId] = (model2.get(env.persistenceId) ?? 0) + amount
        })
        @Expect(resumed.runOnce("cart"), 0)
        @Expect(model2.size, 0)

        // 全量重建：reset 后从头重放（调用方清空读模型）
        model.clear()
        proj.reset()
        @Expect(proj.runOnce("cart"), 22)
        @Expect(model.get("cart-a"), Some(17))
        @Expect(model.get("cart-b"), Some(19))
    }
}
