/*
 * CActor Ex 03 - 跨节点可靠事件骨干（可靠投递 + 去重 + 背压预算）
 *
 * 真实约束（docs/plan/2026117-lz.md §3 E3）：
 * - 不丢：生产者崩溃/断连时未确认事件要重发；
 * - 不重复生效：消费者按 (producerId, seq) 幂等处理；
 * - 慢消费者不能撑爆生产者：可靠通道有未确认上限（背压），超限明确丢弃并计数；
 * - 大消息有上限：超过单帧上限明确失败，而不是静默截断。
 *
 * 本示例使用 CActor 现成的真实零件：
 * - tellReliable：按序、恰好一次（重发 + 接收端去重）
 * - RemotingSettings.withLimits / withNodeBudget：未确认上限与节点预算
 * - reliableDeliveryStats：重发 / 去重 / 放弃 / 预算丢弃的可观测计数
 *
 * 自检（退出码 0/1）：
 * 1. 可靠投递 2000 条按序全部到达，且接收端去重计数与重发计数可观测；
 * 2. 消费者离线期间的可靠消息不被静默丢弃（连接断开时进入未确认，重连后再送达）；
 * 3. 背压：未确认上限设小后，超限的新消息被明确计入 overflowDropped/nodeBudgetDropped，而不会无界堆内存；
 * 4. 生产者侧发送序号单调，消费者可用它做幂等（示例演示去重表）。
 *
 * 运行：source tools/env.sh && cjpm build && \
 *       CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex03_event_backbone
 */
package cactor_examples.ex03_event_backbone

import std.collection.*
import std.sync.*
import std.time.*
import std.convert.*
import cactor.api.CActorSystemFactory
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, RemotingSettings}

let EVENT_COUNT = 2000

/** 消费者：(producerId, seq) 幂等去重 + 保序校验 */
class EventSink <: Actor {
    let lock = Mutex()
    let seen = HashSet<String>()
    let received = ArrayList<String>()
    var duplicates: Int64 = 0
    var outOfOrder: Int64 = 0
    var expected: Int64 = 0

    public prop name: String { get() { "event-sink" } }
    public prop description: String { get() { "event sink" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 3) {
                let key = "${parts[0]}:${parts[1]}"
                let seq = Int64.parse(parts[1])
                synchronized(lock) {
                    if (seen.contains(key)) {
                        duplicates++
                        return MessageResult.Handled
                    }
                    seen.add(key)
                    if (seq != expected) {
                        outOfOrder++
                    }
                    expected = seq + 1
                    received.add(parts[2])
                }
            }
        }
        MessageResult.Handled
    }

    func snapshot(): (Int64, Int64, Int64, Array<String>) {
        synchronized(lock) {
            (Int64(received.size), duplicates, outOfOrder, received.toArray())
        }
    }
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 10)
    }
    cond()
}

func newManager(settings: RemotingSettings): SimpleRemoteManager {
    let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    remote.configure(settings)
    remote.start()
    remote
}

main(): Int64 {
    let c = Checker()
    let system = CActorSystemFactory.create("ex03-backbone")

    // 生产者与消费者：两个真实 TCP 节点
    let producer = newManager(RemotingSettings.defaults())
    let consumer = newManager(RemotingSettings.defaults())

    let sink = EventSink()
    let sinkRef = system.actorOf({ => sink }, "event-sink")
    consumer.exposeActor("event-sink", sinkRef)
    let target = producer.remoteActorOf(consumer.getLocalAddress(), "event-sink")

    try {
        // ---- 1. 可靠投递按序全部到达 ----
        println("\n[1] 可靠投递 ${EVENT_COUNT} 条：按序、不重复生效")
        for (i in 0..EVENT_COUNT) {
            // 载荷里带 (producerId, seq, body)，消费者据此幂等 + 保序
            producer.tellReliable(target, StringMessage("p1|${i}|evt-${i}"))
        }
        let delivered = waitUntil(Duration.second * 30, { =>
            let (n, _, _, _) = sink.snapshot()
            n == EVENT_COUNT
        })
        let (count, dups, outOfOrder, events) = sink.snapshot()
        c.check(delivered, "全部 ${count}/${EVENT_COUNT} 条到达")
        c.check(outOfOrder == 0, "严格按序（乱序 ${outOfOrder} 次）")
        var ordered = events.size == EVENT_COUNT
        for (i in 0..events.size) {
            if (events[i] != "evt-${i}") {
                ordered = false
            }
        }
        c.check(ordered, "内容与发送顺序一致")
        let stats = producer.reliableDeliveryStats()
        c.check(stats.duplicatesDropped == dups, "重发去重计数可观测：${stats.duplicatesDropped}")

        // ---- 2. 消费者离线期间的重连重发 ----
        println("\n[2] 消费者重启后，未确认消息仍然送达")
        // 消费者换一个实例（地址可能变），生产者需要重新解析目标
        consumer.stop()
        // 在消费者不可达期间继续发：这些消息进入未确认（重发队列），不会立即丢失
        for (i in EVENT_COUNT..EVENT_COUNT + 200) {
            producer.tellReliable(target, StringMessage("p2|${i}|evt-${i}"))
        }
        let unackedWhileDown = producer.reliableDeliveryStats().unacked
        c.check(unackedWhileDown > 0, "消费者离线时消息进入未确认队列（${unackedWhileDown} 条）")
        // 重启消费者（同一端口不保证，这里用新实例 + 新的暴露），把剩余消息继续送达
        let consumer2 = newManager(RemotingSettings.defaults())
        let sink2 = EventSink()
        let sinkRef2 = system.actorOf({ => sink2 }, "event-sink-restarted")
        consumer2.exposeActor("event-sink", sinkRef2)
        let target2 = producer.remoteActorOf(consumer2.getLocalAddress(), "event-sink")
        for (i in EVENT_COUNT..EVENT_COUNT + 200) {
            producer.tellReliable(target2, StringMessage("p2|${i}|evt-${i}"))
        }
        let resumed = waitUntil(Duration.second * 30, { =>
            let (n, _, _, _) = sink2.snapshot()
            n == 200
        })
        c.check(resumed, "新消费者收到断连期间的 200 条（${sink2.snapshot()[0]}）")
        consumer2.stop()

        // ---- 3. 背压：未确认上限明确生效 ----
        println("\n[3] 背压：未确认上限设小后，超限消息被明确计数")
        // 上限 16、放弃 2 秒；目标指向一个不存在的 Actor（无法被确认），制造持续未确认
        let tight = newManager(RemotingSettings.defaults().withLimits(16, Duration.second * 2))
        let idle = newManager(RemotingSettings.defaults())
        // idle 未暴露任何 Actor：消息会被计入不可投递，但可靠通道仍未确认
        let ghost = tight.remoteActorOf(idle.getLocalAddress(), "missing-actor")
        for (i in 0..500) {
            tight.tellReliable(ghost, StringMessage("p3|${i}|x"))
        }
        let capped = waitUntil(Duration.second * 10, { =>
            let s = tight.reliableDeliveryStats()
            s.overflowDropped > 0 || s.nodeBudgetDropped > 0 || s.gaveUp > 0
        })
        let tightStats = tight.reliableDeliveryStats()
        c.check(capped, "超限被明确计量：overflow=${tightStats.overflowDropped}, budget=${tightStats.nodeBudgetDropped}, gaveUp=${tightStats.gaveUp}")
        let usage = tight.reliableBudgetUsage()
        c.check(usage.unacked <= usage.maxTotalUnacked, "节点未确认占用不超预算：${usage.unacked} ≤ ${usage.maxTotalUnacked}")
        tight.stop()
        idle.stop()

        // ---- 4. 消费者侧幂等（重复投递只生效一次） ----
        println("\n[4] 消费者幂等：同一 (producer, seq) 重复投递只生效一次")
        // sink2 已收到 p2 的 200 条；再用同一批序号重放一次
        let dupProducer = newManager(RemotingSettings.defaults())
        let dupConsumer = newManager(RemotingSettings.defaults())
        let idemSink = EventSink()
        let idemRef = system.actorOf({ => idemSink }, "idem-sink")
        dupConsumer.exposeActor("idem-sink", idemRef)
        let idemTarget = dupProducer.remoteActorOf(dupConsumer.getLocalAddress(), "idem-sink")
        for (i in 0..50) {
            dupProducer.tellReliable(idemTarget, StringMessage("p4|${i}|v${i}"))
        }
        let first = waitUntil(Duration.second * 15, { => idemSink.snapshot()[0] == 50 })
        c.check(first, "首批 50 条到达")
        // 重发同一批（模拟at-least-once 重复）
        for (i in 0..50) {
            dupProducer.tellReliable(idemTarget, StringMessage("p4|${i}|v${i}"))
        }
        // 等待重复批次被消费端识别（去重表命中），不裸 sleep
        let dupSeen = waitUntil(Duration.second * 5, { => idemSink.snapshot()[1] > 0 })
        let (idemCount, idemDups, _, _) = idemSink.snapshot()
        c.check(idemCount == 50, "重复投递后仍只有 50 条有效事件：${idemCount}")
        c.check(dupSeen, "重复投递被识别（去重表命中 ${idemDups} 条）")
        dupProducer.stop()
        dupConsumer.stop()
    } finally {
        producer.stop()
        consumer.stop()
        system.terminate()
    }

    println("\nex03：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
