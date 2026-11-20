/*
 * CActor - 差距 4.13：至少一次投递 + 幂等消费（用户可见 API）
 *
 * 覆盖：
 *   - 同一个 DeliveryEnvelope 重发两次 → 业务副作用只执行一次，重复计数 1；
 *   - 两个不同 deliveryId → 都执行；
 *   - 去重表落 DurableStateStore：新的接收者（模拟重启）仍识别已处理过的 deliveryId；
 *   - 处理抛异常时不标记，重发会再次执行（至少一次，不静默丢）。
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{Message, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_persistence.{MemoryDurableStateStore}
import cactor_patterns.delivery.*
import cactor_testkit.*

let DELIVERY_WAIT: Duration = Duration.second * 10

@Test
public class AtLeastOnceTest {
    @TestCase
    public func duplicateDeliveryIsProcessedOnce(): Unit {
        let consumer = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let producer = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        consumer.start()
        producer.start()
        try {
            let store = MemoryDeliveryIdStore()
            let receiver = IdempotentReceiver(store)
            let effects = AtomicInt64(0)
            consumer.registerHandler("delivery-sink", { m: Message =>
                if (let Some(e) <- (m as DeliveryEnvelope)) {
                    receiver.handle(e, { _ => effects.fetchAdd(1) })
                }
            })
            let delivery = AtLeastOnceDelivery(producer, instanceId: "t1")
            let target = producer.remoteActorOf(consumer.getLocalAddress(), "delivery-sink")

            // 同一个信封重发两次：必须只生效一次
            let env = delivery.envelope(StringMessage("charge:42"))
            delivery.resend(target, env)
            delivery.resend(target, env)
            @Expect(awaitCondition(DELIVERY_WAIT, { => receiver.stats() == (1, 1) }), true)
            @Expect(effects.load(), 1)

            // 不同 deliveryId 各自生效
            let second = delivery.envelope(StringMessage("charge:43"))
            delivery.resend(target, second)
            @Expect(awaitCondition(DELIVERY_WAIT, { => receiver.stats()[0] == 2 }), true)
            @Expect(env.deliveryId != second.deliveryId, true)

            // 去重表持久化：新接收者（模拟重启）看不到重复执行
            let durable = MemoryDurableStateStore()
            let before = IdempotentReceiver(DurableDeliveryIdStore(durable))
            @Expect(before.handle(env, { _ => effects.fetchAdd(1) }), true)
            let afterRestart = IdempotentReceiver(DurableDeliveryIdStore(durable))
            @Expect(afterRestart.handle(env, { _ => effects.fetchAdd(1) }), false)
            @Expect(effects.load(), 3)
        } finally {
            producer.stop()
            consumer.stop()
        }
    }

    @TestCase
    public func handlerFailureDoesNotMarkDelivery(): Unit {
        let store = MemoryDeliveryIdStore()
        let receiver = IdempotentReceiver(store)
        let env = DeliveryEnvelope("d-fail-1", StringMessage("retry-me"))
        let attempts = AtomicInt64(0)
        try {
            receiver.handle(env, { _ =>
                attempts.fetchAdd(1)
                throw IllegalStateException("暂时失败")
            })
        } catch (_: IllegalStateException) {}
        // 没有标记：重发仍会执行
        @Expect(receiver.handle(env, { _ => attempts.fetchAdd(1) }), true)
        @Expect(attempts.load(), 2)
        @Expect(receiver.stats()[0], 1)
        @Expect(receiver.stats()[1], 0)
    }
}
