/*
 * 出站关联（E40，N1）：send 只入队、永不阻塞；连接与重连在关联的写线程里；优先通道；空闲关联退出
 *
 *   - 黑洞地址（SYN 被丢，connect 挂到超时）不拖累发送方：每次 send 的 P99 < 50 ms，
 *     同时发往健康节点的帧全部送达（此前发送方线程同步 connect，每次卡 3 s、失败再试一次）
 *   - 优先帧插队：普通帧积压几十 MB 时，后提交的控制帧先于大部分积压到达
 *   - 没有连接又没有新数据的关联会退出，不为失效地址堆积写线程
 */
package cactor_foundation.network

import std.sync.*
import std.time.*
import std.collection.*
import std.sort.*
import std.unittest.*
import std.unittest.testmacro.*

// 不可路由地址：本机实测 connect 挂满超时（SYN 无应答）；没有默认路由的机器上会立即失败，同样不应阻塞发送方
let BLACK_HOLE: NetworkAddress = NetworkAddress("10.255.255.1", 1)

@Test
public class AssociationTest {
    @TestCase
    func sendToBlackHoleNeverBlocksTheCaller(): Unit {
        let healthy = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let got = AtomicInt64(0)
        healthy.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        healthy.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.second * 3)
        client.start()
        let n = 200
        let latencies = ArrayList<Int64>()
        for (i in 0..n) {
            let t0 = MonoTime.now()
            client.send(BLACK_HOLE, bytesOf("hb-${i}"))
            client.send(healthy.getLocalAddress(), bytesOf("ok-${i}"))
            latencies.add((MonoTime.now() - t0).toMicroseconds())
        }
        let sorted = latencies.toArray()
        sort(sorted)
        let p99 = sorted[sorted.size * 99 / 100]
        // 两次 send 合计的 P99 < 50 ms（此前首次就要卡满 connect 超时）
        @Expect(p99 < 50_000, true)
        @Expect(waitUntil(Duration.second * 5, { => got.load() == n }), true)
        client.stop()
        healthy.stop()
    }

    @TestCase
    func priorityFramesOvertakeNormalBacklog(): Unit {
        let n = 100_000
        let gate = AtomicBool(false)
        let seen = AtomicInt64(0)
        let priorityAt = AtomicInt64(-1)
        let receiver = TcpTransport(NetworkAddress("127.0.0.1", 0))
        receiver.setDataHandler({ data =>
            // 先卡住接收端，让普通帧积压在发送端
            while (!gate.load()) {
                sleep(Duration.millisecond)
            }
            let idx = seen.fetchAdd(1)
            if (data.size == 1) {
                priorityAt.store(idx)
            }
        })
        receiver.start()
        let sender = TcpTransport(NetworkAddress("127.0.0.1", 0))
        sender.start()
        let payload = Array<UInt8>(400, repeat: 7)
        for (_ in 0..n) {
            sender.send(receiver.getLocalAddress(), payload)
        }
        // 约 40 MB 普通帧之后提交一个控制帧（1 字节，便于识别）
        sender.sendPriority(receiver.getLocalAddress(), Array<UInt8>(1, repeat: 1))
        gate.store(true)
        @Expect(waitUntil(Duration.second * 30, { => seen.load() == n + 1 }), true)
        // 控制帧只需等内核缓冲与当前一段写完，远早于积压的末尾
        @Expect(priorityAt.load() >= 0 && priorityAt.load() < n * 3 / 4, true)
        @Expect(sender.getDroppedFrames(), 0)
        sender.stop()
        receiver.stop()
    }

    @TestCase
    func idleUnconnectedAssociationRetires(): Unit {
        let probe = TcpTransport(NetworkAddress("127.0.0.1", 0))
        probe.start()
        let deadPort = probe.getLocalAddress().port
        probe.stop()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.millisecond * 500)
        client.idleRetireAfter = Duration.millisecond * 200
        client.start()
        client.send(NetworkAddress("127.0.0.1", deadPort), bytesOf("x"))
        @Expect(client.getAssociationCount(), 1)
        // 连接失败、之后没有新数据：关联退出
        @Expect(waitUntil(Duration.second * 5, { => client.getAssociationCount() == 0 }), true)
        @Expect(client.getDroppedFrames(), 1)
        // 再发送会重新建立关联
        let alive = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let got = AtomicInt64(0)
        alive.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        alive.start()
        client.send(alive.getLocalAddress(), bytesOf("y"))
        @Expect(waitUntil(Duration.second * 5, { => got.load() == 1 }), true)
        // 已连接的空闲关联也会回收，下一次发送重新连接。
        @Expect(waitUntil(Duration.second * 5, { => client.getAssociationCount() == 0 }), true)
        client.send(alive.getLocalAddress(), bytesOf("z"))
        @Expect(waitUntil(Duration.second * 5, { => got.load() == 2 }), true)
        client.stop()
        alive.stop()
    }
}

func ignoreInt(_: Int64): Unit {}
