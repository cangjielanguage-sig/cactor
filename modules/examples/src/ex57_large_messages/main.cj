/*
 * CActor Ex 57 - 大消息压缩与分片（差距 4.9：B3 压缩 / F5 单帧上限与分片）
 *
 * 真实约束：
 * - 跨机房带宽有限：可压缩的大 payload 必须在线上变小，而不是原样传输；
 * - 对端协商的单帧上限可能很小（例如 32 KiB）：超过的大消息必须分片并在接收端无损重组，
 *   不能因为"逻辑消息大于单帧"就被拒；
 * - 无论压缩还是分片，业务拿到的内容必须逐字节一致。
 *
 * 组件：`SimpleRemoteManager.configureWire(WireProtocolSettings.withMaxFrameBytes(...))`、
 *       接收端计数 `getCompressedFrameCount / getReassembledFrameCount / getOversizeFrameCount`
 *
 * 自检（退出码 0/1）：
 * 1. 1 MiB 可压缩文本：接收端发生过解压，内容长度与首尾字符正确；
 * 2. 100 KiB 高熵文本（32 KiB 上限）：接收端发生过重组，内容长度与首尾字符正确；
 * 3. 全程没有超限帧、没有发 GoAway。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex57_large_messages
 */
package cactor_examples.ex57_large_messages

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, WireProtocolSettings}

let WAIT = Duration.second * 25

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
        sleep(Duration.millisecond * 20)
    }
    cond()
}

/** 重复字符：可被压缩 */
func repeated(ch: UInt8, times: Int64): String {
    let out = Array<UInt8>(times, repeat: ch)
    String.fromUtf8(out)
}

/** 高熵 ASCII：避免被压缩到阈值以下，真正走到分片路径 */
@OverflowWrapping
func incompressible(bytes: Int64): String {
    let out = ArrayList<UInt8>()
    var x: UInt64 = 0x9E3779B97F4A7C15
    for (_ in 0..bytes) {
        x = x * 6364136223846793005 + 1442695040888963407
        out.add(UInt8(33 + Int64((x >> 33) % 90)))
    }
    String.fromUtf8(out.toArray())
}

class Sink {
    let lock = Mutex()
    let lengths = ArrayList<Int64>()
    let heads = ArrayList<UInt8>()
    let tails = ArrayList<UInt8>()

    func record(size: Int64, head: UInt8, tail: UInt8): Unit {
        synchronized(lock) {
            lengths.add(size)
            heads.add(head)
            tails.add(tail)
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(lengths.size)
        }
    }

    func lengthAt(i: Int64): Int64 {
        synchronized(lock) { lengths[i] }
    }

    func headAt(i: Int64): UInt8 {
        synchronized(lock) { heads[i] }
    }

    func tailAt(i: Int64): UInt8 {
        synchronized(lock) { tails[i] }
    }
}

main(): Int64 {
    let settings = WireProtocolSettings.defaults().withMaxFrameBytes(32 * 1024)
    let sender = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    let receiver = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    sender.configureWire(settings)
    receiver.configureWire(settings)
    sender.start()
    receiver.start()

    let sink = Sink()
    receiver.registerHandler("sink", { m =>
        match (m as StringMessage) {
            case Some(s) =>
                let content = s.getContent()
                let bytes = content.toArray()
                let head = if (bytes.isEmpty()) { UInt8(0) } else { bytes[0] }
                let tail = if (bytes.isEmpty()) { UInt8(0) } else { bytes[bytes.size - 1] }
                sink.record(Int64(bytes.size), head, tail)
            case None => ()
        }
    })
    let ref = sender.remoteActorOf(receiver.getLocalAddress(), "sink")

    let c = Checker()
    try {
        // 先发小消息完成握手，后续编码使用协商出的能力与上限
        ref.tell(StringMessage("warmup"))
        c.check(waitUntil(WAIT, { => sink.count() == 1 }), "握手完成")
        c.check(sender.peerMaxFrameBytes(receiver.getLocalAddress()) == 32 * 1024, "协商单帧上限 32 KiB")

        println("\n[1] 1 MiB 可压缩文本")
        let big = repeated(UInt8(97), 1024 * 1024)
        ref.tell(StringMessage(big))
        c.check(waitUntil(WAIT, { => sink.count() == 2 }), "接收端收到大文本")
        c.check(sink.lengthAt(1) == Int64(big.toArray().size), "长度一致（${sink.lengthAt(1)}）")
        c.check(sink.headAt(1) == UInt8(97) && sink.tailAt(1) == UInt8(97), "首尾字节一致")
        c.check(waitUntil(WAIT, { => receiver.compressedFrameCount() >= 1 }), "线上发生过压缩")

        println("\n[2] 100 KiB 高熵文本（32 KiB 上限）")
        let noisy = incompressible(100 * 1024)
        ref.tell(StringMessage(noisy))
        c.check(waitUntil(WAIT, { => sink.count() == 3 }), "接收端收到高熵文本")
        c.check(sink.lengthAt(2) == Int64(noisy.toArray().size), "长度一致（${sink.lengthAt(2)}）")
        let nb = noisy.toArray()
        c.check(sink.headAt(2) == nb[0] && sink.tailAt(2) == nb[nb.size - 1], "首尾字节一致")
        c.check(waitUntil(WAIT, { => receiver.reassembledFrameCount() >= 1 }), "线上发生过重组")

        println("\n[3] 没有超限与 GoAway")
        c.check(receiver.oversizeFrameCount() == 0, "接收端超限帧 0")
        c.check(sender.goAwaySentCount() == 0, "发送端未发 GoAway")
        c.check(receiver.wireIncompatibleReceived() == 0, "线格式不兼容 0")
    } finally {
        sender.stop()
        receiver.stop()
    }

    println("\nex57：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
