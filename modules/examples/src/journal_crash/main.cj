/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 持久化日志 kill -9 验证与基准（E8′，配合 tools/journal_crash_demo.sh 使用）
 *
 *   cactor_examples.journal_crash writer  <目录> <标识数>          不停追加，每次 append 返回（已 fsync）后在 stderr 打印 "ACK 标识 序号"
 *   cactor_examples.journal_crash corrupt <目录> zero|torn         给每个日志尾部追加断电后的零填充 / 写到一半的记录
 *   cactor_examples.journal_crash verify  <目录> <ACK 文件> <标识数> 校验：已确认的事件一条不少、序号从 1 连续、内容与序号对应
 *   cactor_examples.journal_crash bench   <目录>                    逐条 fsync、组提交、批量写入、不 fsync 四种方式的吞吐与 fsync 次数
 *
 * 写入方用 append(期望序号)：重启后从 highestSequenceNr 继续，不会与上一轮写入的事件交错。
 * 退出码：0 通过，1 失败。
 */
package cactor_examples.journal_crash

import std.fs.*
import std.time.*
import std.random.*
import std.collection.*
import std.convert.*
import cactor_persistence.*

func idsOf(k: Int64): Array<String> {
    Array<String>(k, { i => "acct-${i}" })
}

func writer(dir: String, k: Int64): Int64 {
    let store = FileEventStore(dir)
    let ids = idsOf(k)
    let seqs = Array<Int64>(k, { i => store.highestSequenceNr(ids[i]) })
    var n: Int64 = 0
    while (true) {
        let i = n % k
        let id = ids[i]
        let base = seqs[i]
        // 每 5 次用一批 3 条（persistAll 的写法）：一批要么全部写入，要么一条也不确认
        let size = if (n % 5 == 4) { 3 } else { 1 }
        let events = Array<Event>(size, { b => BasicEvent("${id}-${base + 1 + b}", id, "Deposited", "${id}#${base + 1 + b}") })
        seqs[i] = store.append(id, events, base)
        eprintln("ACK ${id} ${seqs[i]}")
        n++
    }
    0
}

func corrupt(dir: String, mode: String): Int64 {
    // 写入进程可能在创建日志目录之前就被 kill -9（负载高时启动慢）：没有日志可损坏，不算失败
    if (!exists(dir)) {
        println("日志目录还不存在，没有可损坏的日志")
        return 0
    }
    let rnd = Random()
    var n = 0
    for (info in Directory.readFrom(dir)) {
        if (!info.name.endsWith(".journal")) {
            continue
        }
        let garbage = if (mode == "zero") {
            Array<UInt8>(4096, repeat: 0)
        } else {
            // 长度声明 100，实际只写了 4 字节 CRC 与 20 字节负载
            let g = Array<UInt8>(28, repeat: 0)
            g[3] = 100
            for (i in 4..28) {
                g[i] = UInt8(rnd.nextUInt64() % 256)
            }
            g
        }
        let f = File("${dir}/${info.name}", OpenMode.Append)
        f.write(garbage)
        f.close()
        n++
    }
    println("给 ${n} 个日志追加了${if (mode == "zero") { "零填充" } else { "写到一半的记录" }}")
    0
}

func verify(dir: String, ackFile: String, k: Int64): Int64 {
    let acked = HashMap<String, Int64>()
    if (exists(ackFile)) {
        for (line in String.fromUtf8(File.readFrom(ackFile)).split("\n")) {
            let parts = line.trimAscii().split(" ", removeEmpty: true)
            // 被 kill 时可能留下半行：不是 "ACK 标识 序号" 的行跳过（只会少算已确认的条数，校验更宽松而不是更严）
            if (parts.size != 3 || parts[0] != "ACK") {
                continue
            }
            if (let Some(seq) <- Int64.tryParse(parts[2])) {
                if (seq > (acked.get(parts[1]) ?? 0)) {
                    acked[parts[1]] = seq
                }
            }
        }
    }
    let store = FileEventStore(dir)
    var ok = true
    var totalAcked: Int64 = 0
    var totalRecovered: Int64 = 0
    for (id in idsOf(k)) {
        let envs = store.replay(id, 1, Int64.Max)
        var contiguous = true
        for (j in 0..envs.size) {
            let seq = j + 1
            if (envs[j].sequenceNr != seq || envs[j].event.getEventData() != "${id}#${seq}") {
                contiguous = false
            }
        }
        let a = acked.get(id) ?? 0
        let good = contiguous && envs.size >= a && store.highestSequenceNr(id) == envs.size
        if (!good) {
            println("  ${id}：已确认 ${a}，恢复 ${envs.size}，序号连续 ${contiguous} —— 失败")
        }
        ok = ok && good
        totalAcked += a
        totalRecovered += envs.size
    }
    println("已确认 ${totalAcked} 条，恢复 ${totalRecovered} 条（未确认的尾部写入可能多出几条）：${if (ok) { "无丢失" } else { "有丢失或损坏" }}")
    if (ok) { 0 } else { 1 }
}

func rate(n: Int64, d: Duration): Int64 {
    let us = max(1, d.toMicroseconds())
    n * 1_000_000 / us
}

func bench(dir: String): Int64 {
    let n = 1000
    let store = FileEventStore("${dir}/fsync")
    store.append("w", [BasicEvent("w-0", "w", "T", "x")], 0)

    var f0 = persistenceFsyncCount()
    var t0 = MonoTime.now()
    var seq: Int64 = 1
    for (i in 0..n) {
        seq = store.append("w", [BasicEvent("w-${i}", "w", "T", "x")], seq)
    }
    let single = MonoTime.now() - t0
    println("逐条 append（每次 fsync）：${n} 条 ${single.toMilliseconds()} ms，${rate(n, single)} 条/秒，fsync ${persistenceFsyncCount() - f0} 次")

    let threads = 8
    store.append("g", [BasicEvent("g-0", "g", "T", "x")], 0)
    f0 = persistenceFsyncCount()
    t0 = MonoTime.now()
    let futures = Array(threads, { t =>
        spawn {
            for (i in 0..(n / threads)) {
                store.saveEvent(BasicEvent("g-${t}-${i}", "g", "T", "x"))
            }
        }
    })
    for (f in futures) {
        f.get()
    }
    let group = MonoTime.now() - t0
    println("${threads} 个线程并发写同一日志（组提交）：${n} 条 ${group.toMilliseconds()} ms，${rate(n, group)} 条/秒，fsync ${persistenceFsyncCount() - f0} 次")

    store.append("b", [BasicEvent("b-0", "b", "T", "x")], 0)
    f0 = persistenceFsyncCount()
    t0 = MonoTime.now()
    seq = 1
    for (i in 0..(n / 10)) {
        seq = store.append("b", Array<Event>(10, { j => BasicEvent("b-${i}-${j}", "b", "T", "x") }), seq)
    }
    let batched = MonoTime.now() - t0
    println("批量 append（每批 10 条，一次 fsync）：${n} 条 ${batched.toMilliseconds()} ms，${rate(n, batched)} 条/秒，fsync ${persistenceFsyncCount() - f0} 次")

    let nosync = FileEventStore("${dir}/nosync", fsync: false)
    t0 = MonoTime.now()
    seq = 0
    for (i in 0..(n * 10)) {
        seq = nosync.append("w", [BasicEvent("w-${i}", "w", "T", "x")], seq)
    }
    let fast = MonoTime.now() - t0
    println("逐条 append（fsync = false，只防进程崩溃）：${n * 10} 条 ${fast.toMilliseconds()} ms，${rate(n * 10, fast)} 条/秒")
    0
}

main(args: Array<String>): Int64 {
    if (args.size < 2) {
        eprintln("用法: journal_crash writer|corrupt|verify|bench <目录> ...")
        return 2
    }
    match (args[0]) {
        case "writer" where args.size >= 3 => writer(args[1], Int64.parse(args[2]))
        case "corrupt" where args.size >= 3 => corrupt(args[1], args[2])
        case "verify" where args.size >= 4 => verify(args[1], args[2], Int64.parse(args[3]))
        case "bench" => bench(args[1])
        case _ =>
            eprintln("参数不对：${String.join(args, delimiter: " ")}")
            2
    }
}
