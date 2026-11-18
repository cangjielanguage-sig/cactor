/*
 * CActor Ex 24 - 至少一次投递 + 消费端幂等（差距 4.13 的应用层补法）
 *
 * 真实约束：
 * - 生产方在"结果未知"（超时/断连）时会重发同一条业务消息，所以消费方可能收到重复；
 * - "至少一次"保证不丢，但不保证不重：副作用必须按 deliveryId 幂等；
 * - 去重表必须持久化：消费方重启后，旧 deliveryId 再次到达仍要被识别；
 * - 重复与首次投递必须可观测（processed / duplicates 计数）。
 *
 * 说明：CActor 的可靠通道（`tellReliable`）解决的是"链路上不丢/不重"，但业务层的
 * "重试同一笔业务"需要用户可见的 deliveryId 与持久去重表——这是差距 4.13 的应用层补法。
 *
 * 组件：
 * - 两个真实 TCP 节点（生产 / 消费）
 * - 消费端：`FileDurableStateStore` 记录 `seen|<producer>|<deliveryId>`，重复即跳过
 * - Lou：POST /send?n=&repeat=、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 100 条消息各发 2 次 → 副作用 100 次、重复识别 100 次；
 * 2. 消费方重启后重发旧 deliveryId → 仍识别为重复，副作用不增加；
 * 3. 重启后新消息正常处理；
 * 4. Lou 端点与内存计数一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex24_at_least_once
 *   ./target/release/bin/cactor_examples.ex24_at_least_once serve 18109
 */
package cactor_examples.ex24_at_least_once

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_persistence.{DurableStateStore, FileDurableStateStore}

let SINK = "ex24-sink"

class Stats {
    let lock = Mutex()
    var processed: Int64 = 0
    var duplicates: Int64 = 0
    var effects: Int64 = 0
    let effectsLog = ArrayList<String>()

    func onFirst(id: String): Unit {
        synchronized(lock) {
            processed++
            effects++
            effectsLog.add(id)
        }
    }

    func onDuplicate(): Unit {
        synchronized(lock) {
            duplicates++
        }
    }

    func snapshot(): (Int64, Int64, Int64) {
        synchronized(lock) {
            (processed, duplicates, effects)
        }
    }
}

/** 消费端：按 (producer, deliveryId) 幂等；去重表在 DurableStateStore 里 */
class IdempotentSink <: Actor {
    let store: DurableStateStore
    let stats: Stats

    init(store: DurableStateStore, stats: Stats) {
        this.store = store
        this.stats = stats
    }

    public prop name: String { get() { "idempotent-sink" } }
    public prop description: String { get() { "delivery sink" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 4 && parts[0] == "dlv") {
                let producer = parts[1]
                let deliveryId = parts[2]
                let key = "seen|${producer}|${deliveryId}"
                if (store.get(key).isSome()) {
                    stats.onDuplicate()
                } else {
                    store.upsert(key, store.revisionOf(key) + 1, "1")
                    stats.onFirst("${producer}:${deliveryId}:${parts[3]}")
                }
            }
        }
        MessageResult.Handled
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
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func newManager(): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.start()
    m
}

/** 业务重试：同一条消息发 repeat 次（模拟结果未知时的重发） */
func sendBatch(producer: String, target: ActorRef, from: Int64, n: Int64, repeat: Int64): Unit {
    for (i in from..from + n) {
        for (_ in 0..repeat) {
            target.tell(StringMessage("dlv|${producer}|d-${i}|payload-${i}"))
        }
    }
}

func statsJson(stats: Stats, producer: String): String {
    let (p, d, e) = stats.snapshot()
    "{\"processed\":${p},\"duplicates\":${d},\"effects\":${e},\"producer\":\"${producer}\"}"
}

func buildApp(producerId: String, target: ActorRef, stats: Stats, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/send", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        let repeat = ctx.queryInt("repeat") ?? 1
        let from = ctx.queryInt("from") ?? 0
        sendBatch(producerId, target, from, n, repeat)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"sent\":${n},\"repeat\":${repeat}}")
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(statsJson(stats, producerId))
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func startServer(app: Lou): UInt16 {
    spawn { app.listen() }
    for (_ in 0..500) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                return s.port
            }
        }
        sleep(Duration.millisecond * 20)
    }
    0u16
}

func readBody(response: HttpResponse): String {
    let out = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    var n = response.body.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = response.body.read(buf)
    }
    String.fromUtf8(out.toArray())
}

main(args: Array<String>): Int64 {
    let dir = "/tmp/cactor-ex24-${MonoTime.now().hashCode()}"
    let store = FileDurableStateStore("${dir}/seen")
    let producerId = "p1"
    let stats = Stats()
    let producer = newManager()
    let consumer = newManager()
    let consumerSystem = CActorSystemFactory.create("ex24-consumer")
    let sink = consumerSystem.actorOf({ => IdempotentSink(store, stats) }, SINK)
    consumer.exposeActor(SINK, sink)
    let target = producer.remoteActorOf(consumer.getLocalAddress(), SINK)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(producerId, target, stats, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex24：端口绑定失败")
            return 1
        }
        println("ex24 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        // ---- 1. 重复投递只生效一次 ----
        println("\n[1] 100 条消息各发 2 次")
        sendBatch(producerId, target, 0, 100, 2)
        let done1 = waitUntil(Duration.second * 20, { =>
            let (p, d, _) = stats.snapshot()
            p == 100 && d == 100
        })
        let (p1, d1, e1) = stats.snapshot()
        c.check(done1, "processed=${p1}、duplicates=${d1}")
        c.check(e1 == 100, "副作用恰好 100 次（${e1}）")

        // ---- 2. 消费方重启后仍识别旧消息 ----
        println("\n[2] 消费方重启：旧 deliveryId 仍被识别为重复")
        consumer.stop()
        consumerSystem.terminate()
        let consumer2 = newManager()
        let system2 = CActorSystemFactory.create("ex24-consumer-2")
        let sink2 = system2.actorOf({ => IdempotentSink(store, stats) }, SINK)
        consumer2.exposeActor(SINK, sink2)
        let target2 = producer.remoteActorOf(consumer2.getLocalAddress(), SINK)
        sendBatch(producerId, target2, 0, 10, 2)   // 旧 id 重发 20 次
        let done2 = waitUntil(Duration.second * 15, { =>
            let (_, d, _) = stats.snapshot()
            d >= 120
        })
        let (p2, d2, e2) = stats.snapshot()
        c.check(done2 && p2 == 100, "重启后旧消息全部判重（processed=${p2}, duplicates=${d2}）")
        c.check(e2 == 100, "副作用没有增加（${e2}）")

        // ---- 3. 重启后新消息正常处理 ----
        println("\n[3] 重启后新消息正常处理")
        sendBatch(producerId, target2, 100, 10, 2)
        let done3 = waitUntil(Duration.second * 15, { =>
            let (p, d, _) = stats.snapshot()
            p == 110 && d >= 130
        })
        let (p3, d3, e3) = stats.snapshot()
        c.check(done3, "新消息 processed=${p3}、duplicates=${d3}")
        c.check(e3 == 110, "副作用总计 110 次（${e3}）")

        // ---- 4. Lou（消费方重启后，HTTP 必须指向新实例）----
        println("\n[4] Lou 端点")
        let app = buildApp(producerId, target2, stats, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let send = client.post("http://127.0.0.1:${port}/send?n=5&repeat=2&from=200", "")
        c.check(send.status == 200 && readBody(send).contains("\"sent\":5"), "POST /send 发送 5 条 x2")
        c.check(waitUntil(Duration.second * 15, { => stats.snapshot()[0] == 115 }), "HTTP 触发的 5 条生效")
        let st = readBody(client.get("http://127.0.0.1:${port}/stats"))
        c.check(st.contains("\"processed\":115") && st.contains("\"effects\":115"), "GET /stats 一致：${st}")
        client.close()
        app.stop()

        consumer2.stop()
        system2.terminate()
    } finally {
        try {
            producer.stop()
        } catch (_: Exception) {}
        try {
            consumer.stop()
        } catch (_: Exception) {}
    }

    println("\nex24：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
