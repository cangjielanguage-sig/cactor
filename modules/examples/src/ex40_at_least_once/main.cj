/*
 * CActor Ex 40 - 至少一次投递 + 幂等扣费（差距 4.13：用户可见的 AtLeastOnce API）
 *
 * 真实约束：
 * - 支付/扣费这类副作用不能因为网络重发而重复生效，但也不能因为丢包而静默丢失；
 * - 发送方重试（同一笔业务的同一消息）必须携带稳定的 deliveryId，接收方据此去重；
 * - 接收端进程重启后仍要认得"这笔记账处理过了"，否则重启窗口内的重发会重复扣费。
 *
 * 组件：
 * - 框架：`AtLeastOnceDelivery`（生成稳定 deliveryId + 经可靠通道发送）、
 *   `DeliveryEnvelope`、`IdempotentReceiver`、`DurableDeliveryIdStore`（本次新增，`cactor_patterns.delivery`）
 * - 存储：FileDurableStateStore（去重表落盘）
 * - Lou：POST /charge?order=&amount=、GET /charges、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 同一信封发两次 → 只扣一次，重复计数 1；
 * 2. 新订单 → 正常扣费；不同 deliveryId 各自生效；
 * 3. 接收端"重启"（新管理器 + 重新打开去重存储）后重发旧信封 → 不重复扣费；
 * 4. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex40_at_least_once
 *   ./target/release/bin/cactor_examples.ex40_at_least_once serve 18185
 */
package cactor_examples.ex40_at_least_once

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_core.message.{Message, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_persistence.{FileDurableStateStore}
import cactor_patterns.delivery.*

let SINK = "payments"
let WAIT = Duration.second * 10

/** 记账本：order → 已扣金额（重复投递不应再次累加） */
class Charges {
    private let lock = Mutex()
    private let amounts = HashMap<String, Int64>()

    func charge(order: String, amount: Int64): Unit {
        synchronized(lock) {
            amounts[order] = (amounts.get(order) ?? 0) + amount
        }
    }

    func get(order: String): Int64 {
        synchronized(lock) {
            amounts.get(order) ?? 0
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            Int64(amounts.size)
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((order, amount) in amounts) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${order}\":${amount}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

/** 一个接收端：管理器 + 去重接收者 + 记账本 */
class Consumer {
    let remote: SimpleRemoteManager
    let receiver: IdempotentReceiver
    let charges: Charges
    var stopped = false

    init(dir: String) {
        this(dir, Charges())
    }

    init(dir: String, charges: Charges) {
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.receiver = IdempotentReceiver(DurableDeliveryIdStore(FileDurableStateStore("${dir}/dedup")))
        this.charges = charges
        let book = charges
        let sink = receiver
        remote.registerHandler(SINK, { m: Message =>
            if (let Some(e) <- (m as DeliveryEnvelope)) {
                if (let Some(text) <- (e.payload as StringMessage)) {
                    // 协议："order|amount"
                    let parts = text.getContent().split("|")
                    if (parts.size == 2) {
                        sink.handle(e, { _ => book.charge(parts[0], Int64.parse(parts[1])) })
                    }
                }
            }
        })
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            remote.stop()
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
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func buildApp(producer: SimpleRemoteManager, delivery: AtLeastOnceDelivery, consumer: Consumer, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/charge", { ctx =>
        let order = ctx.query("order") ?? "o1"
        let amount = ctx.queryInt("amount") ?? 1
        let target = producer.remoteActorOf(consumer.remote.getLocalAddress(), SINK)
        let payload = StringMessage("${order}|${amount}")
        // 客户端可传 deliveryId 表示"同一笔业务的重试"；不传则由服务端生成
        let env = match (ctx.query("deliveryId")) {
            case Some(id) => DeliveryEnvelope(id, payload)
            case None => delivery.envelope(payload)
        }
        // 模拟发送方重试：同一个信封发两次，deliveryId 不变
        delivery.resend(target, env)
        delivery.resend(target, env)
        let _ = waitUntil(WAIT, { => consumer.charges.get(order) == amount })
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"deliveryId\":\"${env.deliveryId}\",\"charged\":${consumer.charges.get(order)}}")
    })
    app.get("/charges", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(consumer.charges.json())
    })
    app.get("/stats", { ctx =>
        let stats = consumer.receiver.stats()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"processed\":${stats[0]},\"duplicates\":${stats[1]},\"orders\":${consumer.charges.size()}}")
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
    let dir = "/tmp/cactor-ex40-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let consumer = Consumer(dir)
        let producer = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        producer.start()
        let delivery = AtLeastOnceDelivery(producer, instanceId: "svc")
        let app = buildApp(producer, delivery, consumer, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex40：端口绑定失败")
            return 1
        }
        println("ex40 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let consumer = Consumer(dir)
    let producer = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    producer.start()
    let delivery = AtLeastOnceDelivery(producer, instanceId: "svc")
    let target = producer.remoteActorOf(consumer.remote.getLocalAddress(), SINK)

    try {
        println("\n[1] 同一信封重发两次")
        let env = delivery.envelope(StringMessage("o1|5"))
        delivery.resend(target, env)
        delivery.resend(target, env)
        c.check(waitUntil(WAIT, { => consumer.receiver.stats() == (1, 1) }), "只处理 1 次、重复丢弃 1 次")
        c.check(consumer.charges.get("o1") == 5, "o1 只扣一次 5")

        println("\n[2] 新订单与不同 deliveryId")
        delivery.resend(target, delivery.envelope(StringMessage("o2|7")))
        c.check(waitUntil(WAIT, { => consumer.charges.get("o2") == 7 }), "o2 扣费 7")
        c.check(consumer.charges.size() == 2, "共两笔订单")

        println("\n[3] 接收端重启后重发旧信封")
        consumer.shutdown()
        let restarted = Consumer(dir, consumer.charges)
        let target2 = producer.remoteActorOf(restarted.remote.getLocalAddress(), SINK)
        delivery.resend(target2, env)
        // 等接收端明确丢弃这次重发，再断言没有重复扣费
        c.check(waitUntil(WAIT, { => restarted.receiver.stats()[1] == 1 }), "去重存储识别出旧 deliveryId")
        c.check(restarted.charges.get("o1") == 5, "o1 没有被重复扣费")
        delivery.resend(target2, delivery.envelope(StringMessage("o3|3")))
        c.check(waitUntil(WAIT, { => restarted.charges.get("o3") == 3 }), "重启后新订单照常处理")

        println("\n[4] Lou 端点")
        let app = buildApp(producer, delivery, restarted, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let resp = readBody(client.post("http://127.0.0.1:${port}/charge?order=o4&amount=11", ""))
            c.check(resp.contains("\"charged\":11"), "POST /charge 幂等扣费：${resp}")
            let charges = readBody(client.get("http://127.0.0.1:${port}/charges"))
            c.check(charges.contains("\"o4\":11"), "GET /charges 反映记账")
            let stats = readBody(client.get("http://127.0.0.1:${port}/stats"))
            c.check(stats.contains("\"duplicates\":2"), "GET /stats 显示重复被丢弃")
            client.close()
        } finally {
            try {
                app.stop()
            } catch (_: Exception) {}
        }
        restarted.shutdown()
    } finally {
        producer.stop()
    }

    println("\nex40：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
