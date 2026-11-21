/*
 * CActor Ex 47 - Grain 提醒（差距 4.7 剩余部分：对标 Orleans Grain reminder）
 *
 * 真实约束：
 * - 长时间运行的虚拟实体需要"到点被叫醒"（续租、对账、清理），不能靠调用方一直调用；
 * - 提醒必须持久：进程重启后未到期的提醒仍然有效，已确认的提醒不能重复触发；
 * - 业务处理与确认之间崩溃会重发：至少一次，业务按 key 幂等；
 * - 提醒要投递到该标识**当前的激活**（可能在别的节点），而不是固定的旧实例。
 *
 * 组件：
 * - 框架：`Grains.withReminders(store)` + `registerReminder/cancelReminder` + `Grain.onReminder`
 *   （`cluster/src/membership/grains.cj`，本轮新增）
 * - 存储：FileDurableStateStore（提醒状态）
 * - Lou：POST /reminders、POST /reminders/cancel、GET /reminders、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 注册 200ms 提醒 → 到期触发一次，key/payload 正确；
 * 2. 一次性：再等 500ms 不重复触发；
 * 3. 取消未到期提醒 → 不触发；
 * 4. 重开同一存储（新进程）→ 已确认提醒不再投递；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex47_grain_reminder
 *   ./target/release/bin/cactor_examples.ex47_grain_reminder serve 18192
 */
package cactor_examples.ex47_grain_reminder

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.message.{Message, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, Grain, GrainContext, Grains}
import cactor_persistence.{FileDurableStateStore}

let KIND = "session"
let WAIT = Duration.second * 20

/** 提醒触发记录（供 HTTP 观测） */
class ReminderLog {
    private let lock = Mutex()
    private let lines = ArrayList<String>()

    func record(device: String, key: String, payload: String): Unit {
        synchronized(lock) {
            lines.add("${device}|${key}|${payload}")
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(lines.size)
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("[")
            var first = true
            for (line in lines) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${line}\"")
            }
            sb.append("]")
            sb.toString()
        }
    }

    func contains(text: String): Bool {
        synchronized(lock) {
            lines.iterator().any({ l => l.contains(text) })
        }
    }
}

/** 会话 Grain：被提醒叫醒后只记录，业务副作用应在这里按 key 幂等 */
class SessionGrain <: Grain {
    let id: String
    let log: ReminderLog

    init(id: String, log: ReminderLog) {
        this.id = id
        this.log = log
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        StringMessage("ok")
    }

    public func onReminder(key: String, payload: String): Unit {
        log.record(id, key, payload)
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let grains: Grains
    var stopped = false

    init(dir: String, settings: ClusterSettings, log: ReminderLog) {
        this.system = CActorSystemFactory.create("ex47-${MonoTime.now().hashCode()}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.cluster = Cluster(system, remote, [remote.getLocalAddress()], settings)
        let book = log
        this.grains = Grains(cluster, system, remote, KIND, { id => SessionGrain(id, book) },
            Duration.second * 10).withReminders(FileDurableStateStore("${dir}/reminders"),
            pollInterval: Duration.millisecond * 100)
    }

    func register(device: String, key: String, delay: Duration, payload: String): Int64 {
        grains.registerReminder(device, key, delay, payload: payload)
    }

    func cancel(device: String, key: String): Bool {
        grains.cancelReminder(device, key)
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            grains.stop()
            cluster.shutdown()
            remote.stop()
            system.terminate()
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

func buildApp(node: Node, log: ReminderLog, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/reminders", { ctx =>
        let device = ctx.query("device") ?? "d1"
        let key = ctx.query("key") ?? "tick"
        let delay = ctx.queryInt("delayMs") ?? 200
        let payload = ctx.query("payload") ?? ""
        let generation = node.register(device, key, Duration.millisecond * delay, payload)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"device\":\"${device}\",\"key\":\"${key}\",\"generation\":${generation}}")
    })
    app.post("/reminders/cancel", { ctx =>
        let device = ctx.query("device") ?? "d1"
        let key = ctx.query("key") ?? "tick"
        let removed = node.cancel(device, key)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"cancelled\":${removed}}")
    })
    app.get("/reminders", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(log.json())
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"fired\":${log.count()},\"active\":${node.grains.activeIds().size}," +
            "\"owners\":${node.grains.ownerCount()}}")
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
    let dir = "/tmp/cactor-ex47-${MonoTime.now().hashCode()}"
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let log = ReminderLog()
        let node = Node(dir, settings, log)
        let _ = waitUntil(WAIT, { => node.grains.ownerCount() == 1 })
        let app = buildApp(node, log, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex47：端口绑定失败")
            return 1
        }
        println("ex47 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let log = ReminderLog()
    let node = Node(dir, settings, log)
    c.check(waitUntil(WAIT, { => node.grains.ownerCount() == 1 }), "单节点分片视图就绪")

    try {
        println("\n[1] 到期触发一次")
        c.check(node.register("d1", "tick", Duration.millisecond * 200, "p1") > 0, "注册成功")
        c.check(waitUntil(WAIT, { => log.count() == 1 }), "提醒触发 1 次")
        c.check(log.contains("d1|tick|p1"), "key/payload 正确：${log.json()}")

        println("\n[2] 一次性提醒不重复")
        sleep(Duration.millisecond * 500)
        c.check(log.count() == 1, "确认后不再触发（仍为 1）")

        println("\n[3] 取消未到期提醒")
        node.register("d1", "cancel-me", Duration.millisecond * 300, "p2")
        c.check(node.cancel("d1", "cancel-me"), "取消成功")
        sleep(Duration.millisecond * 600)
        c.check(log.count() == 1, "取消后没有触发")

        println("\n[4] 重开同一存储")
        node.shutdown()
        let log2 = ReminderLog()
        let revived = Node(dir, settings, log2)
        c.check(waitUntil(WAIT, { => revived.grains.ownerCount() == 1 }), "新节点就绪")
        sleep(Duration.millisecond * 600)
        c.check(log2.count() == 0, "已确认的提醒不再投递")

        println("\n[5] Lou 端点")
        let app = buildApp(revived, log2, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let created = readBody(client.post(
                "http://127.0.0.1:${port}/reminders?device=d2&key=renew&delayMs=200&payload=hello", ""))
            c.check(created.contains("\"generation\":1"), "POST /reminders：${created}")
            c.check(waitUntil(WAIT, { => log2.count() == 1 }), "HTTP 注册的提醒触发")
            let list = readBody(client.get("http://127.0.0.1:${port}/reminders"))
            c.check(list.contains("d2|renew|hello"), "GET /reminders 反映触发记录")
            let _ = readBody(client.post(
                "http://127.0.0.1:${port}/reminders?device=d2&key=later&delayMs=300", ""))
            let cancelled = readBody(client.post(
                "http://127.0.0.1:${port}/reminders/cancel?device=d2&key=later", ""))
            c.check(cancelled.contains("true"), "POST /reminders/cancel 成功")
            sleep(Duration.millisecond * 700)
            c.check(log2.count() == 1, "取消的提醒没有触发")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"fired\":1"), "GET /state 计数一致")
            client.close()
        } finally {
            try {
                app.stop()
            } catch (_: Exception) {}
        }
        revived.shutdown()
    } finally {
        try {
            node.shutdown()
        } catch (_: Exception) {}
    }

    println("\nex47：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
