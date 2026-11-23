/*
 * CActor Ex 67 - 睡眠会话（平台 lifecycle + Lou，对标 F1/P2：空闲 Actor 不占资源、唤醒恢复状态）
 *
 * 真实约束：
 * - 空闲会话不应常驻一个 Actor；睡下时状态落盘、存活 Actor 数下降；
 * - 下一条消息到达时自动唤醒并按存储里的状态继续，不丢消息、不从零开始；
 * - 多个会话彼此隔离，显式 sleep 与 Idle 自动 sleep 都要能工作。
 *
 * 组件：
 * - `SleepRegistry`（Explicit / Idle 策略）、`SleepableActor`（state/restore/onMessage）
 * - `FileDurableStateStore`（状态落盘）
 * - Lou：POST /add、POST /sleep、GET /state、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 加购 2 件：累计 20、存活 Actor 比基线多 1；
 * 2. 再 3 件：累计 50、件数 2；
 * 3. 显式 sleep：存活数回到基线；再 1 件后累计 60、件数 3（状态已恢复）；
 * 4. 另一个 key 独立（累计 10）；
 * 5. Idle(200ms) 注册表：空闲后自动睡，之后仍能唤醒继续；
 * 6. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex67_sleep_sessions
 *   ./target/release/bin/cactor_examples.ex67_sleep_sessions serve 18211
 */
package cactor_examples.ex67_sleep_sessions

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_persistence.{FileDurableStateStore}
import cactor_platform.lifecycle.*

let WAIT = Duration.second * 25
let PRICE: Int64 = 10
let MONO_EPOCH = MonoTime.now()

/** 会话外部观测：key -> (次数, 金额)，由会话 Actor 每次处理消息时写入（Actor 内串行，读方加锁） */
class Stats {
    private let lock = Mutex()
    private let records = HashMap<String, (Int64, Int64)>()

    func record(key: String, count: Int64, total: Int64): Unit {
        synchronized(lock) {
            records[key] = (count, total)
        }
    }

    func of(key: String): (Int64, Int64) {
        synchronized(lock) {
            records.get(key) ?? (0, 0)
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in records) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                let (c, t) = v
                sb.append("\"${k}\":{\"count\":${c},\"total\":${t}}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

/** 购物车会话：状态编码为 "件数|金额" */
class CartSession <: SleepableActor {
    let key: String
    let stats: Stats
    var count: Int64 = 0
    var total: Int64 = 0

    init(key: String, stats: Stats, restored: Option<String>) {
        super(restored)
        this.key = key
        this.stats = stats
    }

    public prop name: String { get() { "cart-session" } }
    public prop description: String { get() { "sleepable cart" } }

    public func state(): String {
        "${count}|${total}"
    }

    public func restore(state: String): Unit {
        let parts = state.split("|")
        if (parts.size == 2) {
            count = match (Int64.tryParse(parts[0])) {
                case Some(v) => v
                case None => 0
            }
            total = match (Int64.tryParse(parts[1])) {
                case Some(v) => v
                case None => 0
            }
        }
    }

    public func onMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                let text = s.getContent()
                if (text.startsWith("add:")) {
                    let qty = match (Int64.tryParse(text[4..])) {
                        case Some(v) => v
                        case None => 0
                    }
                    if (qty > 0) {
                        count++
                        total += qty * PRICE
                        stats.record(key, count, total)
                    }
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
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

func live(system: CActorSystem): Int64 {
    system.actorBudgetUsage().liveActors
}

func addItem(registry: SleepRegistry, stats: Stats, key: String, qty: Int64): (Int64, Int64) {
    let before = stats.of(key)
    let handle = registry.handle(key, { restored => CartSession(key, stats, restored) })
    handle.tell(StringMessage("add:${qty}"))
    let _ = waitUntil(WAIT, { =>
        let now = stats.of(key)
        now[0] > before[0]
    })
    stats.of(key)
}

func sleepSession(registry: SleepRegistry, stats: Stats, key: String): Unit {
    registry.handle(key, { restored => CartSession(key, stats, restored) }).sleep()
}

func buildApp(registry: SleepRegistry, stats: Stats, baseline: Int64, system: CActorSystem, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/add", { ctx =>
        let key = ctx.query("key") ?? "cart-1"
        let qty = ctx.queryInt("qty") ?? 1
        let (count, total) = addItem(registry, stats, key, qty)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"key\":\"${key}\",\"count\":${count},\"total\":${total},\"live\":${live(system)}}")
    })
    app.post("/sleep", { ctx =>
        let key = ctx.query("key") ?? "cart-1"
        let before = live(system)
        sleepSession(registry, stats, key)
        // 可能有其它会话仍醒着：这里只断言"这个会话让存活数下降了"
        let slept = waitUntil(WAIT, { => live(system) < before })
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"slept\":${slept},\"before\":${before},\"live\":${live(system)}}")
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"live\":${live(system)},\"baseline\":${baseline},\"sessions\":${stats.json()}}")
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

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex67-sleep")
    let dir = "/tmp/cactor-ex67-${(MonoTime.now() - MONO_EPOCH).toNanoseconds()}"
    let registry = SleepRegistry(system, "carts",
        store: FileDurableStateStore("${dir}/carts"), policy: SleepPolicy.Explicit)
    let baseline = live(system)
    let stats = Stats()
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(registry, stats, baseline, system, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex67：端口绑定失败")
                return 1
            }
            println("ex67 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 第一次加购：会话存活")
        let (c1, t1) = addItem(registry, stats, "cart-1", 2)
        c.check(c1 == 1 && t1 == 20, "件数 1、金额 20（实际 ${c1}/${t1}）")
        c.check(live(system) == baseline + 1, "存活 Actor 比基线多 1")

        println("\n[2] 继续加购：同一会话累加")
        let (c2, t2) = addItem(registry, stats, "cart-1", 3)
        c.check(c2 == 2 && t2 == 50, "件数 2、金额 50（实际 ${c2}/${t2}）")

        println("\n[3] 显式 sleep 后唤醒：状态恢复")
        sleepSession(registry, stats, "cart-1")
        c.check(waitUntil(WAIT, { => live(system) <= baseline }), "睡眠后存活数回到基线")
        let (c3, t3) = addItem(registry, stats, "cart-1", 1)
        c.check(c3 == 3 && t3 == 60, "唤醒后件数 3、金额 60（实际 ${c3}/${t3}）")

        println("\n[4] 会话隔离")
        let (c4, t4) = addItem(registry, stats, "cart-2", 1)
        c.check(c4 == 1 && t4 == 10, "另一个 key 独立从 0 开始（实际 ${c4}/${t4}）")

        println("\n[5] Idle 策略自动睡")
        let idleRegistry = SleepRegistry(system, "idle-carts",
            store: FileDurableStateStore("${dir}/idle"), policy: SleepPolicy.Idle(Duration.millisecond * 200))
        let idleBaseline = live(system)
        let idleStats = Stats()
        let (ci, ti) = addItem(idleRegistry, idleStats, "i1", 2)
        c.check(ci == 1 && ti == 20, "空闲会话先处理一次")
        c.check(waitUntil(WAIT, { => live(system) <= idleBaseline }), "空闲 200ms 后自动睡")
        let (ci2, ti2) = addItem(idleRegistry, idleStats, "i1", 1)
        c.check(ci2 == 2 && ti2 == 30, "自动睡后仍能唤醒继续（实际 ${ci2}/${ti2}）")
        idleRegistry.stop()

        println("\n[6] Lou 端点")
        let server = buildApp(registry, stats, baseline, system, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 30).build()
            let add = readBody(client.post("http://127.0.0.1:${port}/add?key=web&qty=4", ""))
            c.check(add.contains("\"total\":40"), "POST /add：${add}")
            let slept = readBody(client.post("http://127.0.0.1:${port}/sleep?key=web", ""))
            c.check(slept.contains("\"slept\":true"), "POST /sleep：${slept}")
            let again = readBody(client.post("http://127.0.0.1:${port}/add?key=web&qty=4", ""))
            c.check(again.contains("\"count\":2") && again.contains("\"total\":80"), "唤醒后累计：${again}")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"web\":{\"count\":2,\"total\":80}"), "GET /state：${state}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex67：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        registry.stop()
        system.terminate()
    }
}
