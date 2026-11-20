/*
 * CActor Ex 38 - 事件 schema 演进（差距 4.7：框架级 upcast 链）
 *
 * 真实约束：
 * - 事件日志一旦写出就不能要求业务"停机改历史"：改字段、改单位后，旧事件必须能被新版本读回；
 * - 升级逻辑属于适配器，不该散落在恢复代码里：框架在 fromJournal 之前先跑 upcast 链；
 * - 升级不能改写日志：新旧格式继续共存，老节点（只认旧格式）仍能读自己写的那段。
 *
 * 组件：
 * - 框架：`EventAdapter.upcast(manifest, data)`（本次新增）+ `EventSourcedBehavior` 恢复路径
 * - 存储：FileEventStore（进程内重启复用同一目录）
 * - Lou：GET /balance、POST /deposit、GET /journal（观察新旧 manifest 共存）
 *
 * 自检（退出码 0/1）：
 * 1. 预置 v1（元）、v2（元;补贴）与当前格式三条旧事件 → 恢复后余额按各自语义正确；
 * 2. 用当前格式追加 7 元 → 余额累加；
 * 3. 关闭系统后重开同一目录 → 余额不变（upcast 在冷恢复时同样生效）；
 * 4. 日志里旧的 `Deposit.v1` / `Deposit.v2` 原样保留，没有被改写；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex38_schema_upcast
 *   ./target/release/bin/cactor_examples.ex38_schema_upcast serve 18183
 */
package cactor_examples.ex38_schema_upcast

import std.collection.*
import std.convert.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_persistence.{BasicEvent, FileEventStore}
import cactor_persistence.typed.*

let ASK_TIMEOUT = Duration.second * 10

// ======================= 协议 =======================

enum BalanceCmd {
    | Deposit(Int64, TypedActorRef<Int64>)
    | Get(TypedActorRef<Int64>)
}

enum BalanceEvt {
    | Deposited(Int64)
}

class BalanceState {
    let total: Int64

    init(total: Int64) {
        this.total = total
    }

    func apply(evt: BalanceEvt): BalanceState {
        match (evt) {
            case Deposited(cents) => BalanceState(total + cents)
        }
    }
}

/**
 * 只看当前格式；历史版本由框架 upcast 先升级：
 * - `Deposit.v1`：data 是"元"（整数）→ 乘 100 转分；
 * - `Deposit.v2`：data 是"元;补贴元" → 求和后乘 100；
 * - `Deposit`：data 已经是分。
 * 链式演进：新版本再加一级时，把 v3→当前 的分支接在这里即可。
 */
class BalanceAdapter <: EventAdapter<BalanceEvt> {
    public func manifest(event: BalanceEvt): String {
        "Deposit"
    }

    public func toJournal(event: BalanceEvt): String {
        match (event) {
            case Deposited(cents) => "${cents}"
        }
    }

    public func fromJournal(manifest: String, data: String): Array<BalanceEvt> {
        match (manifest) {
            case "Deposit" => [Deposited(Int64.parse(data))]
            case other => throw IllegalArgumentException("未知事件类型：${other}")
        }
    }

    public func upcast(manifest: String, data: String): (String, String) {
        match (manifest) {
            case "Deposit.v1" => ("Deposit", "${Int64.parse(data) * 100}")
            case "Deposit.v2" =>
                let parts = data.split(";")
                ("Deposit", "${(Int64.parse(parts[0]) + Int64.parse(parts[1])) * 100}")
            case _ => (manifest, data)
        }
    }
}

// ======================= 环境 =======================

class Env {
    let dir: String
    let pid: String
    let store: FileEventStore
    let system: CActorSystem
    let ref: TypedActorRef<BalanceCmd>

    /** seed=true 时预置三条旧格式事件，供 serve 模式直接观察 */
    init(dir: String, seed: Bool) {
        this.dir = dir
        this.pid = PersistenceId.of("Balance", "acct-1").id
        this.store = FileEventStore("${dir}/events")
        if (seed) {
            this.store.append(pid, [BasicEvent("s1", pid, "Deposit.v1", "5"),
                BasicEvent("s2", pid, "Deposit.v2", "2;3"), BasicEvent("s3", pid, "Deposit", "100")], 0)
        }
        this.system = CActorSystemFactory.create("ex38-${MonoTime.now().hashCode()}")
        let journal = store
        let adapter = BalanceAdapter()
        let behavior = EventSourcedBehavior<BalanceCmd, BalanceEvt, BalanceState>(
            PersistenceId.of("Balance", "acct-1"), emptyState: BalanceState(0),
            commandHandler: { s, cmd, effect =>
                match (cmd) {
                    case Deposit(cents, replyTo) => effect.persist(Deposited(cents)).thenReply(replyTo,
                        { s => s.total })
                    case Get(replyTo) => effect.reply(replyTo, s.total)
                }
            }, eventHandler: { s, e => s.apply(e) }, eventAdapter: adapter, journal: journal).behavior()
        this.ref = system.typedActorOf(behavior, "balance")
    }

    func balance(): Int64 {
        TypedAsk.ask<BalanceCmd, Int64>(ref, { r => Get(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    }

    func deposit(cents: Int64): Int64 {
        TypedAsk.ask<BalanceCmd, Int64>(ref, { r => Deposit(cents, r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    }

    func journalJson(): String {
        let sb = StringBuilder()
        sb.append("[")
        var first = true
        for (env in store.replay(pid, 1, Int64.Max)) {
            if (!first) {
                sb.append(",")
            }
            first = false
            let manifest = env.event.getEventType()
            let data = env.event.getEventData()
            sb.append("{\"manifest\":\"${manifest}\",\"data\":\"${data}\"}")
        }
        sb.append("]")
        sb.toString()
    }

    func close(): Unit {
        system.terminate()
        PersistenceIo.awaitIdle(Duration.second * 20)
        store.close()
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

// ======================= Lou =======================

func buildApp(env: Env, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/balance", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"cents\":${env.balance()}}")
    })
    app.post("/deposit", { ctx =>
        let yuan = ctx.queryInt("yuan") ?? 1
        let after = env.deposit(yuan * 100)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"deposited\":${yuan},\"cents\":${after}}")
    })
    app.get("/journal", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.journalJson())
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
    let dir = "/tmp/cactor-ex38-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let env = Env(dir, true)
        let app = buildApp(env, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex38：端口绑定失败")
            return 1
        }
        println("ex38 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        println("  初始余额（v1+v2+当前）：${env.balance()} 分")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()

    println("\n[1] 旧格式混合日志 → 新代码恢复")
    let env = Env(dir, true)
    // v1: 5 元 = 500；v2: 2+3 元 = 500；当前格式：100 分
    c.check(env.balance() == 1100, "恢复余额 1100 分（实际 ${env.balance()}）")

    println("\n[2] 当前格式追加")
    c.check(env.deposit(700) == 1800, "追加 7 元后 1800 分")

    println("\n[3] 关闭后重开同一目录")
    env.close()
    let reopened = Env(dir, false)
    c.check(reopened.balance() == 1800, "冷恢复仍为 1800 分（实际 ${reopened.balance()}）")

    println("\n[4] 旧事件没有被改写")
    let journal = reopened.journalJson()
    c.check(journal.contains("\"manifest\":\"Deposit.v1\"") && journal.contains("\"manifest\":\"Deposit.v2\"") &&
        journal.contains("\"manifest\":\"Deposit\""), "日志中 v1/v2/当前格式共存")

    println("\n[5] Lou 端点")
    let app = buildApp(reopened, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let bal = readBody(client.get("http://127.0.0.1:${port}/balance"))
        c.check(bal.contains("\"cents\":1800"), "GET /balance 与内存一致：${bal}")
        let dep = readBody(client.post("http://127.0.0.1:${port}/deposit?yuan=5", ""))
        c.check(dep.contains("\"cents\":2300"), "POST /deposit 写入当前格式：${dep}")
        let log = readBody(client.get("http://127.0.0.1:${port}/journal"))
        c.check(log.contains("Deposit.v1") && log.contains("\"data\":\"500\""),
            "GET /journal 显示新旧事件共存")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        reopened.close()
    }

    println("\nex38：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
