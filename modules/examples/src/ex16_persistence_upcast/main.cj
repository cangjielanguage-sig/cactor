/*
 * CActor Ex 16 - 事件 schema 演进 / upcast（对标 Akka Persistence schema evolution）
 *
 * 真实约束：
 * - 线上已有 v1 事件（只有金额，没有币种），新代码必须能重放它们；
 * - 新代码写入 v2 事件（金额 + 币种，新增改名事件）；
 * - 同一个日志里 v1 / v2 混存，恢复时按 manifest（事件类型名）升级到最新模型；
 * - 升级逻辑必须可重复（重放同一段事件结果一致），未知版本必须明确失败，不能静默丢事件。
 *
 * 说明：CActor 的 `EventAdapter.fromJournal(manifest, data)` 已经拿到 manifest，
 * 因此 upcast 可以在适配器里按 manifest 分派完成（本示例即是）——
 * 框架没有单独的 `upcast` 链（差距 4.7 的准确表述），但**不需要**它也能做版本迁移。
 *
 * 组件：
 * - FileEventStore：直接写入 v1 事件（模拟旧版本进程）
 * - EventSourcedBehavior + FunctionEventAdapter：v2 适配器按 manifest 升级 v1/v2
 * - Lou：GET /account、POST /deposit
 *
 * 自检（退出码 0/1）：
 * 1. 只含 v1 事件的日志 → v2 代码恢复出正确状态（v1 默认币种 CNY）；
 * 2. 写入 v2 事件后重启 → v1+v2 混存仍能恢复；
 * 3. upcast 可重复：同一日志恢复两次结果一致；
 * 4. 未知 manifest 明确失败（不静默丢弃）；
 * 5. Lou 端点读写一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex16_persistence_upcast
 *   ./target/release/bin/cactor_examples.ex16_persistence_upcast serve 18101
 */
package cactor_examples.ex16_persistence_upcast

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_persistence.{FileEventStore, BasicEvent, EventStore}
import cactor_persistence.typed.*

let ASK = Duration.second * 10
let PID = "Acct|a1"

// ======================= 最新模型（v2） =======================

enum AcctEvt {
    | Opened(String)                 // owner
    | Deposited(Int64, String)       // amount, currency
    | Renamed(String)                // new owner
}

enum AcctCmd {
    | Deposit(Int64, TypedActorRef<String>)
    | Rename(String, TypedActorRef<String>)
    | Query(TypedActorRef<String>)
}

class AcctState {
    let owner: String
    let balance: Int64
    let currency: String
    let version: Int64

    init(owner: String, balance: Int64, currency: String, version: Int64) {
        this.owner = owner
        this.balance = balance
        this.currency = currency
        this.version = version
    }

    static func empty(): AcctState {
        AcctState("", 0, "CNY", 0)
    }

    func apply(e: AcctEvt): AcctState {
        match (e) {
            case Opened(o) => AcctState(o, balance, currency, version + 1)
            case Deposited(amount, cur) => AcctState(owner, balance + amount, cur, version + 1)
            case Renamed(n) => AcctState(n, balance, currency, version + 1)
        }
    }

    func text(): String {
        "${owner}|${balance}|${currency}|${version}"
    }
}

// ======================= v1 → v2 upcast =======================

func acctManifest(e: AcctEvt): String {
    "acct.v2"
}

func acctToJournal(e: AcctEvt): String {
    match (e) {
        case Opened(o) => "open:${o}"
        case Deposited(amount, cur) => "dep:${amount}:${cur}"
        case Renamed(n) => "rename:${n}"
    }
}

/** 关键：v1 事件没有币种字段，升级时补默认值；未知版本明确失败 */
func acctFromJournal(manifest: String, data: String): AcctEvt {
    let parts = data.split(":")
    match (manifest) {
        case "acct.v1" =>
            if (parts.size >= 2 && parts[0] == "open") {
                Opened(parts[1])
            } else if (parts.size >= 2 && parts[0] == "dep") {
                // v1 升级：只有金额，币种默认 CNY
                Deposited(Int64.parse(parts[1]), "CNY")
            } else {
                throw IllegalArgumentException("无法升级的 v1 事件：${data}")
            }
        case "acct.v2" =>
            if (parts.size >= 2 && parts[0] == "open") {
                Opened(parts[1])
            } else if (parts.size >= 3 && parts[0] == "dep") {
                Deposited(Int64.parse(parts[1]), parts[2])
            } else if (parts.size >= 2 && parts[0] == "rename") {
                Renamed(parts[1])
            } else {
                throw IllegalArgumentException("无法解析的 v2 事件：${data}")
            }
        case _ => throw IllegalArgumentException("未知事件版本：${manifest}")
    }
}

func acctAdapter(): EventAdapter<AcctEvt> {
    FunctionEventAdapter<AcctEvt>(manifest: acctManifest, toJournal: acctToJournal, fromJournal: acctFromJournal)
}

func acctBehavior(journal: EventStore): TypedBehavior<AcctCmd> {
    EventSourcedBehavior<AcctCmd, AcctEvt, AcctState>(PersistenceId.ofUniqueId(PID),
        emptyState: AcctState.empty(),
        commandHandler: { state, cmd, effect =>
            match (cmd) {
                case Deposit(amount, reply) =>
                    effect.persist(Deposited(amount, state.currency)).thenReply(reply, { s => "balance=${s.balance}" })
                case Rename(name, reply) =>
                    effect.persist(Renamed(name)).thenReply(reply, { s => "owner=${s.owner}" })
                case Query(reply) => effect.reply(reply, state.text())
            }
        },
        eventHandler: { state, evt => state.apply(evt) },
        eventAdapter: acctAdapter(), journal: journal).behavior()
}

// ======================= 辅助 =======================

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

func query(ref: TypedActorRef<AcctCmd>): String {
    TypedAsk.ask<AcctCmd, String>(ref, { r => AcctCmd.Query(r) }, ASK).get(ASK)
}

func deposit(ref: TypedActorRef<AcctCmd>, amount: Int64): String {
    TypedAsk.ask<AcctCmd, String>(ref, { r => AcctCmd.Deposit(amount, r) }, ASK).get(ASK)
}

func rename(ref: TypedActorRef<AcctCmd>, name: String): String {
    TypedAsk.ask<AcctCmd, String>(ref, { r => AcctCmd.Rename(name, r) }, ASK).get(ASK)
}

func buildApp(ref: TypedActorRef<AcctCmd>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/account", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"account\":\"${query(ref)}\"}")
    })
    app.post("/deposit", { ctx =>
        let amount = ctx.queryInt("amount") ?? 0
        if (amount <= 0) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_amount\"}")
        } else {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"result\":\"${deposit(ref, amount)}\"}")
        }
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

/** 用旧版本格式（v1）直接写日志：open:<owner> / dep:<amount> */
func seedV1(store: EventStore): Unit {
    store.append(PID, [
        BasicEvent("${PID}-1", PID, "acct.v1", "open:alice"),
        BasicEvent("${PID}-2", PID, "acct.v1", "dep:100"),
        BasicEvent("${PID}-3", PID, "acct.v1", "dep:50")
    ], 0)
}

main(args: Array<String>): Int64 {
    let dir = "/tmp/cactor-ex16-${MonoTime.now().hashCode()}"
    let journalPath = "${dir}/journal"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let store = FileEventStore(journalPath)
        let system = CActorSystemFactory.create("ex16-serve")
        let ref = system.typedActorOf<AcctCmd>(acctBehavior(store), "acct")
        let app = buildApp(ref, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex16：端口绑定失败")
            return 1
        }
        println("ex16 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    // ---- 1. 旧版本写入的 v1 日志 ----
    println("\n[1] 只有 v1 事件的日志，用 v2 代码恢复")
    let seedStore = FileEventStore(journalPath)
    seedV1(seedStore)
    seedStore.close()
    let store1 = FileEventStore(journalPath)
    let system1 = CActorSystemFactory.create("ex16-a")
    let acct1 = system1.typedActorOf<AcctCmd>(acctBehavior(store1), "acct")
    let recovered = query(acct1)
    c.check(recovered == "alice|150|CNY|3", "v1 事件升级后状态正确：${recovered}")

    // ---- 2. 写入 v2 事件后重启 ----
    println("\n[2] 写入 v2 事件后重启，v1+v2 混存恢复")
    let dep = deposit(acct1, 25)
    c.check(dep == "balance=175", "v2 存款成功：${dep}")
    let ren = rename(acct1, "alice2")
    c.check(ren == "owner=alice2", "v2 改名成功：${ren}")
    system1.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "关闭前 IO 落盘")
    store1.close()

    let store2 = FileEventStore(journalPath)
    let system2 = CActorSystemFactory.create("ex16-b")
    let acct2 = system2.typedActorOf<AcctCmd>(acctBehavior(store2), "acct")
    let mixed = query(acct2)
    c.check(mixed == "alice2|175|CNY|5", "v1+v2 混存恢复正确：${mixed}")

    // ---- 3. 可重复 ----
    println("\n[3] upcast 可重复")
    system2.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "关闭前 IO 落盘")
    store2.close()
    let store3 = FileEventStore(journalPath)
    let system3 = CActorSystemFactory.create("ex16-c")
    let acct3 = system3.typedActorOf<AcctCmd>(acctBehavior(store3), "acct")
    c.check(query(acct3) == mixed, "再次恢复结果一致（${query(acct3)}）")

    // ---- 4. 未知版本明确失败 ----
    println("\n[4] 未知 manifest 明确失败")
    var rejected = false
    try {
        acctFromJournal("acct.v99", "dep:1:CNY")
    } catch (e: IllegalArgumentException) {
        rejected = e.message.contains("未知事件版本")
    }
    c.check(rejected, "未知版本抛出明确异常")

    // ---- 5. Lou ----
    println("\n[5] Lou 端点")
    let app = buildApp(acct3, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
    let account = client.get("http://127.0.0.1:${port}/account")
    c.check(account.status == 200 && readBody(account).contains("alice2|175|CNY|5"), "GET /account 与恢复状态一致")
    let more = client.post("http://127.0.0.1:${port}/deposit?amount=10", "")
    c.check(more.status == 200 && readBody(more).contains("balance=185"), "POST /deposit 生效")
    let bad = client.post("http://127.0.0.1:${port}/deposit?amount=0", "")
    c.check(bad.status == 400, "非法金额返回 400")
    client.close()
    try {
        app.stop()
    } catch (_: Exception) {}
    system3.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "最终关闭前 IO 落盘")
    store3.close()

    println("\nex16：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
