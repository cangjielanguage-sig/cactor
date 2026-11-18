/*
 * CActor Ex 21 - 读一致性 minCap（`docs/plan/20261117.md` 差距 4.6 的框架修复）
 *
 * 真实约束：
 * - 写方与读方的成员视图可能不一致（写方 3 节点、读方 5 节点），过半读写可能不相交；
 * - 读方有时只想"至少问到 n 个副本"就返回（更低延迟），而不是必须等过半；
 * - 副本数不足时必须明确失败并报告 replied/needed/total，不能返回不可信值；
 * - 参数非法（n <= 0）必须立即拒绝。
 *
 * 本示例验证新增的 `ReadConsistency.ReadMinCap(n, timeout)`：
 * 4 个副本、2 个不可达时，ReadMajority（需要 3）失败，而 ReadMinCap(2) 成功。
 *
 * 组件：
 * - Replicator / ORSet / ReadConsistency（新增 ReadMinCap）
 * - Lou：POST /write、GET /read?min=2、GET /read/majority、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 全连通：WriteMajority 写入，ReadMinCap(3) 读到值；
 * 2. 停掉 2 个副本：ReadMajority 明确失败（needed=3、replied=2、total=4）；
 * 3. 同样视图下 ReadMinCap(2) 成功；ReadMinCap(1) 本地即可返回；
 * 4. 写侧对称：WriteMajority 失败、WriteMinCap(2) 成功、WriteMinCap(0) 立即拒绝；
 * 5. 非法读参数 ReadMinCap(0) 立即抛 IllegalArgumentException；
 * 6. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex21_read_min_cap
 *   ./target/release/bin/cactor_examples.ex21_read_min_cap serve 18106
 */
package cactor_examples.ex21_read_min_cap

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.ddata.*

let CONVERGE = Duration.second * 15
let SHORT = Duration.millisecond * 800

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let rep: Replicator
    var stopped = false

    init(i: Int64) {
        this.index = i
        this.system = CActorSystemFactory.create("ex21-node-${i}-${MonoTime.now().hashCode()}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.rep = Replicator(system, remote, Duration.millisecond * 30)
    }

    func address(): NetworkAddress {
        remote.getLocalAddress()
    }

    func stopTransport(): Unit {
        remote.stop()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            rep.stop()
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

func setOf(v: Option<ReplicatedValue>): Array<String> {
    match (v) {
        case Some(ORSetValue(s)) => s.elements()
        case _ => Array<String>()
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func readWith(node: Node, min: Int64): (Bool, String) {
    try {
        let v = node.rep.getWith("cap", ReadConsistency.ReadMinCap(min, SHORT)).get(CONVERGE)
        (true, setOf(v).toString())
    } catch (e: ReplicatorConsistencyException) {
        (false, "replied=${e.replied},needed=${e.needed},total=${e.total}")
    } catch (e: Exception) {
        (false, e.message)
    }
}

func buildApp(nodes: Array<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/write", { ctx =>
        let value = ctx.query("value") ?? "v1"
        nodes[0].rep.addToSetWith("cap", value, WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"written\":\"${value}\"}")
    })
    app.get("/read", { ctx =>
        let min = ctx.queryInt("min") ?? 2
        let (ok, detail) = readWith(nodes[0], min)
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"value\":\"${detail}\"}")
        } else {
            ctx.writeStringWithCode(503u16, "{\"error\":\"insufficient_replicas\",\"detail\":\"${detail}\"}")
        }
    })
    app.get("/read/majority", { ctx =>
        var ok = false
        var detail = ""
        try {
            detail = setOf(nodes[0].rep.getWith("cap", ReadConsistency.ReadMajority(SHORT)).get(CONVERGE)).toString()
            ok = true
        } catch (e: ReplicatorConsistencyException) {
            detail = "replied=${e.replied},needed=${e.needed},total=${e.total}"
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"value\":\"${detail}\"}")
        } else {
            ctx.writeStringWithCode(503u16, "{\"error\":\"insufficient_replicas\",\"detail\":\"${detail}\"}")
        }
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"elements\":\"${setOf(nodes[0].rep.get("cap")).toString()}\"}")
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
    let nodeCount = 4
    let nodes = Array<Node>(nodeCount, { i => Node(i) })
    for (a in nodes) {
        for (b in nodes) {
            if (!refEq(a, b)) {
                a.rep.join(b.address())
            }
        }
    }

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodes, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex21：端口绑定失败")
            return 1
        }
        println("ex21 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(nodes, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 全连通写入/读取 ----
        println("\n[1] 全连通：WriteMajority 写入，ReadMinCap(3) 读到")
        nodes[0].rep.addToSetWith("cap", "v1", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        let (ok1, detail1) = readWith(nodes[0], 3)
        c.check(ok1 && detail1.contains("v1"), "ReadMinCap(3) 返回 v1（${detail1}）")

        // ---- 2. 半数不可达：majority 明确失败 ----
        println("\n[2] 停掉 2 个副本：ReadMajority 明确失败")
        nodes[2].stopTransport()
        nodes[3].stopTransport()
        var majorityFailed = false
        var majorityDetail = ""
        try {
            nodes[0].rep.getWith("cap", ReadConsistency.ReadMajority(SHORT)).get(CONVERGE)
        } catch (e: ReplicatorConsistencyException) {
            majorityFailed = e.total == 4 && e.needed == 3 && e.replied == 2
            majorityDetail = "replied=${e.replied},needed=${e.needed},total=${e.total}"
        }
        c.check(majorityFailed, "ReadMajority 失败且报告 2/3/4（${majorityDetail}）")

        // ---- 3. minCap(2) 成功、minCap(1) 本地成功 ----
        println("\n[3] 同样视图下 ReadMinCap(2) / (1) 成功")
        let (ok2, detail2) = readWith(nodes[0], 2)
        c.check(ok2 && detail2.contains("v1"), "ReadMinCap(2) 返回 v1（${detail2}）")
        let (ok3, detail3) = readWith(nodes[0], 1)
        c.check(ok3 && detail3.contains("v1"), "ReadMinCap(1) 返回 v1（${detail3}）")

        // ---- 4. 写侧 minCap ----
        println("\n[4] 写侧对称：WriteMinCap(2) 成功、WriteMajority 失败")
        var writeMajorityFailed = false
        try {
            nodes[0].rep.addToSetWith("cap", "w-majority", WriteConsistency.WriteMajority(SHORT)).get(CONVERGE)
        } catch (_: ReplicatorConsistencyException) {
            writeMajorityFailed = true
        }
        c.check(writeMajorityFailed, "WriteMajority 在 2/4 可达时失败")
        var writeMinOk = true
        try {
            nodes[0].rep.addToSetWith("cap", "w-mincap", WriteConsistency.WriteMinCap(2, SHORT)).get(CONVERGE)
        } catch (_: Exception) {
            writeMinOk = false
        }
        c.check(writeMinOk, "WriteMinCap(2) 成功")
        var writeBadRejected = false
        try {
            nodes[0].rep.addToSetWith("cap", "bad", WriteConsistency.WriteMinCap(0, SHORT)).get(CONVERGE)
        } catch (_: IllegalArgumentException) {
            writeBadRejected = true
        }
        c.check(writeBadRejected, "WriteMinCap(0) 明确拒绝")

        // ---- 5. 参数校验 ----
        println("\n[5] 非法读参数明确拒绝")
        let (ok4, detail4) = readWith(nodes[0], 0)
        c.check(!ok4 && detail4.contains("必须为正"), "ReadMinCap(0) 抛 IllegalArgumentException（${detail4}）")

        // ---- 6. Lou ----
        println("\n[6] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let read = client.get("http://127.0.0.1:${port}/read?min=2")
        c.check(read.status == 200 && readBody(read).contains("v1"), "GET /read?min=2 成功")
        let majority = client.get("http://127.0.0.1:${port}/read/majority")
        c.check(majority.status == 503 && readBody(majority).contains("needed=3"),
            "GET /read/majority 返回 503 且带 needed=3")
        let zero = client.get("http://127.0.0.1:${port}/read?min=0")
        c.check(zero.status == 503 && readBody(zero).contains("必须为正"), "GET /read?min=0 明确拒绝")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex21：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
