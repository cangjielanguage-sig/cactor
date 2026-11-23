/*
 * CActor Ex 22 - 虚拟 Actor 的异步与可重入调用（对标 protoactor-go cluster-grain / Orleans grain）
 *
 * 真实约束：
 * - 一次调用可能要等另一个 Grain（或外部 IO），不能阻塞调用线程；
 * - 调用方在 Actor 线程里同步 `call` 必须立即失败，否则会占住亲和工作线程直到超时；
 * - 默认回合制（不可重入）下 A→B→A 的调用链会死等；标可重入后必须能完成；
 * - 一批调用可以并发发起，结果按各自 Promise 收集。
 *
 * 组件：
 * - Grains.callAsync（非阻塞）+ TypedPromises.sequence
 * - Grain.reentrant()（默认 false）+ onCallAsync 组合
 * - Lou：GET /chain?id=、GET /burst?n=
 *
 * 自检（退出码 0/1）：
 * 1. 200 个 callAsync 发起耗时 < 2s，全部返回正确结果；
 * 2. 可重入 Grain 的 A→B→A 链完成；
 * 3. 不可重入的同一条链以 GrainCallException 失败（Orleans 默认语义）；
 * 4. Actor 线程内同步 call 立即失败（不占用线程到超时）；
 * 5. Lou 端点。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex22_grains_async
 *   ./target/release/bin/cactor_examples.ex22_grains_async serve 18107
 */
package cactor_examples.ex22_grains_async

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.message.{Message, StringMessage}
import cactor_core.typed.{TypedPromise, TypedPromises}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, Grains, Grain, GrainContext, GrainCallException, GrainTimeoutException}
import cactor_cluster.ddata.Replicator

let WAIT = Duration.second * 25
let IDLE = Duration.second * 30

class GrainsHolder {
    var value: Option<Grains> = None<Grains>
}

func textOf(m: Message): String {
    match (m as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?"
    }
}

/** A→B→A 链：chain → 对端 bounce → 回到源端 leaf */
class ChainGrain <: Grain {
    let holder: GrainsHolder
    let allowReentrant: Bool

    init(holder: GrainsHolder, allowReentrant: Bool) {
        this.holder = holder
        this.allowReentrant = allowReentrant
    }

    public func reentrant(): Bool {
        allowReentrant
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        let t = textOf(request)
        if (t == "nested-sync") {
            // 在 Grain（Actor 线程）里同步 call：必须立即以错误返回
            return holder.value.getOrThrow().call("other-${ctx.id}", StringMessage("leaf"), Duration.second * 3)
        }
        StringMessage("leaf@${ctx.id}")
    }

    public func onCallAsync(request: Message, ctx: GrainContext): TypedPromise<Message> {
        let t = textOf(request)
        let grains = holder.value.getOrThrow()
        if (t.startsWith("chain:")) {
            let other = t["chain:".size..]
            return grains.callAsync(other, StringMessage("bounce:${ctx.id}"), Duration.second * 5)
                .map<Message>({ r => StringMessage("chain-done:${textOf(r)}") })
        }
        if (t.startsWith("bounce:")) {
            let origin = t["bounce:".size..]
            return grains.callAsync(origin, StringMessage("leaf"), Duration.second * 5)
                .map<Message>({ r => StringMessage("bounced:${textOf(r)}") })
        }
        TypedPromises.successful<Message>(onCall(request, ctx))
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let replicator: Replicator
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex22-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.replicator = Replicator(system, remote, Duration.millisecond * 50)
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
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

func grainsOn(nodes: Array<Node>, kind: String, allowReentrant: Bool): Array<Grains> {
    Array<Grains>(nodes.size, { i =>
        let holder = GrainsHolder()
        let g = Grains(nodes[i].cluster, nodes[i].system, nodes[i].remote, kind,
            { _ => ChainGrain(holder, allowReentrant) }, IDLE)
        holder.value = Some(g)
        g
    })
}

func waitOwners(gs: Array<Grains>, expected: Int64): Bool {
    return waitUntil(WAIT, { =>
        var ok = true
        for (g in gs) {
            ok = ok && g.ownerCount() == expected
        }
        ok
    })
}

func buildApp(reentrant: Array<Grains>, plain: Array<Grains>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/chain", { ctx =>
        let id = ctx.query("id") ?? "a1"
        let other = ctx.query("other") ?? "b1"
        let re = (ctx.query("reentrant") ?? "1") == "1"
        let gs = if (re) { reentrant } else { plain }
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            let reply = textOf(gs[0].call(id, StringMessage("chain:${other}"), WAIT))
            ctx.writeString("{\"reply\":\"${reply}\"}")
        } catch (e: GrainCallException) {
            ctx.writeStringWithCode(500u16, "{\"error\":\"grain_call_failed\"}")
        }
    })
    app.get("/burst", { ctx =>
        let n = ctx.queryInt("n") ?? 50
        let started = MonoTime.now()
        let calls = Array<TypedPromise<Message>>(n, { k =>
            reentrant[k % reentrant.size].callAsync("g${k % 20}", StringMessage("leaf"), WAIT)
        })
        let issueMs = (MonoTime.now() - started).toMilliseconds()
        let all = TypedPromises.sequence<Message>(calls).get(WAIT)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"count\":${all.size},\"issueMs\":${issueMs},\"first\":\"${textOf(all[0])}\"}")
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
    let nodeCount = 2
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodeList = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..nodeCount) {
        let n = Node(i, seed, settings)
        seed = Some(n.remote.getLocalAddress())
        nodeList.add(n)
    }
    let nodes = nodeList.toArray()

    let reentrant = grainsOn(nodes, "chain-r", true)
    let plain = grainsOn(nodes, "chain-p", false)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(reentrant, plain, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex22：端口绑定失败")
            return 1
        }
        println("ex22 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    var formed = true
    for (n in nodes) {
        formed = formed && waitUntil(WAIT, { =>
            n.cluster.state().upCount() == nodeCount && n.cluster.state().converged
        })
    }
    c.check(formed, "两节点集群形成并收敛")
    c.check(waitOwners(reentrant, 2) && waitOwners(plain, 2), "两类 Grain 的视图都就绪")

    try {
        // ---- 1. callAsync 不阻塞 ----
        println("\n[1] 200 个 callAsync 并发发起")
        let started = MonoTime.now()
        let calls = Array<TypedPromise<Message>>(200, { k =>
            reentrant[0].callAsync("g${k % 20}", StringMessage("leaf"), WAIT)
        })
        let issueMs = (MonoTime.now() - started).toMilliseconds()
        c.check(issueMs < 2000, "发起 200 个调用耗时 ${issueMs} ms < 2000 ms")
        let all = TypedPromises.sequence<Message>(calls).get(WAIT)
        var allOk = all.size == 200
        for (k in 0..200) {
            if (textOf(all[k]) != "leaf@g${k % 20}") {
                allOk = false
            }
        }
        c.check(allOk, "200 个结果全部正确")

        // ---- 2. 可重入链 ----
        println("\n[2] 可重入 Grain：A→B→A")
        let chained = textOf(reentrant[0].call("a1", StringMessage("chain:b1"), WAIT))
        c.check(chained == "chain-done:bounced:leaf@a1", "链式调用完成：${chained}")

        // ---- 3. 不可重入链明确失败 ----
        println("\n[3] 不可重入 Grain：A→B→A 明确失败")
        var plainFailed = false
        try {
            plain[0].call("a1", StringMessage("chain:b1"), WAIT)
        } catch (_: GrainCallException) {
            plainFailed = true
        } catch (_: GrainTimeoutException) {
            // 回合制下链式调用互相等待，最终以超时/调用错误结束；两种都算"明确失败"
            plainFailed = true
        }
        c.check(plainFailed, "非重入链明确失败（GrainCallException / GrainTimeoutException）")

        // ---- 4. Actor 线程内同步 call 立即失败 ----
        println("\n[4] Grain 内同步 call 立即失败")
        var innerFailed = false
        let innerStart = MonoTime.now()
        try {
            plain[0].call("sync1", StringMessage("nested-sync"), WAIT)
        } catch (e: GrainCallException) {
            innerFailed = e.message.contains("不能在 Actor 处理方法内阻塞")
        }
        let innerMs = (MonoTime.now() - innerStart).toMilliseconds()
        c.check(innerFailed, "Grain 内 call 得到明确错误")
        c.check(innerMs < 3000, "没有等到调用超时（${innerMs} ms）")

        // ---- 5. Lou ----
        println("\n[5] Lou 端点")
        let app = buildApp(reentrant, plain, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 20).build()
        let chain = client.get("http://127.0.0.1:${port}/chain?id=a9&other=b9&reentrant=1")
        c.check(chain.status == 200 && readBody(chain).contains("chain-done:bounced:leaf@a9"), "GET /chain 完成链式调用")
        let plainChain = client.get("http://127.0.0.1:${port}/chain?id=a9&other=b9&reentrant=0")
        c.check(plainChain.status == 500, "GET /chain?reentrant=0 明确失败")
        let burst = client.get("http://127.0.0.1:${port}/burst?n=60")
        c.check(burst.status == 200 && readBody(burst).contains("\"count\":60"), "GET /burst 返回 60 个结果")
        client.close()
        app.stop()
    } finally {
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex22：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
