/*
 * CActor Real 09 - 虚拟 Actor（Grains）购物车（对标 protoactor-go examples/cluster-grain、
 *                     Orleans Virtual Actor）
 *
 * 真实约束：
 * - 按用户 id 定位"虚拟实体"：不需要预先创建，第一次调用时自动激活；
 * - 同一个 id 在集群里只有一个激活（单一激活），从任意节点调用都落到同一份状态；
 * - 空闲一段时间后自动停用，释放内存；再调用时从存储恢复（状态不丢）；
 * - 调用方不关心实体在哪个节点（位置透明）。
 *
 * 组件：
 * - Grains（cluster/src/membership/grains.cj）：单一激活、回合制、空闲停用
 * - FileDurableStateStore：激活时按 id 读、每次修改后按修订号写
 * - Lou：POST /cart/:id/add?sku=&qty=、GET /cart/:id
 *
 * 自检（退出码 0/1）：
 * 1. 三个节点调用同一 id 的 where → 同一个节点；全集群该 id 激活数 == 1；
 * 2. 节点 0 加 2、节点 1 加 3 → get 得 5（跨节点共享同一份状态）；
 * 3. 超过空闲时间后激活数归零；再 get 自动重新激活且状态仍为 5（存储恢复）；
 * 4. Lou 端点读写与存储一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.real09_grains_cart
 *   ./target/release/bin/cactor_examples.real09_grains_cart serve 18095
 */
package cactor_examples.real09_grains_cart

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
import cactor_cluster.membership.{Cluster, ClusterSettings, Grains, Grain, GrainContext}
import cactor_persistence.{DurableStateStore, FileDurableStateStore}

let KIND = "cart"
let IDLE = Duration.millisecond * 800
let ASK = Duration.second * 10
let CLUSTER_WAIT = Duration.second * 25

/** "a=2;b=3" 形式的状态编码（真实系统会用 JSON / protobuf） */
func encode(items: HashMap<String, Int64>): String {
    let parts = ArrayList<String>()
    for ((k, v) in items) {
        parts.add("${k}=${v}")
    }
    String.join(parts.toArray(), delimiter: ";")
}

func decode(text: String): HashMap<String, Int64> {
    let items = HashMap<String, Int64>()
    if (!text.isEmpty()) {
        for (pair in text.split(";")) {
            let kv = pair.split("=")
            if (kv.size == 2) {
                items[kv[0]] = Int64.parse(kv[1])
            }
        }
    }
    items
}

class CartGrain <: Grain {
    let store: DurableStateStore
    let node: String
    var items = HashMap<String, Int64>()
    var revision: Int64 = 0

    init(store: DurableStateStore, node: String) {
        this.store = store
        this.node = node
    }

    public func onActivate(ctx: GrainContext): Unit {
        if (let Some(r) <- store.get("cart-${ctx.id}")) {
            items = decode(r.state)
            revision = r.revision
        }
    }

    public func onDeactivate(ctx: GrainContext): Unit {
        // 每次修改已经落盘；这里无需额外动作（真实系统可在这里释放外部资源）
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        let cmd = (request as StringMessage).getOrThrow().getContent()
        if (cmd.startsWith("add:")) {
            let parts = cmd.split(":")
            if (parts.size == 3) {
                let sku = parts[1]
                let qty = Int64.parse(parts[2])
                items[sku] = (items.get(sku) ?? 0) + qty
                revision++
                store.upsert("cart-${ctx.id}", revision, encode(items))
            }
            StringMessage("${node}|${encode(items)}")
        } else if (cmd == "get") {
            StringMessage("${node}|${encode(items)}")
        } else if (cmd == "where") {
            StringMessage(node)
        } else {
            StringMessage("${node}|?")
        }
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let grains: Grains
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, store: DurableStateStore) {
        this.index = index
        this.system = CActorSystemFactory.create("real09-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let self = cluster.selfAddress
        this.grains = Grains(cluster, system, remote, KIND, { _ => CartGrain(store, self) }, IDLE)
    }

    func call(id: String, command: String): String {
        match (grains.call(id, StringMessage(command), ASK) as StringMessage) {
            case Some(s) => s.getContent()
            case None => "?|?"
        }
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

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func active(nodes: Array<Node>): Int64 {
    var n: Int64 = 0
    for (node in nodes) {
        n += Int64(node.grains.activeIds().size)
    }
    n
}

func nodeOf(reply: String): String {
    let parts = reply.split("|")
    if (parts.size >= 1) {
        parts[0]
    } else {
        "?"
    }
}

func bodyOf(reply: String): String {
    let parts = reply.split("|")
    if (parts.size >= 2) {
        parts[1]
    } else {
        "?"
    }
}

func buildApp(node: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/cart/:id", { ctx =>
        let reply = node.call(ctx.param("id"), "get")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"node\":\"${nodeOf(reply)}\",\"items\":\"${bodyOf(reply)}\"}")
    })
    app.post("/cart/:id/add", { ctx =>
        let sku = ctx.query("sku") ?? ""
        let qty = ctx.queryInt("qty") ?? 0
        if (sku.isEmpty() || qty <= 0) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_sku_or_qty\"}")
        } else {
            let reply = node.call(ctx.param("id"), "add:${sku}:${qty}")
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"node\":\"${nodeOf(reply)}\",\"items\":\"${bodyOf(reply)}\"}")
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

func startCluster(settings: ClusterSettings, store: DurableStateStore): Array<Node> {
    let first = Node(0, None<NetworkAddress>, settings, store)
    let seed = first.remote.getLocalAddress()
    [first, Node(1, Some(seed), settings, store), Node(2, Some(seed), settings, store)]
}

main(args: Array<String>): Int64 {
    let dir = "/tmp/cactor-real09-${MonoTime.now().hashCode()}"
    let store = FileDurableStateStore("${dir}/grains")
    let settings = ClusterSettings(Duration.millisecond * 80, Duration.millisecond * 80, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodes = startCluster(settings, store)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodes[0], port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("real09：端口绑定失败")
            return 1
        }
        println("real09 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(nodes[0], 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        let formed = waitUntil(CLUSTER_WAIT, { =>
            var ok = true
            for (n in nodes) {
                ok = ok && n.cluster.state().upCount() == 3 && n.cluster.state().converged
            }
            ok
        })
        c.check(formed, "三节点集群形成并收敛")
        let viewsReady = waitUntil(CLUSTER_WAIT, { =>
            var ok = true
            for (n in nodes) {
                ok = ok && n.grains.ownerCount() == 3
            }
            ok
        })
        c.check(viewsReady, "三个节点都看到 3 个托管者")

        // ---- 1. 单一激活与位置 ----
        println("\n[1] 同一 id 只有一个激活，三个节点看到同一位置")
        let where0 = nodes[0].call("user-42", "where")
        let where1 = nodes[1].call("user-42", "where")
        let where2 = nodes[2].call("user-42", "where")
        c.check(where0 == where1 && where1 == where2, "三个节点都路由到 ${where0}")
        c.check(active(nodes) == 1, "全集群该 id 激活数 == 1（实际 ${active(nodes)}）")

        // ---- 2. 跨节点共享状态 ----
        println("\n[2] 从不同节点累加")
        nodes[0].call("user-42", "add:apple:2")
        let after = nodes[1].call("user-42", "add:apple:3")
        c.check(bodyOf(after) == "apple=5", "apple 累加为 5（${bodyOf(after)}）")
        let readBack = nodes[2].call("user-42", "get")
        c.check(bodyOf(readBack) == "apple=5", "从第三个节点读回 5")

        // ---- 3. 空闲停用与存储恢复 ----
        println("\n[3] 空闲停用后重新激活，状态仍在")
        let deactivated = waitUntil(Duration.second * 10, { => active(nodes) == 0 })
        c.check(deactivated, "空闲超过 ${IDLE.toMilliseconds()} ms 后激活数归零")
        let revived = nodes[0].call("user-42", "get")
        c.check(bodyOf(revived) == "apple=5", "重新激活后从存储恢复 apple=5")
        c.check(active(nodes) == 1, "重新激活后激活数回到 1")

        // ---- 4. Lou 端点 ----
        println("\n[4] Lou 端点读写购物车")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let addResp = client.post("http://127.0.0.1:${port}/cart/user-99/add?sku=pear&qty=4", "")
        c.check(addResp.status == 200 && readBody(addResp).contains("pear=4"), "POST add 返回 pear=4")
        let getResp = client.get("http://127.0.0.1:${port}/cart/user-99")
        c.check(getResp.status == 200 && readBody(getResp).contains("pear=4"), "GET cart 读回 pear=4")
        let badResp = client.post("http://127.0.0.1:${port}/cart/user-99/add?sku=&qty=0", "")
        c.check(badResp.status == 400, "非法参数返回 400")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nreal09：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
