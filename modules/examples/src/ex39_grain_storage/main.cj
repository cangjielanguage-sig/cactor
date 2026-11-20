/*
 * CActor Ex 39 - Grain 状态存储钩子（差距 4.7：对标 Orleans IGrainStorage）
 *
 * 真实约束：
 * - 虚拟 Actor 的激活是短暂的：空闲停用、节点故障都会销毁实例，状态不能只放在字段里；
 * - 业务不该在每个 Grain 里重复写"激活时读、修改后写"的样板，也不该自己算修订号；
 * - 状态要跨进程存活：进程重启后，同一个 id 的计数必须接着上次继续。
 *
 * 组件：
 * - 框架：`GrainStorage` 接口 + `Grain.encodeState/decodeState`（本次新增），
 *   由 GrainHost 在激活前读回、停用后写出；配置在 `Grains(..., storage: Some(...))`
 * - 存储：FileDurableStateStore（示例内的 FileGrainStorage 适配器负责修订号）
 * - Lou：GET /counters/:id、POST /counters/:id/inc、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 两次 inc 后为 2；
 * 2. 空闲停用后存储里是 "2"，写入由框架完成（业务没有直接碰存储）；
 * 3. 再次 inc 自动重新激活 → 3（框架读回并 decodeState）；
 * 4. 关闭后重开同一目录 → get 仍为 3，inc → 4（跨进程存活）；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex39_grain_storage
 *   ./target/release/bin/cactor_examples.ex39_grain_storage serve 18184
 */
package cactor_examples.ex39_grain_storage

import std.collection.*
import std.convert.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.message.{Message, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, Grain, GrainContext, GrainStorage, Grains}
import cactor_persistence.{DurableStateStore, FileDurableStateStore}

let KIND = "counter"
let IDLE = Duration.millisecond * 400
let ASK = Duration.second * 10
let CLUSTER_WAIT = Duration.second * 25

/** 把 GrainStorage 接到 DurableStateStore；修订号由存储的 revisionOf 决定（删除后不回退） */
class FileGrainStorage <: GrainStorage {
    let store: DurableStateStore

    init(dir: String) {
        this.store = FileDurableStateStore(dir)
    }

    private func key(kind: String, id: String): String {
        "grain-${kind}-${id}"
    }

    public func read(kind: String, id: String): Option<String> {
        match (store.get(key(kind, id))) {
            case Some(r) => Some<String>(r.state)
            case None => None<String>
        }
    }

    public func write(kind: String, id: String, state: String): Unit {
        let k = key(kind, id)
        store.upsert(k, store.revisionOf(k) + 1, state)
    }

    public func remove(kind: String, id: String): Unit {
        let k = key(kind, id)
        let revision = store.revisionOf(k)
        if (revision > 0) {
            store.delete(k, revision)
        }
    }
}

/** 只维护内存字段；encodeState/decodeState 由框架在激活/停用边界调用 */
class CounterGrain <: Grain {
    let node: String
    var count: Int64 = 0

    init(node: String) {
        this.node = node
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        let cmd = (request as StringMessage).getOrThrow().getContent()
        match (cmd) {
            case "inc" =>
                count++
                StringMessage("${count}")
            case "get" => StringMessage("${count}")
            case "where" => StringMessage(node)
            case _ => StringMessage("?")
        }
    }

    public func encodeState(): Option<String> {
        Some<String>("${count}")
    }

    public func decodeState(state: String): Unit {
        count = Int64.parse(state)
    }
}

/** 一个节点 + 一套 Grain（单节点集群足够展示生命周期与存储语义） */
class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let storage: FileGrainStorage
    let grains: Grains
    var stopped = false

    init(dir: String, settings: ClusterSettings) {
        this.system = CActorSystemFactory.create("ex39-${MonoTime.now().hashCode()}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.cluster = Cluster(system, remote, [remote.getLocalAddress()], settings)
        this.storage = FileGrainStorage("${dir}/states")
        let self = cluster.selfAddress
        let st = storage
        this.grains = Grains(cluster, system, remote, KIND, { _ => CounterGrain(self) }, IDLE,
            storage: Some<GrainStorage>(st))
    }

    func call(id: String, command: String): String {
        match (grains.call(id, StringMessage(command), ASK) as StringMessage) {
            case Some(s) => s.getContent()
            case None => "?"
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

func buildApp(node: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/counters/:id", { ctx =>
        let id = ctx.param("id")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"id\":\"${id}\",\"count\":${node.call(id, "get")}}")
    })
    app.post("/counters/:id/inc", { ctx =>
        let id = ctx.param("id")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"id\":\"${id}\",\"count\":${node.call(id, "inc")}}")
    })
    app.get("/stats", { ctx =>
        let stats = node.grains.stats()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"activations\":${stats[0]},\"deactivations\":${stats[1]},\"calls\":${stats[2]}," +
            "\"active\":${node.grains.activeIds().size}}")
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
    let dir = "/tmp/cactor-ex39-${MonoTime.now().hashCode()}"
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let node = Node(dir, settings)
        let _ = waitUntil(CLUSTER_WAIT, { => node.grains.ownerCount() == 1 })
        let app = buildApp(node, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex39：端口绑定失败")
            return 1
        }
        println("ex39 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()

    println("\n[1] 激活并累加")
    let node = Node(dir, settings)
    c.check(waitUntil(CLUSTER_WAIT, { => node.grains.ownerCount() == 1 }), "单节点分片视图就绪")
    c.check(node.call("a", "inc") == "1" && node.call("a", "inc") == "2", "两次 inc → 2")
    c.check(node.call("b", "inc") == "1", "另一个 id 独立计数")

    println("\n[2] 空闲停用后状态已由框架写出")
    c.check(waitUntil(CLUSTER_WAIT, { => node.grains.stats()[1] >= 2 }), "a、b 均已停用")
    c.check(node.grains.activeIds().isEmpty(), "本节点没有激活中的 Grain")
    c.check(node.storage.read(KIND, "a") == Some<String>("2"), "存储里 a 的状态为 2")

    println("\n[3] 再次调用自动恢复")
    c.check(node.call("a", "inc") == "3", "重新激活后从 2 继续 → 3")

    println("\n[4] 关闭后重开同一目录")
    node.shutdown()
    let reopened = Node(dir, settings)
    c.check(waitUntil(CLUSTER_WAIT, { => reopened.grains.ownerCount() == 1 }), "新节点分片视图就绪")
    c.check(reopened.call("a", "get") == "3", "进程重启后仍为 3")
    c.check(reopened.call("a", "inc") == "4", "继续累加 → 4")

    println("\n[5] Lou 端点")
    let app = buildApp(reopened, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let get = readBody(client.get("http://127.0.0.1:${port}/counters/a"))
        c.check(get.contains("\"count\":4"), "GET /counters/a 与存储一致：${get}")
        let inc = readBody(client.post("http://127.0.0.1:${port}/counters/a/inc", ""))
        c.check(inc.contains("\"count\":5"), "POST inc 累加：${inc}")
        let stats = readBody(client.get("http://127.0.0.1:${port}/stats"))
        c.check(stats.contains("\"activations\":") && stats.contains("\"calls\":"), "GET /stats 可用")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        reopened.shutdown()
    }

    println("\nex39：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
