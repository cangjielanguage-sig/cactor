/*
 * CActor Ex 46 - CDC 到实时数仓（docs/plan/2026117-lz.md §31 第 5 项）
 *
 * 真实约束：
 * - 上游 CDC 事件持续写入，下游读模型有快有慢；慢消费者必须"有界 + 明确丢弃"，
 *   不能反过来把上游拖死，也不能无限堆积内存；
 * - schema 会演进：v1/v2/v3 混合日志，消费侧必须先 upcast 再计算；
 * - 下游可能离线：重连要按游标补齐，不能漏也不能重复；
 * - 口径变更后要能全量回填（重建），且回填结果与在线消费一致；
 * - 消费位点/落后量要可观测。
 *
 * 组件：
 * - `DurableTopic`（ex41）：CDC 变更流（序号、游标、retention）
 * - `EventAdapter.upcast`（ex38）：CdcV1/CdcV2 → 当前 Cdc
 * - 两个 sink：fast（同步应用）与 slow（容量 5 的有界队列，满则明确丢弃）
 * - Lou：POST /events、GET /models/fast|slow、GET /queue、POST /slow/drain、
 *        GET /lag、POST /rebuild、GET /raw、GET /stream（SSE）
 *
 * 自检（退出码 0/1）：
 * 1. 3 种 schema 混合发布，fast 聚合正确（upcast 生效）；
 * 2. 连发 20 条：slow 队列只保留 5 条、丢弃 15，发布不被阻塞；
 * 3. fast 离线 10 条后重连补齐；
 * 4. rebuild 后 fast 读模型与在线一致；
 * 5. 位点/落后量可观测，SSE 与 Lou 端点一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex46_live_lakehouse
 *   ./target/release/bin/cactor_examples.ex46_live_lakehouse serve 18191
 */
package cactor_examples.ex46_live_lakehouse

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_persistence.{DurableStateStore, DurableSubscription, DurableTopic, FileDurableStateStore}
import cactor_persistence.typed.{EventAdapter}

let TOPIC = "cdc"
let WAIT = Duration.second * 15
let SLOW_CAPACITY: Int64 = 5

/** 当前事件：kind + value + weight */
class CdcEvent {
    let kind: String
    let value: Int64
    let weight: Int64

    init(kind: String, value: Int64, weight: Int64) {
        this.kind = kind
        this.value = value
        this.weight = weight
    }
}

class CdcAdapter <: EventAdapter<CdcEvent> {
    public func manifest(event: CdcEvent): String {
        "Cdc"
    }

    public func toJournal(event: CdcEvent): String {
        "${event.kind}|${event.value}|${event.weight}"
    }

    public func fromJournal(manifest: String, data: String): Array<CdcEvent> {
        match (manifest) {
            case "Cdc" =>
                let parts = data.split("|")
                if (parts.size == 3) {
                    [CdcEvent(parts[0], Int64.parse(parts[1]), Int64.parse(parts[2]))]
                } else {
                    throw IllegalArgumentException("Cdc 字段数错误")
                }
            case other => throw IllegalArgumentException("未知 CDC 事件：${other}")
        }
    }

    public func upcast(manifest: String, data: String): (String, String) {
        match (manifest) {
            case "CdcV1" =>
                let parts = data.split(",")
                ("Cdc", "${parts[0]}|${parts[1]}|1")
            case "CdcV2" =>
                let parts = data.split("|")
                ("Cdc", "${parts[0]}|${parts[1]}|1")
            case _ => (manifest, data)
        }
    }
}

let ADAPTER = CdcAdapter()

/** 把 "manifest|data" 解码成当前事件（先 upcast） */
func decode(payload: String): Option<CdcEvent> {
    match (payload.indexOf("|")) {
        case Some(i) =>
            let manifest = payload[0..i]
            let data = payload[(i + 1)..]
            let (m, d) = ADAPTER.upcast(manifest, data)
            try {
                let events = ADAPTER.fromJournal(m, d)
                if (events.size == 1) {
                    Some<CdcEvent>(events[0])
                } else {
                    None<CdcEvent>
                }
            } catch (_: Exception) {
                None<CdcEvent>
            }
        case None => None<CdcEvent>
    }
}

/** 读模型：按 kind 计数与权重/值求和 */
class Model {
    private let lock = Mutex()
    private let counts = HashMap<String, Int64>()
    private let values = HashMap<String, Int64>()
    private let weights = HashMap<String, Int64>()
    private let totalLock = Mutex()
    private var total: Int64 = 0

    func apply(ev: CdcEvent): Unit {
        synchronized(lock) {
            counts[ev.kind] = (counts.get(ev.kind) ?? 0) + 1
            values[ev.kind] = (values.get(ev.kind) ?? 0) + ev.value
            weights[ev.kind] = (weights.get(ev.kind) ?? 0) + ev.weight
        }
        synchronized(totalLock) {
            total++
        }
    }

    func clear(): Unit {
        synchronized(lock) {
            counts.clear()
            values.clear()
            weights.clear()
        }
        synchronized(totalLock) {
            total = 0
        }
    }

    func totalCount(): Int64 {
        synchronized(totalLock) {
            total
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"total\":${total},\"kinds\":{")
            var first = true
            for ((kind, n) in counts) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${kind}\":{\"count\":${n},\"value\":${values.get(kind) ?? 0}," +
                    "\"weight\":${weights.get(kind) ?? 0}}")
            }
            sb.append("}}")
            sb.toString()
        }
    }
}

/** 慢 sink：容量有限，满则丢弃（DropNew），处理与接收解耦 */
class SlowSink {
    private let lock = Mutex()
    private let queue = ArrayDeque<CdcEvent>()
    private var dropped: Int64 = 0
    private var processed: Int64 = 0
    let model = Model()

    func enqueue(ev: CdcEvent): Unit {
        synchronized(lock) {
            if (Int64(queue.size) >= SLOW_CAPACITY) {
                dropped++
            } else {
                queue.addLast(ev)
            }
        }
    }

    func drain(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            while (true) {
                match (queue.removeFirst()) {
                    case Some(ev) =>
                        model.apply(ev)
                        processed++
                        n++
                    case None => break
                }
            }
            n
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            Int64(queue.size)
        }
    }

    func droppedCount(): Int64 {
        synchronized(lock) {
            dropped
        }
    }

    func processedCount(): Int64 {
        synchronized(lock) {
            processed
        }
    }
}

class Env {
    let topic: DurableTopic
    let offsets: DurableStateStore
    let fast = Model()
    let slow = SlowSink()
    private let lock = Mutex()
    private var fastSub: Option<DurableSubscription> = None<DurableSubscription>
    private var slowSub: Option<DurableSubscription> = None<DurableSubscription>
    private var seq: Int64 = 0

    init(dir: String) {
        this.topic = DurableTopic(TOPIC, FileDurableStateStore("${dir}/topic"), retention: 5000)
        this.offsets = FileDurableStateStore("${dir}/offsets")
    }

    private func payloadOf(version: Int64, kind: String, value: Int64, weight: Int64): String {
        match (version) {
            case 1 => "CdcV1|${kind},${value}"
            case 2 => "CdcV2|${kind}|${value}"
            case _ => "Cdc|${kind}|${value}|${weight}"
        }
    }

    /** 发布一条 CDC 事件（version 用于构造混合 schema） */
    func publish(version: Int64, kind: String, value: Int64, weight: Int64): Int64 {
        synchronized(lock) {
            seq = topic.publish(payloadOf(version, kind, value, weight))
            seq
        }
    }

    func startSinks(): Unit {
        synchronized(lock) {
            fastSub = Some<DurableSubscription>(topic.subscribe("fast", offsets, { seq, payload =>
                match (decode(payload)) {
                    case Some(ev) => fast.apply(ev)
                    case None => ()
                }
            }, fromSeq: 1))
            slowSub = Some<DurableSubscription>(topic.subscribe("slow", offsets, { seq, payload =>
                match (decode(payload)) {
                    case Some(ev) => slow.enqueue(ev)
                    case None => ()
                }
            }, fromSeq: 1))
        }
    }

    func fastOffline(): Bool {
        synchronized(lock) {
            match (fastSub) {
                case Some(s) =>
                    s.unsubscribe()
                    fastSub = None<DurableSubscription>
                    true
                case None => false
            }
        }
    }

    func fastOnline(): Int64 {
        synchronized(lock) {
            let applied = AtomicInt64(0)
            if (fastSub.isNone()) {
                let s = topic.subscribe("fast", offsets, { seq, payload =>
                    if (let Some(ev) <- decode(payload)) {
                        fast.apply(ev)
                        applied.fetchAdd(1)
                    }
                })
                fastSub = Some<DurableSubscription>(s)
            }
            applied.load()
        }
    }

    func lagFast(): Int64 {
        let high = topic.highestSequenceNr()
        high - topic.offsetOf("fast", offsets)
    }

    func rebuildFast(): Int64 {
        let model = fast
        model.clear()
        var n: Int64 = 0
        for ((seq, payload) in topic.history(1)) {
            if (let Some(ev) <- decode(payload)) {
                model.apply(ev)
                n++
            }
        }
        n
    }

    func rawJson(from: Int64): String {
        let sb = StringBuilder()
        sb.append("[")
        var first = true
        for ((seq, payload) in topic.history(from)) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("{\"seq\":${seq},\"payload\":\"${payload}\"}")
        }
        sb.append("]")
        sb.toString()
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

class Gate {
    let lock = Mutex()
    let cond: Condition
    var open = false

    init() {
        this.cond = synchronized(this.lock) {
            this.lock.condition()
        }
    }

    func release(): Unit {
        synchronized(lock) {
            open = true
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Unit {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!open) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return
                }
                cond.wait(timeout: left)
            }
        }
    }
}

func buildApp(env: Env, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/events", { ctx =>
        let kind = ctx.query("kind") ?? "order"
        let value = ctx.queryInt("value") ?? 1
        let weight = ctx.queryInt("weight") ?? 1
        let version = ctx.queryInt("v") ?? 3
        let seq = env.publish(version, kind, value, weight)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"seq\":${seq},\"version\":${version}}")
    })
    app.get("/models/fast", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.fast.json())
    })
    app.get("/models/slow", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.slow.model.json())
    })
    app.get("/queue", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"queued\":${env.slow.size()},\"dropped\":${env.slow.droppedCount()}," +
            "\"processed\":${env.slow.processedCount()}}")
    })
    app.post("/slow/drain", { ctx =>
        let n = env.slow.drain()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"drained\":${n}}")
    })
    app.get("/lag", { ctx =>
        let high = env.topic.highestSequenceNr()
        let cursor = env.topic.offsetOf("fast", env.offsets)
        let lag = env.lagFast()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"published\":${high},\"fastCursor\":${cursor},\"lag\":${lag}}")
    })
    app.post("/rebuild", { ctx =>
        let n = env.rebuildFast()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"reapplied\":${n}}")
    })
    app.get("/raw", { ctx =>
        let from = ctx.queryInt("from") ?? 1
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.rawJson(from))
    })
    app.get("/stream", { ctx =>
        let from = ctx.queryInt("from") ?? 1
        let limit = ctx.queryInt("limit") ?? 20
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let seen = AtomicInt64(0)
            let name = "sse-${MonoTime.now().hashCode()}"
            let sub = env.topic.subscribe(name, env.offsets, { seq, payload =>
                stream.sendEvent("cdc", payload)
                if (seen.fetchAdd(1) + 1 >= limit) {
                    gate.release()
                }
            }, fromSeq: from)
            gate.await(WAIT)
            sub.unsubscribe()
            stream.close()
        })
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
    let dir = "/tmp/cactor-ex46-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let env = Env(dir)
        env.startSinks()
        let app = buildApp(env, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex46：端口绑定失败")
            return 1
        }
        println("ex46 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let env = Env(dir)
    env.startSinks()

    println("\n[1] 混合 schema 消费")
    env.publish(1, "order", 100, 1)
    env.publish(2, "order", 50, 1)
    env.publish(3, "order", 30, 2)
    c.check(env.fast.totalCount() == 3, "fast 消费 3 条")
    let fast = env.fast.json()
    c.check(fast.contains("\"order\":{\"count\":3,\"value\":180,\"weight\":4}"), "upcast 后聚合正确：${fast}")
    c.check(env.slow.size() == 3 && env.slow.droppedCount() == 0, "slow 队列 3 条未丢弃")

    println("\n[2] 慢消费者有界丢弃")
    for (i in 0..20) {
        env.publish(3, "order", 1, 1)
    }
    c.check(env.slow.size() == SLOW_CAPACITY && env.slow.droppedCount() == 18, "slow 队列封顶 5、丢弃 18")
    c.check(env.fast.totalCount() == 23, "fast 不受慢消费者影响（23 条）")
    c.check(env.slow.drain() == SLOW_CAPACITY, "drain 处理 5 条")
    c.check(env.slow.model.totalCount() == 5 && env.slow.processedCount() == 5, "slow 只处理队列内 5 条")

    println("\n[3] fast 离线后补齐")
    c.check(env.fastOffline(), "fast 下线")
    for (i in 0..10) {
        env.publish(3, "payment", 2, 1)
    }
    c.check(env.fast.totalCount() == 23, "离线期间不消费")
    let applied = env.fastOnline()
    c.check(applied == 10 && env.fast.totalCount() == 33, "重连补齐 10 条（共 33）")
    c.check(env.lagFast() == 0, "位点追上（lag=0）")

    println("\n[4] 全量回填")
    let rebuilt = env.rebuildFast()
    c.check(rebuilt == 33, "回填重放 33 条")
    c.check(env.fast.totalCount() == 33, "回填后总数一致")
    let fastAfter = env.fast.json()
    c.check(fastAfter.contains("\"order\":{\"count\":23,") && fastAfter.contains("\"payment\":{\"count\":10,"),
        "回填后聚合一致：${fastAfter}")

    println("\n[5] Lou 端点")
    let app = buildApp(env, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let pub = readBody(client.post("http://127.0.0.1:${port}/events?kind=order&value=7&v=1", ""))
        c.check(pub.contains("\"version\":1"), "POST /events：${pub}")
        let model = readBody(client.get("http://127.0.0.1:${port}/models/fast"))
        c.check(model.contains("\"total\":34"), "GET /models/fast 反映新事件")
        let q = readBody(client.get("http://127.0.0.1:${port}/queue"))
        c.check(q.contains("\"dropped\":"), "GET /queue 可观测")
        let lag = readBody(client.get("http://127.0.0.1:${port}/lag"))
        c.check(lag.contains("\"lag\":0"), "GET /lag")
        let raw = readBody(client.get("http://127.0.0.1:${port}/raw?from=1"))
        c.check(raw.contains("CdcV1") && raw.contains("CdcV2") && raw.contains("\"Cdc|"), "GET /raw 显示混合 schema")
        let sse = readBody(client.get("http://127.0.0.1:${port}/stream?from=30&limit=3"))
        c.check(sse.contains("event: cdc"), "SSE 输出 CDC 事件")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
    }

    println("\nex46：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
