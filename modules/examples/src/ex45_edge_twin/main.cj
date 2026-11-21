/*
 * CActor Ex 45 - 边缘-云数字孪生（docs/plan/2026117-lz.md §31 第 4 项）
 *
 * 真实约束：
 * - 设备在边缘侧持续工作：断网时本地仍可改配置、登记能力，命令不能丢；
 * - 网络恢复后双向收敛：配置按"最后写入胜出"（LWWMap），能力集合按 add-wins（ORMap）；
 * - 冲突不能让旧值复活：云端更新的删除墓碑在合并后必须压过边缘的旧值；
 * - 离线期间的待同步操作要可观测、可持久化（重启后仍能知道欠了多少）；
 * - 收敛必须是 CRDT 合并，而不是"重放旧值"——后者会把旧写覆盖到新墓碑上。
 *
 * 组件：
 * - `LWWMap` / `ORMap`（cluster ddata）：期望配置与能力集合
 * - `DurableTopic`（ex41）：离线操作缓冲（按序号持久化，恢复后仍可统计欠账）
 * - 同一进程内模拟边缘与云端（真实部署需 roles/Multi-DC，见 §31 前置条件）
 * - Lou：POST /config、POST /remove、POST /capability、POST /offline、POST /online、
 *        GET /device、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 在线写入双向可见；
 * 2. 断网：边缘改配置/加能力，云端改配置并删除一个键（更新墓碑），缓冲 3 条；
 * 3. 重连：双向 CRDT 合并，配置收敛（旧值不复活），能力并集，缓冲清零；
 * 4. 再次离线写入后重连，结果仍收敛；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex45_edge_twin
 *   ./target/release/bin/cactor_examples.ex45_edge_twin serve 18190
 */
package cactor_examples.ex45_edge_twin

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_cluster.ddata.{LWWMap, ORMap}
import cactor_persistence.{DurableTopic, FileDurableStateStore}

let CAP_KEY = "cap"
let WAIT = Duration.second * 10

/** 一个副本（边缘或云）：期望配置 + 能力集合 + 逻辑时钟 */
class DeviceTwin {
    let name: String
    private let lock = Mutex()
    private var config = LWWMap()
    private var caps = ORMap()
    private var clock: Int64 = 0

    init(name: String) {
        this.name = name
    }

    /** 时间戳用墙钟毫秒（跨副本可比），同副本内保证严格递增 */
    private func nextTs(): Int64 {
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        clock = if (now > clock) { now } else { clock + 1 }
        clock
    }

    func setConfig(key: String, value: String): Int64 {
        synchronized(lock) {
            let ts = nextTs()
            config = config.put(key, value, ts, name)
            ts
        }
    }

    func removeConfig(key: String): Int64 {
        synchronized(lock) {
            let ts = nextTs()
            config = config.remove(key, ts, name)
            ts
        }
    }

    func addCapability(cap: String): Unit {
        synchronized(lock) {
            caps = caps.put(name, CAP_KEY, cap)
        }
    }

    func configOf(key: String): Option<String> {
        synchronized(lock) {
            config.get(key)
        }
    }

    func capabilities(): Array<String> {
        synchronized(lock) {
            caps.values(CAP_KEY)
        }
    }

    /** 双向合并（抗熵）：两边都取并集/最新 */
    func syncWith(other: DeviceTwin): Unit {
        synchronized(lock) {
            let otherLock = other.lock
            synchronized(otherLock) {
                config = config.merge(other.config)
                caps = caps.merge(other.caps)
                other.config = other.config.merge(config)
                other.caps = other.caps.merge(caps)
            }
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"name\":\"${name}\",\"config\":{")
            var first = true
            for (key in config.keys()) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${key}\":\"${config.get(key) ?? ""}\"")
            }
            sb.append("},\"capabilities\":[")
            let list = caps.values(CAP_KEY)
            first = true
            for (cap in list) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${cap}\"")
            }
            sb.append("]}")
            sb.toString()
        }
    }
}

/** 边缘侧：本地副本 + 离线操作缓冲（DurableTopic） */
class Edge {
    let local: DeviceTwin
    let cloud: DeviceTwin
    let buffer: DurableTopic
    private let lock = Mutex()
    private var connected = true
    private var flushed: Int64 = 0

    init(dir: String, cloud: DeviceTwin) {
        this.local = DeviceTwin("edge")
        this.cloud = cloud
        this.buffer = DurableTopic("edge-ops", FileDurableStateStore("${dir}/buffer"), retention: 1000)
    }

    func isOnline(): Bool {
        synchronized(lock) {
            connected
        }
    }

    private func record(op: String): Unit {
        if (connected) {
            local.syncWith(cloud)
        } else {
            buffer.publish(op)
        }
    }

    func setConfig(key: String, value: String): Int64 {
        synchronized(lock) {
            let ts = local.setConfig(key, value)
            record("config|${key}|${value}")
            ts
        }
    }

    func removeConfig(key: String): Int64 {
        synchronized(lock) {
            let ts = local.removeConfig(key)
            record("remove|${key}")
            ts
        }
    }

    func addCapability(cap: String): Unit {
        synchronized(lock) {
            local.addCapability(cap)
            record("cap|${cap}")
        }
    }

    func offline(): Unit {
        synchronized(lock) {
            connected = false
        }
    }

    /** 重连：双向 CRDT 合并；缓冲中的操作由本地 CRDT 状态承载，清空欠账 */
    func online(): Unit {
        synchronized(lock) {
            local.syncWith(cloud)
            connected = true
            flushed = buffer.highestSequenceNr()
        }
    }

    func pending(): Int64 {
        synchronized(lock) {
            let high = buffer.highestSequenceNr()
            if (high > flushed) { high - flushed } else { 0 }
        }
    }

    func stateJson(): String {
        synchronized(lock) {
            "{\"online\":${connected},\"pending\":${pending()}," +
                "\"edge\":${local.json()},\"cloud\":${cloud.json()}}"
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

func buildApp(cloud: DeviceTwin, edge: Edge, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/config", { ctx =>
        let key = ctx.query("key") ?? "color"
        let value = ctx.query("value") ?? "blue"
        let ts = edge.setConfig(key, value)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"key\":\"${key}\",\"value\":\"${value}\",\"ts\":${ts}}")
    })
    app.post("/remove", { ctx =>
        let key = ctx.query("key") ?? "color"
        let ts = edge.removeConfig(key)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"removed\":\"${key}\",\"ts\":${ts}}")
    })
    app.post("/capability", { ctx =>
        let cap = ctx.query("name") ?? "wifi"
        edge.addCapability(cap)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"capability\":\"${cap}\"}")
    })
    app.post("/offline", { ctx =>
        edge.offline()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"online\":false}")
    })
    app.post("/online", { ctx =>
        edge.online()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"online\":true,\"pending\":${edge.pending()}}")
    })
    app.get("/device", { ctx =>
        let side = ctx.query("side") ?? "edge"
        ctx.setSingleHeader("Content-Type", "application/json")
        if (side == "cloud") {
            ctx.writeString(cloud.json())
        } else {
            ctx.writeString(edge.local.json())
        }
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(edge.stateJson())
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
    let dir = "/tmp/cactor-ex45-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let cloud = DeviceTwin("cloud")
        let edge = Edge(dir, cloud)
        let app = buildApp(cloud, edge, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex45：端口绑定失败")
            return 1
        }
        println("ex45 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let cloud = DeviceTwin("cloud")
    let edge = Edge(dir, cloud)

    println("\n[1] 在线双向可见")
    edge.setConfig("color", "red")
    cloud.setConfig("firmware", "v1")
    edge.online() // 双向合并
    c.check(edge.local.configOf("color") == Some<String>("red"), "边缘看到 color=red")
    c.check(cloud.configOf("color") == Some<String>("red"), "云端看到 color=red")
    c.check(edge.local.configOf("firmware") == Some<String>("v1"), "边缘看到云端 firmware")

    println("\n[2] 断网期间各自写入")
    edge.offline()
    edge.setConfig("color", "blue")
    edge.setConfig("size", "L")
    edge.addCapability("wifi")
    sleep(Duration.millisecond * 20)
    cloud.setConfig("battery", "80")
    // 云端在"离线窗口"里删除了 color（墙钟时间戳晚于边缘的离线写，墓碑必须胜出）
    cloud.removeConfig("color")
    cloud.addCapability("bt")
    c.check(edge.pending() == 3, "边缘缓冲 3 条操作")
    c.check(cloud.configOf("color").isNone(), "云端已删除 color")
    c.check(edge.local.configOf("color") == Some<String>("blue"), "边缘本地仍是 blue（未同步）")
    c.check(!cloud.capabilities().contains("wifi") && !edge.local.capabilities().contains("bt"),
        "断网期间能力各自独立")

    println("\n[3] 重连后 CRDT 收敛")
    edge.online()
    c.check(edge.pending() == 0, "缓冲欠账清零")
    c.check(cloud.configOf("color").isNone() && edge.local.configOf("color").isNone(),
        "旧值不复活：墓碑压过边缘离线写")
    c.check(edge.local.configOf("size") == Some<String>("L") && cloud.configOf("size") == Some<String>("L"),
        "size=L 双向同步")
    c.check(edge.local.configOf("battery") == Some<String>("80") && cloud.configOf("battery") == Some<String>("80"),
        "云端 battery 同步到边缘")
    c.check(edge.local.capabilities() == ["bt", "wifi"] && cloud.capabilities() == ["bt", "wifi"],
        "能力并集：add-wins")

    println("\n[4] 再次离线写入后收敛")
    edge.offline()
    edge.setConfig("mode", "eco")
    edge.setConfig("size", "M")
    c.check(edge.pending() == 2, "再次欠账 2 条")
    edge.online()
    c.check(edge.local.configOf("size") == Some<String>("M") && cloud.configOf("size") == Some<String>("M"),
        "最新写入胜出（size=M）")
    c.check(edge.local.configOf("mode") == Some<String>("eco") && cloud.configOf("mode") == Some<String>("eco"),
        "新键同步")

    println("\n[5] Lou 端点")
    let app = buildApp(cloud, edge, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let cfg = readBody(client.post("http://127.0.0.1:${port}/config?key=color&value=green", ""))
        c.check(cfg.contains("\"key\":\"color\""), "POST /config：${cfg}")
        let dev = readBody(client.get("http://127.0.0.1:${port}/device?side=cloud"))
        c.check(dev.contains("\"color\":\"green\""), "GET /device?side=cloud 已收敛")
        let off = readBody(client.post("http://127.0.0.1:${port}/offline", ""))
        c.check(off.contains("\"online\":false"), "POST /offline")
        let _ = readBody(client.post("http://127.0.0.1:${port}/config?key=color&value=black", ""))
        let state = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(state.contains("\"pending\":1"), "GET /state 显示欠账 1 条")
        let on = readBody(client.post("http://127.0.0.1:${port}/online", ""))
        c.check(on.contains("\"pending\":0"), "POST /online 清零")
        let cap = readBody(client.post("http://127.0.0.1:${port}/capability?name=lora", ""))
        c.check(cap.contains("lora"), "POST /capability")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
    }

    println("\nex45：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
