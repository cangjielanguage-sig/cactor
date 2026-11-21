/*
 * CActor Ex 52 - IoT 设备管理（对标 Akka 官方 IoT 示例：设备/设备组/查询聚合）
 *
 * 真实约束：
 * - 设备按 (group, device) 组织，组内设备由组长按需创建（Actor 层级 = 命名空间 + 监督边界）；
 * - 重复注册必须被拒绝，而不是悄悄覆盖别人的设备；
 * - 查询页要看到组内读数聚合（数量/最小/最大/均值），不能把设备逐个同步阻塞地问一遍；
 * - 组内只保留"设备上报过的最新读数"，未上报的设备查询为无读数。
 *
 * 设计（与 Akka IoT 的差异写在注释里）：设备上报时同时告诉组长，组长维护最新读数映射，
 * 因此查询是组长本地同步应答，不需要在 receive 里阻塞 ask 子设备。
 *
 * 组件：类型化 Actor（Group → Device 子 Actor）、TypedAsk、Lou
 *
 * 自检（退出码 0/1）：
 * 1. 两个组、三个设备分别注册并上报；
 * 2. 组聚合正确（count/min/max/avg），空组为无读数；
 * 3. 重复注册返回 false 且设备数不变；
 * 4. Lee：Lou 端点与内存一致，未知组不报错。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex52_iot_devices
 *   ./target/release/bin/cactor_examples.ex52_iot_devices serve 18197
 */
package cactor_examples.ex52_iot_devices

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*

let ASK = Duration.second * 10

// ======================= 协议 =======================

enum DeviceCmd {
    | Record(Float64, TypedActorRef<Bool>)            // 上报读数，true=已记录
    | Read(TypedActorRef<Option<Float64> >)            // 设备自身的最新读数
}

enum GroupCmd {
    | Register(String, TypedActorRef<Bool>)           // deviceId，false=已存在
    | RecordThroughDevice(String, Float64, TypedActorRef<Bool>)  // 组长转发给设备，设备再上报回来
    | Reported(String, Float64)                       // 设备 → 组长
    | ListDevices(TypedActorRef<Array<String> >)
    | Temperature(String, TypedActorRef<Option<Float64> >)
    | Stats(TypedActorRef<GroupStats>)
}

/** 组内读数聚合（空组时 min/max/avg 为 None） */
class GroupStats {
    let count: Int64
    let minValue: Option<Float64>
    let maxValue: Option<Float64>
    let avgValue: Option<Float64>

    init(count: Int64, minValue: Option<Float64>, maxValue: Option<Float64>, avgValue: Option<Float64>) {
        this.count = count
        this.minValue = minValue
        this.maxValue = maxValue
        this.avgValue = avgValue
    }

    func optText(v: Option<Float64>): String {
        match (v) {
            case Some(x) => "${x}"
            case None => "null"
        }
    }

    func json(): String {
        "{\"count\":${count},\"min\":${optText(minValue)},\"max\":${optText(maxValue)},\"avg\":${optText(avgValue)}}"
    }
}

func statsOf(temps: HashMap<String, Float64>): GroupStats {
    if (temps.isEmpty()) {
        return GroupStats(0, None<Float64>, None<Float64>, None<Float64>)
    }
    var lo = 0.0
    var hi = 0.0
    var sum = 0.0
    var count: Int64 = 0
    for ((_, t) in temps) {
        if (count == 0) {
            lo = t
            hi = t
        } else {
            if (t < lo) { lo = t }
            if (t > hi) { hi = t }
        }
        sum += t
        count++
    }
    GroupStats(count, Some<Float64>(lo), Some<Float64>(hi), Some<Float64>(sum / Float64(count)))
}

// ======================= 设备 =======================

/** 设备自身状态放在可变对象里：闭包不能捕获 var，只能捕获引用 */
class DeviceState {
    var last: Option<Float64> = None<Float64>
}

func deviceBehavior(group: TypedActorRef<GroupCmd>, id: String): TypedBehavior<DeviceCmd> {
    TypedBehaviors.setup<DeviceCmd>({ _ =>
        let st = DeviceState()
        TypedBehaviors.receive<DeviceCmd>({ _, m =>
            match (m) {
                case Record(t, reply) =>
                    st.last = Some<Float64>(t)
                    // 推送最新读数给组长：组查询无需逐个阻塞询问设备
                    group.tell(GroupCmd.Reported(id, t))
                    reply.tell(true)
                case Read(reply) => reply.tell(st.last)
            }
            TypedBehaviors.same<DeviceCmd>()
        })
    })
}

// ======================= 组 =======================

func groupBehavior(): TypedBehavior<GroupCmd> {
    TypedBehaviors.setup<GroupCmd>({ ctx =>
        let devices = HashMap<String, TypedActorRef<DeviceCmd> >()
        let temps = HashMap<String, Float64>()
        let me = ctx.self
        TypedBehaviors.receive<GroupCmd>({ _, m =>
            match (m) {
                case Register(id, reply) =>
                    if (devices.contains(id)) {
                        reply.tell(false)
                    } else {
                        devices[id] = ctx.actorOf(deviceBehavior(me, id), "device-${id}")
                        reply.tell(true)
                    }
                case RecordThroughDevice(id, t, reply) =>
                    match (devices.get(id)) {
                        case Some(ref) => ref.tell(DeviceCmd.Record(t, reply))
                        case None => reply.tell(false)
                    }
                case Reported(id, t) =>
                    // 只接受组内已注册设备的上报，避免历史遗留在设备离开后继续污染聚合
                    if (devices.contains(id)) {
                        temps[id] = t
                    }
                case ListDevices(reply) =>
                    let ids = ArrayList<String>()
                    for ((id, _) in devices) {
                        ids.add(id)
                    }
                    reply.tell(ids.toArray())
                case Temperature(id, reply) =>
                    match (temps.get(id)) {
                        case Some(t) => reply.tell(Some<Float64>(t))
                        case None => reply.tell(None<Float64>)
                    }
                case Stats(reply) => reply.tell(statsOf(temps))
            }
            TypedBehaviors.same<GroupCmd>()
        })
    })
}

// ======================= 应用 =======================

/** 按组名按需创建并缓存组引用（HTTP 线程与自检共用） */
class Home {
    let system: CActorSystem
    private let lock = Mutex()
    private let groups = HashMap<String, TypedActorRef<GroupCmd> >()

    init(system: CActorSystem) {
        this.system = system
    }

    func group(id: String): TypedActorRef<GroupCmd> {
        synchronized(lock) {
            match (groups.get(id)) {
                case Some(r) => r
                case None =>
                    let r = system.typedActorOf<GroupCmd>(groupBehavior(), "group-${id}")
                    groups[id] = r
                    r
            }
        }
    }

    func register(groupId: String, deviceId: String): Bool {
        TypedAsk.ask<GroupCmd, Bool>(group(groupId), { r => GroupCmd.Register(deviceId, r) }, ASK).get(ASK)
    }

    func record(groupId: String, deviceId: String, t: Float64): Bool {
        // 首次上报即注册；随后组长把 Record 转发给设备，设备存下读数并向组长上报
        let _ = register(groupId, deviceId)
        TypedAsk.ask<GroupCmd, Bool>(group(groupId),
            { r => GroupCmd.RecordThroughDevice(deviceId, t, r) }, ASK).get(ASK)
    }

    func devices(groupId: String): Array<String> {
        TypedAsk.ask<GroupCmd, Array<String> >(group(groupId), { r => GroupCmd.ListDevices(r) }, ASK).get(ASK)
    }

    func stats(groupId: String): GroupStats {
        TypedAsk.ask<GroupCmd, GroupStats>(group(groupId), { r => GroupCmd.Stats(r) }, ASK).get(ASK)
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

func buildApp(home: Home, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/register", { ctx =>
        let group = ctx.query("group") ?? "g1"
        let device = ctx.query("device") ?? "d1"
        let temp = Float64.parse(ctx.query("temp") ?? "0")
        let recorded = home.record(group, device, temp)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"group\":\"${group}\",\"device\":\"${device}\",\"recorded\":${recorded}}")
    })
    app.get("/devices", { ctx =>
        let group = ctx.query("group") ?? "g1"
        let ids = home.devices(group)
        let sb = StringBuilder()
        sb.append("[")
        var first = true
        for (id in ids) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("\"${id}\"")
        }
        sb.append("]")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(sb.toString())
    })
    app.get("/stats", { ctx =>
        let group = ctx.query("group") ?? "g1"
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(home.stats(group).json())
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
    let system = CActorSystemFactory.create("ex52")
    let home = Home(system)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let server = buildApp(home, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex52：端口绑定失败")
            return 1
        }
        println("ex52 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 注册与上报")
        let _ = home.record("g1", "d1", 20.0)
        let _ = home.record("g1", "d2", 30.0)
        let _ = home.record("g2", "d3", 40.0)
        c.check(home.register("g1", "d1") == false, "重复注册被拒绝")
        let ids = home.devices("g1")
        c.check(ids.size == 2, "g1 有 2 个设备：${ids}")

        println("\n[2] 聚合")
        let s1 = home.stats("g1")
        c.check(s1.count == 2, "g1 数量 2")
        c.check(s1.minValue == Some<Float64>(20.0), "g1 最小 20")
        c.check(s1.maxValue == Some<Float64>(30.0), "g1 最大 30")
        match (s1.avgValue) {
            case Some(a) => c.check((a - 25.0) < 0.0001 && (25.0 - a) < 0.0001, "g1 均值 25")
            case None => c.check(false, "g1 均值缺失")
        }
        let s2 = home.stats("g2")
        c.check(s2.count == 1 && s2.minValue == Some<Float64>(40.0), "g2 单设备读数")

        println("\n[3] 空组")
        let empty = home.stats("empty")
        c.check(empty.count == 0 && empty.minValue.isNone(), "未知组返回空聚合")

        println("\n[4] Lou 端点")
        let server = buildApp(home, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let reg = readBody(client.post("http://127.0.0.1:${port}/register?group=g3&device=d9&temp=12.5", ""))
            c.check(reg.contains("\"recorded\":true"), "POST /register：${reg}")
            let devs = readBody(client.get("http://127.0.0.1:${port}/devices?group=g3"))
            c.check(devs.contains("\"d9\""), "GET /devices：${devs}")
            let stats = readBody(client.get("http://127.0.0.1:${port}/stats?group=g3"))
            c.check(stats.contains("\"count\":1") && stats.contains("12.5"), "GET /stats：${stats}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }
    } finally {
        system.terminate()
    }

    println("\nex52：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
