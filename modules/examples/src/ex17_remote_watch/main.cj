/*
 * CActor Ex 17 - 远程监视与隔离（对标 protoactor-go remote / Akka DeathWatch + quarantine）
 *
 * 真实约束：
 * - 两个进程（这里用同一进程里的两套真实 TCP 端点模拟）互相发送消息；
 * - 被监视的远程 Actor 停止时，监视方必须收到 Terminated（DeathPact），而不是永远等；
 * - 停止后继续向该路径发消息，必须有明确的失败/超时，而不是挂起；
 * - 判定某个对端"已不可信"（协议错误/旧实例复活）时，隔离它：后续连接直接被拒绝，计数可观测。
 *
 * 组件：
 * - SimpleRemoteManager：exposeActor / remoteActorOf / ask / watchRemote / quarantine
 * - Terminated：远程停止通知（ActorContext.watch 的跨节点版本）
 * - Lou：GET /status
 *
 * 自检（退出码 0/1）：
 * 1. 远程 ask 正常往返；
 * 2. 远程 Actor 自停 → 监视方在超时内收到 Terminated；
 * 3. 停止后 ask 明确失败（超时），不会挂起；
 * 4. 隔离对端 → isQuarantined 为 true、quarantinedCount ≥ 1，向它发送不抛异常；
 * 5. Lou /status 与上述状态一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex17_remote_watch
 *   ./target/release/bin/cactor_examples.ex17_remote_watch serve 18102
 */
package cactor_examples.ex17_remote_watch

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, RemotingSettings}

let ASK = Duration.second * 3
let WATCH_WAIT = Duration.second * 10

/** 远程端：ping 回 pong；stop 让自身停止（触发监视方 Terminated） */
class RemoteWorker <: Actor {
    public prop name: String { get() { "remote-worker" } }
    public prop description: String { get() { "remote worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            match (s.getContent()) {
                case "ping" => context.reply(StringMessage("pong"))
                case "stop" => context.stop(context.self)
                case _ => context.reply(StringMessage("?"))
            }
        }
        MessageResult.Handled
    }
}

/** 监视方：记录收到的 Terminated */
class WatchCollector <: Actor {
    let lock = Mutex()
    let paths = ArrayList<String>()

    public prop name: String { get() { "watch-collector" } }
    public prop description: String { get() { "watch collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case te: Terminated =>
                synchronized(lock) {
                    paths.add(te.actor.path.getPath())
                }
            case _ => ()
        }
        MessageResult.Handled
    }

    func saw(path: String): Bool {
        synchronized(lock) {
            paths.contains(path)
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(paths.size)
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

func newManager(): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.start()
    m
}

func buildApp(local: SimpleRemoteManager, target: NetworkAddress, watcher: WatchCollector, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/status", { ctx =>
        let terminated = watcher.count()
        let quarantine = local.isQuarantined(target)
        let qCount = local.quarantinedCount()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"terminated\":${terminated},\"quarantined\":${quarantine},\"quarantinedCount\":${qCount}}")
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

func textOf(m: Message): String {
    match (m as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?"
    }
}

main(args: Array<String>): Int64 {
    let systemA = CActorSystemFactory.create("ex17-a")
    let systemB = CActorSystemFactory.create("ex17-b")
    let remoteA = newManager()
    let remoteB = newManager()
    let worker = systemB.actorOf({ => RemoteWorker() }, "remote-worker")
    remoteB.exposeActor("remote-worker", worker)
    // 被监视端必须启用远程 watch：它才能在本地 Actor 停止时向订阅方发 DEATH
    remoteB.enableRemoteWatch({ creator => systemB.actorOf(creator, "remote-watcher") })
    let target = remoteA.remoteActorOf(remoteB.getLocalAddress(), "remote-worker")
    let watcher = WatchCollector()
    let watcherRef = systemA.actorOf({ => watcher }, "watch-collector")
    remoteA.watchRemote(target, watcherRef)

    let bAddr = remoteB.getLocalAddress()

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(remoteA, bAddr, watcher, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex17：端口绑定失败")
            return 1
        }
        println("ex17 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(remoteA, bAddr, watcher, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 远程 ask ----
        println("\n[1] 远程 ask 往返")
        let pong = textOf(remoteA.ask(target, StringMessage("ping"), ASK).get(ASK + Duration.second * 2))
        c.check(pong == "pong", "ask 收到 pong（${pong}）")

        // ---- 2. 远程停止 → Terminated ----
        println("\n[2] 远程 Actor 停止 → Terminated")
        target.tell(StringMessage("stop"))
        let terminated = waitUntil(WATCH_WAIT, { => watcher.saw("/user/remote-worker") })
        c.check(terminated, "监视方收到 Terminated（watcher 共 ${watcher.count()} 条）")

        // ---- 3. 停止后 ask 明确失败 ----
        println("\n[3] 停止后 ask 明确失败（不挂起）")
        var askFailed = false
        try {
            remoteA.ask(target, StringMessage("ping"), Duration.millisecond * 500).get(Duration.second * 2)
        } catch (_: Exception) {
            askFailed = true
        }
        c.check(askFailed, "对已停止远程 Actor 的 ask 超时失败")

        // ---- 4. 隔离对端 ----
        println("\n[4] 隔离对端")
        remoteA.quarantine(bAddr, "示例：对端被判不可信")
        c.check(remoteA.isQuarantined(bAddr), "isQuarantined 为 true")
        c.check(remoteA.quarantinedCount() >= 1, "隔离计数 ≥ 1（${remoteA.quarantinedCount()}）")
        var threw = false
        try {
            target.tell(StringMessage("ping"))
        } catch (_: Exception) {
            threw = true
        }
        c.check(!threw, "向被隔离地址发送不抛异常（连接层拒绝，计数可观测）")

        // ---- 5. Lou 状态 ----
        println("\n[5] Lou /status")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let status = client.get("http://127.0.0.1:${port}/status")
        let body = readBody(status)
        c.check(status.status == 200 && body.contains("\"terminated\":1") && body.contains("\"quarantined\":true"),
            "状态与内存一致：${body}")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        remoteA.stop()
        remoteB.stop()
        systemA.terminate()
        systemB.terminate()
    }

    println("\nex17：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
