/*
 * CActor Best 08 - 远程通信：双节点 tell / ask / 可靠投递 / 远程 watch
 *
 * 演示（单进程内两个真实 TCP 节点）：
 * - SimpleRemoteManager：exposeActor / remoteActorOf / tell
 * - 远程 ask：manager.ask(target, msg, timeout)
 * - 可靠投递 tellReliable：按序、恰好一次（1000 条全到）
 * - 远程 watch：被监视 Actor 停止后本地收到 Terminated
 *
 * 自检：四类语义全部符合预期，退出码 0/1。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best08_remote_nodes
 */
package cactor_examples.best08_remote_nodes

import std.sync.*
import std.collection.*
import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, Terminated}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, RemotingSettings}

/** 收到即回显给发送方 */
class Echo <: Actor {
    public prop name: String { get() { "echo" } }
    public prop description: String { get() { "echo" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage => context.reply(StringMessage("echo:${s.getContent()}"))
            case _ => ()
        }
        MessageResult.Handled
    }
}

/** 只收集消息 */
class Sink <: Actor {
    let lock = Mutex()
    let items = ArrayList<String>()
    public prop name: String { get() { "sink" } }
    public prop description: String { get() { "sink" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            synchronized(lock) { items.add(s.getContent()) }
        }
        MessageResult.Handled
    }
    func snapshot(): Array<String> {
        synchronized(lock) { items.toArray() }
    }
}

/** 收集远程 Terminated 通知 */
class WatchCollector <: Actor {
    let lock = Mutex()
    let paths = ArrayList<String>()
    public prop name: String { get() { "watch-collector" } }
    public prop description: String { get() { "watch" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case te: Terminated => synchronized(lock) { paths.add(te.actor.path.getPath()) }
            case _ => ()
        }
        MessageResult.Handled
    }
    func saw(path: String): Bool {
        synchronized(lock) { paths.contains(path) }
    }
}

func newNode(tag: String): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configure(RemotingSettings(Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
        Duration.millisecond * 20, Duration.millisecond * 5))
    m.start()
    m
}

func waitCond(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    cond()
}

main(): Int64 {
    let kit = CActorSystemFactory.create("best08")
    let a = newNode("a")
    let b = newNode("b")
    var ok = true

    // ---------- 1) 普通 tell ----------
    let sink = Sink()
    let sinkRef = kit.actorOf({ => sink }, "sink")
    a.exposeActor("sink", sinkRef)
    let remoteSink = b.remoteActorOf(a.getLocalAddress(), "sink")
    for (i in 0..10) {
        remoteSink.tell(StringMessage("m${i}"))
    }
    ok = ok && waitCond(Duration.second * 5, { => sink.snapshot().size == 10 })
    println("[tell] 收到 ${sink.snapshot().size}/10 条")

    // ---------- 2) 远程 ask ----------
    b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
    let echo = a.remoteActorOf(b.getLocalAddress(), "echo")
    let r = a.ask(echo, StringMessage("ping"), Duration.second * 3).get(Duration.second * 3)
    let reply = (r as StringMessage).getOrThrow().getContent()
    println("[ask] 回复：${reply}")
    ok = ok && reply == "echo:ping"

    // ---------- 3) 可靠投递：1000 条按序恰好一次 ----------
    let sink2 = Sink()
    b.exposeActor("sink2", kit.actorOf({ => sink2 }, "sink2"))
    let reliable = a.remoteActorOf(b.getLocalAddress(), "sink2")
    let n = 1000
    for (i in 0..n) {
        a.tellReliable(reliable, StringMessage("r${i}"))
    }
    ok = ok && waitCond(Duration.second * 15, { => sink2.snapshot().size >= n })
    sleep(Duration.millisecond * 100)
    let got = sink2.snapshot()
    var inOrder = got.size == n
    for (i in 0..got.size) {
        if (got[i] != "r${i}") {
            inOrder = false
        }
    }
    println("[reliable] ${got.size}/${n} 条按序到达：${inOrder}")
    ok = ok && inOrder

    // ---------- 4) 远程 watch ----------
    b.enableRemoteWatch({ creator => kit.actorOf(creator, "remote-watcher") })
    let target = kit.actorOf({ => Sink() }, "target")
    b.exposeActor("target", target)
    let watcher = WatchCollector()
    let watcherRef = kit.actorOf({ => watcher }, "watcher")
    a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), watcherRef)
    kit.stop(target)
    ok = ok && waitCond(Duration.second * 5, { => watcher.saw("/user/target") })
    println("[watch] 收到 Terminated：${watcher.saw("/user/target")}")

    a.stop()
    b.stop()
    kit.terminate()
    println(if (ok) { "best08：通过" } else { "best08：失败" })
    if (ok) { 0 } else { 1 }
}
