/*
 * CActor Cluster - Cluster Singleton（对标 Akka Cluster Singleton）
 *
 * 整个集群中只运行一个实例，位于最老的 Up 成员上（upNumber 最小）。
 * - 选择最老成员时不排除不可达成员：网络分区不会让另一侧再启动一个实例；
 *   只有原节点离开（Leaving）、被 down 或被移除后，实例才会在下一个最老的成员上启动。
 *   因此宕机节点需要手动 down 或配置 downing 策略（推荐 KeepMajority 等脑裂处理策略，见 sbr.cj）。
 * - HandOver 握手（对标 Akka HandOverToMe / HandOverDone，N16）：成为最老成员的节点先向其他每个
 *   Up / Leaving / Exiting 成员发 HANDOVER_TO_ME，对方若运行着实例就发 PoisonPill（先处理完邮箱里的消息），
 *   实例终止后回 HANDOVER_DONE；收齐才启动新实例。任何时刻最多一个实例，即使旧实例停止得很慢。
 *   已 down 的成员不需要回应（down 的节点由 SBR 保证已自行停止）；正常离开（Leaving / Exiting）后被移除的成员
 *   进程通常还在、实例可能还在停止，继续等它回应，最多 SINGLETON_HANDOVER_GRACE_MS（10 s，对标 Akka 的
 *   hand-over-retry）。两种离开方式经集群事件区分：成员被移除前，所有可达节点都必定看到过它的 Exiting 或 Down。
 *   收到请求的节点无条件让出（放弃自己的获取），视图暂时不一致时只影响可用性、不会出现两个实例。
 * - 每个节点都可以通过 proxy 发送消息：消息先进本节点的管理器，本节点运行着实例就直接投递，
 *   否则经远程发给最老成员的管理器（入口 singleton-<名称>）；对方正在握手时由它缓冲，实例启动后按序投递。
 *   还不知道实例在哪里时先缓冲（默认最多 1000 条，超出丢弃最早的）。
 * - 实例所在成员不可达、或发送失败（连接被拒绝等）时，消息留在缓冲区按顺序重试，成员被 down 后发往新位置。
 * - 投递仍是至多一次：已写入连接、但对方在处理前宕机的消息会丢失，需要可靠性时由业务层确认重发。
 * - 透传 sender（E12′，N16）：tell(message, sender) / ask 的发送方随缓冲与远程发送保留，实例 context.reply 即可回复。
 * - 实例自己停止（例如异常后被停止）时，管理器在下一轮重新获取并启动新实例。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated, PoisonPill}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_foundation.detector.{monotonicMillis}
import cactor_remote.{SimpleRemoteManager, RemoteActorRef, parseRemoteSender}
import cactor_cluster.ddata.{FieldWriter, FieldReader}
import cactor_core.typed.{TypedPromise}

class SingletonTick <: Message {
    public func messageType(): String {
        "SingletonTick"
    }
}

class ProxyEnvelope <: Message {
    let payload: Message
    let sender: Option<ActorRef>

    init(payload: Message, sender: Option<ActorRef>) {
        this.payload = payload
        this.sender = sender
    }

    public func messageType(): String {
        "SingletonProxyEnvelope"
    }
}

/** 其他节点管理器发来的 HandOver 控制消息（本节点内传递，文本经 singleton-ctl-<名称> 入口送达） */
class SingletonControl <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "SingletonControl"
    }
}

class SingletonShared {
    let lock = Mutex()
    var runningHere = false
    var startCount: Int64 = 0
    var location: Option<String> = None
    var dropped: Int64 = 0
    var handOvers: Int64 = 0
}

/** 成员离开方式（集群事件经回调转给管理器） */
class SingletonMemberEvent <: Message {
    let address: String
    // "graceful"（Leaving / Exiting）、"downed"、"removed"、"up"（同址新实例）
    let kind: String

    init(address: String, kind: String) {
        this.address = address
        this.kind = kind
    }

    public func messageType(): String {
        "SingletonMemberEvent"
    }
}

/** 一次获取：等待这些成员回 HANDOVER_DONE */
class SingletonAcquisition {
    let round: Int64
    let pending: HashSet<String>
    var lastSentMs: Int64 = -1
    // 正常离开后已被移除的成员：从何时起不在成员表里（超过宽限期不再等）
    let goneSince = HashMap<String, Int64>()

    init(round: Int64, pending: HashSet<String>) {
        this.round = round
        this.pending = pending
    }
}

let SINGLETON_HANDOVER_RESEND_MS: Int64 = 200
/** 正常离开后已被移除、却一直没有回应的成员最多等多久（它的进程多半已退出） */
public let SINGLETON_HANDOVER_GRACE_MS: Int64 = 10000

func singletonEndpoint(name: String): String {
    "singleton-${name}"
}

func singletonControlEndpoint(name: String): String {
    "singleton-ctl-${name}"
}

class SingletonManager <: Actor {
    let cluster: Cluster
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let singletonName: String
    let factory: () -> Actor
    let bufferSize: Int64
    let shared: SingletonShared
    var instance: Option<ActorRef> = None
    // 正在停止的实例（已发 PoisonPill、等 Terminated）与等它停止后要回 DONE 的请求方
    var stopping: Option<ActorRef> = None
    let doneOwed = HashMap<String, Int64>()
    var acquisition: Option<SingletonAcquisition> = None
    var nextRound: Int64 = 0
    var generation: Int64 = 0
    let buffer = ArrayList<ProxyEnvelope>()
    // 地址 -> 是否正常离开（true：Leaving / Exiting；false：被 down）
    let departures = HashMap<String, Bool>()
    let log = Logging.logger("cactor.cluster.singleton")

    init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, singletonName: String,
        factory: () -> Actor, bufferSize: Int64, shared: SingletonShared) {
        this.cluster = cluster
        this.system = system
        this.remote = remote
        this.singletonName = singletonName
        this.factory = factory
        this.bufferSize = bufferSize
        this.shared = shared
    }

    public prop name: String {
        get() { "singleton-manager" }
    }

    public prop description: String {
        get() { "cluster singleton manager" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("check", SingletonTick(), Duration.millisecond * 50)
    }

    public func postStop(): Unit {
        if (let Some(ref) <- instance) {
            system.stop(ref)
            instance = None
        }
        synchronized(shared.lock) {
            shared.runningHere = false
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: SingletonTick =>
                evaluate(context)
                flush()
            case p: ProxyEnvelope =>
                buffer.add(p)
                if (buffer.size > bufferSize) {
                    buffer.remove(at: 0)
                    synchronized(shared.lock) {
                        shared.dropped++
                    }
                }
                flush()
            case c: SingletonControl => onControl(c.text, context)
            case e: SingletonMemberEvent =>
                match (e.kind) {
                    case "graceful" => departures[e.address] = true
                    case "downed" => departures[e.address] = false
                    case "up" => departures.remove(e.address)
                    case _ => ()
                }
            case t: Terminated => onTerminated(t.actor, context)
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    /** 实例应在的成员：最老的 Up 成员（不排除不可达成员） */
    private func targetLocation(): Option<String> {
        if (cluster.isTerminated()) {
            return None
        }
        var best: Option<Member> = None
        for (m in cluster.members()) {
            if (m.status != MemberStatus.Up) {
                continue
            }
            match (best) {
                case Some(b) where b.isOlderThan(m) => ()
                case _ => best = Some(m)
            }
        }
        match (best) {
            case Some(m) => Some(m.address)
            case None => None<String>
        }
    }

    /** 可能运行着实例、需要确认已停止的成员：除自己以外的 Up / Leaving / Exiting 成员 */
    private func askSet(): HashSet<String> {
        let s = HashSet<String>()
        for (m in cluster.members()) {
            if (m.address == cluster.selfAddress) {
                continue
            }
            let mayRun = match (m.status) {
                case Up | Leaving | Exiting => true
                case _ => false
            }
            if (mayRun) {
                s.add(m.address)
            }
        }
        s
    }

    private func evaluate(context: ActorContext): Unit {
        let target = targetLocation()
        synchronized(shared.lock) {
            shared.location = target
        }
        if (target != Some(cluster.selfAddress)) {
            // 不再是（或还不是）最老成员：放弃获取；运行着实例就让它停止（先处理完邮箱）
            acquisition = None
            if (instance.isSome()) {
                log.info("Singleton ${singletonName} 从 ${cluster.selfAddress} 移交")
                beginStop(context)
            }
            return
        }
        if (instance.isSome() || stopping.isSome()) {
            // 已在运行；或自己的旧实例还在停止（它终止后下一轮再获取）
            return
        }
        let asked = askSet()
        let acq = match (acquisition) {
            case Some(a) => a
            case None =>
                nextRound++
                let a = SingletonAcquisition(nextRound, asked)
                acquisition = Some(a)
                a
        }
        // 不再需要回应的成员：已 down；或已被移除且不是正常离开的；或正常离开后移除、超过宽限期仍无回应
        let statuses = HashMap<String, MemberStatus>()
        for (m in cluster.members()) {
            statuses[m.address] = m.status
        }
        let now0 = monotonicMillis()
        let gone = ArrayList<String>()
        for (p in acq.pending) {
            match (statuses.get(p)) {
                case Some(st) where st == MemberStatus.Down => gone.add(p)
                case Some(_) => acq.goneSince.remove(p)
                case None =>
                    if (departures.get(p) == Some(true)) {
                        let since = match (acq.goneSince.get(p)) {
                            case Some(t) => t
                            case None =>
                                acq.goneSince[p] = now0
                                now0
                        }
                        if (now0 - since >= SINGLETON_HANDOVER_GRACE_MS) {
                            log.warn("Singleton ${singletonName}：${p} 离开后 ${SINGLETON_HANDOVER_GRACE_MS} ms 没有确认移交，按已停止处理")
                            gone.add(p)
                        }
                    } else {
                        gone.add(p)
                    }
            }
        }
        for (p in gone) {
            acq.pending.remove(p)
        }
        if (acq.pending.isEmpty()) {
            startInstance(context)
            return
        }
        let now = monotonicMillis()
        if (acq.lastSentMs < 0 || now - acq.lastSentMs >= SINGLETON_HANDOVER_RESEND_MS) {
            acq.lastSentMs = now
            for (p in acq.pending) {
                sendControl(p, "HO", acq.round)
            }
        }
    }

    private func onControl(text: String, context: ActorContext): Unit {
        let (kind, round, from) = try {
            let r = FieldReader(text)
            (r.next(), r.nextInt(), r.next())
        } catch (_: Exception) {
            return
        }
        match (kind) {
            case "HO" =>
                // 对方要接手：无条件让出（放弃自己的获取），实例停止后回 DONE
                acquisition = None
                if (instance.isSome()) {
                    beginStop(context)
                }
                if (stopping.isSome()) {
                    doneOwed[from] = round
                } else {
                    sendControl(from, "HD", round)
                }
            case "HD" =>
                if (let Some(a) <- acquisition) {
                    if (a.round == round) {
                        a.pending.remove(from)
                        if (a.pending.isEmpty() && instance.isNone() && stopping.isNone() &&
                            targetLocation() == Some(cluster.selfAddress)) {
                            startInstance(context)
                        }
                    }
                }
            case _ => ()
        }
    }

    private func onTerminated(ref: ActorRef, context: ActorContext): Unit {
        if (let Some(s) <- stopping) {
            if (s.path.toString() == ref.path.toString()) {
                stopping = None
                for ((to, round) in doneOwed) {
                    sendControl(to, "HD", round)
                }
                doneOwed.clear()
                return
            }
        }
        if (let Some(i) <- instance) {
            if (i.path.toString() == ref.path.toString()) {
                // 实例自己停止了：下一轮重新获取并启动
                log.warn("Singleton ${singletonName} 在 ${cluster.selfAddress} 上意外停止，将重新启动")
                instance = None
                synchronized(shared.lock) {
                    shared.runningHere = false
                }
            }
        }
    }

    private func startInstance(context: ActorContext): Unit {
        acquisition = None
        generation++
        let ref = system.actorOf(factory,
            "singleton-${singletonName}-${generation}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
        context.watch(ref)
        instance = Some(ref)
        log.info("Singleton ${singletonName} 在 ${cluster.selfAddress} 启动")
        synchronized(shared.lock) {
            shared.runningHere = true
            shared.startCount++
        }
        flush()
    }

    /** 让实例停止：PoisonPill 排在已入队的消息之后，邮箱里的消息先处理完（同 Akka 默认 terminationMessage） */
    private func beginStop(context: ActorContext): Unit {
        if (let Some(ref) <- instance) {
            instance = None
            stopping = Some(ref)
            ref.tell(PoisonPill())
            synchronized(shared.lock) {
                shared.runningHere = false
                shared.handOvers++
            }
        }
    }

    private func sendControl(to: String, kind: String, round: Int64): Unit {
        try {
            remote.sendControl(NetworkAddress.parse(to), singletonControlEndpoint(singletonName),
                StringMessage(FieldWriter().put(kind).put(round).put(cluster.selfAddress).done()))
        } catch (_: Exception) {
            // 对方不可达：获取方每 200 ms 重发；对方被 down 后不再需要它回应
        }
    }

    private func flush(): Unit {
        if (buffer.isEmpty()) {
            return
        }
        let target = synchronized(shared.lock) { shared.location }
        match ((target, instance)) {
            case (Some(t), Some(ref)) where t == cluster.selfAddress =>
                for (p in buffer) {
                    match (p.sender) {
                        case Some(s) => ref.tell(p.payload, s)
                        case None => ref.tell(p.payload)
                    }
                }
                buffer.clear()
            case (Some(t), _) where t != cluster.selfAddress =>
                // 实例所在成员已不可达（多半已宕机，等待被 down）：继续缓冲，down 后发往新位置
                for (u in cluster.state().unreachable) {
                    if (u == t) {
                        return
                    }
                }
                // 发给目标节点的管理器入口：对方正在握手时由它缓冲，不会因实例尚未启动而丢失
                let remoteRef = remote.remoteActorOf(NetworkAddress.parse(t), singletonEndpoint(singletonName))
                var sent: Int64 = 0
                for (p in buffer) {
                    try {
                        match (p.sender) {
                            case Some(s) => remoteRef.tell(p.payload, s)
                            case None => remoteRef.tell(p.payload)
                        }
                        sent++
                    } catch (e: Exception) {
                        // 发送失败（例如连接被拒绝）：这一条及之后的消息留在缓冲区，下一轮按顺序重试
                        log.warn("发送给 Singleton ${singletonName}@${t} 失败，稍后重试: ${e}")
                        break
                    }
                }
                buffer.remove(0..sent)
            case _ => () // 位置未知，或本节点是目标但实例还在握手中：继续缓冲
        }
    }
}

/**
 * 本节点上的 Singleton 管理器与代理。集群中每个节点都要创建（使用同样的 name 和 factory）。
 *
 * ```
 * let counter = ClusterSingleton(cluster, system, remote, "counter", { => CounterActor() })
 * counter.tell(StringMessage("inc"))      // 无论在哪个节点调用，都发给唯一的实例
 * ```
 */
public class ClusterSingleton {
    private let manager: ActorRef
    private let shared = SingletonShared()
    private let system: CActorSystem
    private let singletonName: String
    private let remote: SimpleRemoteManager

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, name: String, factory: () -> Actor) {
        this(cluster, system, remote, name, factory, 1000)
    }

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, name: String, factory: () -> Actor,
        bufferSize: Int64) {
        this.system = system
        this.singletonName = name
        this.remote = remote
        let sh = shared
        let mgr = system.actorOf({ => SingletonManager(cluster, system, remote, name, factory, bufferSize, sh) },
            "singleton-manager-${name}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
        this.manager = mgr
        // 其他节点代理发来的消息先进本节点管理器（保留远程发送方，实例 reply 可以送回）；HandOver 控制消息走单独入口
        let rm = remote
        remote.getReceiver().registerHandler(singletonEndpoint(name), { m: Message, from: Option<String> =>
            let sender: Option<ActorRef> = match (from) {
                case Some(f) => match (parseRemoteSender(f)) {
                    case Some((addr, n)) => Some(RemoteActorRef(rm, addr, n))
                    case None => None<ActorRef>
                }
                case None => None<ActorRef>
            }
            mgr.tell(ProxyEnvelope(m, sender))
        })
        remote.getReceiver().registerHandler(singletonControlEndpoint(name), { m: Message, _: Option<String> =>
            if (let Some(s) <- (m as StringMessage)) {
                mgr.tell(SingletonControl(s.getContent()))
            }
        })
        // 成员离开方式：移除前所有可达节点都会看到 Exiting 或 Down，事件可靠地区分"正常离开"与"被 down"
        cluster.subscribe({ e =>
            let ev: Option<SingletonMemberEvent> = match (e) {
                case ClusterEvent.MemberLeft(m) => Some(SingletonMemberEvent(m.address, "graceful"))
                case ClusterEvent.MemberExited(m) => Some(SingletonMemberEvent(m.address, "graceful"))
                case ClusterEvent.MemberDowned(m) => Some(SingletonMemberEvent(m.address, "downed"))
                case ClusterEvent.MemberJoined(m) => Some(SingletonMemberEvent(m.address, "up"))
                case _ => None<SingletonMemberEvent>
            }
            if (let Some(x) <- ev) {
                mgr.tell(x)
            }
        })
    }

    /** 发送给集群中唯一的实例（消息需要能被远程序列化，例如 StringMessage 或注册了编解码器的消息） */
    public func tell(message: Message): Unit {
        manager.tell(ProxyEnvelope(message, None<ActorRef>))
    }

    /** 带发送方发送（E12′）：实例用 context.reply 回复到 sender，实例在其他节点时回复经远程直接送回 */
    public func tell(message: Message, sender: ActorRef): Unit {
        manager.tell(ProxyEnvelope(message, Some(sender)))
    }

    /** 请求-响应（E12′）：实例 context.reply 的回复；timeout 内没有回复时以 TypedAskTimeoutException 失败 */
    public func ask(message: Message, timeout: Duration): TypedPromise<Message> {
        askVia("singleton ${singletonName}", timeout, { replyTo => tell(message, replyTo) })
    }

    /** 实例当前是否运行在本节点 */
    public func isRunningHere(): Bool {
        synchronized(shared.lock) {
            shared.runningHere
        }
    }

    /** 本节点上启动过实例的次数 */
    public func startCount(): Int64 {
        synchronized(shared.lock) {
            shared.startCount
        }
    }

    /** 本节点认为实例所在的成员地址 */
    public func location(): Option<String> {
        synchronized(shared.lock) {
            shared.location
        }
    }

    /** 因缓冲区满而丢弃的消息数 */
    public func droppedCount(): Int64 {
        synchronized(shared.lock) {
            shared.dropped
        }
    }

    /** 本节点让出实例的次数（HandOver：收到更新的最老成员的请求或自己不再是最老成员时停止实例） */
    public func handOverCount(): Int64 {
        synchronized(shared.lock) {
            shared.handOvers
        }
    }

    /** 停止本节点的管理器（本地实例随之停止） */
    public func stop(): Unit {
        remote.getReceiver().unregisterHandler(singletonEndpoint(singletonName))
        remote.getReceiver().unregisterHandler(singletonControlEndpoint(singletonName))
        system.stop(manager)
    }
}
