/*
 * CActor Cluster - Cluster Singleton（对标 Akka Cluster Singleton）
 *
 * 整个集群中只运行一个实例，位于最老的 Up 成员上（upNumber 最小）。
 * - 选择最老成员时不排除不可达成员：网络分区不会让另一侧再启动一个实例；
 *   只有原节点离开（Leaving）、被 down 或被移除后，实例才会在下一个最老的成员上启动。
 *   因此宕机节点需要手动 down 或配置 autoDownUnreachableAfter。
 * - 每个节点都可以通过 proxy 发送消息：本节点就是最老成员时直接投递给本地实例，
 *   否则通过远程发给最老成员；还不知道实例在哪里时先缓冲（默认最多 1000 条，超出丢弃最早的）。
 * - 实例迁移期间发出的消息可能丢失（至多一次），需要可靠性时由业务层确认重发。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}

class SingletonTick <: Message {
    public func messageType(): String {
        "SingletonTick"
    }
}

class ProxyEnvelope <: Message {
    let payload: Message

    init(payload: Message) {
        this.payload = payload
    }

    public func messageType(): String {
        "SingletonProxyEnvelope"
    }
}

class SingletonShared {
    let lock = Mutex()
    var runningHere = false
    var startCount: Int64 = 0
    var location: Option<String> = None
    var dropped: Int64 = 0
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
    var generation: Int64 = 0
    let buffer = ArrayList<Message>()
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
        stopInstance()
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: SingletonTick =>
                evaluate()
                flush()
            case p: ProxyEnvelope =>
                buffer.add(p.payload)
                if (buffer.size > bufferSize) {
                    buffer.remove(at: 0)
                    synchronized(shared.lock) {
                        shared.dropped++
                    }
                }
                flush()
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

    private func evaluate(): Unit {
        let target = targetLocation()
        let here = target == Some(cluster.selfAddress)
        if (here && instance.isNone()) {
            generation++
            let ref = system.actorOf(factory, "singleton-${singletonName}-${generation}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
            instance = Some(ref)
            remote.exposeActor("singleton-${singletonName}", ref)
            log.info("Singleton ${singletonName} 在 ${cluster.selfAddress} 启动")
            synchronized(shared.lock) {
                shared.runningHere = true
                shared.startCount++
            }
        } else if (!here && instance.isSome()) {
            log.info("Singleton ${singletonName} 从 ${cluster.selfAddress} 移交")
            stopInstance()
        }
        synchronized(shared.lock) {
            shared.location = target
        }
    }

    private func stopInstance(): Unit {
        if (let Some(ref) <- instance) {
            remote.unexposeActor("singleton-${singletonName}")
            system.stop(ref)
            instance = None
            synchronized(shared.lock) {
                shared.runningHere = false
            }
        }
    }

    private func flush(): Unit {
        if (buffer.isEmpty()) {
            return
        }
        let target = synchronized(shared.lock) { shared.location }
        match ((target, instance)) {
            case (Some(t), Some(ref)) where t == cluster.selfAddress =>
                for (m in buffer) {
                    ref.tell(m)
                }
                buffer.clear()
            case (Some(t), _) where t != cluster.selfAddress =>
                let remoteRef = remote.remoteActorOf(NetworkAddress.parse(t), "singleton-${singletonName}")
                for (m in buffer) {
                    try {
                        remoteRef.tell(m)
                    } catch (e: Exception) {
                        log.warn("发送给 Singleton ${singletonName}@${t} 失败: ${e}")
                    }
                }
                buffer.clear()
            case _ => () // 位置未知或本地实例尚未启动：继续缓冲
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

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, name: String, factory: () -> Actor) {
        this(cluster, system, remote, name, factory, 1000)
    }

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, name: String, factory: () -> Actor,
        bufferSize: Int64) {
        this.system = system
        let sh = shared
        this.manager = system.actorOf({ => SingletonManager(cluster, system, remote, name, factory, bufferSize, sh) },
            "singleton-manager-${name}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
    }

    /** 发送给集群中唯一的实例（消息需要能被远程序列化，例如 StringMessage 或注册了编解码器的消息） */
    public func tell(message: Message): Unit {
        manager.tell(ProxyEnvelope(message))
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

    /** 停止本节点的管理器（本地实例随之停止） */
    public func stop(): Unit {
        system.stop(manager)
    }
}
