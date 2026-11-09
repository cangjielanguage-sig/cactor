/*
 * CActor Cluster - 分布式发布订阅（对标 Akka DistributedPubSub 的 Publish / Subscribe）
 *
 * - 每个节点为每个有本地订阅者的主题建一个主题 Actor，负责把消息分发给本节点的订阅者，
 *   并监视订阅者：订阅者停止后自动退订。
 * - “哪些节点有某主题的订阅者”保存在 Replicator 的 ORSet（键为 "pubsub/<主题>"）中，在节点间自动同步；
 *   节点被移出集群时，其他节点把它从全部主题中删除。
 * - publish 只发给有订阅者的节点：本节点直接投递，其他节点经远程发给对方的主题 Actor。
 *   投递是至多一次；订阅生效需要等 Replicator 同步（通常一个 gossip 间隔）。
 * - 节点集合的增删由主题 Actor 串行发出（E12′，N12）：本地订阅者 0→1 时加入、→0 时删除，
 *   Replicator 按顺序处理，"最后一个订阅者离开"与紧随其后的新订阅不会颠倒（此前删除在另一个线程异步执行，
 *   可能晚于添加，把仍有订阅者的节点移出主题）。subscribe 不阻塞；需要确认本地生效时用 subscribeAsync。
 *
 * ClusterReplicatorBinding 让 Replicator 的对等节点自动跟随集群成员变化，pub/sub 依赖它，也可以单独使用。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated}
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.{Replicator}
import cactor_core.typed.{TypedPromise}

/** 让 Replicator 的对等节点跟随集群成员：成员 Up 时加入，被移除或 down 时删除 */
public class ClusterReplicatorBinding {
    public static func bind(cluster: Cluster, replicator: Replicator): Unit {
        let me = cluster.selfAddress
        // pruning（E64）：Leader 发起与执行，等全部未 Down 的成员都见过标记
        // 只在视图收敛时以 Leader 身份发起：未收敛时可能同时有两个节点自认为 Leader，各自发起会让数据被并入两次
        replicator.setMembership({ =>
            let st = cluster.state()
            st.converged && st.leader == Some(me)
        }, { =>
            let out = ArrayList<String>()
            for (m in cluster.members()) {
                if (m.status != MemberStatus.Down) {
                    out.add(m.uniqueAddress())
                }
            }
            out.toArray()
        })
        cluster.subscribe({ e =>
            match (e) {
                case ClusterEvent.MemberUp(m) where m.address != me => replicator.join(NetworkAddress.parse(m.address))
                case ClusterEvent.MemberJoined(m) where m.address != me => replicator.join(NetworkAddress.parse(m.address))
                case ClusterEvent.MemberRemoved(m) =>
                    replicator.nodeRemoved(m.uniqueAddress())
                    leaveUnlessReplaced(cluster, replicator, m)
                case ClusterEvent.MemberDowned(m) => leaveUnlessReplaced(cluster, replicator, m)
                case _ => ()
            }
        })
    }
}

/** 同址重启的新实例可能已经加入：地址上的现任成员是别的实例时继续向该地址推送 */
func leaveUnlessReplaced(cluster: Cluster, replicator: Replicator, m: Member): Unit {
    match (cluster.state().member(m.address)) {
        case Some(cur) where cur.uid != m.uid => ()
        case _ => replicator.leave(NetworkAddress.parse(m.address))
    }
}

class SubscribeCmd <: Message {
    let ref: ActorRef
    // subscribeAsync：本地订阅登记且节点集合的添加生效后完成
    let ack: Option<TypedPromise<Unit>>

    init(ref: ActorRef, ack: Option<TypedPromise<Unit>>) {
        this.ref = ref
        this.ack = ack
    }

    public func messageType(): String {
        "PubSubSubscribe"
    }
}

class UnsubscribeCmd <: Message {
    let ref: ActorRef

    init(ref: ActorRef) {
        this.ref = ref
    }

    public func messageType(): String {
        "PubSubUnsubscribe"
    }
}

/** 主题在本节点的订阅者集合（主题 Actor 写，外部读计数） */
class TopicState {
    let lock = Mutex()
    var count: Int64 = 0
    var delivered: Int64 = 0
}

class TopicActor <: Actor {
    let topic: String
    let state: TopicState
    let replicator: Replicator
    let selfNode: String
    let subscribers = HashMap<String, ActorRef>()

    init(topic: String, state: TopicState, replicator: Replicator, selfNode: String) {
        this.topic = topic
        this.state = state
        this.replicator = replicator
        this.selfNode = selfNode
    }

    public prop name: String {
        get() { "pubsub-topic" }
    }

    public prop description: String {
        get() { "pubsub topic ${topic}" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: SubscribeCmd =>
                let key = s.ref.path.toString()
                if (!subscribers.contains(key)) {
                    subscribers[key] = s.ref
                    context.watch(s.ref)
                }
                syncCount()
                if (let Some(ack) <- s.ack) {
                    // 屏障：本 Actor 之前发出的节点集合更新都生效后才确认
                    replicator.afterPendingUpdates({ => ack.trySuccess(()) |> discardCompleted })
                }
            case u: UnsubscribeCmd =>
                if (subscribers.remove(u.ref.path.toString()).isSome()) {
                    context.unwatch(u.ref)
                }
                syncCount()
            case t: Terminated =>
                subscribers.remove(t.actor.path.toString())
                syncCount()
            case m =>
                for ((_, ref) in subscribers) {
                    ref.tell(m)
                }
                synchronized(state.lock) {
                    state.delivered += subscribers.size
                }
        }
        MessageResult.Handled
    }

    /**
     * 本地订阅者数变化：0→1 把本节点加入主题的节点集合，→0 删除。
     * 只有本 Actor 发出这两种更新、Replicator 按顺序处理，所以最终状态总与最后一次变化一致
     */
    private func syncCount(): Unit {
        let before = synchronized(state.lock) {
            let b = state.count
            state.count = subscribers.size
            b
        }
        let key = "pubsub/${topic}"
        if (before == 0 && !subscribers.isEmpty()) {
            replicator.addToSetAsync(key, selfNode)
        } else if (before > 0 && subscribers.isEmpty()) {
            replicator.removeFromSetAsync(key, selfNode)
        }
    }
}

/**
 * 本节点的分布式发布订阅
 *
 * ```
 * let pubsub = DistributedPubSub(cluster, system, remote, replicator)
 * pubsub.subscribe("news", myActorRef)
 * pubsub.publish("news", StringMessage("hello"))   // 发给全集群的 news 订阅者
 * ```
 */
public class DistributedPubSub {
    private let cluster: Cluster
    private let system: CActorSystem
    private let remote: SimpleRemoteManager
    private let replicator: Replicator
    private let lock = Mutex()
    private let topics = HashMap<String, (ActorRef, TopicState)>()
    private let sent = AtomicInt64(0)
    private let log = Logging.logger("cactor.cluster.pubsub")

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, replicator: Replicator) {
        this.cluster = cluster
        this.system = system
        this.remote = remote
        this.replicator = replicator
        ClusterReplicatorBinding.bind(cluster, replicator)
        // 被移出集群的节点不会自己退订：由每个看到移除事件的节点代为删除（ORSet 的并发删除是安全的）
        let rep = replicator
        cluster.subscribe({ e =>
            let gone = match (e) {
                case ClusterEvent.MemberRemoved(m) => Some(m.address)
                case ClusterEvent.MemberDowned(m) => Some(m.address)
                case _ => None<String>
            }
            if (let Some(addr) <- gone) {
                spawn {
                    for (key in rep.keys()) {
                        if (key.startsWith("pubsub/")) {
                            for (n in rep.setElements(key)) {
                                if (n == addr) {
                                    rep.removeFromSet(key, addr)
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    private static func checkTopic(topic: String): Unit {
        if (topic.isEmpty()) {
            throw IllegalArgumentException("主题不能为空")
        }
        for (b in topic) {
            let ok = (b >= UInt8(UInt32(r'a')) && b <= UInt8(UInt32(r'z'))) || (b >= UInt8(UInt32(r'A')) && b <= UInt8(UInt32(r'Z'))) ||
                (b >= UInt8(UInt32(r'0')) && b <= UInt8(UInt32(r'9'))) || b == UInt8(UInt32(r'-')) || b == UInt8(UInt32(r'_')) ||
                b == UInt8(UInt32(r'.'))
            if (!ok) {
                throw IllegalArgumentException("主题只能包含字母、数字、'-'、'_'、'.'：${topic}")
            }
        }
    }

    private func topicActor(topic: String): ActorRef {
        synchronized(lock) {
            match (topics.get(topic)) {
                case Some((ref, _)) => ref
                case None =>
                    let st = TopicState()
                    let rep = replicator
                    let me = cluster.selfAddress
                    let ref = system.actorOf({ => TopicActor(topic, st, rep, me) },
                        "pubsub-${topic}-${me.replace(":", "_").replace(".", "_")}")
                    remote.exposeActor("pubsub-${topic}", ref)
                    topics[topic] = (ref, st)
                    ref
            }
        }
    }

    /**
     * 订阅主题；订阅者停止后自动退订。不阻塞（E12′：此前同步等待 Replicator，最长 10 s）：
     * 主题 Actor 登记订阅者并把本节点加入主题的节点集合，其他节点在 Replicator 同步后才会发来消息
     */
    public func subscribe(topic: String, subscriber: ActorRef): Unit {
        checkTopic(topic)
        topicActor(topic).tell(SubscribeCmd(subscriber, None<TypedPromise<Unit>>))
    }

    /**
     * 订阅并在本地生效后完成（对标 Akka 的 SubscribeAck）：订阅者已登记、本节点已在本地副本的节点集合里。
     * 其他节点仍要等 Replicator 同步（通常一个 gossip 间隔）
     */
    public func subscribeAsync(topic: String, subscriber: ActorRef): TypedPromise<Unit> {
        checkTopic(topic)
        let ack = TypedPromise<Unit>()
        topicActor(topic).tell(SubscribeCmd(subscriber, Some(ack)))
        ack
    }

    public func unsubscribe(topic: String, subscriber: ActorRef): Unit {
        checkTopic(topic)
        let ref = synchronized(lock) { topics.get(topic) }
        if (let Some((t, _)) <- ref) {
            t.tell(UnsubscribeCmd(subscriber))
        }
    }

    /** 发给全集群该主题的全部订阅者，返回投递到的节点数（消息需要能被远程序列化） */
    public func publish(topic: String, message: Message): Int64 {
        checkTopic(topic)
        var nodes: Int64 = 0
        for (addr in replicator.setElements("pubsub/${topic}")) {
            if (addr == cluster.selfAddress) {
                let local = synchronized(lock) { topics.get(topic) }
                if (let Some((ref, _)) <- local) {
                    ref.tell(message)
                    nodes++
                }
            } else {
                try {
                    remote.remoteActorOf(NetworkAddress.parse(addr), "pubsub-${topic}").tell(message)
                    nodes++
                } catch (e: Exception) {
                    log.warn("发布到 ${addr} 的主题 ${topic} 失败: ${e}")
                }
            }
        }
        sent.fetchAdd(nodes)
        nodes
    }

    /** 当前知道有订阅者的节点（按地址排序） */
    public func subscriberNodes(topic: String): Array<String> {
        replicator.setElements("pubsub/${topic}")
    }

    /** 本节点该主题的订阅者数 */
    public func localSubscriberCount(topic: String): Int64 {
        let t = synchronized(lock) { topics.get(topic) }
        match (t) {
            case Some((_, st)) => synchronized(st.lock) { st.count }
            case None => 0
        }
    }
}
