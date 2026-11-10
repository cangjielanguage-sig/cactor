/*
 * CActor Cluster - 集群版 Receptionist（对标 Akka Cluster Receptionist，计划 E63 的分布式部分）
 *
 * 服务发现：任意节点登记的 Actor，在全集群都能按服务名找到。
 * - 登记表是 Replicator 中的 ORSet（键 "receptionist/<服务名>"），元素为 "地址#uid|暴露名"；
 *   登记时把 Actor 以暴露名交给远程层，其他节点 find 得到指向它的 RemoteActorRef，本节点得到本地引用。
 * - 元素带实例标识（uid）：同址重启的新进程不会被旧进程的登记冒充。
 * - 自动注销：登记的 Actor 停止后由监视者 Actor 删除其登记（同 Akka：Terminated 触发 Deregister）；
 *   节点被 down 或移除后，看到事件的每个节点删除它的全部登记（ORSet 的并发删除是安全的）。
 * - find 默认只返回可达成员上的实例（对标 Listing.serviceInstances），findAll 包含不可达成员上的实例
 *   （对标 allServiceInstances）。
 * - 登记与注销只写本地副本，经 Replicator 推送与反熵传播（通常一个 gossip 间隔内全集群可见）；
 *   subscribe 在本地副本变化时回调（回调在 Replicator Actor 线程执行，应当很快）。
 * 发往远程实例的消息需要能被序列化（内置消息、注册了编解码器的消息或 @Message 宏生成的消息）。
 * 类型化入口（E63）：registerTyped / findTyped 用 ServiceKey<T> 保证"找到的引用协议正确"，
 * 返回的 TypedActorRef<T> 在本节点是本地引用、在其他节点是远程类型化引用。
 */
package cactor_cluster.membership

import std.sync.*
import std.collection.*
import std.sort.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated}
import cactor_core.message.{Message, MessageResult}
import cactor_core.typed.{ServiceKey, TypedActorRef, ClassicTypedRef}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.{Replicator}

class ReceptionistWatch <: Message {
    let ref: ActorRef

    init(ref: ActorRef) {
        this.ref = ref
    }

    public func messageType(): String {
        "ClusterReceptionistWatch"
    }
}

/** 监视登记的本地实例：停止后回调（在本 Actor 线程，发出的 Replicator 删除按顺序排在登记之后） */
class ReceptionistWatcher <: Actor {
    let onStopped: (ActorRef) -> Unit

    init(onStopped: (ActorRef) -> Unit) {
        this.onStopped = onStopped
    }

    public prop name: String { get() { "cluster-receptionist-watcher" } }
    public prop description: String { get() { "cluster receptionist watcher" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case w: ReceptionistWatch => context.watch(w.ref)
            case t: Terminated => onStopped(t.actor)
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

class LocalService {
    let key: String
    let name: String
    let element: String
    let ref: ActorRef

    init(key: String, name: String, element: String, ref: ActorRef) {
        this.key = key
        this.name = name
        this.element = element
        this.ref = ref
    }
}

/** 构造期间还不能引用 this：监视者的回调经它转到 Receptionist（init 最后一步填入） */
class ReceptionistHandle {
    var target: Option<ClusterReceptionist> = None
}

func receptionistKey(service: String): String {
    "receptionist/${service}"
}

/** "地址#uid|暴露名" -> (地址, uid 文本, 暴露名) */
func parseServiceElement(e: String): Option<(String, String, String)> {
    let bar = match (e.indexOf("|")) {
        case Some(i) => i
        case None => return None
    }
    let node = e[0..bar]
    let name = e[(bar + 1)..]
    match (node.lastIndexOf("#")) {
        case Some(h) => Some((node[0..h], node[(h + 1)..], name))
        case None => None
    }
}

/**
 * 本节点的集群 Receptionist。每个节点各建一个（共用本节点的 Replicator）。
 *
 * ```
 * let receptionist = ClusterReceptionist(cluster, system, remote, replicator)
 * receptionist.register("translator", translatorRef)        // 本节点的实例
 * for (t in receptionist.find("translator")) { t.tell(Translate("hello"), self) }   // 全集群的实例
 * receptionist.subscribe("translator", { refs => router.update(refs) })   // 实例变化时更新路由
 * ```
 */
public class ClusterReceptionist {
    private let cluster: Cluster
    private let remote: SimpleRemoteManager
    private let replicator: Replicator
    private let selfNode: String
    private let lock = Mutex()
    // 暴露名 -> 本节点登记的实例
    private let local = HashMap<String, LocalService>()
    private let seq = AtomicInt64(0)
    private let watcher: ActorRef
    private let log = Logging.logger("cactor.cluster.receptionist")

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, replicator: Replicator) {
        this.cluster = cluster
        this.remote = remote
        this.replicator = replicator
        this.selfNode = cluster.selfUniqueAddress()
        ClusterReplicatorBinding.bind(cluster, replicator)
        let handle = ReceptionistHandle()
        this.watcher = system.actorOf({ => ReceptionistWatcher({ ref =>
            if (let Some(r) <- handle.target) {
                r.onStopped(ref)
            }
        }) }, "cluster-receptionist-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
        // 被 down / 移除的节点不会自己注销：看到事件的每个节点代为删除它的全部登记
        let rep = replicator
        cluster.subscribe({ e =>
            let gone: Option<Member> = match (e) {
                case ClusterEvent.MemberRemoved(m) => Some(m)
                case ClusterEvent.MemberDowned(m) => Some(m)
                case _ => None<Member>
            }
            if (let Some(m) <- gone) {
                let prefix = "${m.uniqueAddress()}|"
                for (key in rep.keys()) {
                    if (key.startsWith("receptionist/")) {
                        for (el in rep.setElements(key)) {
                            if (el.startsWith(prefix)) {
                                rep.removeFromSetAsync(key, el)
                            }
                        }
                    }
                }
            }
        })
        handle.target = Some(this)
    }

    private static func checkService(service: String): Unit {
        if (service.isEmpty() || service.contains("|") || service.contains("/")) {
            throw IllegalArgumentException("服务名不能为空，也不能包含 '|' 或 '/'：${service}")
        }
    }

    /**
     * 把本地 Actor 登记为服务实例。同一 Actor 对同一服务重复登记只算一次；Actor 停止后自动注销
     */
    public func register(service: String, ref: ActorRef): Unit {
        checkService(service)
        let path = ref.path.toString()
        let maybe: Option<LocalService> = synchronized(lock) {
            var exists = false
            for ((_, s) in local) {
                if (s.key == service && s.ref.path.toString() == path) {
                    exists = true
                }
            }
            if (exists) {
                None<LocalService>
            } else {
                let name = "rcpt-${service}-${seq.fetchAdd(1) + 1}"
                let svc = LocalService(service, name, "${selfNode}|${name}", ref)
                local[name] = svc
                Some(svc)
            }
        }
        let created = match (maybe) {
            case Some(c) => c
            case None => return
        }
        remote.exposeActor(created.name, ref)
        replicator.addToSetAsync(receptionistKey(service), created.element)
        watcher.tell(ReceptionistWatch(ref))
    }

    /** 注销本地 Actor 在该服务下的登记 */
    public func deregister(service: String, ref: ActorRef): Unit {
        let path = ref.path.toString()
        let removed = synchronized(lock) {
            let out = ArrayList<LocalService>()
            for ((_, s) in local) {
                if (s.key == service && s.ref.path.toString() == path) {
                    out.add(s)
                }
            }
            for (s in out) {
                local.remove(s.name)
            }
            out
        }
        release(removed)
    }

    func onStopped(ref: ActorRef): Unit {
        let path = ref.path.toString()
        let removed = synchronized(lock) {
            let out = ArrayList<LocalService>()
            for ((_, s) in local) {
                if (s.ref.path.toString() == path) {
                    out.add(s)
                }
            }
            for (s in out) {
                local.remove(s.name)
            }
            out
        }
        release(removed)
    }

    private func release(services: ArrayList<LocalService>): Unit {
        for (s in services) {
            replicator.removeFromSetAsync(receptionistKey(s.key), s.element)
            remote.unexposeActor(s.name)
        }
    }

    /**
     * 登记类型化服务实例（E63）：必须是由 Actor 支撑的引用（ClassicTypedRef），
     * 一次性回复引用（PromiseRef 等）没有经典路径，无法跨节点寻址。
     */
    public func registerTyped<T>(key: ServiceKey<T>, ref: TypedActorRef<T>): Unit {
        match (ref.toClassic()) {
            case Some(classic) => register(key.id, classic)
            case None => throw IllegalArgumentException(
                "${ref.pathString()} 不是由 Actor 支撑的类型化引用，不能跨节点登记（只能登记 TypedActorRef 的经典实现）")
        }
    }

    /** 注销类型化服务实例 */
    public func deregisterTyped<T>(key: ServiceKey<T>, ref: TypedActorRef<T>): Unit {
        match (ref.toClassic()) {
            case Some(classic) => deregister(key.id, classic)
            case None => ()
        }
    }

    /**
     * 按 ServiceKey 在全集群查找类型化实例：本节点为本地引用，其他节点为远程类型化引用，
     * 直接 `tell` 即可（载荷须实现 Message，见 core 的 TypedPayloadBoxCodec）。
     */
    public func findTyped<T>(key: ServiceKey<T>): Array<TypedActorRef<T>> {
        let out = ArrayList<TypedActorRef<T>>()
        for (r in find(key.id)) {
            out.add(ClassicTypedRef<T>(r))
        }
        out.toArray()
    }

    /** 含不可达成员实例的类型化查找（对标 Listing.allServiceInstances） */
    public func findAllTyped<T>(key: ServiceKey<T>): Array<TypedActorRef<T>> {
        let out = ArrayList<TypedActorRef<T>>()
        for (r in findAll(key.id)) {
            out.add(ClassicTypedRef<T>(r))
        }
        out.toArray()
    }

    /** 全集群可达成员上的实例（本节点的为本地引用，其他节点的为 RemoteActorRef），按登记元素排序 */
    public func find(service: String): Array<ActorRef> {
        resolve(service, false)
    }

    /** 包括不可达成员上的实例（对标 Akka Listing.allServiceInstances） */
    public func findAll(service: String): Array<ActorRef> {
        resolve(service, true)
    }

    private func resolve(service: String, includeUnreachable: Bool): Array<ActorRef> {
        let snap = cluster.state()
        // 只认当前成员（且是同一个实例）上的登记：已 down / 移除、或同址换了新实例的旧登记不返回
        let live = HashMap<String, Member>()
        for (m in snap.members) {
            if (m.status != MemberStatus.Down) {
                live[m.address] = m
            }
        }
        let unreachable = HashSet<String>()
        for (u in snap.unreachable) {
            unreachable.add(u)
        }
        let out = ArrayList<ActorRef>()
        for (el in replicator.setElements(receptionistKey(service))) {
            let (addr, uidText, name) = match (parseServiceElement(el)) {
                case Some(t) => t
                case None => continue
            }
            if ("${addr}#${uidText}" == selfNode) {
                if (let Some(s) <- synchronized(lock) { local.get(name) }) {
                    out.add(s.ref)
                }
                continue
            }
            match (live.get(addr)) {
                case Some(m) where m.uid.toString() == uidText =>
                    if (includeUnreachable || !unreachable.contains(addr)) {
                        out.add(remote.remoteActorOf(NetworkAddress.parse(addr), name))
                    }
                case _ => ()
            }
        }
        out.toArray()
    }

    /** 服务的实例变化时回调当前的 find 结果（本地副本变化即回调，在 Replicator Actor 线程执行） */
    public func subscribe(service: String, callback: (Array<ActorRef>) -> Unit): Unit {
        checkService(service)
        let me = this
        replicator.subscribe(receptionistKey(service), { _ =>
            try {
                callback(me.find(service))
            } catch (e: Exception) {
                me.log.warn("Receptionist 订阅回调异常（${service}）：${e}")
            }
        })
    }

    /** 本节点登记的实例数（全部服务合计） */
    public func localRegistrationCount(): Int64 {
        synchronized(lock) {
            local.size
        }
    }
}
