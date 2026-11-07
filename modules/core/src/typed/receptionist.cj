/*
 * CActor Core.Typed - 服务注册与发现（对标 Akka Typed Receptionist）
 *
 * - ServiceKey<T>：服务标识，类型参数保证 find 得到的引用协议正确
 * - register / deregister：登记 / 注销服务实例；实例停止后自动注销（通过一个监视者 Actor）
 * - find：查询当前实例列表
 * - subscribe：订阅变化，立即收到一次当前列表，之后每次变化都会收到新的 Listing；
 *   返回 ReceptionistSubscription，可 unsubscribe；以 Actor 订阅时订阅者停止后自动退订（T12）
 *
 * 本实现是单节点的；集群版本见计划 R6。
 */
package cactor_core.typed

import std.sync.*
import std.collection.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}

/** 服务标识 */
public class ServiceKey<T> {
    public let id: String

    public init(id: String) {
        this.id = id
    }

    public func toString(): String {
        "ServiceKey(${id})"
    }
}

/** 某个服务当前的全部实例 */
public class Listing<T> {
    public let key: ServiceKey<T>
    public let services: Array<TypedActorRef<T>>

    public init(key: ServiceKey<T>, services: Array<TypedActorRef<T>>) {
        this.key = key
        this.services = services
    }

    public func isEmpty(): Bool {
        services.isEmpty()
    }

    public func size(): Int64 {
        services.size
    }
}

class ServiceEntry {
    let keyId: String
    let path: String
    let classic: Option<ActorRef>
    let ref: Any

    init(keyId: String, path: String, classic: Option<ActorRef>, ref: Any) {
        this.keyId = keyId
        this.path = path
        this.classic = classic
        this.ref = ref
    }
}

/** 订阅句柄：传给 Receptionist.unsubscribe 取消订阅 */
public class ReceptionistSubscription {
    let id: Int64
    let keyId: String

    init(id: Int64, keyId: String) {
        this.id = id
        this.keyId = keyId
    }
}

class SubscriberEntry {
    let id: Int64
    // 以 Actor 订阅时为订阅者路径：它停止后自动退订；回调订阅为 None，只能显式 unsubscribe
    let ownerPath: Option<String>
    let fn: () -> Unit

    init(id: Int64, ownerPath: Option<String>, fn: () -> Unit) {
        this.id = id
        this.ownerPath = ownerPath
        this.fn = fn
    }
}

class ReceptionistWatch <: Message {
    let target: ActorRef

    init(target: ActorRef) {
        this.target = target
    }

    public func messageType(): String {
        "ReceptionistWatch"
    }
}

/** 监视已登记的实例，实例停止后注销 */
class ReceptionistWatcher <: Actor {
    private let owner: Receptionist

    init(owner: Receptionist) {
        this.owner = owner
    }

    public prop name: String {
        get() { "receptionist" }
    }

    public prop description: String {
        get() { "服务实例的死亡监视" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case w: ReceptionistWatch =>
                context.watch(w.target)
                MessageResult.Handled
            case t: Terminated =>
                owner.removeAll(t.actor.path.getPath())
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/**
 * 服务注册表（线程安全）
 */
public class Receptionist {
    private let lock = Mutex()
    private let entries = ArrayList<ServiceEntry>()
    // keyId -> 订阅者（通知函数在锁外调用）
    private let subscribers = HashMap<String, ArrayList<SubscriberEntry>>()
    private var nextSubscriptionId: Int64 = 0
    private var watcher: Option<ActorRef> = None<ActorRef>

    /**
     * @param spawnWatcher 创建监视者 Actor 的函数；传 None 时不会自动注销已停止的实例
     */
    public init(spawnWatcher: Option<(() -> Actor) -> ActorRef>) {
        if (let Some(spawnFn) <- spawnWatcher) {
            let me = this
            this.watcher = Some(spawnFn({ => ReceptionistWatcher(me) }))
        }
    }

    /** 登记服务实例（重复登记同一实例无效果） */
    public func register<T>(key: ServiceKey<T>, ref: TypedActorRef<T>): Unit {
        let path = ref.pathString()
        let added = synchronized(lock) {
            for (e in entries) {
                if (e.keyId == key.id && e.path == path) {
                    return
                }
            }
            entries.add(ServiceEntry(key.id, path, ref.toClassic(), ref))
            true
        }
        if (added) {
            if (let Some(w) <- watcher) {
                if (let Some(c) <- ref.toClassic()) {
                    w.tell(ReceptionistWatch(c))
                }
            }
            notify(key.id)
        }
    }

    /** 注销服务实例 */
    public func deregister<T>(key: ServiceKey<T>, ref: TypedActorRef<T>): Unit {
        let path = ref.pathString()
        let removed = synchronized(lock) {
            let before = entries.size
            entries.removeIf({ e => e.keyId == key.id && e.path == path })
            entries.size != before
        }
        if (removed) {
            notify(key.id)
        }
    }

    /** 当前全部实例 */
    public func find<T>(key: ServiceKey<T>): Listing<T> {
        let refs = ArrayList<TypedActorRef<T>>()
        synchronized(lock) {
            for (e in entries) {
                if (e.keyId == key.id) {
                    if (let Some(r) <- (e.ref as TypedActorRef<T>)) {
                        refs.add(r)
                    }
                }
            }
        }
        Listing<T>(key, refs.toArray())
    }

    /**
     * 订阅：立即收到当前列表，之后每次变化都会收到。
     * 订阅者停止后自动退订（此前订阅永不移除，停止的订阅者仍被反复通知、闭包一直被持有，T12）
     */
    public func subscribe<T>(key: ServiceKey<T>, subscriber: TypedActorRef<Listing<T>>): ReceptionistSubscription {
        let sub = addSubscriber<T>(key, Some(subscriber.pathString()), { listing: Listing<T> => subscriber.tell(listing) })
        if (let Some(w) <- watcher) {
            if (let Some(c) <- subscriber.toClassic()) {
                w.tell(ReceptionistWatch(c))
            }
        }
        sub
    }

    /** 以回调方式订阅（回调可能在任意线程执行）；不再需要时调用 unsubscribe */
    public func subscribe<T>(key: ServiceKey<T>, callback: (Listing<T>) -> Unit): ReceptionistSubscription {
        addSubscriber<T>(key, None<String>, callback)
    }

    /** 取消订阅；返回订阅是否仍然存在 */
    public func unsubscribe(subscription: ReceptionistSubscription): Bool {
        synchronized(lock) {
            match (subscribers.get(subscription.keyId)) {
                case Some(list) =>
                    let before = list.size
                    list.removeIf({ e => e.id == subscription.id })
                    list.size != before
                case None => false
            }
        }
    }

    /** 某个服务当前的订阅数 */
    public func subscriberCount<T>(key: ServiceKey<T>): Int64 {
        synchronized(lock) {
            match (subscribers.get(key.id)) {
                case Some(list) => list.size
                case None => 0
            }
        }
    }

    private func addSubscriber<T>(key: ServiceKey<T>, ownerPath: Option<String>, callback: (Listing<T>) -> Unit): ReceptionistSubscription {
        let me = this
        let fn = { => callback(me.find<T>(key)) }
        let sub = synchronized(lock) {
            nextSubscriptionId++
            let entry = SubscriberEntry(nextSubscriptionId, ownerPath, fn)
            match (subscribers.get(key.id)) {
                case Some(list) => list.add(entry)
                case None =>
                    let list = ArrayList<SubscriberEntry>()
                    list.add(entry)
                    subscribers.add(key.id, list)
            }
            ReceptionistSubscription(nextSubscriptionId, key.id)
        }
        fn()
        sub
    }

    /** 已登记的实例总数 */
    public func size(): Int64 {
        synchronized(lock) {
            entries.size
        }
    }

    /** 实例停止：注销其全部服务，并移除它作为订阅者的全部订阅 */
    func removeAll(path: String): Unit {
        let keys = synchronized(lock) {
            let ks = HashSet<String>()
            for (e in entries) {
                if (e.path == path) {
                    ks.add(e.keyId)
                }
            }
            entries.removeIf({ e => e.path == path })
            for ((_, list) in subscribers) {
                list.removeIf({ e => e.ownerPath == Some(path) })
            }
            ks.toArray()
        }
        for (k in keys) {
            notify(k)
        }
    }

    private func notify(keyId: String): Unit {
        let fns = synchronized(lock) {
            match (subscribers.get(keyId)) {
                case Some(list) => Array<() -> Unit>(list.size, { i => list[i].fn })
                case None => Array<() -> Unit>()
            }
        }
        for (f in fns) {
            try {
                f()
            } catch (_: Exception) {
                // 订阅者异常不影响其他订阅者
            }
        }
    }
}
