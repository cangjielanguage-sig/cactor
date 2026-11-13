/*
 * CActor Foundation.Coordination - 租约（Lease）与 fencing token（DA4，对标 Akka Coordination Lease）
 *
 * 为什么需要：成员协议 / SBR / 单例 / 分片的单活保证都建立在"各节点对成员视图的判断"之上。
 * 非对称分区、长时间 GC 停顿、旧宿主恢复时，两个宿主可能同时认为自己拥有同一个单例或分片。
 * 租约把"谁是所有者"交给一个与集群网络无关的外部协调点（共享目录、数据库、k8s 等）判定：
 *   - acquire 成功才允许启动受保护的实例；持有期间由心跳续约；续约失败或到期即回调 onLost，调用方必须停止实例。
 *   - 每次成功获取都分配一个严格递增的 fencing token。租约只能保证"持有者认为自己持有"的时段互不重叠，
 *     无法阻止已经发出、却在停顿后才到达存储的旧写入；存储端按 token 拒绝过期写者（见 cactor_persistence 的
 *     FencingTokenStore / FencedEventStore）才闭合这一缺口（Kleppmann, "How to do distributed locking"）。
 *
 * 放在 foundation 层的原因：接口与测试实现只依赖标准库；持久化（L2，需要 token 做存储侧 fencing、并提供文件租约）
 * 与集群（L5，单例 / 分片在启动前获取租约）都能直接使用，而两者之间没有依赖关系，不必为此引入新的模块依赖。
 *
 * 时间假设（AbstractLease 的所有实现共享）：
 *   - 持有者用单调时钟计算本地有效期：validUntil = 获取 / 续约开始时刻 + ttl - clockSkewMargin。
 *     后端判断过期用的是自己的时钟（文件租约里是各宿主的墙钟）；只要宿主间时钟偏差 < clockSkewMargin，
 *     后端认为过期的时刻一定晚于持有者自认为失效的时刻，两段"自认为持有"的时间不会重叠。
 *   - 续约必须在原有效期内完成才算数：停顿跨过有效期的续约即使写成功也按失去处理（期间可能已有人接手）。
 *   - 超出上述假设（时钟跳变、偏差过大、停顿发生在检查与写入之间）由 fencing token 兜底，而不是租约本身。
 */
package cactor_foundation.coordination

import std.sync.*
import std.time.*
import std.collection.*
import cactor_foundation.detector.{monotonicMillis}

/** 租约操作失败（后端不可达、记录损坏等）或失去租约的原因 */
public class LeaseException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 租约设置（不可变，withXxx 返回新值）。
 *   - leaseName：受保护资源的名字（单例名、分片名）；同名租约在后端互斥，fencing token 也按它计数。
 *   - ownerName：持有者标识，必须在所有竞争者之间唯一（推荐"地址#启动标识"）；同名持有者会被视为同一方。
 *   - ttl：后端在最后一次续约后多久判定过期（Akka heartbeat-timeout）。
 *   - heartbeatInterval：续约间隔，必须小于 ttl - clockSkewMargin，留出至少一次重试。
 *   - clockSkewMargin：容忍的宿主间时钟偏差；持有者提前这么久认为自己失效。
 */
public struct LeaseSettings {
    public let leaseName: String
    public let ownerName: String
    public let ttl: Duration
    public let heartbeatInterval: Duration
    public let clockSkewMargin: Duration

    /** 默认 ttl 10 s、心跳 1 s、时钟偏差余量 1 s */
    public init(leaseName: String, ownerName: String) {
        this(leaseName, ownerName, Duration.second * 10, Duration.second, Duration.second)
    }

    public init(leaseName: String, ownerName: String, ttl: Duration, heartbeatInterval: Duration,
        clockSkewMargin: Duration) {
        if (leaseName.isEmpty() || ownerName.isEmpty()) {
            throw IllegalArgumentException("租约名与持有者名不能为空")
        }
        if (clockSkewMargin < Duration.Zero || ttl <= clockSkewMargin) {
            throw IllegalArgumentException("租约 ttl（${ttl}）必须大于时钟偏差余量（${clockSkewMargin}）且余量不能为负")
        }
        if (heartbeatInterval <= Duration.Zero || heartbeatInterval >= ttl - clockSkewMargin) {
            throw IllegalArgumentException("心跳间隔（${heartbeatInterval}）必须为正且小于 ttl - 时钟偏差余量（${ttl - clockSkewMargin}）")
        }
        this.leaseName = leaseName
        this.ownerName = ownerName
        this.ttl = ttl
        this.heartbeatInterval = heartbeatInterval
        this.clockSkewMargin = clockSkewMargin
    }

    public func withLeaseName(name: String): LeaseSettings {
        LeaseSettings(name, ownerName, ttl, heartbeatInterval, clockSkewMargin)
    }

    public func withOwnerName(owner: String): LeaseSettings {
        LeaseSettings(leaseName, owner, ttl, heartbeatInterval, clockSkewMargin)
    }

    public func withTtl(d: Duration): LeaseSettings {
        LeaseSettings(leaseName, ownerName, d, heartbeatInterval, clockSkewMargin)
    }

    public func withHeartbeatInterval(d: Duration): LeaseSettings {
        LeaseSettings(leaseName, ownerName, ttl, d, clockSkewMargin)
    }

    public func withClockSkewMargin(d: Duration): LeaseSettings {
        LeaseSettings(leaseName, ownerName, ttl, heartbeatInterval, d)
    }

    /** 持有者本地有效期长度（毫秒） */
    public func validityMillis(): Int64 {
        (ttl - clockSkewMargin).toMilliseconds()
    }
}

/**
 * 租约（对标 akka.coordination.lease.scaladsl.Lease）。
 *
 * - acquire 是阻塞调用（可能访问外部存储），Actor 内应在独立线程执行、结果以消息回送（ClusterSingleton 即如此）。
 * - onLost 在成功获取之后失去租约时最多调用一次（续约被拒、续约超出有效期、后端撤销），在租约内部线程上执行，
 *   回调里只应发消息，不要阻塞。主动 release 不触发 onLost。
 * - 同一个 Lease 对象不应并发调用 acquire / release。
 */
public interface Lease {
    prop settings: LeaseSettings

    /** 获取租约（已持有时只更新回调并返回 true）；后端暂时不可用时抛 LeaseException */
    func acquire(onLost: (Option<Exception>) -> Unit): Bool

    /** 释放租约；本来就不持有时返回 false */
    func release(): Bool

    /** 本地视角下当前是否仍持有（不访问后端，可在热路径上调用） */
    func checkLease(): Bool

    /** 当前持有时的 fencing token；每次成功获取严格递增，续约不变 */
    func fencingToken(): Option<Int64>
}

/**
 * 心跳续约、本地有效期与 lost 回调的公共实现；子类只需提供后端上的三个原子操作。
 * clock 是持有者本地的单调时钟（毫秒），测试可注入手动时钟。
 */
public abstract class AbstractLease <: Lease {
    private let leaseSettings: LeaseSettings
    private let clock: () -> Int64
    private let lock = Mutex()
    private var token: Option<Int64> = None
    private var validUntil: Int64 = 0
    private var lostCallback: Option<(Option<Exception>) -> Unit> = None
    // 每次获取 / 释放 / 失去都加一：旧的心跳线程据此退出，旧 token 的迟到信号据此忽略
    private var epoch: Int64 = 0
    private let lostTotal = AtomicInt64(0)

    protected init(settings: LeaseSettings, clock: () -> Int64) {
        this.leaseSettings = settings
        this.clock = clock
    }

    protected init(settings: LeaseSettings) {
        this(settings, monotonicMillis)
    }

    public prop settings: LeaseSettings {
        get() { leaseSettings }
    }

    /** 后端上的原子获取：成功返回新的 token（严格大于此前发出的任何 token），被他人持有返回 None，后端故障抛异常 */
    protected func tryAcquireOnce(): Option<Int64>

    /** 续约：仍是 token 的持有者并已延长到期时间返回 true；确定已失去返回 false；无法判断时抛异常 */
    protected func renewOnce(token: Int64): Bool

    /** 释放：仍是持有者并已释放返回 true */
    protected func releaseOnce(token: Int64): Bool

    public func acquire(onLost: (Option<Exception>) -> Unit): Bool {
        synchronized(lock) {
            if (holdsLocked()) {
                lostCallback = Some(onLost)
                return true
            }
        }
        // 有效期从操作开始算：后端写入的到期时间不早于这一刻 + ttl，本地据此保守地提前失效
        let start = clock()
        match (tryAcquireOnce()) {
            case Some(t) =>
                if (clock() >= start + leaseSettings.validityMillis()) {
                    // 获取本身耗时超过了有效期：拿到时已不可信，交还后报告失败
                    try {
                        releaseOnce(t)
                    } catch (_: Exception) {}
                    return false
                }
                let myEpoch = synchronized(lock) {
                    epoch++
                    token = Some(t)
                    validUntil = start + leaseSettings.validityMillis()
                    lostCallback = Some(onLost)
                    epoch
                }
                spawn {
                    heartbeatLoop(myEpoch, t)
                }
                true
            case None => false
        }
    }

    public func release(): Bool {
        let held = synchronized(lock) {
            let t = token
            token = None
            lostCallback = None
            epoch++
            t
        }
        match (held) {
            case Some(t) =>
                try {
                    releaseOnce(t)
                } catch (_: Exception) {
                    // 后端不可达：记录会在 ttl 后自然过期；本地已不再持有
                    false
                }
            case None => false
        }
    }

    public func checkLease(): Bool {
        synchronized(lock) {
            holdsLocked()
        }
    }

    public func fencingToken(): Option<Int64> {
        synchronized(lock) {
            if (holdsLocked()) { token } else { None<Int64> }
        }
    }

    /** 本对象失去租约的累计次数（不含主动释放） */
    public func lostCount(): Int64 {
        lostTotal.load()
    }

    private func holdsLocked(): Bool {
        token.isSome() && clock() < validUntil
    }

    /** 后端主动通知 token 已失效（例如测试存储撤销、外部协调服务推送）；token 不是当前持有的则忽略 */
    protected func signalLost(t: Int64, reason: Exception): Unit {
        let cur = synchronized(lock) { (token, epoch) }
        if (cur[0] == Some(t)) {
            lose(cur[1], reason)
        }
    }

    private func lose(myEpoch: Int64, reason: Exception): Unit {
        let cb = synchronized(lock) {
            if (epoch != myEpoch) {
                return
            }
            epoch++
            token = None
            let c = lostCallback
            lostCallback = None
            c
        }
        lostTotal.fetchAdd(1)
        if (let Some(f) <- cb) {
            try {
                f(Some(reason))
            } catch (_: Exception) {
                // 回调异常不能打断租约状态机
            }
        }
    }

    private func heartbeatLoop(myEpoch: Int64, t: Int64): Unit {
        while (true) {
            sleep(leaseSettings.heartbeatInterval)
            let (alive, deadline) = synchronized(lock) { (epoch == myEpoch, validUntil) }
            if (!alive) {
                return
            }
            let start = clock()
            if (start >= deadline) {
                // 线程被饿死或进程停顿跨过了有效期：期间可能已有人接手，不能再续
                lose(myEpoch, LeaseException("租约 ${leaseSettings.leaseName} 在续约前已过本地有效期（停顿或线程饥饿）"))
                return
            }
            var renewed = false
            var failure: Option<Exception> = None
            try {
                renewed = renewOnce(t)
            } catch (e: Exception) {
                failure = Some(e)
            }
            let done = clock()
            if (renewed && done < deadline) {
                synchronized(lock) {
                    if (epoch == myEpoch) {
                        validUntil = start + leaseSettings.validityMillis()
                    }
                }
                continue
            }
            if (renewed) {
                // 续约在原有效期之后才完成：后端可能在此之前已判定过期并被他人获取
                lose(myEpoch, LeaseException("租约 ${leaseSettings.leaseName} 的续约在原有效期之后才完成"))
                return
            }
            match (failure) {
                case None =>
                    lose(myEpoch, LeaseException("租约 ${leaseSettings.leaseName} 已被他人持有或记录已不存在"))
                    return
                case Some(e) where done >= deadline =>
                    lose(myEpoch, e)
                    return
                case Some(_) => () // 暂时性故障：有效期内下一次心跳再试
            }
        }
    }
}

// ---------------- 测试 / 单进程用的内存后端 ----------------

struct InMemoryLeaseRecord {
    let owner: String
    let token: Int64
    let expiresAt: Int64
    let holder: InMemoryLease

    init(owner: String, token: Int64, expiresAt: Int64, holder: InMemoryLease) {
        this.owner = owner
        this.token = token
        this.expiresAt = expiresAt
        this.holder = holder
    }
}

/**
 * 进程内的"外部协调服务"（对标 Akka TestLease 的控制面）：多个 InMemoryLease 共享同一个存储即互斥。
 * 与集群网络无关——测试里用黑洞隔开节点后，租约存储仍对双方可见，正是非对称分区下租约要发挥作用的场景。
 * clock 可注入手动时钟以验证过期；setAvailable(false) 模拟存储不可达；revoke 模拟服务端撤销并推送失效。
 */
public class InMemoryLeaseStore {
    private let lock = Mutex()
    private let storeClock: () -> Int64
    private let records = HashMap<String, InMemoryLeaseRecord>()
    private let issued = HashMap<String, Int64>()
    private let denied = HashMap<String, Int64>()
    private var available = true
    // 与存储断连的持有者（模拟某个宿主到协调服务的单向故障）
    private let isolated = HashSet<String>()

    public init() {
        this(monotonicMillis)
    }

    public init(clock: () -> Int64) {
        this.storeClock = clock
    }

    public prop clock: () -> Int64 {
        get() { storeClock }
    }

    func tryAcquire(name: String, owner: String, ttlMs: Int64, holder: InMemoryLease): Option<Int64> {
        synchronized(lock) {
            if (!available || isolated.contains(owner)) {
                throw LeaseException("租约存储不可用")
            }
            let now = storeClock()
            if (let Some(r) <- records.get(name)) {
                if (r.expiresAt > now && r.owner != owner) {
                    denied[name] = (denied.get(name) ?? 0) + 1
                    return None
                }
            }
            // 同一持有者再次获取（例如进程重启）也发新 token：旧实例在途的写入因此会被存储拒绝
            let t = (issued.get(name) ?? 0) + 1
            issued[name] = t
            records[name] = InMemoryLeaseRecord(owner, t, now + ttlMs, holder)
            Some(t)
        }
    }

    func renew(name: String, owner: String, token: Int64, ttlMs: Int64): Bool {
        synchronized(lock) {
            if (!available || isolated.contains(owner)) {
                throw LeaseException("租约存储不可用")
            }
            let now = storeClock()
            match (records.get(name)) {
                case Some(r) where r.owner == owner && r.token == token && r.expiresAt > now =>
                    records[name] = InMemoryLeaseRecord(owner, token, now + ttlMs, r.holder)
                    true
                case _ => false
            }
        }
    }

    func release(name: String, owner: String, token: Int64): Bool {
        synchronized(lock) {
            if (!available || isolated.contains(owner)) {
                throw LeaseException("租约存储不可用")
            }
            match (records.get(name)) {
                case Some(r) where r.owner == owner && r.token == token =>
                    records.remove(name)
                    true
                case _ => false
            }
        }
    }

    /** 服务端撤销当前持有者（立即通知它失去租约），之后任何人都可获取 */
    public func revoke(name: String): Unit {
        let r = synchronized(lock) { records.remove(name) }
        if (let Some(rec) <- r) {
            rec.holder.notifyRevoked(rec.token)
        }
    }

    /** false 时所有操作抛 LeaseException（模拟存储断连） */
    public func setAvailable(v: Bool): Unit {
        synchronized(lock) {
            available = v
        }
    }

    /** reachable = false 时该持有者的所有操作抛 LeaseException（它与存储之间断连，其他持有者不受影响） */
    public func setOwnerReachable(owner: String, reachable: Bool): Unit {
        synchronized(lock) {
            if (reachable) {
                isolated.remove(owner)
            } else {
                isolated.add(owner)
            }
        }
    }

    /** 当前未过期的持有者 */
    public func owner(name: String): Option<String> {
        synchronized(lock) {
            match (records.get(name)) {
                case Some(r) where r.expiresAt > storeClock() => Some(r.owner)
                case _ => None<String>
            }
        }
    }

    /** 当前记录的到期时刻（存储时钟；没有记录为 0） */
    public func expiresAt(name: String): Int64 {
        synchronized(lock) {
            match (records.get(name)) {
                case Some(r) => r.expiresAt
                case None => 0
            }
        }
    }

    /** 已发出的最大 token（从未获取过为 0） */
    public func lastToken(name: String): Int64 {
        synchronized(lock) {
            issued.get(name) ?? 0
        }
    }

    /** 因他人持有而被拒绝的获取次数 */
    public func deniedCount(name: String): Int64 {
        synchronized(lock) {
            denied.get(name) ?? 0
        }
    }
}

/** 基于 InMemoryLeaseStore 的租约；本地有效期使用存储的时钟（同一进程，没有时钟偏差） */
public class InMemoryLease <: AbstractLease {
    private let store: InMemoryLeaseStore

    public init(store: InMemoryLeaseStore, settings: LeaseSettings) {
        super(settings, store.clock)
        this.store = store
    }

    protected func tryAcquireOnce(): Option<Int64> {
        store.tryAcquire(settings.leaseName, settings.ownerName, settings.ttl.toMilliseconds(), this)
    }

    protected func renewOnce(token: Int64): Bool {
        store.renew(settings.leaseName, settings.ownerName, token, settings.ttl.toMilliseconds())
    }

    protected func releaseOnce(token: Int64): Bool {
        store.release(settings.leaseName, settings.ownerName, token)
    }

    func notifyRevoked(token: Int64): Unit {
        signalLost(token, LeaseException("租约 ${settings.leaseName} 被存储撤销"))
    }
}
