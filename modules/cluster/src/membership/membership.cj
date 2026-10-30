/*
 * CActor Cluster - 集群成员协议（对标 Akka Cluster 的成员管理）
 *
 * 成员状态只会向前推进：Joining → Up → Leaving → Exiting → Removed，任何状态都可以被标记为 Down，Down → Removed。
 * 每个节点保存一份 Gossip（全部成员及其状态 + 已看到这份状态的节点集合 seen），定期随机发给其他成员：
 * - 合并规则：同一成员取状态更靠后的一方（状态单调推进，因此合并满足交换律、结合律、幂等律，gossip 必然收敛）；
 *   upNumber（成为 Up 的先后顺序，越小越老）取非零最小值。
 * - 收敛：所有可达、且未 Down / Removed 的成员都在 seen 中，说明大家看到的是同一份状态。
 * - Leader：可达的 Up / Leaving 成员中地址最小者（还没有 Up 成员时在 Joining 中选）。
 *   只有 Leader 推进状态，并且只在收敛时推进：Joining → Up（分配 upNumber）、Leaving → Exiting、
 *   Exiting → Removed、Down → Removed。因此所有节点看到的成员变化顺序一致。
 * - 可达性：节点之间定期心跳，用 Phi Accrual 故障检测器判定；不可达成员会阻止收敛，
 *   需要手动 down，或配置 downing 策略（见 sbr.cj：KeepMajority / KeepOldest / StaticQuorum，或不安全的 AutoDownAfter）。
 *
 * 传输复用 cactor_remote：每个节点把成员守护 Actor 以名称 "cluster" 暴露出去。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.sort.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.detector.{PhiAccrualFailureDetector, monotonicMillis}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.{FieldWriter, FieldReader}

// ======================= 成员 =======================

public enum MemberStatus <: ToString & Equatable<MemberStatus> {
    | Joining
    | Up
    | Leaving
    | Exiting
    | Down
    | Removed

    /** 状态的先后：合并时取较大者 */
    public func rank(): Int64 {
        match (this) {
            case Joining => 0
            case Up => 1
            case Leaving => 2
            case Exiting => 3
            case Down => 4
            case Removed => 5
        }
    }

    public func toString(): String {
        match (this) {
            case Joining => "Joining"
            case Up => "Up"
            case Leaving => "Leaving"
            case Exiting => "Exiting"
            case Down => "Down"
            case Removed => "Removed"
        }
    }

    public operator func ==(other: MemberStatus): Bool {
        rank() == other.rank()
    }

    public operator func !=(other: MemberStatus): Bool {
        rank() != other.rank()
    }

    public static func parse(s: String): MemberStatus {
        match (s) {
            case "Joining" => Joining
            case "Up" => Up
            case "Leaving" => Leaving
            case "Exiting" => Exiting
            case "Down" => Down
            case "Removed" => Removed
            case _ => throw IllegalArgumentException("未知的成员状态: ${s}")
        }
    }
}

/** 集群成员（不可变） */
public class Member <: ToString {
    /** 远程地址 "host:port" */
    public let address: String
    public let status: MemberStatus
    /** 成为 Up 的顺序号，越小越老；尚未 Up 时为 0 */
    public let upNumber: Int64

    public init(address: String, status: MemberStatus, upNumber: Int64) {
        this.address = address
        this.status = status
        this.upNumber = upNumber
    }

    public func withStatus(s: MemberStatus): Member {
        Member(address, s, upNumber)
    }

    /** 比 other 更老（upNumber 更小；相同时按地址），用于选择 Singleton 所在节点 */
    public func isOlderThan(other: Member): Bool {
        if (upNumber != other.upNumber) {
            if (upNumber == 0) {
                return false
            }
            if (other.upNumber == 0) {
                return true
            }
            return upNumber < other.upNumber
        }
        address < other.address
    }

    func sameAs(other: Member): Bool {
        address == other.address && status == other.status && upNumber == other.upNumber
    }

    public func toString(): String {
        "Member(${address}, ${status}, up#${upNumber})"
    }
}

// ======================= Gossip =======================

/** 一份成员视图（不可变） */
/** 某个观察者看到的不可达成员（只有观察者自己修改，版本号递增） */
class ReachRow {
    let version: Int64
    let subjects: HashSet<String>

    init(version: Int64, subjects: HashSet<String>) {
        this.version = version
        this.subjects = subjects
    }
}

class Gossip {
    let members: HashMap<String, Member>
    let seen: HashSet<String>
    // 可达性表：观察者 -> 它看到的不可达成员（随 gossip 传播，合并时每个观察者取版本更高的一行）
    let rows: HashMap<String, ReachRow>

    init(members: HashMap<String, Member>, seen: HashSet<String>) {
        this(members, seen, HashMap<String, ReachRow>())
    }

    init(members: HashMap<String, Member>, seen: HashSet<String>, rows: HashMap<String, ReachRow>) {
        this.members = members
        this.seen = seen
        this.rows = rows
    }

    static func mergeRows(a: HashMap<String, ReachRow>, b: HashMap<String, ReachRow>): HashMap<String, ReachRow> {
        let r = HashMap<String, ReachRow>(a)
        for ((obs, row) in b) {
            match (r.get(obs)) {
                case Some(cur) where cur.version >= row.version => ()
                case _ => r[obs] = row
            }
        }
        r
    }

    static func sameRows(x: HashMap<String, ReachRow>, y: HashMap<String, ReachRow>): Bool {
        if (x.size != y.size) {
            return false
        }
        for ((k, v) in x) {
            match (y.get(k)) {
                case Some(w) where w.version == v.version => ()
                case _ => return false
            }
        }
        true
    }

    func withRow(observer: String, row: ReachRow): Gossip {
        let r = HashMap<String, ReachRow>(rows)
        r[observer] = row
        Gossip(members, seen, r)
    }

    static func merge(a: Gossip, b: Gossip): Gossip {
        let m = HashMap<String, Member>(a.members)
        for ((addr, mb) in b.members) {
            match (m.get(addr)) {
                case Some(ma) =>
                    let status = if (mb.status.rank() > ma.status.rank()) { mb.status } else { ma.status }
                    let up = if (ma.upNumber == 0) {
                        mb.upNumber
                    } else if (mb.upNumber == 0) {
                        ma.upNumber
                    } else {
                        min(ma.upNumber, mb.upNumber)
                    }
                    m[addr] = Member(addr, status, up)
                case None => m[addr] = mb
            }
        }
        let sameA = sameMembers(m, a.members)
        let sameB = sameMembers(m, b.members)
        let seen = HashSet<String>()
        if (sameA) {
            seen.add(all: a.seen)
        }
        if (sameB) {
            seen.add(all: b.seen)
        }
        Gossip(m, seen, mergeRows(a.rows, b.rows))
    }

    static func sameMembers(x: HashMap<String, Member>, y: HashMap<String, Member>): Bool {
        if (x.size != y.size) {
            return false
        }
        for ((k, v) in x) {
            match (y.get(k)) {
                case Some(w) where v.sameAs(w) => ()
                case _ => return false
            }
        }
        true
    }

    func withMembers(m: HashMap<String, Member>, self: String): Gossip {
        let seen = HashSet<String>()
        seen.add(self)
        Gossip(m, seen, rows)
    }

    func seenBy(addr: String): Gossip {
        if (seen.contains(addr)) {
            return this
        }
        let s = HashSet<String>(seen)
        s.add(addr)
        Gossip(members, s, rows)
    }

    func encode(): String {
        let w = FieldWriter()
        w.put(members.size)
        for ((_, m) in members) {
            w.put(m.address)
            w.put(m.status.toString())
            w.put(m.upNumber)
        }
        w.put(seen.size)
        for (s in seen) {
            w.put(s)
        }
        // 可达性表追加在末尾：旧版本节点读到这里就停止，新版本读旧数据时为空
        // 已离开成员表的观察者不再传播
        let live = ArrayList<(String, ReachRow)>()
        for ((obs, row) in rows) {
            if (members.contains(obs)) {
                live.add((obs, row))
            }
        }
        w.put(live.size)
        for ((obs, row) in live) {
            w.put(obs)
            w.put(row.version)
            w.put(row.subjects.size)
            for (x in row.subjects) {
                w.put(x)
            }
        }
        w.done()
    }

    static func decodeFrom(r: FieldReader): Gossip {
        let n = r.nextInt()
        let m = HashMap<String, Member>()
        for (_ in 0..n) {
            let addr = r.next()
            let status = MemberStatus.parse(r.next())
            m[addr] = Member(addr, status, r.nextInt())
        }
        let k = r.nextInt()
        let seen = HashSet<String>()
        for (_ in 0..k) {
            seen.add(r.next())
        }
        let rows = HashMap<String, ReachRow>()
        if (r.hasMore()) {
            let nr = r.nextInt()
            for (_ in 0..nr) {
                let obs = r.next()
                let version = r.nextInt()
                let ns = r.nextInt()
                let subjects = HashSet<String>()
                for (_ in 0..ns) {
                    subjects.add(r.next())
                }
                rows[obs] = ReachRow(version, subjects)
            }
        }
        Gossip(m, seen, rows)
    }
}

// ======================= 事件与设置 =======================

/** 集群事件 */
public enum ClusterEvent <: ToString {
    | MemberJoined(Member)
    | MemberUp(Member)
    | MemberLeft(Member)
    | MemberExited(Member)
    | MemberDowned(Member)
    | MemberRemoved(Member)
    | UnreachableMember(Member)
    | ReachableMember(Member)
    | LeaderChanged(Option<String>)

    public func toString(): String {
        match (this) {
            case MemberJoined(m) => "MemberJoined(${m.address})"
            case MemberUp(m) => "MemberUp(${m.address})"
            case MemberLeft(m) => "MemberLeft(${m.address})"
            case MemberExited(m) => "MemberExited(${m.address})"
            case MemberDowned(m) => "MemberDowned(${m.address})"
            case MemberRemoved(m) => "MemberRemoved(${m.address})"
            case UnreachableMember(m) => "UnreachableMember(${m.address})"
            case ReachableMember(m) => "ReachableMember(${m.address})"
            case LeaderChanged(l) => "LeaderChanged(${l ?? "none"})"
        }
    }
}

public struct ClusterSettings {
    public let gossipInterval: Duration
    public let heartbeatInterval: Duration
    public let phiThreshold: Float64
    public let acceptableHeartbeatPause: Duration
    /** 不可达成员的处理方式（脑裂处理），见 sbr.cj */
    public let downing: DowningStrategy

    /** 兼容旧接口：Some(d) 等价于 DowningStrategy.AutoDownAfter(d)，None 等价于 NoDowning */
    public init(gossipInterval: Duration, heartbeatInterval: Duration, phiThreshold: Float64,
        acceptableHeartbeatPause: Duration, autoDownUnreachableAfter: Option<Duration>) {
        this(gossipInterval, heartbeatInterval, phiThreshold, acceptableHeartbeatPause,
            match (autoDownUnreachableAfter) {
                case Some(d) => DowningStrategy.AutoDownAfter(d)
                case None => DowningStrategy.NoDowning
            })
    }

    public init(gossipInterval: Duration, heartbeatInterval: Duration, phiThreshold: Float64,
        acceptableHeartbeatPause: Duration, downing: DowningStrategy) {
        this.gossipInterval = gossipInterval
        this.heartbeatInterval = heartbeatInterval
        this.phiThreshold = phiThreshold
        this.acceptableHeartbeatPause = acceptableHeartbeatPause
        this.downing = downing
    }

    /** gossip 1s、心跳 1s、phi 8、可接受停顿 3s、不自动 down（与 Akka 默认值相同） */
    public static func defaults(): ClusterSettings {
        ClusterSettings(Duration.second, Duration.second, 8.0, Duration.second * 3, DowningStrategy.NoDowning)
    }

    /** 默认值 + KeepMajority 脑裂处理（可达性视图稳定 stableAfter 后判定） */
    public static func withKeepMajority(stableAfter: Duration): ClusterSettings {
        ClusterSettings(Duration.second, Duration.second, 8.0, Duration.second * 3, DowningStrategy.KeepMajority(stableAfter))
    }
}

/** 集群状态快照 */
public class ClusterSnapshot {
    /** 未被移除的成员，按地址排序 */
    public let members: Array<Member>
    public let unreachable: Array<String>
    public let leader: Option<String>
    public let converged: Bool

    init(members: Array<Member>, unreachable: Array<String>, leader: Option<String>, converged: Bool) {
        this.members = members
        this.unreachable = unreachable
        this.leader = leader
        this.converged = converged
    }

    public func member(address: String): Option<Member> {
        for (m in members) {
            if (m.address == address) {
                return Some(m)
            }
        }
        None
    }

    public func statusOf(address: String): Option<MemberStatus> {
        match (member(address)) {
            case Some(m) => Some(m.status)
            case None => None<MemberStatus>
        }
    }

    /** 状态为 Up 的成员数 */
    public func upCount(): Int64 {
        var n: Int64 = 0
        for (m in members) {
            if (m.status == MemberStatus.Up) {
                n++
            }
        }
        n
    }
}

// ======================= 守护 Actor =======================

class Tick <: Message {
    let kind: String

    init(kind: String) {
        this.kind = kind
    }

    public func messageType(): String {
        "ClusterTick"
    }
}

class LocalCommand <: Message {
    let kind: String
    let target: String

    init(kind: String, target: String) {
        this.kind = kind
        this.target = target
    }

    public func messageType(): String {
        "ClusterCommand"
    }
}

/** 守护 Actor 与外部共享的状态 */
class ClusterShared {
    let lock = Mutex()
    var snapshot: ClusterSnapshot = ClusterSnapshot([], [], None<String>, false)
    let subscribers = ArrayList<(ClusterEvent) -> Unit>()
    var terminated = false
    // 本节点因脑裂处理判定为不存活而停止
    var downedBySbr = false
    var gossipsSent: Int64 = 0
}

class ClusterDaemon <: Actor {
    let selfAddress: String
    let seeds: Array<String>
    let remote: SimpleRemoteManager
    let settings: ClusterSettings
    let shared: ClusterShared
    var gossip: Gossip
    let detectors = HashMap<String, PhiAccrualFailureDetector>()
    // 地址 -> 首次被判定不可达的时间（毫秒）
    let unreachableSince = HashMap<String, Int64>()
    var leaderValue: Option<String> = None
    let rnd = Random()
    var stopped = false
    // 不可达集合最近一次变化的时间（毫秒），SBR 据此判断视图是否稳定
    var reachabilityChangedAt: Int64 = 0
    // 本节点可达性记录的版本，以及最近一次计算的间接连接成员
    var ownRowVersion: Int64 = 0
    var lastIndirect = HashSet<String>()
    let log = Logging.logger("cactor.cluster")

    init(selfAddress: String, seeds: Array<String>, remote: SimpleRemoteManager, settings: ClusterSettings, shared: ClusterShared) {
        this.selfAddress = selfAddress
        this.seeds = seeds
        this.remote = remote
        this.settings = settings
        this.shared = shared
        let m = HashMap<String, Member>()
        m[selfAddress] = Member(selfAddress, MemberStatus.Joining, 0)
        let seen = HashSet<String>()
        seen.add(selfAddress)
        this.gossip = Gossip(m, seen)
    }

    public prop name: String {
        get() { "cluster" }
    }

    public prop description: String {
        get() { "cluster membership daemon" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("gossip", Tick("gossip"), settings.gossipInterval)
        context.timers.startTimerWithFixedDelay("heartbeat", Tick("heartbeat"), settings.heartbeatInterval)
        publish(gossip, gossip, None<String>, HashSet<String>())
        if (seeds.isEmpty() || seeds[0] == selfAddress) {
            // 第一个种子节点（或单节点集群）自己组成集群
            leaderActions()
        }
        sendJoin()
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (stopped) {
            return MessageResult.Handled
        }
        match (message) {
            case t: Tick =>
                if (t.kind == "gossip") {
                    onGossipTick()
                } else {
                    onHeartbeatTick()
                }
            case c: LocalCommand => onCommand(c)
            case s: StringMessage => onRemote(s.getContent())
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    // ---------- 发送 ----------

    private func send(to: String, text: String): Unit {
        if (to == selfAddress) {
            return
        }
        try {
            remote.remoteActorOf(NetworkAddress.parse(to), "cluster").tell(StringMessage(text))
        } catch (_: Exception) {
            // 目标暂不可达：心跳会判定不可达，gossip 下一轮重试
        }
    }

    private func sendGossip(to: String): Unit {
        let w = FieldWriter()
        w.put("GOSSIP")
        w.put(selfAddress)
        send(to, w.done() + gossip.encode())
        synchronized(shared.lock) {
            shared.gossipsSent++
        }
    }

    /** 不是第一个种子节点，且还没收到任何集群的成员表 */
    private func notYetJoined(): Bool {
        selfStatus() == MemberStatus.Joining && gossip.members.size == 1 && seeds.size > 0 && seeds[0] != selfAddress
    }

    private func sendJoin(): Unit {
        if (selfStatus() != MemberStatus.Joining || gossip.members.size > 1) {
            return
        }
        for (s in seeds) {
            if (s != selfAddress) {
                send(s, FieldWriter().put("JOIN").put(selfAddress).done())
            }
        }
    }

    // ---------- 接收 ----------

    private func onRemote(text: String): Unit {
        try {
            let r = FieldReader(text)
            let kind = r.next()
            let from = r.next()
            match (kind) {
                case "JOIN" => onJoin(from)
                case "GOSSIP" =>
                    heartbeatFrom(from)
                    onGossip(from, Gossip.decodeFrom(r))
                case "HB" =>
                    heartbeatFrom(from)
                    send(from, FieldWriter().put("HBR").put(selfAddress).done())
                case "HBR" => heartbeatFrom(from)
                case _ => ()
            }
        } catch (e: Exception) {
            log.warn("忽略无法解析的集群消息: ${e}")
        }
    }

    private func onJoin(from: String): Unit {
        match (gossip.members.get(from)) {
            case Some(m) where m.status == MemberStatus.Removed || m.status == MemberStatus.Down =>
                log.warn("拒绝已被移除的地址重新加入: ${from}")
                return
            case Some(_) => ()
            case None =>
                if (notYetJoined()) {
                    // 自己还没有加入任何集群，不能接纳别人
                    return
                }
                let m = HashMap<String, Member>(gossip.members)
                m[from] = Member(from, MemberStatus.Joining, 0)
                update(gossip.withMembers(m, selfAddress))
        }
        sendGossip(from)
    }

    private func onGossip(from: String, incoming: Gossip): Unit {
        if (!incoming.members.contains(selfAddress)) {
            // 对方还不认识自己（例如自己正在加入，或已被移除后对方清理了记录）
            if (selfStatus() == MemberStatus.Joining) {
                send(from, FieldWriter().put("JOIN").put(selfAddress).done())
            }
            return
        }
        let merged = Gossip.merge(gossip, incoming).seenBy(selfAddress)
        let changedForSender = !Gossip.sameMembers(merged.members, incoming.members) || !incoming.seen.contains(selfAddress) ||
            !Gossip.sameRows(merged.rows, incoming.rows)
        update(merged)
        if (changedForSender && !stopped) {
            // push-pull：对方的版本比合并结果旧，回送一份
            sendGossip(from)
        }
    }

    private func heartbeatFrom(addr: String): Unit {
        detectorFor(addr).heartbeat()
    }

    private func detectorFor(addr: String): PhiAccrualFailureDetector {
        match (detectors.get(addr)) {
            case Some(d) => d
            case None =>
                let d = PhiAccrualFailureDetector(settings.phiThreshold, 200, Duration.millisecond * 100,
                    settings.acceptableHeartbeatPause, settings.heartbeatInterval, { => monotonicMillis() })
                // 从加入时开始监控：从未回应过的节点也会被判定不可达
                d.heartbeat()
                detectors[addr] = d
                d
        }
    }

    // ---------- 定时 ----------

    private func onGossipTick(): Unit {
        sendJoin()
        let targets = ArrayList<String>()
        for ((addr, m) in gossip.members) {
            if (addr != selfAddress && m.status != MemberStatus.Removed && m.status != MemberStatus.Down) {
                targets.add(addr)
            }
        }
        // 随机选最多 3 个，优先选还没看到当前状态的
        let notSeen = ArrayList<String>()
        for (t in targets) {
            if (!gossip.seen.contains(t)) {
                notSeen.add(t)
            }
        }
        let pool = if (notSeen.isEmpty()) { targets } else { notSeen }
        var count = 0
        while (count < 3 && !pool.isEmpty()) {
            let i = Int64(rnd.nextUInt64() % UInt64(pool.size))
            sendGossip(pool.remove(at: i))
            count++
        }
        leaderActions()
    }

    private func onHeartbeatTick(): Unit {
        for ((addr, m) in gossip.members) {
            if (addr != selfAddress && m.status != MemberStatus.Removed && m.status != MemberStatus.Down) {
                detectorFor(addr)
                send(addr, FieldWriter().put("HB").put(selfAddress).done())
            }
        }
        // 可达性变化
        let before = currentUnreachable()
        let now = monotonicMillis()
        for ((addr, m) in gossip.members) {
            if (addr == selfAddress || m.status == MemberStatus.Removed || m.status == MemberStatus.Down) {
                unreachableSince.remove(addr)
                continue
            }
            let available = detectorFor(addr).isAvailable()
            if (!available && !unreachableSince.contains(addr)) {
                unreachableSince[addr] = now
            } else if (available) {
                unreachableSince.remove(addr)
            }
        }
        let after = currentUnreachable()
        // 本节点的一行：本地不可达集合变化时版本加一，随 gossip 传播
        let ownRow = gossip.rows.get(selfAddress)
        let ownSame = match (ownRow) {
            case Some(row) => sameSet(row.subjects, after)
            case None => after.isEmpty()
        }
        if (!ownSame) {
            ownRowVersion++
            gossip = gossip.withRow(selfAddress, ReachRow(ownRowVersion, after))
        }
        // 可达性视图（本地不可达 + 间接连接）变化时重新计时
        let indirect = indirectlyConnected()
        if (!sameSet(before, after) || !sameSet(lastIndirect, indirect)) {
            reachabilityChangedAt = now
        }
        lastIndirect = indirect
        publish(gossip, gossip, leaderValue, before)
        selfDowningCheck(now)
    }

    /**
     * 间接连接的成员：本节点能连上，但某个本节点能连上的观察者报告连不上它（非对称分区，例如只有 A 与 B 之间断开）。
     * 只采用本节点可达的观察者的报告：已宕机或在对侧的观察者留下的旧记录不会误伤。
     * 这类成员无法让集群收敛，SBR 会把它们与不可达成员一起 down（与 Akka 的处理相同）。
     */
    private func indirectlyConnected(): HashSet<String> {
        let result = HashSet<String>()
        for ((obs, row) in gossip.rows) {
            if (obs == selfAddress || !isReachable(obs) || !isLive(obs)) {
                continue
            }
            for (x in row.subjects) {
                if (x != selfAddress && x != obs && isReachable(x) && isLive(x)) {
                    result.add(x)
                }
            }
        }
        result
    }

    private func isLive(addr: String): Bool {
        match (gossip.members.get(addr)) {
            case Some(m) => m.status != MemberStatus.Removed && m.status != MemberStatus.Down
            case None => false
        }
    }

    private static func sameSet(a: HashSet<String>, b: HashSet<String>): Bool {
        if (a.size != b.size) {
            return false
        }
        for (x in a) {
            if (!b.contains(x)) {
                return false
            }
        }
        true
    }

    /**
     * 可达性视图已稳定足够久时的 SBR 判定；None 表示不使用 SBR 或尚未稳定
     *
     * margin：额外等待的毫秒数。存活一侧的 Leader 比不存活一侧的自我停止多等 acceptableHeartbeatPause，
     * 覆盖两侧检测到分区的时间差，保证对侧的 Singleton / 分片实体先停止，本侧再接管。
     */
    private func sbrDecision(now: Int64, margin: Int64): Option<SbrDecision> {
        match (settings.downing.stableAfter()) {
            case Some(stable) =>
                if ((unreachableSince.isEmpty() && lastIndirect.isEmpty()) ||
                    now - reachabilityChangedAt < stable.toMilliseconds() + margin) {
                    return None
                }
                let live = ArrayList<Member>()
                for ((_, m) in gossip.members) {
                    if (m.status != MemberStatus.Removed && m.status != MemberStatus.Down) {
                        live.add(m)
                    }
                }
                Some(sbrDecide(settings.downing, live.toArray(), currentUnreachable(), lastIndirect))
            case None => None
        }
    }

    /** SBR 判定本侧不存活时，每个节点各自停止参与集群（不依赖 Leader，也不需要与对侧通信） */
    private func selfDowningCheck(now: Int64): Unit {
        if (stopped) {
            return
        }
        if (let Some(SbrDecision.DownSelf) <- sbrDecision(now, 0)) {
            log.warn("脑裂处理：${selfAddress} 所在的一侧不满足存活条件（不可达 ${unreachableSince.size} 个），停止参与集群")
            stopped = true
            synchronized(shared.lock) {
                shared.terminated = true
                shared.downedBySbr = true
            }
            publish(gossip, gossip, leaderValue, currentUnreachable())
        }
    }

    private func currentUnreachable(): HashSet<String> {
        let s = HashSet<String>()
        for ((a, _) in unreachableSince) {
            s.add(a)
        }
        s
    }

    // ---------- 本地命令 ----------

    private func onCommand(c: LocalCommand): Unit {
        match (c.kind) {
            case "leave" => setStatus(c.target, MemberStatus.Leaving)
            case "down" => setStatus(c.target, MemberStatus.Down)
            case _ => ()
        }
    }

    private func setStatus(addr: String, s: MemberStatus): Unit {
        match (gossip.members.get(addr)) {
            case Some(m) where m.status.rank() < s.rank() =>
                let next = HashMap<String, Member>(gossip.members)
                next[addr] = m.withStatus(s)
                update(gossip.withMembers(next, selfAddress))
                // 立即通知其他成员
                for ((a, mm) in gossip.members) {
                    if (a != selfAddress && mm.status != MemberStatus.Removed) {
                        sendGossip(a)
                    }
                }
            case _ => ()
        }
    }

    // ---------- Leader ----------

    private func isReachable(addr: String): Bool {
        addr == selfAddress || !unreachableSince.contains(addr)
    }

    /**
     * Leader：可达成员中地址最小者，按候选集合的优先级依次选择：
     * Up / Leaving → Exiting（集群中剩下的成员都在离开时，由它们自己完成移除）→ Joining（集群刚启动）
     */
    private func computeLeader(g: Gossip): Option<String> {
        var best: Option<String> = None
        var bestExiting: Option<String> = None
        var bestJoining: Option<String> = None
        for ((addr, m) in g.members) {
            if (!isReachable(addr)) {
                continue
            }
            if (m.status == MemberStatus.Up || m.status == MemberStatus.Leaving) {
                best = minOf(best, addr)
            } else if (m.status == MemberStatus.Exiting) {
                bestExiting = minOf(bestExiting, addr)
            } else if (m.status == MemberStatus.Joining) {
                bestJoining = minOf(bestJoining, addr)
            }
        }
        best ?? (bestExiting ?? bestJoining)
    }

    private static func minOf(cur: Option<String>, addr: String): Option<String> {
        match (cur) {
            case Some(b) where b <= addr => cur
            case _ => Some(addr)
        }
    }

    private func converged(g: Gossip): Bool {
        for ((addr, m) in g.members) {
            if (m.status == MemberStatus.Down || m.status == MemberStatus.Removed) {
                continue
            }
            if (!isReachable(addr)) {
                return false
            }
            if (!g.seen.contains(addr)) {
                return false
            }
        }
        true
    }

    private func leaderActions(): Unit {
        if (notYetJoined()) {
            // 还没加入任何集群的非首个种子节点：成员表里只有自己，不能自己组成集群
            //（否则加入应答较慢时会各自 Up、各自分配 upNumber，随后合并出重复的 upNumber）
            return
        }
        if (computeLeader(gossip) != Some(selfAddress)) {
            return
        }
        let next = HashMap<String, Member>(gossip.members)
        var changed = false
        let removedNow = ArrayList<String>()
        // down 不可达成员：不需要收敛（不可达成员本身就阻止收敛）
        let now = monotonicMillis()
        let toDown = ArrayList<String>()
        match (settings.downing) {
            case AutoDownAfter(after) =>
                for ((addr, since) in unreachableSince) {
                    if (now - since >= after.toMilliseconds()) {
                        toDown.add(addr)
                    }
                }
            case _ =>
                if (let Some(SbrDecision.DownUnreachable(list)) <- sbrDecision(now, settings.acceptableHeartbeatPause.toMilliseconds())) {
                    toDown.add(all: list)
                }
        }
        for (addr in toDown) {
            if (let Some(m) <- next.get(addr)) {
                if (m.status.rank() < MemberStatus.Down.rank()) {
                    log.info("down 不可达成员 ${addr}")
                    next[addr] = m.withStatus(MemberStatus.Down)
                    changed = true
                }
            }
        }
        if (converged(gossip)) {
            var maxUp: Int64 = 0
            for ((_, m) in next) {
                maxUp = max(maxUp, m.upNumber)
            }
            // 按地址顺序推进，保证 upNumber 的分配确定
            let addrs = ArrayList<String>()
            for ((a, _) in next) {
                addrs.add(a)
            }
            sort(addrs)
            for (addr in addrs) {
                let m = next[addr]
                match (m.status) {
                    case Joining =>
                        maxUp++
                        next[addr] = Member(addr, MemberStatus.Up, maxUp)
                        changed = true
                    case Leaving =>
                        next[addr] = m.withStatus(MemberStatus.Exiting)
                        changed = true
                    case Exiting | Down =>
                        next[addr] = m.withStatus(MemberStatus.Removed)
                        removedNow.add(addr)
                        changed = true
                    case _ => ()
                }
            }
        }
        if (changed) {
            update(gossip.withMembers(next, selfAddress))
            for (a in removedNow) {
                // 被移除的节点不再出现在 gossip 目标中，单独通知它一次
                sendGossip(a)
            }
        }
    }

    // ---------- 状态更新与事件 ----------

    private func selfStatus(): MemberStatus {
        match (gossip.members.get(selfAddress)) {
            case Some(m) => m.status
            case None => MemberStatus.Removed
        }
    }

    private func update(next: Gossip): Unit {
        let old = gossip
        gossip = next
        publish(old, next, leaderValue, currentUnreachable())
        let st = selfStatus()
        if (st == MemberStatus.Removed || st == MemberStatus.Down) {
            if (!stopped) {
                log.info("${selfAddress} 已${if (st == MemberStatus.Down) { "被 down" } else { "移除" }}，停止参与集群")
                stopped = true
                synchronized(shared.lock) {
                    shared.terminated = true
                }
            }
        }
    }

    /** 计算 old → next 的成员事件、可达性事件与 Leader 变化，更新快照并通知订阅者 */
    private func publish(old: Gossip, next: Gossip, oldLeader: Option<String>, oldUnreachable: HashSet<String>): Unit {
        let events = ArrayList<ClusterEvent>()
        let addrs = ArrayList<String>()
        for ((a, _) in next.members) {
            addrs.add(a)
        }
        sort(addrs)
        for (addr in addrs) {
            let m = next.members[addr]
            let prevRank = match (old.members.get(addr)) {
                case Some(p) => p.status.rank()
                case None => -1
            }
            if (refEq(old, next) && prevRank >= 0) {
                continue
            }
            if (m.status.rank() > prevRank || prevRank == -1) {
                match (m.status) {
                    case Joining => events.add(ClusterEvent.MemberJoined(m))
                    case Up => events.add(ClusterEvent.MemberUp(m))
                    case Leaving => events.add(ClusterEvent.MemberLeft(m))
                    case Exiting => events.add(ClusterEvent.MemberExited(m))
                    case Down => events.add(ClusterEvent.MemberDowned(m))
                    case Removed => events.add(ClusterEvent.MemberRemoved(m))
                }
            }
        }
        let unreachable = currentUnreachable()
        for (a in unreachable) {
            if (!oldUnreachable.contains(a)) {
                if (let Some(m) <- next.members.get(a)) {
                    events.add(ClusterEvent.UnreachableMember(m))
                }
            }
        }
        for (a in oldUnreachable) {
            if (!unreachable.contains(a)) {
                if (let Some(m) <- next.members.get(a)) {
                    if (m.status.rank() < MemberStatus.Down.rank()) {
                        events.add(ClusterEvent.ReachableMember(m))
                    }
                }
            }
        }
        let newLeader = computeLeader(next)
        if (newLeader != oldLeader) {
            events.add(ClusterEvent.LeaderChanged(newLeader))
        }
        leaderValue = newLeader
        // 快照
        let live = ArrayList<Member>()
        for (a in addrs) {
            let m = next.members[a]
            if (m.status != MemberStatus.Removed) {
                live.add(m)
            }
        }
        let unr = ArrayList<String>()
        for (a in unreachable) {
            unr.add(a)
        }
        sort(unr)
        let snap = ClusterSnapshot(live.toArray(), unr.toArray(), newLeader, converged(next))
        let subs = synchronized(shared.lock) {
            shared.snapshot = snap
            shared.subscribers.toArray()
        }
        for (e in events) {
            for (cb in subs) {
                try {
                    cb(e)
                } catch (ex: Exception) {
                    log.warn("集群事件订阅者异常: ${ex}")
                }
            }
        }
    }
}

// ======================= 对外接口 =======================

/**
 * 本节点的集群成员身份
 *
 * ```
 * let cluster = Cluster(system, remote, [seed1, seed2], ClusterSettings.defaults())
 * cluster.subscribe({ e => log.info(e.toString()) })
 * cluster.awaitUp(Duration.second * 10)
 * ```
 */
public class Cluster {
    public let selfAddress: String
    private let shared = ClusterShared()
    private let daemon: ActorRef
    private let system: CActorSystem

    public init(system: CActorSystem, remote: SimpleRemoteManager, seeds: Array<NetworkAddress>, settings: ClusterSettings) {
        this.system = system
        this.selfAddress = remote.getLocalAddress().toString()
        let seedAddrs = Array<String>(seeds.size, { i => seeds[i].toString() })
        let me = selfAddress
        let sh = shared
        this.daemon = system.actorOf({ => ClusterDaemon(me, seedAddrs, remote, settings, sh) },
            "cluster-${me.replace(":", "_").replace(".", "_")}")
        remote.exposeActor("cluster", daemon)
    }

    public func state(): ClusterSnapshot {
        synchronized(shared.lock) {
            shared.snapshot
        }
    }

    public func members(): Array<Member> {
        state().members
    }

    public func selfMember(): Option<Member> {
        state().member(selfAddress)
    }

    public func leader(): Option<String> {
        state().leader
    }

    public func isUp(): Bool {
        match (selfMember()) {
            case Some(m) => m.status == MemberStatus.Up
            case None => false
        }
    }

    /** 已离开或被 down，不再参与集群 */
    public func isTerminated(): Bool {
        synchronized(shared.lock) {
            shared.terminated
        }
    }

    /** 本节点是否因脑裂处理（本侧不满足存活条件）而停止参与集群 */
    public func isDownedBySbr(): Bool {
        synchronized(shared.lock) {
            shared.downedBySbr
        }
    }

    /** 最老的、可达的 Up 成员（Cluster Singleton 运行在这里） */
    public func oldest(): Option<Member> {
        let s = state()
        var best: Option<Member> = None
        for (m in s.members) {
            if (m.status != MemberStatus.Up) {
                continue
            }
            var unreachable = false
            for (u in s.unreachable) {
                if (u == m.address) {
                    unreachable = true
                }
            }
            if (unreachable) {
                continue
            }
            match (best) {
                case Some(b) where b.isOlderThan(m) => ()
                case _ => best = Some(m)
            }
        }
        best
    }

    /**
     * 订阅集群事件。订阅时先按当前状态回放（每个成员一条与其状态对应的事件、当前 Leader），
     * 之后推送变化；回调在集群守护 Actor 线程中执行，不要在其中阻塞。
     */
    public func subscribe(callback: (ClusterEvent) -> Unit): Unit {
        let snap = synchronized(shared.lock) {
            shared.subscribers.add(callback)
            shared.snapshot
        }
        for (m in snap.members) {
            let e = match (m.status) {
                case Joining => ClusterEvent.MemberJoined(m)
                case Up => ClusterEvent.MemberUp(m)
                case Leaving => ClusterEvent.MemberLeft(m)
                case Exiting => ClusterEvent.MemberExited(m)
                case Down => ClusterEvent.MemberDowned(m)
                case Removed => ClusterEvent.MemberRemoved(m)
            }
            callback(e)
        }
        if (snap.leader.isSome()) {
            callback(ClusterEvent.LeaderChanged(snap.leader))
        }
    }

    /** 优雅离开：Leaving → Exiting → Removed */
    public func leave(): Unit {
        daemon.tell(LocalCommand("leave", selfAddress))
    }

    /** 把某个成员标记为 Down（通常用于确认已宕机的不可达节点） */
    public func down(address: String): Unit {
        daemon.tell(LocalCommand("down", address))
    }

    /** 等待本节点变为 Up */
    public func awaitUp(timeout: Duration): Bool {
        let deadline = MonoTime.now() + timeout
        while (!isUp()) {
            if (MonoTime.now() > deadline) {
                return false
            }
            sleep(Duration.millisecond * 5)
        }
        true
    }

    /** (已发送的 gossip 数) */
    public func gossipCount(): Int64 {
        synchronized(shared.lock) {
            shared.gossipsSent
        }
    }

    /** 停止本节点的集群守护 Actor（不通知其他成员；模拟宕机请同时停止远程管理器） */
    public func shutdown(): Unit {
        system.stop(daemon)
    }
}
