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
 * 节点实例（对标 Akka UniqueAddress，N4）：成员记录带上远程层的启动标识 uid，"地址#uid" 唯一确定一个进程。
 * 同一地址重启的新进程不继承旧成员的身份：加入时旧实例先被 down、由 Leader 移除，之后新实例才以 Joining 加入；
 * 被替换的旧实例记入墓碑（tombstones），随 gossip 传播，迟到的旧视图不能把它复活。被移除的实例永远不能再加入。
 *
 * 种子加入（对标 Akka JoinSeedNodeProcess / FirstSeedNodeProcess，N5）：首个种子不再立即自己组成集群，
 * 而是先向其他种子发 JOIN；还没加入集群的节点回 NACK，已在集群中的节点接纳它或回 WAIT（旧实例移除中）。
 * 其他种子全部回 NACK（大家都是新启动的），或 seedNodeTimeout 内没有任何集群回应，首个种子才自己组成集群。
 * 因此首个种子重启后会重新加入原集群，而不是另起一个（两个集群、两个 Singleton）。
 *
 * 传输复用 cactor_remote：每个节点把成员守护 Actor 以名称 "cluster" 暴露出去。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.sort.*
import std.convert.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.detector.{PhiAccrualFailureDetector, monotonicMillis}
import cactor_foundation.config.{LayeredConfig, ConfigException}
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

/** "地址#uid"：唯一确定一个节点实例（对标 Akka UniqueAddress） */
public func uniqueAddressOf(address: String, uid: UInt64): String {
    "${address}#${uid}"
}

/** 集群成员（不可变） */
public class Member <: ToString {
    /** 远程地址 "host:port" */
    public let address: String
    public let status: MemberStatus
    /** 成为 Up 的顺序号，越小越老；尚未 Up 时为 0 */
    public let upNumber: Int64
    /** 该地址上进程的启动标识（SimpleRemoteManager.selfUid）；同一地址重启后不同 */
    public let uid: UInt64

    public init(address: String, status: MemberStatus, upNumber: Int64) {
        this(address, status, upNumber, 0)
    }

    public init(address: String, status: MemberStatus, upNumber: Int64, uid: UInt64) {
        this.address = address
        this.status = status
        this.upNumber = upNumber
        this.uid = uid
    }

    public func withStatus(s: MemberStatus): Member {
        Member(address, s, upNumber, uid)
    }

    /** "地址#uid" */
    public func uniqueAddress(): String {
        uniqueAddressOf(address, uid)
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
        address == other.address && uid == other.uid && status == other.status && upNumber == other.upNumber
    }

    public func toString(): String {
        "Member(${address}#${uid}, ${status}, up#${upNumber})"
    }
}

// ======================= Gossip =======================

/** 某个观察者（某个实例）看到的不可达成员（只有观察者自己修改，版本号递增） */
class ReachRow {
    let version: Int64
    let subjects: HashSet<String>
    // 观察者的启动标识：同一地址重启后版本号从 0 重新计数，旧实例的行不能压过新实例的行
    let uid: UInt64

    init(version: Int64, subjects: HashSet<String>, uid: UInt64) {
        this.version = version
        this.subjects = subjects
        this.uid = uid
    }
}

/** 一份成员视图（不可变） */
class Gossip {
    // 地址 -> 该地址当前的实例（同一地址同一时刻只保留一个实例的记录）
    let members: HashMap<String, Member>
    let seen: HashSet<String>
    // 可达性表：观察者 -> 它看到的不可达成员（随 gossip 传播，合并时每个观察者取版本更高的一行）
    let rows: HashMap<String, ReachRow>
    // 已被移除且被同址新实例替换的旧实例（"地址#uid"）：只增，合并取并集
    let tombstones: HashSet<String>

    init(members: HashMap<String, Member>, seen: HashSet<String>) {
        this(members, seen, HashMap<String, ReachRow>(), HashSet<String>())
    }

    init(members: HashMap<String, Member>, seen: HashSet<String>, rows: HashMap<String, ReachRow>,
        tombstones: HashSet<String>) {
        this.members = members
        this.seen = seen
        this.rows = rows
        this.tombstones = tombstones
    }

    /** 每个观察者只保留与其当前实例 uid 一致的行；同一实例取版本更高的一行 */
    static func mergeRows(a: HashMap<String, ReachRow>, b: HashMap<String, ReachRow>, m: HashMap<String, Member>): HashMap<String, ReachRow> {
        let r = HashMap<String, ReachRow>()
        for (src in [a, b]) {
            for ((obs, row) in src) {
                match (m.get(obs)) {
                    case Some(mb) where mb.uid == row.uid => ()
                    case _ => continue
                }
                match (r.get(obs)) {
                    case Some(cur) where cur.version >= row.version => ()
                    case _ => r[obs] = row
                }
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
                case Some(w) where w.version == v.version && w.uid == v.uid => ()
                case _ => return false
            }
        }
        true
    }

    func withRow(observer: String, row: ReachRow): Gossip {
        let r = HashMap<String, ReachRow>(rows)
        r[observer] = row
        Gossip(members, seen, r, tombstones)
    }

    /** 同一实例的两条记录：状态取更靠后者，upNumber 取非零最小值 */
    private static func mergeSame(x: Member, y: Member): Member {
        let status = if (y.status.rank() > x.status.rank()) { y.status } else { x.status }
        let up = if (x.upNumber == 0) {
            y.upNumber
        } else if (y.upNumber == 0) {
            x.upNumber
        } else {
            min(x.upNumber, y.upNumber)
        }
        Member(x.address, status, up, x.uid)
    }

    /** 墓碑里的实例一律视为 Removed */
    private static func buried(x: Member, tomb: HashSet<String>): Member {
        if (x.status != MemberStatus.Removed && tomb.contains(x.uniqueAddress())) {
            x.withStatus(MemberStatus.Removed)
        } else {
            x
        }
    }

    /**
     * 同一地址、不同实例的两条记录：活着的（不在墓碑中、未 Removed）胜出。
     * 新实例只有在旧实例被移除、并记入墓碑之后才会被接纳，带着新实例的 gossip 必然带着旧实例的墓碑，
     * 所以迟到的旧视图（旧实例仍是 Up）合并后也会让位给新实例。两者都活着或都已移除时取 uid 大者，保证合并确定
     */
    private static func pick(x: Member, y: Member, tomb: HashSet<String>): Member {
        if (x.uid == y.uid) {
            return buried(mergeSame(x, y), tomb)
        }
        let bx = buried(x, tomb)
        let by = buried(y, tomb)
        let xDead = bx.status == MemberStatus.Removed
        let yDead = by.status == MemberStatus.Removed
        if (xDead != yDead) {
            return if (xDead) { by } else { bx }
        }
        if (bx.uid > by.uid) { bx } else { by }
    }

    static func merge(a: Gossip, b: Gossip): Gossip {
        let tomb = HashSet<String>(a.tombstones)
        tomb.add(all: b.tombstones)
        let m = HashMap<String, Member>()
        for ((addr, ma) in a.members) {
            m[addr] = match (b.members.get(addr)) {
                case Some(mb) => pick(ma, mb, tomb)
                case None => buried(ma, tomb)
            }
        }
        for ((addr, mb) in b.members) {
            if (!a.members.contains(addr)) {
                m[addr] = buried(mb, tomb)
            }
        }
        let seen = HashSet<String>()
        if (sameState(m, tomb, a)) {
            seen.add(all: a.seen)
        }
        if (sameState(m, tomb, b)) {
            seen.add(all: b.seen)
        }
        Gossip(m, seen, mergeRows(a.rows, b.rows, m), tomb)
    }

    private static func sameState(m: HashMap<String, Member>, tomb: HashSet<String>, g: Gossip): Bool {
        tomb.size == g.tombstones.size && sameMembers(m, g.members)
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
        withMembers(m, self, tombstones)
    }

    func withMembers(m: HashMap<String, Member>, self: String, tomb: HashSet<String>): Gossip {
        let seen = HashSet<String>()
        seen.add(self)
        // 地址上换了新实例时，旧实例的可达性行随即作废（否则要等下一次合并才被过滤，期间会被当成间接连接的证据）
        let live = HashMap<String, ReachRow>()
        for ((obs, row) in rows) {
            match (m.get(obs)) {
                case Some(mb) where mb.uid == row.uid => live[obs] = row
                case _ => ()
            }
        }
        Gossip(m, seen, live, tomb)
    }

    func seenBy(addr: String): Gossip {
        if (seen.contains(addr)) {
            return this
        }
        let s = HashSet<String>(seen)
        s.add(addr)
        Gossip(members, s, rows, tombstones)
    }

    /** 该地址当前记录的实例是否就是 uid 这一个 */
    func hasInstance(addr: String, uid: UInt64): Bool {
        match (members.get(addr)) {
            case Some(m) => m.uid == uid
            case None => false
        }
    }

    func encode(): String {
        let w = FieldWriter()
        w.put(members.size)
        for ((_, m) in members) {
            w.put(m.address)
            w.put(m.status.toString())
            w.put(m.upNumber)
            w.put(m.uid.toString())
        }
        w.put(seen.size)
        for (s in seen) {
            w.put(s)
        }
        // 已离开成员表（或已被同址新实例替换）的观察者不再传播
        let live = ArrayList<(String, ReachRow)>()
        for ((obs, row) in rows) {
            if (hasInstance(obs, row.uid)) {
                live.add((obs, row))
            }
        }
        w.put(live.size)
        for ((obs, row) in live) {
            w.put(obs)
            w.put(row.uid.toString())
            w.put(row.version)
            w.put(row.subjects.size)
            for (x in row.subjects) {
                w.put(x)
            }
        }
        w.put(tombstones.size)
        for (t in tombstones) {
            w.put(t)
        }
        w.done()
    }

    static func decodeFrom(r: FieldReader): Gossip {
        let n = r.nextInt()
        let m = HashMap<String, Member>()
        for (_ in 0..n) {
            let addr = r.next()
            let status = MemberStatus.parse(r.next())
            let up = r.nextInt()
            m[addr] = Member(addr, status, up, parseUid(r.next()))
        }
        let k = r.nextInt()
        let seen = HashSet<String>()
        for (_ in 0..k) {
            seen.add(r.next())
        }
        let rows = HashMap<String, ReachRow>()
        let nr = r.nextInt()
        for (_ in 0..nr) {
            let obs = r.next()
            let uid = parseUid(r.next())
            let version = r.nextInt()
            let ns = r.nextInt()
            let subjects = HashSet<String>()
            for (_ in 0..ns) {
                subjects.add(r.next())
            }
            rows[obs] = ReachRow(version, subjects, uid)
        }
        let nt = r.nextInt()
        let tomb = HashSet<String>()
        for (_ in 0..nt) {
            tomb.add(r.next())
        }
        Gossip(m, seen, rows, tomb)
    }
}

func parseUid(text: String): UInt64 {
    match (UInt64.tryParse(text)) {
        case Some(u) => u
        case None => throw IllegalArgumentException("编码损坏：启动标识不是整数")
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

let DEFAULT_SEED_NODE_TIMEOUT = Duration.second * 5

public struct ClusterSettings {
    public let gossipInterval: Duration
    public let heartbeatInterval: Duration
    public let phiThreshold: Float64
    public let acceptableHeartbeatPause: Duration
    /** 不可达成员的处理方式（脑裂处理），见 sbr.cj */
    public let downing: DowningStrategy
    /**
     * 首个种子等待其他种子回应的时限（同 Akka seed-node-timeout，默认 5 s）：
     * 时限内其他种子全部回 NACK 就立即组成集群；没有任何集群回应才在时限到后自己组成集群
     */
    public let seedNodeTimeout: Duration

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
        this(gossipInterval, heartbeatInterval, phiThreshold, acceptableHeartbeatPause, downing, DEFAULT_SEED_NODE_TIMEOUT)
    }

    public init(gossipInterval: Duration, heartbeatInterval: Duration, phiThreshold: Float64,
        acceptableHeartbeatPause: Duration, downing: DowningStrategy, seedNodeTimeout: Duration) {
        this.gossipInterval = gossipInterval
        this.heartbeatInterval = heartbeatInterval
        this.phiThreshold = phiThreshold
        this.acceptableHeartbeatPause = acceptableHeartbeatPause
        this.downing = downing
        this.seedNodeTimeout = seedNodeTimeout
    }

    public func withSeedNodeTimeout(d: Duration): ClusterSettings {
        ClusterSettings(gossipInterval, heartbeatInterval, phiThreshold, acceptableHeartbeatPause, downing, d)
    }

    /** gossip 1s、心跳 1s、phi 8、可接受停顿 3s、不自动 down（与 Akka 默认值相同） */
    public static func defaults(): ClusterSettings {
        ClusterSettings(Duration.second, Duration.second, 8.0, Duration.second * 3, DowningStrategy.NoDowning)
    }

    /**
     * 从分层配置读取（键均在 cluster. 下，缺少的项取默认值）：
     *   gossip-interval、heartbeat-interval、phi-threshold、acceptable-heartbeat-pause（时长，例如 1s）
     *   downing = none | auto-down | keep-majority | keep-oldest | static-quorum
     *   stable-after（时长，SBR 策略使用）、auto-down-after（时长）、quorum-size（整数）
     *   seed-node-timeout（时长，默认 5s）
     */
    public static func fromConfig(cfg: LayeredConfig): ClusterSettings {
        let d = defaults()
        let stable = cfg.getDuration("cluster.stable-after", Duration.second * 7)
        let downing = match (cfg.getString("cluster.downing", "none")) {
            case "none" => DowningStrategy.NoDowning
            case "auto-down" => DowningStrategy.AutoDownAfter(cfg.getDuration("cluster.auto-down-after", Duration.second * 10))
            case "keep-majority" => DowningStrategy.KeepMajority(stable)
            case "keep-oldest" => DowningStrategy.KeepOldest(stable)
            case "static-quorum" => DowningStrategy.StaticQuorum(cfg.getInt("cluster.quorum-size"), stable)
            case other => throw ConfigException(
                "配置 cluster.downing = \"${other}\"（来自 ${cfg.origin("cluster.downing") ?? "?"}）不是 none / auto-down / keep-majority / keep-oldest / static-quorum")
        }
        ClusterSettings(cfg.getDuration("cluster.gossip-interval", d.gossipInterval),
            cfg.getDuration("cluster.heartbeat-interval", d.heartbeatInterval),
            cfg.getFloat("cluster.phi-threshold", d.phiThreshold),
            cfg.getDuration("cluster.acceptable-heartbeat-pause", d.acceptableHeartbeatPause), downing,
            cfg.getDuration("cluster.seed-node-timeout", d.seedNodeTimeout))
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
    // 本进程的启动标识：成员记录、可达性行都带上它，同一地址重启后不继承旧身份（N4）
    let selfUid: UInt64
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

    // ---- 种子加入（N5）----
    // 首个种子（或没有种子的单节点集群）才可能自己组成集群
    let firstSeed: Bool
    let otherSeeds: Array<String>
    // 本节点已经自己组成了集群（首个种子在其他种子全部 NACK 或超时后）
    var formed: Bool
    var startedAt: Int64 = 0
    // 最近一次得知"已有集群在运行"的时间（收到 WAIT，或收到不含本实例的 gossip），-1 表示从未
    var lastClusterEvidence: Int64 = -1
    // 回过 NACK（自己也还没加入集群）的其他种子
    let nacks = HashSet<String>()
    // 首个种子尚未组成集群时收到的 JOIN：组成集群后立即接纳，不必等对方下一轮重试
    let pendingJoins = HashMap<String, UInt64>()

    init(selfAddress: String, selfUid: UInt64, seeds: Array<String>, remote: SimpleRemoteManager, settings: ClusterSettings,
        shared: ClusterShared) {
        this.selfAddress = selfAddress
        this.selfUid = selfUid
        this.seeds = seeds
        this.remote = remote
        this.settings = settings
        this.shared = shared
        let m = HashMap<String, Member>()
        m[selfAddress] = Member(selfAddress, MemberStatus.Joining, 0, selfUid)
        let seen = HashSet<String>()
        seen.add(selfAddress)
        this.gossip = Gossip(m, seen)
        this.firstSeed = seeds.isEmpty() || seeds[0] == selfAddress
        let others = ArrayList<String>()
        for (s in seeds) {
            if (s != selfAddress && !others.contains(s)) {
                others.add(s)
            }
        }
        this.otherSeeds = others.toArray()
        // 没有其他种子可问：直接组成集群
        this.formed = firstSeed && otherSeeds.isEmpty()
    }

    public prop name: String {
        get() { "cluster" }
    }

    public prop description: String {
        get() { "cluster membership daemon" }
    }

    public func onStarted(context: ActorContext): Unit {
        startedAt = monotonicMillis()
        context.timers.startTimerWithFixedDelay("gossip", Tick("gossip"), settings.gossipInterval)
        context.timers.startTimerWithFixedDelay("heartbeat", Tick("heartbeat"), settings.heartbeatInterval)
        publish(gossip, gossip, None<String>, HashSet<String>())
        if (formed) {
            // 单节点集群，或种子列表里只有自己：直接组成集群
            leaderActions()
        }
        // 首个种子也先向其他种子发 JOIN：原集群还在（例如首个种子重启）时应加入它，而不是另起一个
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
            // 成员协议（JOIN / GOSSIP / HB / HBR）走优先通道；发送只入队、不阻塞（E40）：
            // 此前同步 connect，黑洞一个地址就让守护 Actor 每次发送卡 3–6 s，心跳停发、健康节点被误判（N1）
            remote.sendControl(NetworkAddress.parse(to), "cluster", StringMessage(text))
        } catch (_: Exception) {
            // 地址非法或传输已停止：心跳会判定不可达，gossip 下一轮重试
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

    /** 还没加入任何集群：成员表里只有自己，且自己没有组成集群 */
    private func notYetJoined(): Bool {
        !formed && selfStatus() == MemberStatus.Joining && gossip.members.size == 1
    }

    private func joinText(): String {
        FieldWriter().put("JOIN").put(selfAddress).put(selfUid.toString()).done()
    }

    private func sendJoin(): Unit {
        if (!notYetJoined()) {
            return
        }
        for (s in otherSeeds) {
            send(s, joinText())
        }
    }

    /**
     * 首个种子决定是否自己组成集群（对标 Akka FirstSeedNodeProcess）：
     * 最近 seedNodeTimeout 内得知已有集群在运行就继续加入它；否则其他种子全部回 NACK、或等满 seedNodeTimeout 才组成
     */
    private func maybeFormCluster(): Unit {
        if (formed || !firstSeed || !notYetJoined()) {
            return
        }
        let now = monotonicMillis()
        let timeout = settings.seedNodeTimeout.toMilliseconds()
        if (lastClusterEvidence >= 0 && now - lastClusterEvidence < timeout) {
            return
        }
        var allNacked = true
        for (s in otherSeeds) {
            if (!nacks.contains(s)) {
                allNacked = false
            }
        }
        if (!allNacked && now - startedAt < timeout) {
            return
        }
        formed = true
        log.info("${selfAddress} 组成新集群（${if (allNacked) { "其他种子都还没有加入集群" } else { "等待 ${settings.seedNodeTimeout} 未收到任何集群的回应" }}）")
        leaderActions()
        let pending = ArrayList<(String, UInt64)>()
        for ((a, u) in pendingJoins) {
            pending.add((a, u))
        }
        pendingJoins.clear()
        for ((a, u) in pending) {
            onJoin(a, u)
        }
    }

    // ---------- 接收 ----------

    private func onRemote(text: String): Unit {
        try {
            let r = FieldReader(text)
            let kind = r.next()
            let from = r.next()
            match (kind) {
                case "JOIN" => onJoin(from, parseUid(r.next()))
                case "NACK" =>
                    // 对方也还没加入集群（首个种子据此判断是否可以自己组成集群）
                    nacks.add(from)
                    maybeFormCluster()
                case "WAIT" =>
                    // 已有集群在运行，只是本地址的旧实例还没移除：不能自己组成集群，继续重试 JOIN
                    lastClusterEvidence = monotonicMillis()
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

    /**
     * 加入请求（from 地址上启动标识为 uid 的实例）：
     * - 自己还没加入集群：回 NACK（首个种子据此判断大家是否都是新启动的），首个种子记下请求，组成集群后立即接纳；
     * - 已是成员：回送 gossip；该实例已被 down / 移除：拒绝（被移除的实例永远不能再加入）；
     * - 同一地址的旧实例还在（进程在同一地址重启，N4）：down 旧实例并回 WAIT，Leader 移除旧实例后，
     *   新实例下一次 JOIN 才被接纳（同 Akka）。新实例从不继承旧实例的 upNumber 与状态
     */
    private func onJoin(from: String, uid: UInt64): Unit {
        if (from == selfAddress) {
            return
        }
        if (notYetJoined()) {
            send(from, FieldWriter().put("NACK").put(selfAddress).done())
            if (firstSeed) {
                pendingJoins[from] = uid
            }
            return
        }
        if (gossip.tombstones.contains(uniqueAddressOf(from, uid))) {
            log.warn("拒绝已被移除的实例重新加入: ${uniqueAddressOf(from, uid)}")
            return
        }
        match (gossip.members.get(from)) {
            case Some(m) where m.uid == uid =>
                if (m.status == MemberStatus.Removed || m.status == MemberStatus.Down) {
                    log.warn("拒绝已被移除的实例重新加入: ${m.uniqueAddress()}")
                    return
                }
            case Some(old) where old.status == MemberStatus.Removed =>
                // 旧实例已移除：记入墓碑，新实例以 Joining 加入
                let next = HashMap<String, Member>(gossip.members)
                next[from] = Member(from, MemberStatus.Joining, 0, uid)
                let tomb = HashSet<String>(gossip.tombstones)
                tomb.add(old.uniqueAddress())
                update(gossip.withMembers(next, selfAddress, tomb))
            case Some(old) =>
                if (old.status.rank() < MemberStatus.Down.rank()) {
                    log.info("${from} 以新的启动标识 ${uid} 请求加入，down 旧实例 ${old.uniqueAddress()}")
                    setStatus(from, MemberStatus.Down)
                }
                send(from, FieldWriter().put("WAIT").put(selfAddress).done())
                return
            case None =>
                let m = HashMap<String, Member>(gossip.members)
                m[from] = Member(from, MemberStatus.Joining, 0, uid)
                update(gossip.withMembers(m, selfAddress))
        }
        sendGossip(from)
    }

    private func onGossip(from: String, incoming: Gossip): Unit {
        if (incoming.tombstones.contains(uniqueAddressOf(selfAddress, selfUid))) {
            // 本实例已被移除并被替换（只可能是误判后被隔离的僵尸进程）：停止参与集群
            if (let Some(m) <- gossip.members.get(selfAddress)) {
                if (m.uid == selfUid && m.status != MemberStatus.Removed) {
                    let next = HashMap<String, Member>(gossip.members)
                    next[selfAddress] = m.withStatus(MemberStatus.Removed)
                    update(gossip.withMembers(next, selfAddress))
                }
            }
            return
        }
        if (!incoming.hasInstance(selfAddress, selfUid)) {
            // 对方还不认识本实例：自己正在加入，或对方记录的是本地址上的旧实例（本进程是重启后的新实例）。
            // 不合并：旧实例的状态与 upNumber 不属于本实例。这份 gossip 说明已有集群在运行，首个种子不再自己组成集群
            lastClusterEvidence = monotonicMillis()
            if (selfStatus() == MemberStatus.Joining) {
                send(from, joinText())
            }
            return
        }
        let merged = Gossip.merge(gossip, incoming).seenBy(selfAddress)
        let changedForSender = !Gossip.sameMembers(merged.members, incoming.members) || !incoming.seen.contains(selfAddress) ||
            !Gossip.sameRows(merged.rows, incoming.rows) || merged.tombstones.size != incoming.tombstones.size
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
        maybeFormCluster()
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
            gossip = gossip.withRow(selfAddress, ReachRow(ownRowVersion, after, selfUid))
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
                        next[addr] = Member(addr, MemberStatus.Up, maxUp, m.uid)
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

    /** 本实例的状态；本地址的记录已换成别的实例时视为 Removed */
    private func selfStatus(): MemberStatus {
        match (gossip.members.get(selfAddress)) {
            case Some(m) where m.uid == selfUid => m.status
            case _ => MemberStatus.Removed
        }
    }

    private func update(next: Gossip): Unit {
        let old = gossip
        gossip = next
        let unreachableBefore = currentUnreachable()
        // 地址上换了新实例：旧实例的故障检测历史与不可达记录不属于新实例
        for ((addr, m) in next.members) {
            if (let Some(p) <- old.members.get(addr)) {
                if (p.uid != m.uid) {
                    detectors.remove(addr)
                    unreachableSince.remove(addr)
                }
            }
        }
        publish(old, next, leaderValue, unreachableBefore)
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
                case Some(p) where p.uid != m.uid =>
                    // 地址上换了新实例：旧实例若还没发过移除事件，先补一条，新实例按新成员发事件
                    if (p.status != MemberStatus.Removed) {
                        events.add(ClusterEvent.MemberRemoved(p.withStatus(MemberStatus.Removed)))
                    }
                    -1
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
                    // 换了新实例的地址不算"恢复可达"：不可达的是旧实例
                    let sameInstance = match (old.members.get(a)) {
                        case Some(p) => p.uid == m.uid
                        case None => false
                    }
                    if (sameInstance && m.status.rank() < MemberStatus.Down.rank()) {
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
    /** 本进程的启动标识（同 SimpleRemoteManager.selfUid） */
    public let selfUid: UInt64
    private let shared = ClusterShared()
    private let daemon: ActorRef
    private let system: CActorSystem

    public init(system: CActorSystem, remote: SimpleRemoteManager, seeds: Array<NetworkAddress>, settings: ClusterSettings) {
        this.system = system
        this.selfAddress = remote.getLocalAddress().toString()
        this.selfUid = remote.selfUid()
        let seedAddrs = Array<String>(seeds.size, { i => seeds[i].toString() })
        let me = selfAddress
        let uid = selfUid
        let sh = shared
        this.daemon = system.actorOf({ => ClusterDaemon(me, uid, seedAddrs, remote, settings, sh) },
            "cluster-${me.replace(":", "_").replace(".", "_")}")
        remote.exposeActor("cluster", daemon)
    }

    public func state(): ClusterSnapshot {
        synchronized(shared.lock) {
            shared.snapshot
        }
    }

    /**
     * 一次加锁同时读取"快照 + 是否已终止"。
     * 分片路由每次投递都要判断成员视图与终止状态，分成两次加锁会在热路径上多一次锁往返；
     * 快照按不可变对象整体替换，返回引用即可安全使用。
     */
    public func stateAndTerminated(): (ClusterSnapshot, Bool) {
        synchronized(shared.lock) {
            (shared.snapshot, shared.terminated)
        }
    }

    public func members(): Array<Member> {
        state().members
    }

    /** "地址#uid"：本节点实例的唯一标识（对标 Akka Cluster.selfUniqueAddress） */
    public func selfUniqueAddress(): String {
        uniqueAddressOf(selfAddress, selfUid)
    }

    /** 本实例的成员记录（本地址的记录属于别的实例时为 None） */
    public func selfMember(): Option<Member> {
        match (state().member(selfAddress)) {
            case Some(m) where m.uid == selfUid => Some(m)
            case _ => None<Member>
        }
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
