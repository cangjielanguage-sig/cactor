/*
 * CActor Platform.Matchmaker - Lobby 大厅（F3 扩展，对标 rivet-matchmaker 的 Lobby 操作）
 *
 * 与 Matchmaker（把两张等待票配对）不同，Lobby 是一个"已创建、可再加入"的房间：
 * 容量、就绪状态、连接状态与关闭状态都显式可查。操作全部在同一把锁内完成，
 * 返回的都是不可变快照，调用方后续修改不会影响内部状态。
 *
 * 语义对齐 Rivet：
 *   FindLobby          -> find(mode, attrs)
 *   JoinLobby          -> join(lobbyId, playerId)
 *   ListLobbies        -> list()
 *   LobbyReady         -> setReady(lobbyId, playerId)
 *   SetLobbyClosed     -> close(lobbyId)
 *   PlayerConnected    -> playerConnected(lobbyId, playerId, connected: true)
 *   PlayerDisconnected -> playerConnected(lobbyId, playerId, connected: false)
 *
 * 单节点实现：跨节点大厅需要把注册表放在集群单例或分片上（与 Matchmaker 相同）。
 */
package cactor_platform.matchmaker

import std.collection.*
import std.sync.*
import std.time.*

/** 大厅里的玩家快照 */
public class LobbyPlayer {
    public let playerId: String
    public let ready: Bool
    public let connected: Bool
    public let joinedAtMillis: Int64

    public init(playerId: String, ready: Bool, connected: Bool, joinedAtMillis: Int64) {
        this.playerId = playerId
        this.ready = ready
        this.connected = connected
        this.joinedAtMillis = joinedAtMillis
    }
}

/** 大厅快照：不可变，可安全跨线程传递 */
public class LobbyInfo {
    public let id: String
    public let mode: String
    public let attrs: HashMap<String, String>
    public let capacity: Int64
    public let players: Array<LobbyPlayer>
    public let ready: Bool
    public let closed: Bool
    public let createdAtMillis: Int64

    public init(id: String, mode: String, attrs: HashMap<String, String>, capacity: Int64,
        players: Array<LobbyPlayer>, ready: Bool, closed: Bool, createdAtMillis: Int64) {
        this.id = id
        this.mode = mode
        this.attrs = attrs
        this.capacity = capacity
        this.players = players
        this.ready = ready
        this.closed = closed
        this.createdAtMillis = createdAtMillis
    }

    public func playerCount(): Int64 {
        Int64(players.size)
    }

    public func isFull(): Bool {
        Int64(players.size) >= capacity
    }
}

/** 大厅操作结果：失败带明确原因，成功附带最新快照 */
public struct LobbyOutcome {
    public let ok: Bool
    public let reason: String
    public let lobby: Option<LobbyInfo>

    public init(ok: Bool, reason: String, lobby: Option<LobbyInfo>) {
        this.ok = ok
        this.reason = reason
        this.lobby = lobby
    }
}

/** 内部可变的玩家条目 */
class PlayerEntry {
    let playerId: String
    let joinedAtMillis: Int64
    var ready: Bool = false
    var connected: Bool = true

    init(playerId: String, joinedAtMillis: Int64) {
        this.playerId = playerId
        this.joinedAtMillis = joinedAtMillis
    }
}

/** 内部可变的大厅条目 */
class LobbyEntry {
    let id: String
    let mode: String
    let attrs: HashMap<String, String>
    let capacity: Int64
    let createdAtMillis: Int64
    let players = ArrayList<PlayerEntry>()
    var closed: Bool = false

    init(id: String, mode: String, attrs: HashMap<String, String>, capacity: Int64,
        createdAtMillis: Int64) {
        this.id = id
        this.mode = mode
        this.attrs = attrs
        this.capacity = capacity
        this.createdAtMillis = createdAtMillis
    }

    func indexOf(playerId: String): Int64 {
        for (i in 0..players.size) {
            if (players[i].playerId == playerId) {
                return i
            }
        }
        -1
    }
}

/** 大厅注册表：单节点、线程安全 */
public class LobbyRegistry {
    private let lock = Mutex()
    private let lobbies = HashMap<String, LobbyEntry>()
    private var seq: Int64 = 0

    public init() {}

    private func nowMillis(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }

    /** 属性兼容 = 公共键取值相同（与 Matchmaker.compatible 同口径） */
    private func compatible(a: HashMap<String, String>, b: HashMap<String, String>): Bool {
        for ((k, v) in a) {
            match (b.get(k)) {
                case Some(other) =>
                    if (other != v) {
                        return false
                    }
                case None => ()
            }
        }
        true
    }

    private func copyAttrs(attrs: HashMap<String, String>): HashMap<String, String> {
        let out = HashMap<String, String>()
        for ((k, v) in attrs) {
            out[k] = v
        }
        out
    }

    private func infoOf(entry: LobbyEntry): LobbyInfo {
        let players = ArrayList<LobbyPlayer>()
        var allReady = entry.players.size > 0
        for (p in entry.players) {
            players.add(LobbyPlayer(p.playerId, p.ready, p.connected, p.joinedAtMillis))
            if (!p.ready) {
                allReady = false
            }
        }
        LobbyInfo(entry.id, entry.mode, copyAttrs(entry.attrs), entry.capacity,
            players.toArray(), allReady, entry.closed, entry.createdAtMillis)
    }

    private func createLocked(mode: String, attrs: HashMap<String, String>, capacity: Int64): LobbyInfo {
        seq++
        let entry = LobbyEntry("lobby-${seq}", mode, copyAttrs(attrs), capacity, nowMillis())
        lobbies[entry.id] = entry
        infoOf(entry)
    }

    /** 创建大厅；容量必须为正，否则抛 IllegalArgumentException */
    public func createLobby(mode: String, attrs: HashMap<String, String>, capacity!: Int64 = 2): LobbyInfo {
        if (capacity <= 0) {
            throw IllegalArgumentException("大厅容量必须为正: ${capacity}")
        }
        var info = LobbyInfo("", mode, HashMap<String, String>(), capacity, Array<LobbyPlayer>(),
            false, false, 0)
        synchronized(lock) {
            info = createLocked(mode, attrs, capacity)
        }
        info
    }

    private func joinLocked(lobbyId: String, playerId: String): LobbyOutcome {
        match (lobbies.get(lobbyId)) {
            case None => LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
            case Some(entry) =>
                if (entry.closed) {
                    LobbyOutcome(false, "lobby is closed", Some<LobbyInfo>(infoOf(entry)))
                } else if (entry.indexOf(playerId) >= 0) {
                    LobbyOutcome(false, "player already in lobby", Some<LobbyInfo>(infoOf(entry)))
                } else if (Int64(entry.players.size) >= entry.capacity) {
                    LobbyOutcome(false, "lobby is full", Some<LobbyInfo>(infoOf(entry)))
                } else {
                    entry.players.add(PlayerEntry(playerId, nowMillis()))
                    LobbyOutcome(true, "", Some<LobbyInfo>(infoOf(entry)))
                }
        }
    }

    /** 加入大厅（JoinLobby）：不存在/已关闭/已满/重复加入都给出明确原因 */
    public func join(lobbyId: String, playerId: String): LobbyOutcome {
        var out = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        synchronized(lock) {
            out = joinLocked(lobbyId, playerId)
        }
        out
    }

    private func leaveLocked(lobbyId: String, playerId: String): LobbyOutcome {
        match (lobbies.get(lobbyId)) {
            case None => LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
            case Some(entry) =>
                let idx = entry.indexOf(playerId)
                if (idx < 0) {
                    LobbyOutcome(false, "player not in lobby", Some<LobbyInfo>(infoOf(entry)))
                } else {
                    entry.players.remove(at: idx)
                    LobbyOutcome(true, "", Some<LobbyInfo>(infoOf(entry)))
                }
        }
    }

    /** 离开大厅（供组队加入失败时回滚；不是 Rivet 的独立操作） */
    public func leave(lobbyId: String, playerId: String): LobbyOutcome {
        var out = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        synchronized(lock) {
            out = leaveLocked(lobbyId, playerId)
        }
        out
    }

    private func setReadyLocked(lobbyId: String, playerId: String, ready: Bool): LobbyOutcome {
        match (lobbies.get(lobbyId)) {
            case None => LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
            case Some(entry) =>
                let idx = entry.indexOf(playerId)
                if (idx < 0) {
                    LobbyOutcome(false, "player not in lobby", Some<LobbyInfo>(infoOf(entry)))
                } else if (entry.closed) {
                    LobbyOutcome(false, "lobby is closed", Some<LobbyInfo>(infoOf(entry)))
                } else {
                    entry.players[idx].ready = ready
                    LobbyOutcome(true, "", Some<LobbyInfo>(infoOf(entry)))
                }
        }
    }

    /** 设置玩家就绪（LobbyReady）；所有玩家就绪时快照的 ready=true */
    public func setReady(lobbyId: String, playerId: String, ready!: Bool = true): LobbyOutcome {
        var out = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        synchronized(lock) {
            out = setReadyLocked(lobbyId, playerId, ready)
        }
        out
    }

    private func connectedLocked(lobbyId: String, playerId: String, connected: Bool): LobbyOutcome {
        match (lobbies.get(lobbyId)) {
            case None => LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
            case Some(entry) =>
                let idx = entry.indexOf(playerId)
                if (idx < 0) {
                    LobbyOutcome(false, "player not in lobby", Some<LobbyInfo>(infoOf(entry)))
                } else {
                    entry.players[idx].connected = connected
                    LobbyOutcome(true, "", Some<LobbyInfo>(infoOf(entry)))
                }
        }
    }

    /** 玩家连接状态（PlayerConnected / PlayerDisconnected）：只改连接位，不改变成员关系 */
    public func playerConnected(lobbyId: String, playerId: String, connected!: Bool = true): LobbyOutcome {
        var out = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        synchronized(lock) {
            out = connectedLocked(lobbyId, playerId, connected)
        }
        out
    }

    private func closeLocked(lobbyId: String): LobbyOutcome {
        match (lobbies.get(lobbyId)) {
            case None => LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
            case Some(entry) =>
                entry.closed = true
                LobbyOutcome(true, "", Some<LobbyInfo>(infoOf(entry)))
        }
    }

    /** 关闭大厅（SetLobbyClosed）：幂等；关闭后拒绝新玩家，已有玩家仍可读 */
    public func close(lobbyId: String): LobbyOutcome {
        var out = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        synchronized(lock) {
            out = closeLocked(lobbyId)
        }
        out
    }

    /** 查找未关闭、属性兼容且未满的大厅（FindLobby）；没有则 None */
    public func find(mode: String, attrs: HashMap<String, String>): Option<LobbyInfo> {
        var out = None<LobbyInfo>
        synchronized(lock) {
            for ((_, entry) in lobbies) {
                if (!entry.closed && entry.mode == mode && compatible(entry.attrs, attrs)
                    && Int64(entry.players.size) < entry.capacity) {
                    out = Some<LobbyInfo>(infoOf(entry))
                    break
                }
            }
        }
        out
    }

    /** 大厅快照（不存在返回 None） */
    public func get(lobbyId: String): Option<LobbyInfo> {
        var out = None<LobbyInfo>
        synchronized(lock) {
            match (lobbies.get(lobbyId)) {
                case Some(entry) => out = Some<LobbyInfo>(infoOf(entry))
                case None => ()
            }
        }
        out
    }

    /** 大厅列表（ListLobbies）；默认只列未关闭的大厅 */
    public func list(openOnly!: Bool = true): Array<LobbyInfo> {
        let out = ArrayList<LobbyInfo>()
        synchronized(lock) {
            for ((_, entry) in lobbies) {
                if (!openOnly || !entry.closed) {
                    out.add(infoOf(entry))
                }
            }
        }
        out.toArray()
    }

    /** 移除大厅（清理用）；返回是否确实存在 */
    public func remove(lobbyId: String): Bool {
        var existed = false
        synchronized(lock) {
            existed = lobbies.contains(lobbyId)
            if (existed) {
                lobbies.remove(lobbyId)
            }
        }
        existed
    }

    public func count(): Int64 {
        var n: Int64 = 0
        synchronized(lock) {
            n = Int64(lobbies.size)
        }
        n
    }
}
