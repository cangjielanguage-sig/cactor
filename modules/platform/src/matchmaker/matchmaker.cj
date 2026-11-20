/*
 * CActor Platform.Matchmaker - 按属性匹配（F3 / AR9）
 *
 * 提交一张票（mode + 属性表 + 可选权重），第一个兼容的等待票与之配对；
 * 超过 TTL 未匹配则返回 Expired。匹配只在同一 mode 的等待列表内进行，
 * 不会全表扫描；属性兼容 = 所有共同键取值相同（未列出的键不参与约束）。
 *
 * 并发安全：匹配与入队在同一把锁内完成，同一张票不会被匹配两次；
 * 后台清扫线程只负责过期，50ms 粒度足够（TTL 是业务量级，不是调度精度）。
 */
package cactor_platform.matchmaker

import std.collection.*
import std.math.*
import std.sync.*
import std.time.*
import cactor_core.typed.{TypedPromise}

/** 一张匹配票；attrs 是自由形式的属性表（例如 region/rank/level），rank 用于区间匹配 */
public class MatchTicket {
    public let id: String
    public let mode: String
    public let attrs: HashMap<String, String>
    public let rank: Float64

    public init(id: String, mode: String, attrs: HashMap<String, String>, rank!: Float64 = 0.0) {
        this.id = id
        this.mode = mode
        this.attrs = attrs
        this.rank = rank
    }
}

/** 匹配结果：Matched 带双方票据；Expired 带超时的那张 */
public enum MatchResult {
    | Matched(MatchTicket, MatchTicket)
    | Expired(MatchTicket)
}

/** 等待中的票（内部） */
class WaitingTicket {
    let ticket: MatchTicket
    let promise: TypedPromise<MatchResult>
    let deadline: MonoTime

    init(ticket: MatchTicket, promise: TypedPromise<MatchResult>, deadline: MonoTime) {
        this.ticket = ticket
        this.promise = promise
        this.deadline = deadline
    }
}

/**
 * Matchmaker：可单机使用，也可每个节点各建一个（票只在本地队列匹配；跨节点匹配需把
 * 提交路由到同一实例，例如经集群单例或分片）。
 */
public class Matchmaker {
    private let lock = Mutex()
    private let waiting = HashMap<String, ArrayList<WaitingTicket>>()
    private let rankTolerance: Float64
    private let sweepInterval: Duration
    private let running: AtomicBool = AtomicBool(true)
    private var sweeper: Option<Future<Unit>> = None<Future<Unit>>

    private init(rankTolerance: Float64, sweepInterval: Duration) {
        this.rankTolerance = rankTolerance
        this.sweepInterval = sweepInterval
    }

    /** 创建并启动后台过期清扫；用静态工厂避免在 init 里让 this 逃逸到新线程 */
    public static func create(rankTolerance!: Float64 = 0.0,
        sweepInterval!: Duration = Duration.millisecond * 50): Matchmaker {
        if (sweepInterval <= Duration.Zero) {
            throw IllegalArgumentException("清扫间隔必须为正")
        }
        let m = Matchmaker(rankTolerance, sweepInterval)
        let me = m
        m.sweeper = Some<Future<Unit>>(spawn {
            me.sweepLoop()
        })
        m
    }

    private func compatible(a: MatchTicket, b: MatchTicket): Bool {
        if (a.mode != b.mode) {
            return false
        }
        if (rankTolerance > 0.0 && abs(a.rank - b.rank) > rankTolerance) {
            return false
        }
        for ((k, v) in a.attrs) {
            match (b.attrs.get(k)) {
                case Some(other) =>
                    if (other != v) {
                        return false
                    }
                case None => ()
            }
        }
        true
    }

    /**
     * 提交一张票，返回匹配结果；未匹配时在 ttl 内等待，超时以 Expired 完成。
     * 不阻塞提交线程，结果从 Promise 取。
     */
    public func submit(ticket: MatchTicket, ttl!: Duration = Duration.second * 5): TypedPromise<MatchResult> {
        if (ttl <= Duration.Zero) {
            throw IllegalArgumentException("匹配 TTL 必须为正")
        }
        let promise = TypedPromise<MatchResult>()
        var partner: Option<WaitingTicket> = None<WaitingTicket>
        synchronized(lock) {
            match (waiting.get(ticket.mode)) {
                case Some(list) =>
                    var index: Int64 = -1
                    for (i in 0..list.size) {
                        if (compatible(ticket, list[i].ticket)) {
                            index = i
                            break
                        }
                    }
                    if (index >= 0) {
                        partner = Some<WaitingTicket>(list[index])
                        list.remove(at: index)
                    } else {
                        list.add(WaitingTicket(ticket, promise, MonoTime.now() + ttl))
                    }
                case None =>
                    let list = ArrayList<WaitingTicket>()
                    list.add(WaitingTicket(ticket, promise, MonoTime.now() + ttl))
                    waiting[ticket.mode] = list
            }
        }
        // 完成回调在锁外：使用方的 onComplete 不会与匹配锁互相阻塞
        match (partner) {
            case Some(other) =>
                other.promise.trySuccess(MatchResult.Matched(other.ticket, ticket))
                promise.trySuccess(MatchResult.Matched(ticket, other.ticket))
            case None => ()
        }
        promise
    }

    private func sweepLoop(): Unit {
        while (running.load()) {
            sleep(sweepInterval)
            expireNow()
        }
    }

    /** 立即清扫一次过期票（测试与手动驱动用；后台线程也会定期调用） */
    public func expireNow(): Unit {
        let now = MonoTime.now()
        let expired = ArrayList<WaitingTicket>()
        synchronized(lock) {
            let modes = ArrayList<String>()
            for ((mode, _) in waiting) {
                modes.add(mode)
            }
            for (mode in modes) {
                let list = waiting[mode]
                let keep = ArrayList<WaitingTicket>()
                for (w in list) {
                    if (now >= w.deadline) {
                        expired.add(w)
                    } else {
                        keep.add(w)
                    }
                }
                if (keep.size == 0) {
                    waiting.remove(mode)
                } else {
                    waiting[mode] = keep
                }
            }
        }
        for (w in expired) {
            w.promise.trySuccess(MatchResult.Expired(w.ticket))
        }
    }

    /** 停止清扫线程，并把仍在等待的票按 Expired 完成（调用方不会永久挂起） */
    public func stop(): Unit {
        running.store(false)
        match (sweeper) {
            case Some(f) =>
                try {
                    f.get(Duration.second * 2)
                } catch (e: Exception) {
                    ()
                }
            case None => ()
        }
        let remaining = ArrayList<WaitingTicket>()
        synchronized(lock) {
            for ((_, list) in waiting) {
                for (w in list) {
                    remaining.add(w)
                }
            }
            waiting.clear()
        }
        for (w in remaining) {
            w.promise.trySuccess(MatchResult.Expired(w.ticket))
        }
    }

    /** 当前等待中的票数（诊断/断言） */
    public func waitingCount(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, list) in waiting) {
                n += Int64(list.size)
            }
            n
        }
    }
}
