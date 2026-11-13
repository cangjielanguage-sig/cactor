/*
 * CActor Persistence - 文件租约（DA4：不依赖外部服务的 Lease 实现，共享目录即协调点）
 *
 * 目录布局：<dir>/<hex(租约名)>.<token>.lease，每次成功获取新建一个编号更大的文件，编号就是 fencing token。
 *   文件内容：持有者、token、到期时间（持有者墙钟毫秒）、是否已释放。
 *
 * 为什么这样设计：
 * - 互斥靠 link(2) 的原子性：竞争者先把完整内容写进自己独有的临时文件并 fsync，再 link 成"当前最大编号 + 1"；
 *   目标已存在时 link 失败，同一个编号只可能有一个赢家。不用 O_EXCL 直接写目标，是为了读者永远看不到写了一半的内容；
 *   不用锁文件，是因为锁文件的持有者崩溃后需要"破锁"，破锁本身又需要租约。
 * - 当前状态 = 编号最大的文件。编号只增不减（旧文件只删比当前小的），token 天然单调，进程重启后也不回退。
 * - 续约 / 释放只由持有者改写自己那个编号的文件（临时文件 + rename 原子替换），改写后再确认最大编号仍是自己；
 *   已有更大编号说明期间被他人接手，续约按失败处理（AbstractLease 报告 lost）。
 *
 * 时间假设（与 cactor_foundation.coordination 文件头一致，部署前必须确认）：
 *   1. 共享目录所在文件系统提供原子 link / rename，以及跨宿主的写后读可见性（本地文件系统、NFS close-to-open 满足；
 *      最终一致的对象存储挂载不满足）。
 *   2. 宿主间墙钟偏差 < clockSkewMargin：竞争者用自己的墙钟判断"到期时间已过"，持有者用单调时钟提前 margin 失效。
 *   3. 持有者在有效期内完成续约；进程停顿跨过有效期会被判为 lost，停顿期间已发出的写入由 fencing token 拒绝。
 * 同一持有者名再次获取（例如进程重启后）总是拿到新编号：旧实例在途的写入因此也会被 fencing 拒绝。
 */
package cactor_persistence

import std.fs.*
import std.time.*
import std.convert.*
import std.collection.*
import cactor_foundation.coordination.{AbstractLease, LeaseSettings, LeaseException}
import cactor_foundation.detector.{monotonicMillis}
import cactor_foundation.hashing.{toHex}

foreign func link(oldpath: CString, newpath: CString): Int32

foreign func unlink(path: CString): Int32

/** 墙钟毫秒（Unix 纪元）：文件租约的到期时间要跨宿主比较，只能用墙钟 */
public func wallClockMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

struct FileLeaseRecord {
    let owner: String
    let token: Int64
    let expiresAt: Int64
    let released: Bool

    init(owner: String, token: Int64, expiresAt: Int64, released: Bool) {
        this.owner = owner
        this.token = token
        this.expiresAt = expiresAt
        this.released = released
    }

    func encode(): Array<UInt8> {
        RecordWriter().putString(owner).putInt64(token).putInt64(expiresAt).putInt64(if (released) { 1 } else { 0 })
            .toRecord()
    }
}

func linkFile(from: String, to: String): Bool {
    let a = unsafe { LibC.mallocCString(from) }
    let b = unsafe { LibC.mallocCString(to) }
    try {
        unsafe { link(a, b) } == 0
    } finally {
        unsafe {
            LibC.free(a)
            LibC.free(b)
        }
    }
}

func unlinkFile(path: String): Unit {
    let a = unsafe { LibC.mallocCString(path) }
    try {
        unsafe { unlink(a) }
    } finally {
        unsafe { LibC.free(a) }
    }
    ()
}

/**
 * 共享目录上的租约。所有竞争者（不同进程 / 宿主）用同一个 directory 和 leaseName、各自唯一的 ownerName。
 *
 * ```
 * let lease = FileLease("/shared/leases", LeaseSettings("singleton-ledger", "${addr}#${uid}"))
 * let singleton = ClusterSingleton(cluster, system, remote, "ledger", factory,
 *     ClusterSingletonSettings().withLease(lease))
 * ```
 * wallClock / monoClock 只为测试注入（例如模拟时钟偏差或停顿）。
 */
public class FileLease <: AbstractLease {
    private let dir: String
    private let prefix: String
    private let wallClock: () -> Int64
    private let sync: Bool

    public init(directory: String, settings: LeaseSettings) {
        this(directory, settings, wallClock: wallClockMillis, monoClock: monotonicMillis, sync: true)
    }

    public init(directory: String, settings: LeaseSettings, wallClock!: () -> Int64, monoClock!: () -> Int64,
        sync!: Bool) {
        super(settings, monoClock)
        this.dir = directory
        this.prefix = safeFileName(settings.leaseName)
        this.wallClock = wallClock
        this.sync = sync
        ensureDirectory(directory)
    }

    private func pathOf(token: Int64): String {
        "${dir}/${prefix}.${token}.lease"
    }

    /** 目录里的最大编号（没有文件时为 0） */
    private func latestToken(): Int64 {
        var best: Int64 = 0
        let head = prefix + "."
        for (info in Directory.readFrom(dir)) {
            let name = info.name
            if (name.startsWith(head) && name.endsWith(".lease")) {
                let mid = name[head.size..name.size - ".lease".size]
                if (let Some(n) <- Int64.tryParse(mid)) {
                    if (n > best) {
                        best = n
                    }
                }
            }
        }
        best
    }

    private func readRecord(token: Int64): FileLeaseRecord {
        let path = pathOf(token)
        let (records, _) = splitRecords(File.readFrom(path))
        if (records.isEmpty()) {
            // link / rename 保证不会读到半个文件；真出现时视为"仍被持有"，宁可不可用也不双主
            throw LeaseException("租约文件损坏：${path}")
        }
        let r = RecordReader(records[0])
        let owner = r.getString()
        let t = r.getInt64()
        let exp = r.getInt64()
        let rel = r.getInt64() == 1
        FileLeaseRecord(owner, t, exp, rel)
    }

    protected func tryAcquireOnce(): Option<Int64> {
        let now = wallClock()
        let current = latestToken()
        if (current > 0) {
            let rec = readRecord(current)
            let free = rec.released || now >= rec.expiresAt || rec.owner == settings.ownerName
            if (!free) {
                return None
            }
        }
        let next = current + 1
        let rec = FileLeaseRecord(settings.ownerName, next, now + settings.ttl.toMilliseconds(), false)
        // 临时文件名对每个竞争者唯一：持有者名 + 单调时钟，避免两个竞争者互相覆盖临时文件
        let tmp = "${dir}/.${prefix}.${next}.${toHex(settings.ownerName.toArray())}.${monotonicMillis()}.tmp"
        let f = File(tmp, OpenMode.Write)
        try {
            f.write(rec.encode())
            if (sync) {
                fsyncFile(f, tmp)
            } else {
                f.flush()
            }
        } finally {
            f.close()
        }
        let won = linkFile(tmp, pathOf(next))
        unlinkFile(tmp)
        if (!won) {
            if (exists(pathOf(next))) {
                // 同一编号已被别人抢先创建
                return None
            }
            throw LeaseException("创建租约文件失败：${pathOf(next)}")
        }
        if (sync) {
            syncDirectory(dir)
        }
        cleanupBelow(next - 1)
        Some(next)
    }

    /** 删除比 keepFrom 小的旧编号（保留上一个便于排查）；只删非最大编号，不影响当前状态 */
    private func cleanupBelow(keepFrom: Int64): Unit {
        try {
            let head = prefix + "."
            for (info in Directory.readFrom(dir)) {
                let name = info.name
                if (name.startsWith(head) && name.endsWith(".lease")) {
                    let mid = name[head.size..name.size - ".lease".size]
                    if (let Some(n) <- Int64.tryParse(mid)) {
                        if (n < keepFrom) {
                            remove("${dir}/${name}")
                        }
                    }
                }
            }
        } catch (_: Exception) {
            // 清理失败不影响正确性
        }
    }

    protected func renewOnce(token: Int64): Bool {
        let now = wallClock()
        if (latestToken() != token) {
            return false
        }
        let rec = readRecord(token)
        if (rec.owner != settings.ownerName || rec.released) {
            return false
        }
        writeFileAtomically(pathOf(token),
            FileLeaseRecord(settings.ownerName, token, now + settings.ttl.toMilliseconds(), false).encode(), sync)
        // 改写期间若已有人建了更大编号，这次续约不算数
        latestToken() == token
    }

    protected func releaseOnce(token: Int64): Bool {
        if (latestToken() != token) {
            return false
        }
        let rec = readRecord(token)
        if (rec.owner != settings.ownerName || rec.released) {
            return false
        }
        writeFileAtomically(pathOf(token), FileLeaseRecord(settings.ownerName, token, 0, true).encode(), sync)
        true
    }

    /** 目录里当前最大的 token（诊断用） */
    public func latestIssuedToken(): Int64 {
        latestToken()
    }
}
