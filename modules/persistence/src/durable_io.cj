/*
 * CActor Persistence - 落盘工具（B7）
 *
 * 为什么需要 FFI：std.fs.File.flush 只把用户态缓冲交给内核，数据仍在页缓存里，断电或内核崩溃会丢；
 * 标准库没有 fsync，这里通过仓颉 FFI 直接调用 libc 的 fsync(2)。
 * - fsyncFile：文件内容与元数据落盘（日志追加、临时文件写完后调用）；
 * - syncDirectory：目录项落盘——新建文件、rename 之后不 fsync 目录，断电后文件可能"不存在"或仍是旧版本；
 * - writeFileAtomically：写临时文件 → fsync → rename 覆盖 → fsync 目录，任何时刻读到的要么是旧版本要么是新版本，
 *   且返回后新版本已落盘（快照、持久状态、日志改写共用）。
 */
package cactor_persistence

import std.fs.*
import std.sync.*

foreign func fsync(fd: Int32): Int32

foreign func open(path: CString, flags: Int32, ...): Int32

foreign func close(fd: Int32): Int32

// Linux：O_RDONLY = 0，O_DIRECTORY = 0o200000
let O_DIRECTORY_READ: Int32 = 0o200000

// 进程内 fsync 次数（含目录），供测试与统计观察组提交的效果
let fsyncCounter = AtomicInt64(0)

/** 进程内累计的 fsync 次数（含目录 fsync） */
public func persistenceFsyncCount(): Int64 {
    fsyncCounter.load()
}

/** 把已 flush 的文件内容落盘；失败抛 PersistenceException（不能假装已落盘） */
func fsyncFile(f: File, what: String): Unit {
    f.flush()
    let fd = Int32(f.fileDescriptor.fileHandle)
    let rc = unsafe { fsync(fd) }
    fsyncCounter.fetchAdd(1)
    if (rc != 0) {
        throw PersistenceException("fsync 失败（${what}）")
    }
}

/** 目录项落盘（新建 / rename 之后调用） */
func syncDirectory(dir: String): Unit {
    let cs = unsafe { LibC.mallocCString(dir) }
    try {
        let fd = unsafe { open(cs, O_DIRECTORY_READ) }
        if (fd < 0) {
            throw PersistenceException("打开目录失败，无法 fsync：${dir}")
        }
        let rc = unsafe { fsync(fd) }
        unsafe { close(fd) }
        fsyncCounter.fetchAdd(1)
        if (rc != 0) {
            throw PersistenceException("目录 fsync 失败：${dir}")
        }
    } finally {
        unsafe { LibC.free(cs) }
    }
}

/** 路径所在目录（没有 / 时为当前目录） */
func parentDirectory(path: String): String {
    match (path.lastIndexOf("/")) {
        case Some(0) => "/"
        case Some(i) => path[0..i]
        case None => "."
    }
}

/**
 * 原子替换写：tmp → fsync → rename → fsync 目录。sync = false 时只保证原子替换、不保证落盘（测试或可丢数据的场景）
 */
func writeFileAtomically(path: String, content: Array<UInt8>, sync: Bool): Unit {
    let tmp = path + ".tmp"
    let f = File(tmp, OpenMode.Write)
    try {
        f.write(content)
        if (sync) {
            fsyncFile(f, tmp)
        } else {
            f.flush()
        }
    } finally {
        f.close()
    }
    rename(tmp, to: path, overwrite: true)
    if (sync) {
        syncDirectory(parentDirectory(path))
    }
}
