/*
 * CActor - 持久状态原子替换写的并发隔离（DA8 混合版本长压暴露的缺陷）
 *
 * 任何时刻读者读到的都必须是完整记录（旧版本或新版本），不能出现"文件不存在"（修订号 0）或解析异常。
 * 两条用例分别覆盖替换写的两个缺陷：
 *   1. 单写者反复替换 + 并发读者：std.fs.rename(overwrite: true) 会先删目标再改名，读者在窗口内
 *      exists 后 open 会 ENOENT（修订号被读成 0）。修复：改用 POSIX rename(2) 原子替换。
 *   2. 两个写者（各自独立的 FileDurableStateStore，模拟两个实例/进程共享目录）+ 并发读者：
 *      旧实现用固定的 "<path>.tmp"，两个写者共享临时文件，后者 truncate 前者正在写的内容，
 *      前者 rename 后后者的 fd 直接指向目标 inode，读者会读到截断记录。修复：每次使用唯一临时名。
 *
 * 注意：FileDurableStateStore 本身不提供跨实例的修订号 CAS（这是 DA4 租约 + fencing 的职责）；
 * 用例 2 只断言"记录完整"，不要求两个无 fencing 的写者之间修订号单调。
 */
package cactor_tests.unit.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.random.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.*

func atomicTestDir(): String {
    let dir = "/tmp/cactor-da8-dstate-${Random().nextUInt64()}"
    Directory.create(dir, recursive: true)
    dir
}

func cleanupDir(dir: String): Unit {
    try {
        remove(dir, recursive: true)
    } catch (_: Exception) {}
}

/** 内容放大到约 2 KB，拉宽"写临时文件 → 替换"的窗口，让并发问题更容易出现 */
func bigPayload(): String {
    let sb = StringBuilder()
    for (_ in 0..500) {
        sb.append("0123")
    }
    sb.toString()
}

@Test
public class DurableStateAtomicReplaceTest {
    @TestCase
    public func singleWriterReplacementsNeverExposeMissingRecords(): Unit {
        let dir = atomicTestDir()
        try {
            let id = "da8-single"
            let writer = FileDurableStateStore(dir)
            let reader = FileDurableStateStore(dir)
            writer.upsert(id, 1, "init")
            let stop = AtomicBool(false)
            let missing = AtomicInt64(0)
            let errors = AtomicInt64(0)
            let regressions = AtomicInt64(0)
            let payload = bigPayload()
            let wt = spawn {
                var rev: Int64 = 1
                while (!stop.load()) {
                    rev++
                    writer.upsert(id, rev, "w-${rev}-${payload}")
                }
            }
            let rt = spawn {
                var last: Int64 = 1
                while (!stop.load()) {
                    try {
                        let r = reader.revisionOf(id)
                        if (r == 0) {
                            missing.fetchAdd(1)
                        } else if (r < last) {
                            regressions.fetchAdd(1)
                        }
                        last = max(last, r)
                    } catch (_: Exception) {
                        errors.fetchAdd(1)
                    }
                }
            }
            let deadline = MonoTime.now() + Duration.millisecond * 600
            while (MonoTime.now() < deadline) {
                sleep(Duration.millisecond * 10)
            }
            stop.store(true)
            wt.get()
            rt.get()
            @Expect(missing.load(), 0)
            @Expect(errors.load(), 0)
            @Expect(regressions.load(), 0)
            @Expect(reader.revisionOf(id) > 1, true)
        } finally {
            cleanupDir(dir)
        }
    }

    @TestCase
    public func concurrentWritersNeverExposePartialRecords(): Unit {
        let dir = atomicTestDir()
        try {
            let id = "da8-concurrent"
            let a = FileDurableStateStore(dir)
            let b = FileDurableStateStore(dir)
            let reader = FileDurableStateStore(dir)
            a.upsert(id, 1, "init")
            let stop = AtomicBool(false)
            let missing = AtomicInt64(0)
            let errors = AtomicInt64(0)
            let writes = AtomicInt64(0)
            let payload = bigPayload()
            let writer = { store: FileDurableStateStore, tag: String =>
                spawn {
                    while (!stop.load()) {
                        let rev = store.revisionOf(id) + 1
                        try {
                            store.upsert(id, rev, "${tag}-${rev}-${payload}")
                            writes.fetchAdd(1)
                        } catch (_: Exception) {
                            // 版本冲突（另一个写者先写了）是预期结果
                        }
                    }
                }
            }
            let wa = writer(a, "a")
            let wb = writer(b, "b")
            let rt = spawn {
                while (!stop.load()) {
                    try {
                        if (reader.revisionOf(id) == 0) {
                            missing.fetchAdd(1)
                        }
                    } catch (_: Exception) {
                        errors.fetchAdd(1)
                    }
                }
            }
            let deadline = MonoTime.now() + Duration.second
            while (MonoTime.now() < deadline) {
                sleep(Duration.millisecond * 10)
            }
            stop.store(true)
            wa.get()
            wb.get()
            rt.get()
            @Expect(missing.load(), 0)
            @Expect(errors.load(), 0)
            @Expect(writes.load() > 0, true)
            @Expect(reader.revisionOf(id) > 0, true)
        } finally {
            cleanupDir(dir)
        }
    }
}
