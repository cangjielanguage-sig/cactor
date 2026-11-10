/* CActor TestKit - 日志断言（E62，对标 Akka LoggingTestKit）
 *
 * 用法：
 *
 *   let logs = LoggingTestKit.start(LogLevel.Debug)   // 全局安装内存 sink，并把级别调到 Debug
 *   ...
 *   logs.expectMessage(LogLevel.Warn, "不可达", 3.seconds)
 *   logs.expectNoMessage(LogLevel.Error, "OOM", 500.millis)
 *   logs.close()                                      // 还原原来的 sink 与级别
 *
 * 全局性：foundation 的 Logging 是进程级单例，因此本工具在安装期间会替换全局 sink。
 * 不要与其他同时依赖真实 stderr 输出的用例并行；用例结束务必 close（或用 try/finally）。
 * 断言的是"从安装/上次 clear 之后新写入的记录"，已存在的记录不会误判为新日志。
 */
package cactor_testkit

import std.sync.*
import std.time.*
import std.collection.*
import cactor_foundation.logging.{LogLevel, LogRecord, Logging, MemorySink, StderrSink, LogSink}

public class LoggingTestKit {
    private let sink: MemorySink
    private let previousSink: LogSink
    private let previousLevel: LogLevel
    private let lock = Mutex()
    private var baseline: Int64 = 0
    private var closed: Bool = false

    private init(level: LogLevel) {
        this.sink = MemorySink()
        this.previousSink = Logging.currentSink()
        this.previousLevel = Logging.level()
        Logging.setLevel(level)
        Logging.setSink(sink)
        this.baseline = sink.snapshot().size
    }

    /** 安装内存 sink 并设置最低级别 */
    public static func start(level: LogLevel): LoggingTestKit {
        LoggingTestKit(level)
    }

    /** 默认级别 Debug（断言 warn/error 时也能看到上下文） */
    public static func start(): LoggingTestKit {
        LoggingTestKit(LogLevel.Debug)
    }

    /** 等一条 level 及以上、文本包含 contains 的日志；超时抛 TestKitAssertionException */
    public func expectMessage(level: LogLevel, contains: String, timeout: Duration): LogRecord {
        let deadline = MonoTime.now() + timeout
        var found = findNew(level, contains)
        while (found.isNone()) {
            let left = deadline - MonoTime.now()
            if (left <= Duration.Zero) {
                throw TestKitAssertionException("${timeout} 内没有出现 ${level.label()} 级别、包含「${contains}」的日志；最近记录：${recent()}")
            }
            sleep(Duration.millisecond * 5)
            found = findNew(level, contains)
        }
        found.getOrThrow()
    }

    public func expectMessage(level: LogLevel, contains: String): LogRecord {
        expectMessage(level, contains, Duration.second * 3)
    }

    /** duration 内不应出现匹配的日志 */
    public func expectNoMessage(level: LogLevel, contains: String, duration: Duration): Unit {
        let deadline = MonoTime.now() + duration
        while (MonoTime.now() < deadline) {
            if (let Some(r) <- findNew(level, contains)) {
                throw TestKitAssertionException("不应出现但出现了：${r.format()}")
            }
            sleep(Duration.millisecond * 5)
        }
    }

    public func expectNoMessage(level: LogLevel, contains: String): Unit {
        expectNoMessage(level, contains, Duration.millisecond * 500)
    }

    /** 当前缓冲的全部记录（安装之后写入的） */
    public func records(): Array<LogRecord> {
        sink.snapshot()
    }

    /** 丢弃已收集的记录，之后的断言只看新日志 */
    public func clear(): Unit {
        sink.clear()
        synchronized(lock) {
            baseline = 0
        }
    }

    /** 还原 sink 与级别；可重复调用 */
    public func close(): Unit {
        synchronized(lock) {
            if (closed) {
                return
            }
            closed = true
        }
        Logging.setSink(previousSink)
        Logging.setLevel(previousLevel)
    }

    /** 只匹配本用例新写入的记录（下标 >= baseline），并且级别 >= 请求级别 */
    private func findNew(level: LogLevel, contains: String): Option<LogRecord> {
        let all = sink.snapshot()
        var i = synchronized(lock) { baseline }
        while (i < all.size) {
            let r = all[i]
            if (r.level.ordinal() >= level.ordinal() && r.message.contains(contains)) {
                return Some(r)
            }
            i++
        }
        None<LogRecord>
    }

    private func recent(): String {
        let all = sink.snapshot()
        let from = if (all.size > 5) { all.size - 5 } else { 0 }
        let sb = StringBuilder()
        for (i in from..all.size) {
            if (i > from) {
                sb.append(" | ")
            }
            sb.append(all[i].format())
        }
        sb.toString()
    }
}
