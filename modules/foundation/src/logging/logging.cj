/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 日志抽象
 *
 * - 级别：TRACE < DEBUG < INFO < WARN < ERROR < OFF，默认 WARN，可用环境变量 CACTOR_LOG_LEVEL 设置
 * - 输出：默认写 stderr，可用 Logging.setSink 替换（例如接入 stdx.log，或测试中收集日志）
 * - 热路径：先用 isEnabled 判断，或使用接受闭包的重载，避免级别关闭时仍做字符串拼接
 */
package cactor_foundation.logging

import std.sync.*
import std.time.*
import std.env.*
import std.collection.*

public enum LogLevel {
    | Trace
    | Debug
    | Info
    | Warn
    | Error
    | Off

    public func ordinal(): Int64 {
        match (this) {
            case Trace => 0
            case Debug => 1
            case Info => 2
            case Warn => 3
            case Error => 4
            case Off => 5
        }
    }

    public func label(): String {
        match (this) {
            case Trace => "TRACE"
            case Debug => "DEBUG"
            case Info => "INFO"
            case Warn => "WARN"
            case Error => "ERROR"
            case Off => "OFF"
        }
    }

    public static func parse(text: String): Option<LogLevel> {
        match (text.trimAscii().toAsciiLower()) {
            case "trace" => Some(Trace)
            case "debug" => Some(Debug)
            case "info" => Some(Info)
            case "warn" | "warning" => Some(Warn)
            case "error" => Some(Error)
            case "off" | "none" => Some(Off)
            case _ => None
        }
    }

    public static func fromOrdinal(n: Int64): LogLevel {
        match (n) {
            case 0 => Trace
            case 1 => Debug
            case 2 => Info
            case 3 => Warn
            case 4 => Error
            case _ => Off
        }
    }
}

/** 一条日志 */
public struct LogRecord {
    public let level: LogLevel
    public let source: String
    public let message: String
    public let timestampMillis: Int64

    public init(level: LogLevel, source: String, message: String, timestampMillis: Int64) {
        this.level = level
        this.source = source
        this.message = message
        this.timestampMillis = timestampMillis
    }

    public func format(): String {
        "${timestampMillis} ${level.label()} [${source}] ${message}"
    }
}

/** 日志输出端 */
public interface LogSink {
    func write(record: LogRecord): Unit
}

/** 写到 stderr（加锁，避免多线程输出交错） */
public class StderrSink <: LogSink {
    private let lock = Mutex()

    public init() {}

    public func write(record: LogRecord): Unit {
        let line = record.format() + "\n"
        synchronized(lock) {
            let err = getStdErr()
            err.write(line)
            err.flush()
        }
    }
}

/** 在内存中收集日志，主要用于测试断言 */
public class MemorySink <: LogSink {
    private let lock = Mutex()
    private let records = ArrayList<LogRecord>()

    public init() {}

    public func write(record: LogRecord): Unit {
        synchronized(lock) {
            records.add(record)
        }
    }

    public func snapshot(): Array<LogRecord> {
        synchronized(lock) {
            records.toArray()
        }
    }

    public func clear(): Unit {
        synchronized(lock) {
            records.clear()
        }
    }

    /** 包含指定文本的记录条数 */
    public func count(level: LogLevel, contains: String): Int64 {
        var n: Int64 = 0
        for (r in snapshot()) {
            if (r.level.ordinal() == level.ordinal() && r.message.contains(contains)) {
                n++
            }
        }
        n
    }
}

/** 全局日志配置 */
public class Logging {
    private static let levelValue: AtomicInt64 = AtomicInt64(initialLevel())
    private static let sinkLock: Mutex = Mutex()
    private static var sink: LogSink = StderrSink()

    private static func initialLevel(): Int64 {
        if (let Some(v) <- getVariable("CACTOR_LOG_LEVEL")) {
            if (let Some(l) <- LogLevel.parse(v)) {
                return l.ordinal()
            }
        }
        LogLevel.Warn.ordinal()
    }

    public static func setLevel(level: LogLevel): Unit {
        levelValue.store(level.ordinal())
    }

    public static func level(): LogLevel {
        LogLevel.fromOrdinal(levelValue.load())
    }

    public static func setSink(newSink: LogSink): Unit {
        synchronized(sinkLock) {
            sink = newSink
        }
    }

    public static func isEnabled(level: LogLevel): Bool {
        level.ordinal() >= levelValue.load() && level.ordinal() < LogLevel.Off.ordinal()
    }

    public static func logger(source: String): Logger {
        Logger(source)
    }

    static func emit(level: LogLevel, source: String, message: String): Unit {
        let record = LogRecord(level, source, message, DateTime.now().toUnixTimeStamp().toMilliseconds())
        let target = synchronized(sinkLock) { sink }
        try {
            target.write(record)
        } catch (_: Exception) {
            // 日志输出失败不能影响业务
        }
    }
}

/** 带来源标签的日志记录器（无状态，可随意创建） */
public class Logger {
    public let source: String

    public init(source: String) {
        this.source = source
    }

    public func isEnabled(level: LogLevel): Bool {
        Logging.isEnabled(level)
    }

    public func log(level: LogLevel, message: String): Unit {
        if (Logging.isEnabled(level)) {
            Logging.emit(level, source, message)
        }
    }

    /** 惰性版本：级别关闭时不会执行 message 闭包 */
    public func log(level: LogLevel, message: () -> String): Unit {
        if (Logging.isEnabled(level)) {
            Logging.emit(level, source, message())
        }
    }

    public func trace(message: String): Unit { log(LogLevel.Trace, message) }
    public func debug(message: String): Unit { log(LogLevel.Debug, message) }
    public func info(message: String): Unit { log(LogLevel.Info, message) }
    public func warn(message: String): Unit { log(LogLevel.Warn, message) }
    public func error(message: String): Unit { log(LogLevel.Error, message) }

    public func debug(message: () -> String): Unit { log(LogLevel.Debug, message) }
    public func info(message: () -> String): Unit { log(LogLevel.Info, message) }
}
