/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 日志抽象
 *
 * - 级别：TRACE < DEBUG < INFO < WARN < ERROR < OFF，默认 WARN，可用环境变量 CACTOR_LOG_LEVEL 设置
 * - 输出：默认写 stderr，可用 Logging.setSink 替换（例如接入 stdx.log，或测试中收集日志）
 * - 热路径：先用 isEnabled 判断，或使用接受闭包的重载，避免级别关闭时仍做字符串拼接
 * - 结构化字段：Logger.withField 附加固定字段；Mdc 附加当前线程（协程）的上下文字段；
 *   JsonLinesSink 以每行一个 JSON 对象输出，便于日志平台采集
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
    /** 结构化字段（Logger 的固定字段在前，MDC 字段在后） */
    public let fields: Array<(String, String)>

    public init(level: LogLevel, source: String, message: String, timestampMillis: Int64) {
        this(level, source, message, timestampMillis, Array<(String, String)>())
    }

    public init(level: LogLevel, source: String, message: String, timestampMillis: Int64, fields: Array<(String, String)>) {
        this.level = level
        this.source = source
        this.message = message
        this.timestampMillis = timestampMillis
        this.fields = fields
    }

    /** 字段值（同名取最后一个） */
    public func field(key: String): Option<String> {
        var result = None<String>
        for ((k, v) in fields) {
            if (k == key) {
                result = Some(v)
            }
        }
        result
    }

    public func format(): String {
        if (fields.isEmpty()) {
            return "${timestampMillis} ${level.label()} [${source}] ${message}"
        }
        let sb = StringBuilder("${timestampMillis} ${level.label()} [${source}] ${message}")
        for ((k, v) in fields) {
            sb.append(" ")
            sb.append(k)
            sb.append("=")
            sb.append(v)
        }
        sb.toString()
    }

    /** 一个 JSON 对象（单行）：ts、level、source、message 以及全部字段 */
    public func toJson(): String {
        let sb = StringBuilder()
        sb.append("{\"ts\":${timestampMillis},\"level\":")
        appendJsonString(sb, level.label())
        sb.append(",\"source\":")
        appendJsonString(sb, source)
        sb.append(",\"message\":")
        appendJsonString(sb, message)
        for ((k, v) in fields) {
            sb.append(",")
            appendJsonString(sb, k)
            sb.append(":")
            appendJsonString(sb, v)
        }
        sb.append("}")
        sb.toString()
    }
}

func appendJsonString(sb: StringBuilder, s: String): Unit {
    sb.append("\"")
    for (r in s.runes()) {
        match (r) {
            case '"' => sb.append("\\\"")
            case '\\' => sb.append("\\\\")
            case '\n' => sb.append("\\n")
            case '\r' => sb.append("\\r")
            case '\t' => sb.append("\\t")
            case _ =>
                let c = UInt32(r)
                if (c < 0x20) {
                    sb.append("\\u00")
                    let hex = "0123456789abcdef"
                    sb.append(Rune(UInt32(hex[Int64(c >> 4)])))
                    sb.append(Rune(UInt32(hex[Int64(c & 0xF)])))
                } else {
                    sb.append(r)
                }
        }
    }
    sb.append("\"")
}

/**
 * MDC（Mapped Diagnostic Context）：当前线程（仓颉协程）上附加到每条日志的字段，
 * 例如请求 ID、租户。Actor 在共享线程上执行，因此在 Actor 中请用 Mdc.scoped，
 * 它在代码块结束（包括抛出异常）时恢复之前的字段，不会泄漏给同一线程上的下一个 Actor。
 */
public class Mdc {
    private static let current: ThreadLocal<Array<(String, String)>> = ThreadLocal<Array<(String, String)>>()

    public static func get(): Array<(String, String)> {
        current.get() ?? Array<(String, String)>()
    }

    public static func get(key: String): Option<String> {
        var result = None<String>
        for ((k, v) in get()) {
            if (k == key) {
                result = Some(v)
            }
        }
        result
    }

    public static func put(key: String, value: String): Unit {
        let next = ArrayList<(String, String)>()
        for ((k, v) in get()) {
            if (k != key) {
                next.add((k, v))
            }
        }
        next.add((key, value))
        current.set(next.toArray())
    }

    public static func remove(key: String): Unit {
        let next = ArrayList<(String, String)>()
        for ((k, v) in get()) {
            if (k != key) {
                next.add((k, v))
            }
        }
        current.set(next.toArray())
    }

    public static func clear(): Unit {
        current.set(None<Array<(String, String)>>)
    }

    /** 在 block 执行期间附加 fields，结束后恢复原来的 MDC */
    public static func scoped<T>(fields: Array<(String, String)>, block: () -> T): T {
        let saved = current.get()
        for ((k, v) in fields) {
            put(k, v)
        }
        try {
            block()
        } finally {
            current.set(saved)
        }
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

/** 每行一个 JSON 对象（JSON Lines），写到 stderr 或自定义的行输出函数 */
public class JsonLinesSink <: LogSink {
    private let lock = Mutex()
    private let out: Option<(String) -> Unit>

    public init() {
        this.out = None<(String) -> Unit>
    }

    /** 自定义输出（例如写文件、发送到采集端）；out 在锁内调用，保证行不交错 */
    public init(out: (String) -> Unit) {
        this.out = Some(out)
    }

    public func write(record: LogRecord): Unit {
        let line = record.toJson()
        synchronized(lock) {
            match (out) {
                case Some(f) => f(line)
                case None =>
                    let err = getStdErr()
                    err.write(line + "\n")
                    err.flush()
            }
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

    static func emit(level: LogLevel, source: String, message: String, fields: Array<(String, String)>): Unit {
        let mdc = Mdc.get()
        let all = if (mdc.isEmpty()) {
            fields
        } else if (fields.isEmpty()) {
            mdc
        } else {
            fields.concat(mdc)
        }
        let record = LogRecord(level, source, message, DateTime.now().toUnixTimeStamp().toMilliseconds(), all)
        let target = synchronized(sinkLock) { sink }
        try {
            target.write(record)
        } catch (_: Exception) {
            // 日志输出失败不能影响业务
        }
    }
}

/** 带来源标签的日志记录器（不可变，可随意创建和共享） */
public class Logger {
    public let source: String
    /** 附加到这个记录器每条日志上的字段 */
    public let fields: Array<(String, String)>

    public init(source: String) {
        this(source, Array<(String, String)>())
    }

    public init(source: String, fields: Array<(String, String)>) {
        this.source = source
        this.fields = fields
    }

    /** 返回附加了一个字段的新记录器（原记录器不变） */
    public func withField(key: String, value: String): Logger {
        Logger(source, fields.concat([(key, value)]))
    }

    public func withFields(more: Array<(String, String)>): Logger {
        Logger(source, fields.concat(more))
    }

    public func isEnabled(level: LogLevel): Bool {
        Logging.isEnabled(level)
    }

    public func log(level: LogLevel, message: String): Unit {
        if (Logging.isEnabled(level)) {
            Logging.emit(level, source, message, fields)
        }
    }

    /** 惰性版本：级别关闭时不会执行 message 闭包 */
    public func log(level: LogLevel, message: () -> String): Unit {
        if (Logging.isEnabled(level)) {
            Logging.emit(level, source, message(), fields)
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
