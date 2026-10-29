/*
 * CActor Foundation - 日志门面
 * 只依赖 std；后端可替换（默认写 stderr，cactor_integration 可接 stdx.logger）。
 * 关闭的级别只做一次原子读取，不拼接消息。
 */
package cactor_foundation.log

import std.sync.*
import std.time.*

public enum LogLevel <: ToString {
    | Trace
    | Debug
    | Info
    | Warn
    | Error
    | Off

    public func severity(): Int64 {
        match (this) {
            case Trace => 0
            case Debug => 1
            case Info => 2
            case Warn => 3
            case Error => 4
            case Off => 5
        }
    }

    public func toString(): String {
        match (this) {
            case Trace => "TRACE"
            case Debug => "DEBUG"
            case Info => "INFO"
            case Warn => "WARN"
            case Error => "ERROR"
            case Off => "OFF"
        }
    }

    public static func fromSeverity(v: Int64): LogLevel {
        match (v) {
            case 0 => Trace
            case 1 => Debug
            case 2 => Info
            case 3 => Warn
            case 4 => Error
            case _ => Off
        }
    }
}

/** 日志后端：负责格式化与输出 */
public interface LogBackend {
    func write(level: LogLevel, loggerName: String, message: String): Unit
}

/** 默认后端：一行一条写入 stderr */
public class StderrLogBackend <: LogBackend {
    public init() {}

    public func write(level: LogLevel, loggerName: String, message: String): Unit {
        eprintln("${DateTime.now()} ${level} [${loggerName}] ${message}")
    }
}

/** 测试用后端：把日志保存在内存中 */
public class MemoryLogBackend <: LogBackend {
    private let lock = Mutex()
    private var lines: Array<String> = Array<String>()

    public init() {}

    public func write(level: LogLevel, loggerName: String, message: String): Unit {
        synchronized(lock) {
            lines = lines.concat(["${level} [${loggerName}] ${message}"])
        }
    }

    public func entries(): Array<String> {
        synchronized(lock) { lines }
    }
}

public class Logger {
    public let name: String

    init(name: String) {
        this.name = name
    }

    public func isEnabled(level: LogLevel): Bool {
        level.severity() >= LoggerFactory.levelSeverity() && level.severity() < LogLevel.Off.severity()
    }

    public func log(level: LogLevel, message: String): Unit {
        if (isEnabled(level)) {
            LoggerFactory.backend().write(level, name, message)
        }
    }

    /** 延迟拼接：级别关闭时不执行 message */
    public func log(level: LogLevel, message: () -> String): Unit {
        if (isEnabled(level)) {
            LoggerFactory.backend().write(level, name, message())
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

public class LoggerFactory {
    private static let lock = Mutex()
    private static let level = AtomicInt64(LogLevel.Info.severity())
    private static var currentBackend: LogBackend = StderrLogBackend()

    public static func getLogger(name: String): Logger {
        Logger(name)
    }

    public static func setLevel(l: LogLevel): Unit {
        level.store(l.severity())
    }

    public static func getLevel(): LogLevel {
        LogLevel.fromSeverity(level.load())
    }

    public static func setBackend(b: LogBackend): Unit {
        synchronized(lock) {
            currentBackend = b
        }
    }

    static func levelSeverity(): Int64 {
        level.load()
    }

    static func backend(): LogBackend {
        synchronized(lock) { currentBackend }
    }
}
