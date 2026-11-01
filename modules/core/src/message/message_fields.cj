/*
 * Copyright (c) 2024. All rights reserved.
 * Core Layer - 消息字段的二进制读写（@Message 宏生成的编解码器使用，也可手写编解码器使用）
 *
 * 格式（大端）：Int64 8 字节；Float64 按 IEEE 754 位模式 8 字节；Bool 1 字节；
 * String 为 4 字节长度 + UTF-8；数组为 4 字节元素数 + 逐个元素；
 * Option<T> 为 1 字节标记（0=None，1=Some）+ 值；HashMap<String,T> 为 4 字节条目数 + 逐条（键 + 值）。
 * 读越界时抛 DeserializationException。
 */
package cactor_core.message

import std.collection.*

public class MessageFieldWriter {
    // 手工管理容量的字节缓冲：ArrayList<UInt8>.add 逐字节追加时每个字节都要查容量、改 size，
    // 实测编码一条 283 字节的消息比数组缓冲 + 下标写入慢约 1.5 倍
    private var buf: Array<UInt8> = Array<UInt8>(64, repeat: 0)
    private var pos: Int64 = 0

    public init() {}

    private func ensure(n: Int64): Unit {
        let required = pos + n
        if (required > buf.size) {
            var cap = buf.size * 2
            while (cap < required) {
                cap *= 2
            }
            let grown = Array<UInt8>(cap, repeat: 0)
            buf.copyTo(grown, 0, 0, pos)
            buf = grown
        }
    }

    private func put(b: UInt8): Unit {
        ensure(1)
        buf[pos] = b
        pos++
    }

    // 有符号与无符号之间按位模式转换（仓颉默认检查溢出，这里需要回绕）
    @OverflowWrapping
    public func writeInt64(v: Int64): MessageFieldWriter {
        writeBits(UInt64(v))
    }

    public func writeFloat64(v: Float64): MessageFieldWriter {
        writeBits(v.toBits())
    }

    private func writeBits(u: UInt64): MessageFieldWriter {
        ensure(8)
        buf[pos] = UInt8(u >> 56)
        buf[pos + 1] = UInt8((u >> 48) & 0xFF)
        buf[pos + 2] = UInt8((u >> 40) & 0xFF)
        buf[pos + 3] = UInt8((u >> 32) & 0xFF)
        buf[pos + 4] = UInt8((u >> 24) & 0xFF)
        buf[pos + 5] = UInt8((u >> 16) & 0xFF)
        buf[pos + 6] = UInt8((u >> 8) & 0xFF)
        buf[pos + 7] = UInt8(u & 0xFF)
        pos += 8
        this
    }

    public func writeBool(v: Bool): MessageFieldWriter {
        put(if (v) { 1 } else { 0 })
        this
    }

    public func writeString(v: String): MessageFieldWriter {
        let bytes = v.toArray()
        writeLength(bytes.size)
        ensure(bytes.size)
        bytes.copyTo(buf, 0, pos, bytes.size)
        pos += bytes.size
        this
    }

    public func writeInt64s(v: Array<Int64>): MessageFieldWriter {
        writeLength(v.size)
        for (x in v) {
            writeInt64(x)
        }
        this
    }

    public func writeFloat64s(v: Array<Float64>): MessageFieldWriter {
        writeLength(v.size)
        for (x in v) {
            writeFloat64(x)
        }
        this
    }

    public func writeBools(v: Array<Bool>): MessageFieldWriter {
        writeLength(v.size)
        for (x in v) {
            writeBool(x)
        }
        this
    }

    public func writeStrings(v: Array<String>): MessageFieldWriter {
        writeLength(v.size)
        for (x in v) {
            writeString(x)
        }
        this
    }

    // ---------- Option<T>：1 字节标记（0=None，1=Some）+ 值 ----------

    public func writeOptionString(v: Option<String>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                put(1)
                writeString(x)
            case None =>
                put(0)
        }
        this
    }

    public func writeOptionInt64(v: Option<Int64>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                put(1)
                writeInt64(x)
            case None =>
                put(0)
        }
        this
    }

    public func writeOptionFloat64(v: Option<Float64>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                put(1)
                writeFloat64(x)
            case None =>
                put(0)
        }
        this
    }

    public func writeOptionBool(v: Option<Bool>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                put(1)
                writeBool(x)
            case None =>
                put(0)
        }
        this
    }

    // ---------- HashMap<String, T>：4 字节条目数 + 逐条（键 + 值） ----------

    public func writeStringMap(v: HashMap<String, String>): MessageFieldWriter {
        writeLength(v.size)
        for ((k, val) in v) {
            writeString(k)
            writeString(val)
        }
        this
    }

    public func writeInt64Map(v: HashMap<String, Int64>): MessageFieldWriter {
        writeLength(v.size)
        for ((k, val) in v) {
            writeString(k)
            writeInt64(val)
        }
        this
    }

    public func writeFloat64Map(v: HashMap<String, Float64>): MessageFieldWriter {
        writeLength(v.size)
        for ((k, val) in v) {
            writeString(k)
            writeFloat64(val)
        }
        this
    }

    public func writeBoolMap(v: HashMap<String, Bool>): MessageFieldWriter {
        writeLength(v.size)
        for ((k, val) in v) {
            writeString(k)
            writeBool(val)
        }
        this
    }

    public func toArray(): Array<UInt8> {
        buf[0..pos].clone()
    }

    private func writeLength(n: Int64): Unit {
        if (n > Int64(Int32.Max)) {
            throw SerializationException("字段过长: ${n}")
        }
        ensure(4)
        buf[pos] = UInt8((n >> 24) & 0xFF)
        buf[pos + 1] = UInt8((n >> 16) & 0xFF)
        buf[pos + 2] = UInt8((n >> 8) & 0xFF)
        buf[pos + 3] = UInt8(n & 0xFF)
        pos += 4
    }
}

public class MessageFieldReader {
    private let data: Array<UInt8>
    private var pos: Int64 = 0

    public init(data: Array<UInt8>) {
        this.data = data
    }

    private func need(n: Int64): Unit {
        if (n < 0 || pos + n > data.size) {
            throw DeserializationException("消息字段数据不完整：位置 ${pos} 需要 ${n} 字节，共 ${data.size} 字节")
        }
    }

    @OverflowWrapping
    public func readInt64(): Int64 {
        Int64(readBits())
    }

    public func readFloat64(): Float64 {
        Float64.fromBits(readBits())
    }

    private func readBits(): UInt64 {
        need(8)
        var u: UInt64 = 0
        for (i in 0..8) {
            u = (u << 8) | UInt64(data[pos + i])
        }
        pos += 8
        u
    }

    public func readBool(): Bool {
        need(1)
        let b = data[pos]
        pos++
        b != 0
    }

    public func readString(): String {
        let n = readLength()
        need(n)
        // 非法 UTF-8 时 fromUtf8 抛的是 IllegalArgumentException；统一成 DeserializationException，
        // 调用方按"数据损坏"一类异常处理即可，不会漏接
        let s = try {
            String.fromUtf8(data[pos..(pos + n)])
        } catch (e: IllegalArgumentException) {
            throw DeserializationException("字符串字段不是合法的 UTF-8：位置 ${pos}，长度 ${n}")
        }
        pos += n
        s
    }

    public func readInt64s(): Array<Int64> {
        let n = readLength()
        need(n * 8)
        Array<Int64>(n, { _ => readInt64() })
    }

    public func readFloat64s(): Array<Float64> {
        let n = readLength()
        need(n * 8)
        Array<Float64>(n, { _ => readFloat64() })
    }

    public func readBools(): Array<Bool> {
        let n = readLength()
        need(n)
        Array<Bool>(n, { _ => readBool() })
    }

    public func readStrings(): Array<String> {
        let n = readLength()
        // 每个字符串至少 4 字节长度
        need(n * 4)
        Array<String>(n, { _ => readString() })
    }

    // ---------- Option<T>：1 字节标记（0=None，1=Some）+ 值 ----------

    public func readOptionString(): Option<String> {
        if (readTag()) {
            Some(readString())
        } else {
            None
        }
    }

    public func readOptionInt64(): Option<Int64> {
        if (readTag()) {
            Some(readInt64())
        } else {
            None
        }
    }

    public func readOptionFloat64(): Option<Float64> {
        if (readTag()) {
            Some(readFloat64())
        } else {
            None
        }
    }

    public func readOptionBool(): Option<Bool> {
        if (readTag()) {
            Some(readBool())
        } else {
            None
        }
    }

    // 只接受 0 / 1：其他值说明数据已损坏，按 Some 继续读会把后续字段整体错位
    private func readTag(): Bool {
        need(1)
        let b = data[pos]
        pos++
        match (b) {
            case 0 => false
            case 1 => true
            case _ => throw DeserializationException("Option 标记只能是 0 或 1，位置 ${pos - 1} 是 ${b}")
        }
    }

    // ---------- HashMap<String, T>：4 字节条目数 + 逐条（键 + 值） ----------

    // 条目数来自对端：先按每条的最小字节数核对剩余数据，再拿它预分配容量，
    // 伪造的超大条目数会在这里被拒绝，而不是触发巨量分配（预分配比逐步扩容快 1.4–3 倍）
    private func readMapSize(minEntryBytes: Int64): Int64 {
        let n = readLength()
        need(n * minEntryBytes)
        n
    }

    // 合法编码来自 HashMap，不会有重复键；出现重复说明数据被篡改或损坏
    private func checkNoDuplicateKeys(actual: Int64, declared: Int64): Unit {
        if (actual != declared) {
            throw DeserializationException("HashMap 字段有重复键：声明 ${declared} 条，去重后 ${actual} 条")
        }
    }

    public func readStringMap(): HashMap<String, String> {
        let n = readMapSize(8)
        let m = HashMap<String, String>(n)
        for (_ in 0..n) {
            let k = readString()
            m[k] = readString()
        }
        checkNoDuplicateKeys(m.size, n)
        m
    }

    public func readInt64Map(): HashMap<String, Int64> {
        let n = readMapSize(12)
        let m = HashMap<String, Int64>(n)
        for (_ in 0..n) {
            let k = readString()
            m[k] = readInt64()
        }
        checkNoDuplicateKeys(m.size, n)
        m
    }

    public func readFloat64Map(): HashMap<String, Float64> {
        let n = readMapSize(12)
        let m = HashMap<String, Float64>(n)
        for (_ in 0..n) {
            let k = readString()
            m[k] = readFloat64()
        }
        checkNoDuplicateKeys(m.size, n)
        m
    }

    public func readBoolMap(): HashMap<String, Bool> {
        let n = readMapSize(5)
        let m = HashMap<String, Bool>(n)
        for (_ in 0..n) {
            let k = readString()
            m[k] = readBool()
        }
        checkNoDuplicateKeys(m.size, n)
        m
    }

    /** 是否已读完全部数据（解码完成后用于检查多余字节） */
    public func isAtEnd(): Bool {
        pos == data.size
    }

    private func readLength(): Int64 {
        need(4)
        let n = (Int64(data[pos]) << 24) | (Int64(data[pos + 1]) << 16) | (Int64(data[pos + 2]) << 8) | Int64(data[pos + 3])
        pos += 4
        n
    }
}
