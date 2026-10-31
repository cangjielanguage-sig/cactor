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
    private let out = ArrayList<UInt8>()

    public init() {}

    // 有符号与无符号之间按位模式转换（仓颉默认检查溢出，这里需要回绕）
    @OverflowWrapping
    public func writeInt64(v: Int64): MessageFieldWriter {
        writeBits(UInt64(v))
    }

    public func writeFloat64(v: Float64): MessageFieldWriter {
        writeBits(v.toBits())
    }

    private func writeBits(u: UInt64): MessageFieldWriter {
        for (i in 0..8) {
            out.add(UInt8((u >> UInt64(56 - i * 8)) & 0xFF))
        }
        this
    }

    public func writeBool(v: Bool): MessageFieldWriter {
        out.add(if (v) { 1 } else { 0 })
        this
    }

    public func writeString(v: String): MessageFieldWriter {
        let bytes = v.toArray()
        writeLength(bytes.size)
        out.add(all: bytes)
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
                out.add(1)
                writeString(x)
            case None =>
                out.add(0)
        }
        this
    }

    public func writeOptionInt64(v: Option<Int64>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                out.add(1)
                writeInt64(x)
            case None =>
                out.add(0)
        }
        this
    }

    public func writeOptionFloat64(v: Option<Float64>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                out.add(1)
                writeFloat64(x)
            case None =>
                out.add(0)
        }
        this
    }

    public func writeOptionBool(v: Option<Bool>): MessageFieldWriter {
        match (v) {
            case Some(x) =>
                out.add(1)
                writeBool(x)
            case None =>
                out.add(0)
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
        out.toArray()
    }

    private func writeLength(n: Int64): Unit {
        if (n > Int64(Int32.Max)) {
            throw SerializationException("字段过长: ${n}")
        }
        out.add(UInt8((n >> 24) & 0xFF))
        out.add(UInt8((n >> 16) & 0xFF))
        out.add(UInt8((n >> 8) & 0xFF))
        out.add(UInt8(n & 0xFF))
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
        let s = String.fromUtf8(data[pos..(pos + n)])
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

    private func readTag(): Bool {
        need(1)
        let b = data[pos]
        pos++
        b != 0
    }

    // ---------- HashMap<String, T>：4 字节条目数 + 逐条（键 + 值） ----------

    public func readStringMap(): HashMap<String, String> {
        let n = readLength()
        let m = HashMap<String, String>()
        for (_ in 0..n) {
            let k = readString()
            m[k] = readString()
        }
        m
    }

    public func readInt64Map(): HashMap<String, Int64> {
        let n = readLength()
        let m = HashMap<String, Int64>()
        for (_ in 0..n) {
            let k = readString()
            m[k] = readInt64()
        }
        m
    }

    public func readFloat64Map(): HashMap<String, Float64> {
        let n = readLength()
        let m = HashMap<String, Float64>()
        for (_ in 0..n) {
            let k = readString()
            m[k] = readFloat64()
        }
        m
    }

    public func readBoolMap(): HashMap<String, Bool> {
        let n = readLength()
        let m = HashMap<String, Bool>()
        for (_ in 0..n) {
            let k = readString()
            m[k] = readBool()
        }
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
