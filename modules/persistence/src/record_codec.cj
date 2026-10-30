/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 持久化记录编码
 * 记录格式：[4 字节大端长度][负载]；负载由若干字段组成：
 *   字符串字段 = [4 字节长度][UTF-8 字节]，整数字段 = 8 字节大端
 */
package cactor_persistence

import std.collection.*
import std.fs.*

/** 记录写入器 */
class RecordWriter {
    private let buf = ArrayList<UInt8>()

    func putString(s: String): RecordWriter {
        let bytes = s.toArray()
        putInt32(bytes.size)
        buf.add(all: bytes)
        this
    }

    func putInt64(v: Int64): RecordWriter {
        for (shift in [56, 48, 40, 32, 24, 16, 8, 0]) {
            buf.add(UInt8((v >> shift) & 0xFF))
        }
        this
    }

    private func putInt32(v: Int64): Unit {
        buf.add(UInt8((v >> 24) & 0xFF))
        buf.add(UInt8((v >> 16) & 0xFF))
        buf.add(UInt8((v >> 8) & 0xFF))
        buf.add(UInt8(v & 0xFF))
    }

    /** 输出带长度前缀的完整记录 */
    func toRecord(): Array<UInt8> {
        let payload = buf.toArray()
        let out = ArrayList<UInt8>()
        let n = payload.size
        out.add(UInt8((n >> 24) & 0xFF))
        out.add(UInt8((n >> 16) & 0xFF))
        out.add(UInt8((n >> 8) & 0xFF))
        out.add(UInt8(n & 0xFF))
        out.add(all: payload)
        out.toArray()
    }
}

/** 记录读取器：在一个负载内顺序读取字段，越界抛出异常 */
class RecordReader {
    private let data: Array<UInt8>
    private var pos: Int64 = 0

    init(data: Array<UInt8>) {
        this.data = data
    }

    func getString(): String {
        let n = getInt32()
        if (n < 0 || pos + n > data.size) {
            throw PersistenceCorruptionException("字符串字段越界")
        }
        let s = String.fromUtf8(data[pos..pos + n])
        pos += n
        s
    }

    func getInt64(): Int64 {
        if (pos + 8 > data.size) {
            throw PersistenceCorruptionException("整数字段越界")
        }
        var v: Int64 = 0
        for (i in 0..8) {
            v = (v << 8) | Int64(data[pos + i])
        }
        pos += 8
        v
    }

    private func getInt32(): Int64 {
        if (pos + 4 > data.size) {
            throw PersistenceCorruptionException("长度字段越界")
        }
        let v = (Int64(data[pos]) << 24) | (Int64(data[pos + 1]) << 16) | (Int64(data[pos + 2]) << 8) | Int64(data[pos + 3])
        pos += 4
        v
    }
}

/**
 * 把文件内容切分为记录负载
 * @return (完整记录列表, 有效数据长度)；有效长度之后是写入中断留下的残缺尾部
 */
func splitRecords(content: Array<UInt8>): (ArrayList<Array<UInt8>>, Int64) {
    let records = ArrayList<Array<UInt8>>()
    var pos: Int64 = 0
    while (pos + 4 <= content.size) {
        let n = (Int64(content[pos]) << 24) | (Int64(content[pos + 1]) << 16) | (Int64(content[pos + 2]) << 8) | Int64(content[pos + 3])
        if (pos + 4 + n > content.size) {
            break
        }
        records.add(content[pos + 4..pos + 4 + n])
        pos += 4 + n
    }
    (records, pos)
}

/** 把任意 actorId 编码为安全的文件名（十六进制），避免路径穿越 */
func safeFileName(actorId: String): String {
    let hex = "0123456789abcdef"
    let sb = StringBuilder()
    for (b in actorId.toArray()) {
        sb.append(Rune(UInt32(hex[Int64(b >> 4)])))
        sb.append(Rune(UInt32(hex[Int64(b & 0x0F)])))
    }
    if (sb.size == 0) { "_empty" } else { sb.toString() }
}

/** 目录不存在时创建（已存在时不报错，可安全地重复打开同一存储） */
func ensureDirectory(path: String): Unit {
    if (!exists(path)) {
        try {
            Directory.create(path, recursive: true)
        } catch (e: Exception) {
            // 并发创建时可能已被其他线程创建
            if (!exists(path)) {
                throw e
            }
        }
    }
}

/** 持久化数据损坏 */
public class PersistenceCorruptionException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** safeFileName 的逆变换；不是合法的十六进制名称时返回 None */
func unhexName(name: String): Option<String> {
    if (name == "_empty") {
        return Some("")
    }
    if (name.size % 2 != 0) {
        return None
    }
    let bytes = Array<UInt8>(name.size / 2, repeat: 0)
    for (i in 0..bytes.size) {
        let hi = hexValue(name[2 * i])
        let lo = hexValue(name[2 * i + 1])
        if (hi < 0 || lo < 0) {
            return None
        }
        bytes[i] = UInt8(hi * 16 + lo)
    }
    try {
        Some(String.fromUtf8(bytes))
    } catch (_: Exception) {
        None
    }
}

func hexValue(b: UInt8): Int64 {
    if (b >= 48 && b <= 57) {
        Int64(b) - 48
    } else if (b >= 97 && b <= 102) {
        Int64(b) - 87
    } else {
        -1
    }
}
