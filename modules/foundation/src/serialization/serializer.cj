/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 序列化框架
 * 提供通用序列化和反序列化功能，零依赖实现
 */
package cactor_foundation.serialization

import std.collection.*
import std.convert.*

/**
 * 通用序列化器接口 - 零依赖
 */
public interface Serializer<T> {
    /**
     * 序列化对象为字节数组
     */
    func serialize(obj: T): Array<UInt8>

    /**
     * 从字节数组反序列化对象
     */
    func deserialize(data: Array<UInt8>): T

    /**
     * 获取序列化器名称
     */
    func getName(): String
}

/**
 * 字节序列化器 - 处理原始字节数据
 */
public class ByteSerializer <: Serializer<Array<UInt8>> {
    public init() {}

    public func serialize(obj: Array<UInt8>): Array<UInt8> {
        // 直接返回字节数组
        obj
    }

    public func deserialize(data: Array<UInt8>): Array<UInt8> {
        // 直接返回字节数组
        data
    }

    public func getName(): String {
        "ByteSerializer"
    }
}

/**
 * 字符串序列化器
 */
public class StringSerializer <: Serializer<String> {
    public init() {}

    public func serialize(obj: String): Array<UInt8> {
        stringToBytes(obj)
    }

    public func deserialize(data: Array<UInt8>): String {
        bytesToString(data)
    }

    public func getName(): String {
        "StringSerializer"
    }

    private func stringToBytes(str: String): Array<UInt8> {
        // UTF-8 编码
        str.toArray()
    }

    private func bytesToString(data: Array<UInt8>): String {
        // UTF-8 解码；非法字节序列抛出 DeserializationException
        try {
            String.fromUtf8(data)
        } catch (e: Exception) {
            throw DeserializationException("非法的 UTF-8 数据: ${e.message}")
        }
    }
}

/**
 * 序列化异常
 */
public class SerializationException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 反序列化异常
 */
public class DeserializationException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * JSON序列化器 - 处理JSON字符串
 */
public class JsonSerializer <: Serializer<String> {
    public init() {}

    public func serialize(obj: String): Array<UInt8> {
        try {
            let json = "{\"data\":\"${escapeJson(obj)}\"}"
            stringToBytes(json)
        } catch (e: Exception) {
            throw SerializationException("JSON序列化失败: ${e.message}")
        }
    }

    public func deserialize(data: Array<UInt8>): String {
        try {
            let json = bytesToString(data)
            parseJsonString(json)
        } catch (e: Exception) {
            throw DeserializationException("JSON反序列化失败: ${e.message}")
        }
    }

    public func getName(): String {
        "JsonSerializer"
    }

    private func parseJsonString(json: String): String {
        // 真实解析（2026-11-06 修复：此前任何输入都返回硬编码 "parsed_from_json"）。
        // 定位顶层 "data" 字段的字符串值，处理 \" \\ \/ \n \r \t \b \f \uXXXX 转义。
        let dataKey = "\"data\""
        let keyIdx = match (json.indexOf(dataKey)) {
            case Some(i) => i
            case None => throw DeserializationException("JSON 中缺少 data 字段")
        }
        var i = keyIdx + dataKey.size
        // 跳过空白
        while (i < json.size && isWs(json[i])) { i++ }
        if (i >= json.size || json[i] != b':') {
            throw DeserializationException("JSON 格式错误：data 后缺少冒号")
        }
        i++
        while (i < json.size && isWs(json[i])) { i++ }
        if (i >= json.size || json[i] != b'"') {
            throw DeserializationException("JSON 格式错误：data 值不是字符串")
        }
        i++
        let sb = StringBuilder()
        while (i < json.size) {
            let c = json[i]
            if (c == b'"') {
                return sb.toString()
            }
            if (c == b'\\') {
                i++
                if (i >= json.size) {
                    throw DeserializationException("JSON 格式错误：转义符后无字符")
                }
                let esc = json[i]
                if (esc == b'"') {
                    sb.append("\"")
                } else if (esc == b'\\') {
                    sb.append("\\")
                } else if (esc == b'/') {
                    sb.append("/")
                } else if (esc == b'n') {
                    sb.append("\n")
                } else if (esc == b'r') {
                    sb.append("\r")
                } else if (esc == b't') {
                    sb.append("\t")
                } else if (esc == b'b') {
                    sb.append("\u{8}")
                } else if (esc == b'f') {
                    sb.append("\u{c}")
                } else if (esc == b'u') {
                    appendUnicodeEscape(json, i, sb)
                    i += 4
                } else {
                    throw DeserializationException("JSON 格式错误：未知转义 \\${esc}")
                }
            } else {
                // 多字节 UTF-8 字符逐字节追加
                let bytes = ArrayList<UInt8>()
                bytes.add(c)
                var j = i + 1
                while (j < json.size && (json[j] & 0xC0) == 0x80) {
                    bytes.add(json[j])
                    j++
                }
                i = j - 1
                try {
                    sb.append(String.fromUtf8(bytes.toArray()))
                } catch (e: Exception) {
                    throw DeserializationException("非法 UTF-8 序列: ${e.message}")
                }
            }
            i++
        }
        throw DeserializationException("JSON 格式错误：data 字符串值未闭合")
    }

    private func isWs(c: UInt8): Bool {
        c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
    }

    /** 解析 \uXXXX 转义并追加到 sb。json 的 escape 起始位置为 escapeIdx（指向 'u'）。 */
    private func appendUnicodeEscape(json: String, escapeIdx: Int64, sb: StringBuilder): Unit {
        if (escapeIdx + 4 >= json.size) {
            throw DeserializationException("JSON 格式错误：\\u 转义不完整")
        }
        let hex = json[escapeIdx + 1..escapeIdx + 5]
        let cpRes = match (UInt32.tryParse(hex, radix: 16)) {
            case Some(v) => v
            case None => throw DeserializationException("JSON 格式错误：非法 \\u 转义")
        }
        sb.append(Rune(UInt32(cpRes)))
    }


    private func escapeJson(str: String): String {
        // 转义特殊字符（2026-11-06 补全：控制字符此前未处理，会产生非法 JSON）
        let sb = StringBuilder()
        for (c in str.runes()) {
            if (c == r'"') {
                sb.append("\\\"")
            } else if (c == r'\\') {
                sb.append("\\\\")
            } else if (c == r'\n') {
                sb.append("\\n")
            } else if (c == r'\r') {
                sb.append("\\r")
            } else if (c == r'\t') {
                sb.append("\\t")
            } else if (c == r'\u{8}') {
                sb.append("\\b")
            } else if (c == r'\u{c}') {
                sb.append("\\f")
            } else {
                sb.append(c)
            }
        }
        sb.toString()
    }



    private func stringToBytes(str: String): Array<UInt8> {
        // UTF-8 编码
        str.toArray()
    }

    private func bytesToString(bytes: Array<UInt8>): String {
        // UTF-8 解码；非法字节序列抛出 DeserializationException
        try {
            String.fromUtf8(bytes)
        } catch (e: Exception) {
            throw DeserializationException("非法的 UTF-8 数据: ${e.message}")
        }
    }
}

/**
 * 二进制序列化器 - 处理整数数据
 */
public class IntSerializer <: Serializer<Int64> {
    public init() {}

    public func serialize(obj: Int64): Array<UInt8> {
        try {
            intToBinary(obj)
        } catch (e: Exception) {
            throw SerializationException("二进制序列化失败: ${e.message}")
        }
    }

    public func deserialize(data: Array<UInt8>): Int64 {
        try {
            binaryToInt(data)
        } catch (e: Exception) {
            throw DeserializationException("二进制反序列化失败: ${e.message}")
        }
    }

    public func getName(): String {
        "IntSerializer"
    }

    private func intToBinary(value: Int64): Array<UInt8> {
        let result = ArrayList<UInt8>()
        writeInt64(result, value)
        result.toArray()
    }

    private func binaryToInt(data: Array<UInt8>): Int64 {
        if (data.size < 8) {
            throw DeserializationException("数据不完整，需要8字节")
        }
        readInt64(data, 0)
    }

    private func writeInt64(buffer: ArrayList<UInt8>, value: Int64): Unit {
        buffer.add(UInt8((value >> 56) & 0xFF))
        buffer.add(UInt8((value >> 48) & 0xFF))
        buffer.add(UInt8((value >> 40) & 0xFF))
        buffer.add(UInt8((value >> 32) & 0xFF))
        buffer.add(UInt8((value >> 24) & 0xFF))
        buffer.add(UInt8((value >> 16) & 0xFF))
        buffer.add(UInt8((value >> 8) & 0xFF))
        buffer.add(UInt8(value & 0xFF))
    }

    private func readInt64(data: Array<UInt8>, offset: Int64): Int64 {
        (Int64(data[offset]) << 56) |
        (Int64(data[offset + 1]) << 48) |
        (Int64(data[offset + 2]) << 40) |
        (Int64(data[offset + 3]) << 32) |
        (Int64(data[offset + 4]) << 24) |
        (Int64(data[offset + 5]) << 16) |
        (Int64(data[offset + 6]) << 8) |
        Int64(data[offset + 7])
    }

}
