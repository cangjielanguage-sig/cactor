/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 增强序列化框架
 * Plan 0.20 Phase 4.2: 序列化框架优化
 * 实现高性能二进制序列化、版本兼容、自定义序列化器机制
 */
package cactor_foundation.serialization

import std.collection.*
import std.sync.*
import std.time.*

/**
 * 序列化版本信息
 */
public struct SerializationVersion {
    public let major: UInt16
    public let minor: UInt16
    public let patch: UInt16
    
    public init(major: UInt16, minor: UInt16, patch: UInt16) {
        this.major = major
        this.minor = minor
        this.patch = patch
    }
    
    public func toString(): String {
        "${major}.${minor}.${patch}"
    }
    
    public func isCompatibleWith(other: SerializationVersion): Bool {
        // 主版本相同，次版本向后兼容
        this.major == other.major && this.minor >= other.minor
    }
    
    public static func current(): SerializationVersion {
        SerializationVersion(1, 0, 0)
    }
}

/**
 * 序列化元数据
 */
public struct SerializationMetadata {
    public let version: SerializationVersion
    public let timestamp: Int64
    public let serializerId: String
    public let dataSize: Int64
    public let checksum: UInt32
    
    public init(version: SerializationVersion, timestamp: Int64, serializerId: String, dataSize: Int64, checksum: UInt32) {
        this.version = version
        this.timestamp = timestamp
        this.serializerId = serializerId
        this.dataSize = dataSize
        this.checksum = checksum
    }
}

/**
 * 高性能二进制序列化器接口
 */
public interface EnhancedSerializer<T> {
    /**
     * 序列化对象为字节数组，包含版本信息
     */
    func serialize(obj: T, version: SerializationVersion): Array<UInt8>
    
    /**
     * 从字节数组反序列化对象，支持版本兼容
     */
    func deserialize(data: Array<UInt8>): T
    
    /**
     * 获取序列化器标识
     */
    func getSerializerId(): String
    
    /**
     * 获取支持的版本范围
     */
    func getSupportedVersions(): Array<SerializationVersion>
    
    /**
     * 验证数据完整性
     */
    func validateData(data: Array<UInt8>): Bool
}

/**
 * 高性能字符串序列化器
 * 支持UTF-8编码和压缩
 */
public class EnhancedStringSerializer <: EnhancedSerializer<String> {
    private let enableCompression: Bool
    private let maxStringLength: Int64
    
    public init(enableCompression: Bool, maxStringLength: Int64) {
        this.enableCompression = enableCompression
        this.maxStringLength = maxStringLength
    }
    
    public func serialize(obj: String, version: SerializationVersion): Array<UInt8> {
        if (obj.size > maxStringLength) {
            throw SerializationException("字符串长度超过限制: ${obj.size} > ${maxStringLength}")
        }
        
        let result = ArrayList<UInt8>()
        
        // 写入版本信息
        writeVersion(result, version)
        
        // 写入时间戳
        writeInt64(result, getCurrentTimeNanos())
        
        // 写入序列化器ID
        let serializerId = getSerializerId()
        writeString(result, serializerId)
        
        // 序列化字符串数据
        let stringBytes = stringToUtf8Bytes(obj)
        let finalBytes = if (enableCompression) {
            compressBytes(stringBytes)
        } else {
            stringBytes
        }
        
        // 写入数据长度
        writeInt64(result, Int64(finalBytes.size))
        
        // 写入压缩标志
        result.add(if (enableCompression) { UInt8(1) } else { UInt8(0) })
        
        // 写入数据
        for (byte in finalBytes) {
            result.add(byte)
        }
        
        // 计算并写入校验和
        let checksum = calculateChecksum(finalBytes)
        writeUInt32(result, checksum)
        
        result.toArray()
    }
    
    public func deserialize(data: Array<UInt8>): String {
        if (data.size < 32) {  // 最小头部大小
            throw DeserializationException("数据太小，无法包含完整头部")
        }
        
        var offset = 0
        
        // 读取版本信息
        let version = readVersion(data, offset)
        offset += 6
        
        // 验证版本兼容性
        if (!version.isCompatibleWith(SerializationVersion.current())) {
            throw DeserializationException("版本不兼容: ${version.toString()}")
        }
        
        // 读取时间戳
        let timestamp = readInt64(data, offset)
        offset += 8
        
        // 读取序列化器ID长度和ID
        let serializerIdLength = readInt32(data, offset)
        offset += 4
        let serializerId = readString(data, offset, Int64(serializerIdLength))
        offset += Int64(serializerIdLength)
        
        // 验证序列化器ID
        if (serializerId != getSerializerId()) {
            throw DeserializationException("序列化器ID不匹配: ${serializerId}")
        }
        
        // 读取数据长度
        let dataLength = readInt64(data, offset)
        offset += 8
        
        // 读取压缩标志
        let isCompressed = data[offset] == UInt8(1)
        offset += 1
        
        // 读取数据
        if (offset + dataLength + 4 > data.size) {
            throw DeserializationException("数据长度不匹配")
        }
        
        let stringBytes = ArrayList<UInt8>()
        for (i in 0..dataLength) {
            stringBytes.add(data[offset + i])
        }
        let stringBytesArray = stringBytes.toArray()
        offset += dataLength
        
        // 读取校验和
        let expectedChecksum = readUInt32(data, offset)
        let actualChecksum = calculateChecksum(stringBytesArray)
        
        if (expectedChecksum != actualChecksum) {
            throw DeserializationException("校验和不匹配")
        }
        
        // 解压缩（如果需要）
        let finalBytes = if (isCompressed) {
            decompressBytes(stringBytesArray)
        } else {
            stringBytesArray
        }
        
        // 转换为字符串
        utf8BytesToString(finalBytes)
    }
    
    public func getSerializerId(): String {
        "enhanced_string_v1"
    }
    
    public func getSupportedVersions(): Array<SerializationVersion> {
        [SerializationVersion(1, 0, 0), SerializationVersion(1, 1, 0)]
    }
    
    public func validateData(data: Array<UInt8>): Bool {
        try {
            deserialize(data)
            true
        } catch (e: Exception) {
            false
        }
    }
    
    // 辅助方法
    private func stringToUtf8Bytes(str: String): Array<UInt8> {
        let bytes = ArrayList<UInt8>()
        for (i in 0..str.size) {
            let char = str[i]
            let runeValue = Int32(char)
            
            if (runeValue <= 0x7F) {
                // ASCII字符 (0-127)
                bytes.add(UInt8(runeValue))
            } else if (runeValue <= 0x7FF) {
                // 2字节UTF-8
                bytes.add(UInt8(0xC0 | (runeValue >> 6)))
                bytes.add(UInt8(0x80 | (runeValue & 0x3F)))
            } else if (runeValue <= 0xFFFF) {
                // 3字节UTF-8
                bytes.add(UInt8(0xE0 | (runeValue >> 12)))
                bytes.add(UInt8(0x80 | ((runeValue >> 6) & 0x3F)))
                bytes.add(UInt8(0x80 | (runeValue & 0x3F)))
            } else {
                // 4字节UTF-8
                bytes.add(UInt8(0xF0 | (runeValue >> 18)))
                bytes.add(UInt8(0x80 | ((runeValue >> 12) & 0x3F)))
                bytes.add(UInt8(0x80 | ((runeValue >> 6) & 0x3F)))
                bytes.add(UInt8(0x80 | (runeValue & 0x3F)))
            }
        }
        bytes.toArray()
    }
    
    private func utf8BytesToString(data: Array<UInt8>): String {
        let chars = ArrayList<Rune>()
        var i = 0
        
        while (i < data.size) {
            let byte1 = data[i]
            
            if (byte1 <= 0x7F) {
                // ASCII字符
                chars.add(Rune(Int32(byte1)))
                i += 1
            } else if ((byte1 & 0xE0) == 0xC0) {
                // 2字节UTF-8
                if (i + 1 >= data.size) {
                    i = data.size  // 退出循环
                } else {
                    let byte2 = data[i + 1]
                    let codepoint = ((Int32(byte1) & 0x1F) << 6) | (Int32(byte2) & 0x3F)
                    chars.add(Rune(codepoint))
                    i += 2
                }
            } else if ((byte1 & 0xF0) == 0xE0) {
                // 3字节UTF-8
                if (i + 2 >= data.size) {
                    i = data.size  // 退出循环
                } else {
                    let byte2 = data[i + 1]
                    let byte3 = data[i + 2]
                    let codepoint = ((Int32(byte1) & 0x0F) << 12) | ((Int32(byte2) & 0x3F) << 6) | (Int32(byte3) & 0x3F)
                    chars.add(Rune(codepoint))
                    i += 3
                }
            } else if ((byte1 & 0xF8) == 0xF0) {
                // 4字节UTF-8
                if (i + 3 >= data.size) {
                    i = data.size  // 退出循环
                } else {
                    let byte2 = data[i + 1]
                    let byte3 = data[i + 2]
                    let byte4 = data[i + 3]
                    let codepoint = ((Int32(byte1) & 0x07) << 18) | ((Int32(byte2) & 0x3F) << 12) | ((Int32(byte3) & 0x3F) << 6) | (Int32(byte4) & 0x3F)
                    chars.add(Rune(codepoint))
                    i += 4
                }
            } else {
                // 无效字节，跳过
                i += 1
            }
        }
        
        String(chars.toArray())
    }
    
    private func compressBytes(data: Array<UInt8>): Array<UInt8> {
        // 简化的压缩实现：RLE (Run Length Encoding)
        let result = ArrayList<UInt8>()
        var i = 0
        
        while (i < data.size) {
            let currentByte = data[i]
            var count = 1
            
            // 计算连续相同字节的数量
            while (i + count < data.size && data[i + count] == currentByte && count < 255) {
                count += 1
            }
            
            if (count > 3) {
                // 使用RLE编码
                result.add(UInt8(0xFF))  // 压缩标记
                result.add(UInt8(count))
                result.add(currentByte)
            } else {
                // 直接存储
                for (j in 0..count) {
                    result.add(currentByte)
                }
            }
            
            i += count
        }
        
        result.toArray()
    }
    
    private func decompressBytes(data: Array<UInt8>): Array<UInt8> {
        // 简化的解压缩实现
        let result = ArrayList<UInt8>()
        var i = 0
        
        while (i < data.size) {
            if (data[i] == UInt8(0xFF) && i + 2 < data.size) {
                // RLE解码
                let count = Int64(data[i + 1])
                let byte = data[i + 2]
                for (j in 0..count) {
                    result.add(byte)
                }
                i += 3
            } else {
                result.add(data[i])
                i += 1
            }
        }
        
        result.toArray()
    }
    
    private func calculateChecksum(data: Array<UInt8>): UInt32 {
        // 简化的CRC32实现
        var checksum: UInt32 = 0xFFFFFFFF
        for (byte in data) {
            checksum = checksum ^ UInt32(byte)
            for (i in 0..8) {
                if ((checksum & 1) != 0) {
                    checksum = (checksum >> 1) ^ 0xEDB88320
                } else {
                    checksum = checksum >> 1
                }
            }
        }
        checksum ^ 0xFFFFFFFF
    }
}

/**
 * 高性能整数序列化器
 * 支持多种整数类型和压缩
 */
public class EnhancedIntSerializer <: EnhancedSerializer<Int64> {
    private let useVarInt: Bool

    public init(useVarInt: Bool) {
        this.useVarInt = useVarInt
    }

    public func serialize(obj: Int64, version: SerializationVersion): Array<UInt8> {
        let result = ArrayList<UInt8>()

        // 写入版本信息
        writeVersion(result, version)

        // 写入时间戳
        writeInt64(result, getCurrentTimeNanos())

        // 写入序列化器ID
        let serializerId = getSerializerId()
        writeString(result, serializerId)

        // 序列化整数数据
        let intBytes = if (useVarInt) {
            encodeVarInt(obj)
        } else {
            encodeFixedInt(obj)
        }

        // 写入数据长度
        writeInt64(result, Int64(intBytes.size))

        // 写入编码类型标志
        result.add(if (useVarInt) { UInt8(1) } else { UInt8(0) })

        // 写入数据
        for (byte in intBytes) {
            result.add(byte)
        }

        // 计算并写入校验和
        let checksum = calculateChecksum(intBytes)
        writeUInt32(result, checksum)

        result.toArray()
    }

    public func deserialize(data: Array<UInt8>): Int64 {
        if (data.size < 32) {
            throw DeserializationException("数据太小，无法包含完整头部")
        }

        var offset = 0

        // 读取版本信息
        let version = readVersion(data, offset)
        offset += 6

        // 验证版本兼容性
        if (!version.isCompatibleWith(SerializationVersion.current())) {
            throw DeserializationException("版本不兼容: ${version.toString()}")
        }

        // 读取时间戳
        let timestamp = readInt64(data, offset)
        offset += 8

        // 读取序列化器ID
        let serializerIdLength = readInt32(data, offset)
        offset += 4
        let serializerId = readString(data, offset, Int64(serializerIdLength))
        offset += Int64(serializerIdLength)

        // 验证序列化器ID
        if (serializerId != getSerializerId()) {
            throw DeserializationException("序列化器ID不匹配: ${serializerId}")
        }

        // 读取数据长度
        let dataLength = readInt64(data, offset)
        offset += 8

        // 读取编码类型标志
        let isVarInt = data[offset] == UInt8(1)
        offset += 1

        // 读取数据
        if (offset + dataLength + 4 > data.size) {
            throw DeserializationException("数据长度不匹配")
        }

        let intBytesArray = ArrayList<UInt8>()
        for (i in 0..dataLength) {
            intBytesArray.add(data[offset + i])
        }
        let intBytes = intBytesArray.toArray()
        offset += dataLength

        // 读取校验和
        let expectedChecksum = readUInt32(data, offset)
        let actualChecksum = calculateChecksum(intBytes)

        if (expectedChecksum != actualChecksum) {
            throw DeserializationException("校验和不匹配")
        }

        // 解码整数
        if (isVarInt) {
            decodeVarInt(intBytes)
        } else {
            decodeFixedInt(intBytes)
        }
    }

    public func getSerializerId(): String {
        "enhanced_int_v1"
    }

    public func getSupportedVersions(): Array<SerializationVersion> {
        [SerializationVersion(1, 0, 0)]
    }

    public func validateData(data: Array<UInt8>): Bool {
        try {
            deserialize(data)
            true
        } catch (e: Exception) {
            false
        }
    }

    // VarInt编码：小整数使用更少字节
    private func encodeVarInt(value: Int64): Array<UInt8> {
        let result = ArrayList<UInt8>()
        var remaining = if (value < 0) { UInt64(value + 9223372036854775807 + 1) } else { UInt64(value) }

        while (remaining >= 0x80) {
            result.add(UInt8(remaining & 0x7F) | 0x80)
            remaining >>= 7
        }
        result.add(UInt8(remaining & 0x7F))

        result.toArray()
    }

    private func decodeVarInt(data: Array<UInt8>): Int64 {
        var result: UInt64 = 0
        var shift = 0

        for (byte in data) {
            result |= UInt64(byte & 0x7F) << shift
            if ((byte & 0x80) == 0) {
                break
            }
            shift += 7
            if (shift >= 64) {
                throw DeserializationException("VarInt太长")
            }
        }

        Int64(result)
    }

    private func encodeFixedInt(value: Int64): Array<UInt8> {
        let result = ArrayList<UInt8>()
        writeInt64(result, value)
        result.toArray()
    }

    private func decodeFixedInt(data: Array<UInt8>): Int64 {
        if (data.size < 8) {
            throw DeserializationException("固定整数需要8字节")
        }
        readInt64(data, 0)
    }

    private func calculateChecksum(data: Array<UInt8>): UInt32 {
        // 简化的CRC32实现
        var checksum: UInt32 = 0xFFFFFFFF
        for (byte in data) {
            checksum = checksum ^ UInt32(byte)
            for (i in 0..8) {
                if ((checksum & 1) != 0) {
                    checksum = (checksum >> 1) ^ 0xEDB88320
                } else {
                    checksum = checksum >> 1
                }
            }
        }
        checksum ^ 0xFFFFFFFF
    }
}

/**
 * 序列化辅助函数
 */

// 写入版本信息
func writeVersion(buffer: ArrayList<UInt8>, version: SerializationVersion): Unit {
    writeUInt16(buffer, version.major)
    writeUInt16(buffer, version.minor)
    writeUInt16(buffer, version.patch)
}

// 读取版本信息
func readVersion(data: Array<UInt8>, offset: Int64): SerializationVersion {
    let major = readUInt16(data, offset)
    let minor = readUInt16(data, offset + 2)
    let patch = readUInt16(data, offset + 4)
    SerializationVersion(major, minor, patch)
}

// 写入字符串（长度前缀）
func writeString(buffer: ArrayList<UInt8>, str: String): Unit {
    let bytes = stringToBytes(str)
    writeInt32(buffer, Int32(bytes.size))
    for (byte in bytes) {
        buffer.add(byte)
    }
}

// 读取字符串
func readString(data: Array<UInt8>, offset: Int64, length: Int64): String {
    if (offset + length > data.size) {
        throw DeserializationException("字符串数据不完整")
    }
    let bytes = Array<UInt8>(length) { i => data[offset + i] }
    bytesToString(bytes)
}

// 写入16位无符号整数
func writeUInt16(buffer: ArrayList<UInt8>, value: UInt16): Unit {
    buffer.add(UInt8(value & 0xFF))
    buffer.add(UInt8((value >> 8) & 0xFF))
}

// 读取16位无符号整数
func readUInt16(data: Array<UInt8>, offset: Int64): UInt16 {
    if (offset + 2 > data.size) {
        throw DeserializationException("UInt16数据不完整")
    }
    UInt16(data[offset]) | (UInt16(data[offset + 1]) << 8)
}

// 写入32位无符号整数
func writeUInt32(buffer: ArrayList<UInt8>, value: UInt32): Unit {
    buffer.add(UInt8(value & 0xFF))
    buffer.add(UInt8((value >> 8) & 0xFF))
    buffer.add(UInt8((value >> 16) & 0xFF))
    buffer.add(UInt8((value >> 24) & 0xFF))
}

// 读取32位无符号整数
func readUInt32(data: Array<UInt8>, offset: Int64): UInt32 {
    if (offset + 4 > data.size) {
        throw DeserializationException("UInt32数据不完整")
    }
    UInt32(data[offset]) |
    (UInt32(data[offset + 1]) << 8) |
    (UInt32(data[offset + 2]) << 16) |
    (UInt32(data[offset + 3]) << 24)
}

// 写入32位有符号整数
func writeInt32(buffer: ArrayList<UInt8>, value: Int32): Unit {
    writeUInt32(buffer, UInt32(value))
}

// 读取32位有符号整数
func readInt32(data: Array<UInt8>, offset: Int64): Int32 {
    Int32(readUInt32(data, offset))
}

// 写入64位有符号整数
func writeInt64(buffer: ArrayList<UInt8>, value: Int64): Unit {
    for (i in 0..8) {
        buffer.add(UInt8((value >> (i * 8)) & 0xFF))
    }
}

// 读取64位有符号整数
func readInt64(data: Array<UInt8>, offset: Int64): Int64 {
    if (offset + 8 > data.size) {
        throw DeserializationException("Int64数据不完整")
    }
    var result: Int64 = 0
    for (i in 0..8) {
        result |= Int64(data[offset + i]) << (i * 8)
    }
    result
}

// 字符串转字节数组（简化实现）
func stringToBytes(str: String): Array<UInt8> {
    let bytes = ArrayList<UInt8>()
    for (i in 0..str.size) {
        let char = str[i]
        let runeValue = Int32(char)
        if (runeValue <= 127) {
            bytes.add(UInt8(runeValue))
        } else {
            bytes.add(UInt8(63))  // '?' 字符作为替代
        }
    }
    bytes.toArray()
}

// 字节数组转字符串（简化实现）
func bytesToString(data: Array<UInt8>): String {
    let chars = ArrayList<Rune>()
    for (byte in data) {
        if (byte <= 127) {
            chars.add(Rune(Int32(byte)))
        }
    }
    String(chars.toArray())
}

/**
 * 获取当前时间纳秒
 */
func getCurrentTimeNanos(): Int64 {
    // 简化实现：返回固定时间戳
    1642694400000000000  // 2022-01-20 12:00:00 UTC in nanoseconds
}
