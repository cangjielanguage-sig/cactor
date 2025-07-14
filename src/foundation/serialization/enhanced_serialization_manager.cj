/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 增强序列化管理器
 * Plan 0.20 Phase 4.2: 序列化框架优化
 * 提供高性能序列化管理、版本兼容、自定义序列化器注册
 */
package cactor.foundation.serialization

import std.collection.*
import std.sync.*

/**
 * 序列化器注册表
 */
public class SerializerRegistry {
    private let stringSerializers: HashMap<String, EnhancedSerializer<String>>
    private let intSerializers: HashMap<String, EnhancedSerializer<Int64>>
    private let customSerializers: HashMap<String, Any>
    private let mutex: Mutex
    
    public init() {
        this.stringSerializers = HashMap<String, EnhancedSerializer<String>>()
        this.intSerializers = HashMap<String, EnhancedSerializer<Int64>>()
        this.customSerializers = HashMap<String, Any>()
        this.mutex = Mutex()
        
        // 注册默认序列化器
        registerDefaultSerializers()
    }
    
    /**
     * 注册字符串序列化器
     */
    public func registerStringSerializer(id: String, serializer: EnhancedSerializer<String>): Unit {
        mutex.lock()
        stringSerializers[id] = serializer
        mutex.unlock()
    }
    
    /**
     * 注册整数序列化器
     */
    public func registerIntSerializer(id: String, serializer: EnhancedSerializer<Int64>): Unit {
        mutex.lock()
        intSerializers[id] = serializer
        mutex.unlock()
    }
    
    /**
     * 获取字符串序列化器
     */
    public func getStringSerializer(id: String): Option<EnhancedSerializer<String>> {
        mutex.lock()
        let result = stringSerializers.get(id)
        mutex.unlock()
        result
    }
    
    /**
     * 获取整数序列化器
     */
    public func getIntSerializer(id: String): Option<EnhancedSerializer<Int64>> {
        mutex.lock()
        let result = intSerializers.get(id)
        mutex.unlock()
        result
    }
    
    /**
     * 获取所有注册的序列化器ID
     */
    public func getAllSerializerIds(): Array<String> {
        mutex.lock()
        let result = ArrayList<String>()
        
        for ((id, _) in stringSerializers) {
            result.add("string:${id}")
        }
        
        for ((id, _) in intSerializers) {
            result.add("int:${id}")
        }
        
        mutex.unlock()
        result.toArray()
    }
    
    private func registerDefaultSerializers(): Unit {
        // 注册默认字符串序列化器
        let defaultStringSerializer = EnhancedStringSerializer(false, 1024 * 1024)
        stringSerializers["default"] = defaultStringSerializer
        stringSerializers["compressed"] = EnhancedStringSerializer(true, 1024 * 1024)
        
        // 注册默认整数序列化器
        let defaultIntSerializer = EnhancedIntSerializer(true)
        intSerializers["default"] = defaultIntSerializer
        intSerializers["fixed"] = EnhancedIntSerializer(false)
    }
}

/**
 * 增强序列化管理器
 * 提供高性能序列化、版本兼容、自定义序列化器支持
 */
public class EnhancedSerializationManager {
    private let registry: SerializerRegistry
    private let defaultVersion: SerializationVersion
    private let performanceMetrics: SerializationMetrics
    private let mutex: Mutex
    
    public init() {
        this.registry = SerializerRegistry()
        this.defaultVersion = SerializationVersion.current()
        this.performanceMetrics = SerializationMetrics()
        this.mutex = Mutex()
    }
    
    /**
     * 序列化字符串（使用默认序列化器）
     */
    public func serializeString(str: String): Array<UInt8> {
        serializeStringWithSerializer(str, "default")
    }
    
    /**
     * 序列化字符串（指定序列化器）
     */
    public func serializeStringWithSerializer(str: String, serializerId: String): Array<UInt8> {
        let startTime = getCurrentTimeNanos()
        
        match (registry.getStringSerializer(serializerId)) {
            case Some(serializer) =>
                try {
                    let result = serializer.serialize(str, defaultVersion)
                    
                    // 记录性能指标
                    let endTime = getCurrentTimeNanos()
                    performanceMetrics.recordSerialization(serializerId, endTime - startTime, Int64(result.size))
                    
                    result
                } catch (e: Exception) {
                    performanceMetrics.recordError(serializerId, "serialization", e.message)
                    throw SerializationException("字符串序列化失败: ${e.message}")
                }
            case None =>
                throw SerializationException("未找到序列化器: ${serializerId}")
        }
    }
    
    /**
     * 反序列化字符串
     */
    public func deserializeString(data: Array<UInt8>, serializerId: String): String {
        let startTime = getCurrentTimeNanos()
        
        match (registry.getStringSerializer(serializerId)) {
            case Some(serializer) =>
                try {
                    let result = serializer.deserialize(data)
                    
                    // 记录性能指标
                    let endTime = getCurrentTimeNanos()
                    performanceMetrics.recordDeserialization(serializerId, endTime - startTime, Int64(data.size))
                    
                    result
                } catch (e: Exception) {
                    performanceMetrics.recordError(serializerId, "deserialization", e.message)
                    throw DeserializationException("字符串反序列化失败: ${e.message}")
                }
            case None =>
                throw DeserializationException("未找到序列化器: ${serializerId}")
        }
    }
    
    /**
     * 序列化整数（使用默认序列化器）
     */
    public func serializeInt(value: Int64): Array<UInt8> {
        serializeIntWithSerializer(value, "default")
    }
    
    /**
     * 序列化整数（指定序列化器）
     */
    public func serializeIntWithSerializer(value: Int64, serializerId: String): Array<UInt8> {
        let startTime = getCurrentTimeNanos()
        
        match (registry.getIntSerializer(serializerId)) {
            case Some(serializer) =>
                try {
                    let result = serializer.serialize(value, defaultVersion)
                    
                    // 记录性能指标
                    let endTime = getCurrentTimeNanos()
                    performanceMetrics.recordSerialization(serializerId, endTime - startTime, Int64(result.size))
                    
                    result
                } catch (e: Exception) {
                    performanceMetrics.recordError(serializerId, "serialization", e.message)
                    throw SerializationException("整数序列化失败: ${e.message}")
                }
            case None =>
                throw SerializationException("未找到序列化器: ${serializerId}")
        }
    }
    
    /**
     * 反序列化整数
     */
    public func deserializeInt(data: Array<UInt8>, serializerId: String): Int64 {
        let startTime = getCurrentTimeNanos()
        
        match (registry.getIntSerializer(serializerId)) {
            case Some(serializer) =>
                try {
                    let result = serializer.deserialize(data)
                    
                    // 记录性能指标
                    let endTime = getCurrentTimeNanos()
                    performanceMetrics.recordDeserialization(serializerId, endTime - startTime, Int64(data.size))
                    
                    result
                } catch (e: Exception) {
                    performanceMetrics.recordError(serializerId, "deserialization", e.message)
                    throw DeserializationException("整数反序列化失败: ${e.message}")
                }
            case None =>
                throw DeserializationException("未找到序列化器: ${serializerId}")
        }
    }
    
    /**
     * 验证数据完整性
     */
    public func validateData(data: Array<UInt8>, serializerId: String, dataType: String): Bool {
        match (dataType) {
            case "string" =>
                match (registry.getStringSerializer(serializerId)) {
                    case Some(serializer) => serializer.validateData(data)
                    case None => false
                }
            case "int" =>
                match (registry.getIntSerializer(serializerId)) {
                    case Some(serializer) => serializer.validateData(data)
                    case None => false
                }
            case _ => false
        }
    }
    
    /**
     * 注册自定义字符串序列化器
     */
    public func registerStringSerializer(id: String, serializer: EnhancedSerializer<String>): Unit {
        registry.registerStringSerializer(id, serializer)
    }
    
    /**
     * 注册自定义整数序列化器
     */
    public func registerIntSerializer(id: String, serializer: EnhancedSerializer<Int64>): Unit {
        registry.registerIntSerializer(id, serializer)
    }
    
    /**
     * 获取性能指标
     */
    public func getPerformanceMetrics(): SerializationMetrics {
        performanceMetrics
    }
    
    /**
     * 获取支持的序列化器列表
     */
    public func getSupportedSerializers(): Array<String> {
        registry.getAllSerializerIds()
    }
    
    /**
     * 重置性能指标
     */
    public func resetMetrics(): Unit {
        performanceMetrics.reset()
    }
}

/**
 * 序列化性能指标
 */
public class SerializationMetrics {
    private var serializationCount: Int64
    private var deserializationCount: Int64
    private var totalSerializationTime: Int64
    private var totalDeserializationTime: Int64
    private var totalSerializedBytes: Int64
    private var totalDeserializedBytes: Int64
    private var errorCount: Int64
    private let errors: ArrayList<String>
    private let mutex: Mutex
    
    public init() {
        this.serializationCount = 0
        this.deserializationCount = 0
        this.totalSerializationTime = 0
        this.totalDeserializationTime = 0
        this.totalSerializedBytes = 0
        this.totalDeserializedBytes = 0
        this.errorCount = 0
        this.errors = ArrayList<String>()
        this.mutex = Mutex()
    }
    
    public func recordSerialization(serializerId: String, time: Int64, bytes: Int64): Unit {
        mutex.lock()
        serializationCount += 1
        totalSerializationTime += time
        totalSerializedBytes += bytes
        mutex.unlock()
    }
    
    public func recordDeserialization(serializerId: String, time: Int64, bytes: Int64): Unit {
        mutex.lock()
        deserializationCount += 1
        totalDeserializationTime += time
        totalDeserializedBytes += bytes
        mutex.unlock()
    }
    
    public func recordError(serializerId: String, operation: String, message: String): Unit {
        mutex.lock()
        errorCount += 1
        errors.add("${serializerId}:${operation} - ${message}")
        if (errors.size > 100) {  // 限制错误日志数量
            // 简化实现：清空所有错误
            errors.clear()
        }
        mutex.unlock()
    }
    
    public func getStats(): String {
        mutex.lock()
        let avgSerTime = if (serializationCount > 0) { totalSerializationTime / serializationCount } else { 0 }
        let avgDeserTime = if (deserializationCount > 0) { totalDeserializationTime / deserializationCount } else { 0 }
        let result = "序列化统计: 序列化${serializationCount}次(平均${avgSerTime}ns), 反序列化${deserializationCount}次(平均${avgDeserTime}ns), 错误${errorCount}次"
        mutex.unlock()
        result
    }
    
    public func reset(): Unit {
        mutex.lock()
        serializationCount = 0
        deserializationCount = 0
        totalSerializationTime = 0
        totalDeserializationTime = 0
        totalSerializedBytes = 0
        totalDeserializedBytes = 0
        errorCount = 0
        errors.clear()
        mutex.unlock()
    }
}
