/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation Layer - 基础设施层
 * 提供零依赖的基础设施组件：内存管理、并发原语、序列化、网络传输
 */
package cactor.foundation

// 基础设施组件导出
public import cactor.foundation.memory.*
public import cactor.foundation.queue.*
public import cactor.foundation.serialization.*
public import cactor.foundation.network.*

// 新增的架构优化组件（暂时禁用有问题的模块）
// public import cactor.foundation.di.*
// public import cactor.foundation.abstractions.*
// public import cactor.foundation.contracts.*
// public import cactor.foundation.config.*  // 暂时禁用复杂配置系统
public import cactor.foundation.simple_config.*  // 简化配置系统
public import cactor.foundation.monitoring.*     // 监控系统
