/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation Layer - 基础设施层
 * 提供零依赖的基础设施组件：内存管理、并发原语、序列化、网络传输
 */
package cactor_foundation

// 基础设施组件导出
public import cactor_foundation.memory.*
public import cactor_foundation.queue.*
public import cactor_foundation.serialization.*
public import cactor_foundation.network.*

// 配置只有一套入口：cactor_foundation.config（LayeredConfig）。
// 早期并行的 di / abstractions / contracts / simple_config 已删除（见 docs/plan/20261106.md E46/E47）：
// 它们没有任何生产消费者，能力由 core 的真实类型、tools/check_module_deps.py 与 LayeredConfig 覆盖。
// public import cactor_foundation.config.*  // 暂不整体重导出，避免与 core/config 同名类型混淆
public import cactor_foundation.monitoring.*     // 监控系统
