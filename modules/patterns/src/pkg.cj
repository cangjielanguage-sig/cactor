/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Patterns Layer - 模式层
 * 提供高级模式：Ask、路由、断路器（背压已由 cactor_streams 的 Reactive Streams 提供）
 */
package cactor_patterns

// 模式组件导出
public import cactor_patterns.ask.*
public import cactor_patterns.routing.*
public import cactor_patterns.spawnprotocol.*
public import cactor_patterns.delivery.*
public import cactor_foundation.circuit_breaker.*
// 背压（FlowController / BackpressureStrategy）已由 cactor_streams（Reactive Streams）取代，
// 旧实现零消费者，已随 docs/plan/20261106.md E46 删除。
