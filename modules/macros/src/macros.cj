/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 宏包入口
 *
 * 按功能拆分（对照 docs/plan/dsl20261030.md §6.1 / P0）：
 *   actormacro/actor_macro.cj      @actor —— 类内 onXxx(msg, ctx) → 生成类型分派 receive
 *   message/message_macro.cj       @Message —— 生成 messageType、二进制编解码器、registerCodec
 *   statemachine/state_machine_macro.cj
 *                                  @StateMachine —— 状态枚举 + 转换表 → 生成状态机骨架与编译期检查
 *   common/                       共享工具
 *
 *   注：原计划的 @within / @retry 修饰宏经 P0 实验证伪 —— cjc 1.2.0 属性宏的
 *   attr 为表达式（如 3.seconds、3000、"..."）时在成员声明位置有解析歧义
 *   （仅关键字型 attr 如 public 可用），改走运行时组合子 cactor_core.dsl
 *   （within / retry），符合计划 §8.2 降级方案。
 *
 * 旧宏清理（P0 第 2 项）：以下占位宏已删除——
 *   @actor(占位版)、@handler、@props、@actor_system、@message_handler、@message_route、
 *   @state_machine、@router、@perf_monitor、@log_info、@log_debug、@log_error、
 *   @safe_execute、@repeat_times、@batch_create_actors、@smart_route、
 *   @generate_receive_method、@actor_enhance、@typed_actor(两参/一参占位版)
 * 兼容别名：@actorReceive 保留一个版本周期，等价于 @actor。
 */
macro package cactor_macros

public import cactor_macros.message.*
public import cactor_macros.actormacro.*
public import cactor_macros.statemachine.*
// 注意：宏包内不能再有与宏同名的子包/顶层声明（如名为 actor 的子包会与宏 @actor 冲突：
// "top-level declaration 'actor' is conflicted with possible sub-package"），故子包命名为 actormacro。
