/*
 * CActor Platform.Compat - 单模块接入所需的类型重导出（AR11）
 *
 * 背景：`cactor` 是 cjpm workspace，模块之间用 `path = "../x"` 声明依赖。外部项目若用
 * `git = "…/cactor.git"` 依赖其中多个模块，会出现"同一模块既是 git 依赖又是 platform 的 path 依赖"
 * 的冲突（cjpm 报 `only one of 'path' or 'git' is allowed`）。
 *
 * 因此外部接入方**只依赖 `cactor_platform` 一个 git 模块**，用它需要的 core / 门面类型由本包重导出。
 * 这不改变各类型的归属（实现仍在 core / persistence / cactor），只是收窄外部依赖面。
 */
package cactor_platform.compat

/** 门面：Actor 系统与工厂 */
public import cactor.api.{CActorSystem, CActorSystemFactory}
/** 经典 Actor 与消息（平台原语的输入/输出类型） */
public import cactor_core.actor.{Actor, ActorContext, ActorRef}
public import cactor_core.message.{Message, MessageResult, StringMessage}
/** 平台原语使用的状态存储 */
public import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}
