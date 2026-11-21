/*
 * CActor API - 系统设置重导出（D1 门面收敛）
 *
 * 类型本体定义在 cactor_core.system.actor_system_settings：
 * RuntimeActorSystem 位于 runtime 模块，而 runtime 不能反向依赖门面（分层检查会报红）。
 * 因此设置类型放在 core，由本文件在 cactor.api 下公开重导出，
 * 使用者可直接 `import cactor.api.ActorSystemSettings` 或经顶层 `cactor` 包使用。
 */
package cactor.api

public import cactor_core.system.{ActorSystemSettings}
