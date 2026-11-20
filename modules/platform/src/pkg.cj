/*
 * CActor Platform - 平台原语层（L6，对标 Rivet 的平台能力，但不含 Agent 语义）
 *
 * 本模块只提供通用原语，不被任何库模块依赖（tests/examples 除外）：
 *   exec      受监督执行：把任意可失败调用放进受监督 Actor（P1）
 *   session   按 sessionId 定位 Actor（P8，复用集群分片）
 *   lifecycle 空闲睡/醒与唤醒不丢消息（P2/F1）
 *   durable   Durable Streams：游标续读、长轮询、fork、TTL（P4/G3）
 *   edge      无状态接入层：连接 ↔ 集群 Actor、断线按恢复令牌续传（P3/F4）
 *
 * Agent 语义（LLM 客户端、agent loop、工具定义、提示词、记忆）由 ~/pix/pic 实现，
 * CActor 不提供这些能力，也不反向依赖 pic。
 */
package cactor_platform

let platformVersion = "1.2.1"
