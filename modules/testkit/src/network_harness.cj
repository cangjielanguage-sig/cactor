/*
 * CActor TestKit - 网络故障注入（E62 的多节点 harness 部分）
 *
 * 实现已移到 `cactor_foundation.network`（E62 的进程级注入需要示例进程与运维端点直接构造），
 * 这里只做重导出，保持既有 `cactor_testkit.BlackholeTransport` 用法不变。
 */
package cactor_testkit

public import cactor_foundation.network.BlackholeTransport
