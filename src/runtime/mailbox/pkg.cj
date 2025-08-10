/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 邮箱包导出
 */
package cactor.runtime.mailbox

// 邮箱系统包 - 避免循环依赖，通过文件自动导出


import cactor.core.message.Envelope


// 批处理可选语义接口（PR-2 将用到）
public interface BatchDequeueSemantics {
  func dequeueBatch(max: Int32): Array<Envelope>
}
