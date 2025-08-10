/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 邮箱包导出
 */
package cactor.runtime.mailbox

// 邮箱系统包 - 避免循环依赖，通过文件自动导出


import cactor.core.message.Envelope


/**
 * 批处理可选语义接口
 * 支持批量出队操作，提高消息处理吞吐量
 */
public interface BatchDequeueSemantics {
    /**
     * 批量出队消息
     * @param max 最大出队数量
     * @return 实际出队的消息数组，可能少于max
     */
    func dequeueBatch(max: Int32): Array<Envelope>
}
