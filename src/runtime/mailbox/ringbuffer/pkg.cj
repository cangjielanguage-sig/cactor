/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 环形缓冲区邮箱包
 *
 * @deprecated 此包已标记为弃用，建议迁移到以下替代方案：
 * - 高性能场景：使用 FoundationMailbox（基于LockFreeQueue）
 * - 批处理场景：使用 BatchingMailbox 或 AdaptiveBatchingMailbox
 * - 优先级场景：使用 QueueBasedPriorityMailbox
 *
 * 迁移指南：
 * 1. RingBufferMailbox -> FoundationMailbox(capacity)
 * 2. 如需背压控制，可通过MailboxConfig配置或使用包装器模式
 * 3. 性能特性基本一致，FoundationMailbox提供更好的可维护性
 *
 * 计划在下一个主要版本中移除此包。
 */
package cactor.runtime.mailbox.ringbuffer
