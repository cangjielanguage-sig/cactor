/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Core - 调度工作线程标记
 *
 * 默认调度器是亲和调度：每个 Actor 固定在一个工作线程上，且没有工作窃取。
 * 在这类线程上阻塞等待（例如在处理方法里等另一个 Actor 的回复）会卡住同一线程上的所有 Actor；
 * 被等的 Actor 恰好分在同一线程时就是死锁，只能等超时。
 *
 * 运行时在亲和工作线程启动时打上标记，核心层的阻塞式 API（askInbox().get 等）据此立即报错，
 * 而不是把死锁伪装成一次超时。"blocking" 调度器的线程不打标记：那里本来就允许阻塞。
 */
package cactor_core.actor

let affinityWorkerFlag: ThreadLocal<Bool> = ThreadLocal<Bool>()

/** 由运行时在亲和调度工作线程启动时调用 */
public func markAffinityWorkerThread(): Unit {
    affinityWorkerFlag.set(true)
}

/** 当前线程是否是亲和调度工作线程（在其上阻塞会卡住同线程的其他 Actor） */
public func isAffinityWorkerThread(): Bool {
    affinityWorkerFlag.get() ?? false
}
