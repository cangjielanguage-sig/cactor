/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 一次性完成单元
 * 基于 Mutex + Condition 的等待/通知，替代轮询
 */
package cactor_patterns.ask

import std.sync.*
import std.time.*
import std.collection.*

/**
 * 只能完成一次的结果单元
 * - tryComplete：第一次调用生效，唤醒所有等待者，并在锁外执行回调
 * - await：在 Condition 上等待，直到完成或超时（基于单调时钟）
 * - onComplete：已完成则立即回调，否则登记回调
 */
class Completion<R> {
    private let lock: Mutex
    private let cond: Condition
    private var value: Option<R>
    private let callbacks: ArrayList<(R) -> Unit>

    init() {
        this.lock = Mutex()
        this.cond = synchronized(lock) { lock.condition() }
        this.value = None<R>
        this.callbacks = ArrayList<(R) -> Unit>()
    }

    func tryComplete(v: R): Bool {
        let pending: Option<Array<(R) -> Unit>> = synchronized(lock) {
            if (value.isSome()) {
                None<Array<(R) -> Unit>>
            } else {
                value = Some(v)
                cond.notifyAll()
                let cbs = callbacks.toArray()
                callbacks.clear()
                Some(cbs)
            }
        }
        match (pending) {
            case Some(cbs) =>
                for (cb in cbs) {
                    runCallback(cb, v)
                }
                true
            case None => false
        }
    }

    func await(timeout: Duration): Option<R> {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (value.isNone()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                cond.wait(timeout: remaining)
            }
            value
        }
    }

    func onComplete(cb: (R) -> Unit): Unit {
        let ready: Option<R> = synchronized(lock) {
            if (value.isNone()) {
                callbacks.add(cb)
            }
            value
        }
        if (let Some(v) <- ready) {
            runCallback(cb, v)
        }
    }

    func peek(): Option<R> {
        synchronized(lock) { value }
    }

    func isDone(): Bool {
        synchronized(lock) { value.isSome() }
    }

    private func runCallback(cb: (R) -> Unit, v: R): Unit {
        try {
            cb(v)
        } catch (e: Exception) {
            println("Ask回调执行失败: ${e.message}")
        }
    }
}
