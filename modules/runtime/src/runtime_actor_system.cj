/*
 * CActor Runtime - 以真实运行时实现 core 的 ActorSystem 接口（E29，2026-11-06）
 *
 * 此前两个公开入口都是假的：
 *   - CActorFactory.createSystem / CActorSystemBuilder.build 恒抛 "Not implemented yet"；
 *   - cactor.api.extensions 返回 SimpleActorSystem：tell 在调用方线程无锁同步执行 receive，
 *     多线程发送即并发执行同一 Actor、Props 被忽略、terminate 不停止 Actor。
 * 现在它们都返回 RuntimeActorSystem：底层是与 CActorSystem 相同的 CActorRuntime
 * （每 Actor 一个 Cell、串行邮箱、亲和调度、监督树）。
 */
package cactor_runtime

import std.time.*
import cactor_core.actor.{Actor, ActorRef, Props}
import cactor_core.message.{Message}
import cactor_core.system.{ActorSystem, ActorSelection, SimpleFuture}
import cactor_core.dsl.{AskInbox}

public class RuntimeActorSystem <: ActorSystem {
    private let runtime: CActorRuntime

    /** 创建并启动一个新的运行时 */
    public init(name: String) {
        this(name, CActorRuntimeConfig.createDefault())
    }

    public init(name: String, config: CActorRuntimeConfig) {
        this.runtime = CActorRuntime(name, config)
        runtime.start()
    }

    /** 包装已有运行时（CActorRuntime.getActorSystem 用），不另起运行时 */
    init(runtime: CActorRuntime) {
        this.runtime = runtime
    }

    public prop name: String {
        get() { runtime.getSystemName() }
    }

    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        runtime.actorOf(props, name)
    }

    public func actorOf(props: Props<Actor>): ActorRef {
        runtime.actorOf(props)
    }

    public func actorSelection(path: String): ActorSelection {
        RuntimeActorSelection(runtime, path)
    }

    /** 停止全部 Actor（子 Actor 先于父）并关闭调度器 */
    public func terminate(): Unit {
        runtime.shutdown()
    }

    public func isTerminated(): Bool {
        !runtime.getRuntimeStats().isRunning
    }
}

/** 按路径选择：每次调用时解析（目标可能在选择之后才创建或已重建） */
class RuntimeActorSelection <: ActorSelection {
    // SimpleFuture.get() 没有超时参数；与 TypedAsk 的常用超时一致
    static let ASK_TIMEOUT = Duration.second * 5

    private let runtime: CActorRuntime
    private let path: String

    init(runtime: CActorRuntime, path: String) {
        this.runtime = runtime
        this.path = path
    }

    private func resolve(): ActorRef {
        match (runtime.lookup(path)) {
            case Some(r) => r
            case None => throw Exception("未找到 Actor: ${path}")
        }
    }

    public func resolveOne(): SimpleFuture<ActorRef> {
        ReadyFuture<ActorRef>(resolve())
    }

    public func tell(message: Message): Unit {
        resolve().tell(message)
    }

    /**
     * 真实 ask：以 AskInbox 作为发送方投递，目标经 ctx.sender 回复即完成。
     * 此前实现直接把请求消息原样当作"回复"返回。
     */
    public func ask(message: Message): SimpleFuture<Message> {
        let inbox = AskInbox()
        resolve().tell(message, inbox)
        InboxFuture(inbox)
    }
}

class ReadyFuture<T> <: SimpleFuture<T> {
    private let value: T

    init(value: T) {
        this.value = value
    }

    public func get(): T {
        value
    }

    public func isCompleted(): Bool {
        true
    }
}

class InboxFuture <: SimpleFuture<Message> {
    private let inbox: AskInbox

    init(inbox: AskInbox) {
        this.inbox = inbox
    }

    /** 阻塞等待回复（超时抛 TypedAskTimeoutException）；不可在亲和工作线程上调用，见 AskInbox */
    public func get(): Message {
        inbox.get(RuntimeActorSelection.ASK_TIMEOUT)
    }

    public func isCompleted(): Bool {
        inbox.received() > 0
    }
}
