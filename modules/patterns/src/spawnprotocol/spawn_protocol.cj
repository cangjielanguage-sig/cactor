/*
 * CActor - SpawnProtocol（对标 Akka SpawnProtocol）
 *
 * 一个通用的"生成器"Actor：收到 Spawn(props, replyTo) 后，按给定的 Props<Actor> 生成一个子 Actor，
 * 并把新 Actor 的 ActorRef 发回 replyTo。常用于"把生成权集中到一个 Actor，避免调用方直接拿
 * ActorContext"的场景（例如在 ask 链路中按需生成工作 Actor）。
 */
package cactor_patterns.spawnprotocol

import std.sync.*
import cactor_core.actor.{Actor, ActorContext, ActorRef, Props}
import cactor_core.message.{Message, MessageResult}

/** 生成请求：props 描述要生成的 Actor，replyTo 接收生成结果 */
public class Spawn <: Message {
    public let props: Props<Actor>
    public let name: Option<String>
    public let replyTo: ActorRef

    public init(props: Props<Actor>, replyTo: ActorRef) {
        this(props, None<String>, replyTo)
    }

    public init(props: Props<Actor>, name: Option<String>, replyTo: ActorRef) {
        this.props = props
        this.name = name
        this.replyTo = replyTo
    }

    public func messageType(): String { "cactor.SpawnProtocol.Spawn" }
}

/** 生成结果 */
public class Spawned <: Message {
    public let ref: ActorRef
    public init(ref: ActorRef) {
        this.ref = ref
    }
    public func messageType(): String { "cactor.SpawnProtocol.Spawned" }
}

/** 生成失败 */
public class SpawnFailed <: Message {
    public let reason: String
    public init(reason: String) {
        this.reason = reason
    }
    public func messageType(): String { "cactor.SpawnProtocol.SpawnFailed" }
}

/** SpawnProtocol Actor：处理 Spawn 请求 */
public class SpawnProtocol <: Actor {
    public prop name: String { get() { "spawn-protocol" } }
    public prop description: String { get() { "spawn protocol" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message as Spawn) {
            case Some(sp) =>
                try {
                    let childName = match (sp.name) {
                        case Some(n) => n
                        case None => "spawned-${context.children().size}"
                    }
                    let ref = context.actorOf(sp.props, childName)
                    sp.replyTo.tell(Spawned(ref))
                } catch (e: Exception) {
                    sp.replyTo.tell(SpawnFailed(e.toString()))
                }
                MessageResult.Handled
            case None => MessageResult.Unhandled
        }
    }
}
