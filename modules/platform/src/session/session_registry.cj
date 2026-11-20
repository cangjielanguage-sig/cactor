/*
 * CActor Platform.Session - 按 sessionId 定位 Actor（P8）
 *
 * 会话（AI 会话、用户会话、租户）是"按 id 寻址的长期 Actor"，CActor 的分片已经能做这件事：
 * entityId 经会合哈希落到某个节点的某个分片，跨节点时由区域转发。这里只做一层薄封装：
 *   - 不为每个 id 维护本地表（分片区域已经是权威）；
 *   - session<T>(id) 返回一个"路由型" TypedActorRef：tell 直接进分片，不占本地 Actor；
 *   - 消息协议由使用方定义（CActor 不定义 SessionMsg）。
 *
 * 睡/醒（F1/P2）在 AR5 接入 lifecycle 后，本类会在寻址前查询睡醒注册表；
 * 当前版本只保证"同一个 id 路由到同一个实体"。
 */
package cactor_platform.session

import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_core.typed.{TypedActorRef, TypedPromise}
import cactor_cluster.membership.{ClusterSharding}

/** 按 sessionId 路由到集群分片实体的引用（不是本地 Actor，toClassic 返回 None） */
class SessionTypedRef<T> <: TypedActorRef<T> where T <: Message {
    private let registry: SessionRegistry
    private let sessionId: String
    private let path: String

    init(registry: SessionRegistry, sessionId: String, path: String) {
        this.registry = registry
        this.sessionId = sessionId
        this.path = path
    }

    public func tell(message: T): Unit {
        registry.tell(sessionId, message)
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<T>): Bool {
        other.pathString() == path
    }

    public func toString(): String {
        "SessionRef(${path})"
    }
}

/**
 * 会话寻址（P8）：把 sessionId 映射到集群分片实体。
 *
 * @param sharding 由使用方按会话实体类型创建的分片区域（消息需要可远程序列化）
 */
public class SessionRegistry {
    private let sharding: ClusterSharding
    private let prefix: String

    public init(sharding: ClusterSharding) {
        this.sharding = sharding
        this.prefix = "/session/${sharding.typeName}"
    }

    /** 分片区域使用的实体类型名 */
    public func typeName(): String {
        sharding.typeName
    }

    /** 发给会话实体（本地或远程由分片区域决定） */
    public func tell(sessionId: String, message: Message): Unit {
        sharding.tell(sessionId, message)
    }

    /** 带发送方发送：实体可用 context.reply 回复（跨节点也可） */
    public func tell(sessionId: String, message: Message, sender: ActorRef): Unit {
        sharding.tell(sessionId, message, sender)
    }

    /** 请求-响应（不阻塞当前线程；Actor 内请用 onComplete / pipeToSelf） */
    public func ask(sessionId: String, message: Message, timeout: Duration): TypedPromise<Message> {
        sharding.ask(sessionId, message, timeout)
    }

    /**
     * 取得"路由型"类型化引用：普通 tell 直接进分片区域。
     * 同一 sessionId 在同一集群视图下解析到同一个实体（与分片哈希一致）。
     */
    public func session<T>(sessionId: String): TypedActorRef<T> where T <: Message {
        SessionTypedRef<T>(this, sessionId, pathOf(sessionId))
    }

    /** 会话引用的路径（诊断与断言用） */
    public func pathOf(sessionId: String): String {
        "${prefix}/${sessionId}"
    }

    /** 本节点当前托管的会话实体 id（诊断用） */
    public func localSessionIds(): Array<String> {
        sharding.localEntityIds()
    }

    /** 分片区域当前用于路由的 Up 成员数（0 = 集群视图尚未就绪） */
    public func ownerCount(): Int64 {
        sharding.ownerCount()
    }

    /** 本节点托管的实体数 */
    public func localSessionCount(): Int64 {
        sharding.localEntityCount()
    }
}
