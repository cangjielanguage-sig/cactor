/*
 * CActor Platform.Session - 按 sessionId 定位 Actor（P8）
 *
 * 会话（AI 会话、用户会话、租户）是"按 id 寻址的长期 Actor"，CActor 的分片已经能做这件事：
 * entityId 经会合哈希落到某个节点的某个分片，跨节点时由区域转发。这里只做一层薄封装：
 *   - 不为每个 id 维护本地表（分片区域已经是权威）；
 *   - session<T>(id) 返回一个"路由型" TypedActorRef：tell 直接进分片，不占本地 Actor；
 *   - 消息协议由使用方定义（CActor 不定义 SessionMsg）。
 *
 * 睡醒模式（F1/P2）：可选接入 `ClusterSleepRegistry`。此时会话按 sessionId 的归属节点托管，
 * 睡着的会话在下次寻址时按存储里的状态唤醒（`sessionWakesIfSleeping`），节点宕机后由幸存节点接手。
 */
package cactor_platform.session

import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_core.typed.{TypedActorRef, TypedPromise}
import cactor_cluster.membership.{ClusterSharding}
import cactor_platform.lifecycle.{ClusterSleepRegistry}

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
 * 会话寻址（P8）：把 sessionId 映射到集群分片实体；可选接入睡醒注册表。
 *
 * 两种模式：
 *   - 仅分片（默认）：`tell` 直接进分片区域，会话实体由分片按需创建（既有行为）；
 *   - 睡醒模式（传入 `sleeper`）：`tell` 交给 `ClusterSleepRegistry`，
 *     睡着的会话在归属节点按存储里的状态唤醒——这正是 `sessionWakesIfSleeping` 的语义。
 *     该模式下 **不支持 `ask`**（跨节点转发只带载荷、不携带回复方）。
 */
public class SessionRegistry {
    private let sharding: Option<ClusterSharding>
    private let sleeper: Option<ClusterSleepRegistry>
    private let entityTypeName: String
    private let prefix: String

    /** 仅分片模式：由使用方按会话实体类型创建的分片区域 */
    public init(sharding: ClusterSharding) {
        this.sharding = Some<ClusterSharding>(sharding)
        this.sleeper = None<ClusterSleepRegistry>
        this.entityTypeName = sharding.typeName
        this.prefix = "/session/${sharding.typeName}"
    }

    /** 分片 + 睡醒：投递走睡醒注册表；分片区域保留用于类型名/路径等诊断 */
    public init(sharding: ClusterSharding, sleeper: ClusterSleepRegistry) {
        this.sharding = Some<ClusterSharding>(sharding)
        this.sleeper = Some<ClusterSleepRegistry>(sleeper)
        this.entityTypeName = sharding.typeName
        this.prefix = "/session/${sharding.typeName}"
    }

    /** 纯睡醒模式（不需要分片区域）：会话按 sessionId 的归属节点托管并可睡/醒 */
    public init(sleeper: ClusterSleepRegistry, typeName!: String = "session") {
        this.sharding = None<ClusterSharding>
        this.sleeper = Some<ClusterSleepRegistry>(sleeper)
        this.entityTypeName = typeName
        this.prefix = "/session/${typeName}"
    }

    /** 分片区域使用的实体类型名 */
    public func typeName(): String {
        entityTypeName
    }

    /** 发给会话实体（睡醒模式下由睡醒注册表唤醒并投递；否则由分片区域路由） */
    public func tell(sessionId: String, message: Message): Unit {
        match (sleeper) {
            case Some(s) => s.deliver(sessionId, message, None<ActorRef>)
            case None =>
                match (sharding) {
                    case Some(sh) => sh.tell(sessionId, message)
                    case None => ()
                }
        }
    }

    /** 带发送方发送：实体可用 context.reply 回复（分片模式跨节点也可；睡醒模式仅本地保留 sender） */
    public func tell(sessionId: String, message: Message, sender: ActorRef): Unit {
        match (sleeper) {
            case Some(s) => s.deliver(sessionId, message, Some<ActorRef>(sender))
            case None =>
                match (sharding) {
                    case Some(sh) => sh.tell(sessionId, message, sender)
                    case None => ()
                }
        }
    }

    /**
     * 请求-响应（不阻塞当前线程；Actor 内请用 onComplete / pipeToSelf）。
     * 睡醒模式不支持：跨节点转发只带载荷、不携带回复方。
     */
    public func ask(sessionId: String, message: Message, timeout: Duration): TypedPromise<Message> {
        match (sleeper) {
            case Some(_) =>
                throw IllegalStateException("SessionRegistry 配置了睡醒注册表时不支持 ask，请用 tell + reply")
            case None =>
                match (sharding) {
                    case Some(sh) => sh.ask(sessionId, message, timeout)
                    case None => throw IllegalStateException("SessionRegistry 未配置分片区域，无法 ask")
                }
        }
    }

    /**
     * 取得"路由型"类型化引用：普通 tell 直接进分片区域（或睡醒注册表）。
     * 同一 sessionId 在同一集群视图下解析到同一个实体（与分片哈希一致）。
     */
    public func session<T>(sessionId: String): TypedActorRef<T> where T <: Message {
        SessionTypedRef<T>(this, sessionId, pathOf(sessionId))
    }

    /** 会话引用的路径（诊断与断言用） */
    public func pathOf(sessionId: String): String {
        match (sleeper) {
            case Some(s) => s.pathOf(sessionId)
            case None => "${prefix}/${sessionId}"
        }
    }

    /** 本节点当前托管的会话 id（诊断用） */
    public func localSessionIds(): Array<String> {
        match (sleeper) {
            case Some(s) => s.localActiveKeys()
            case None =>
                match (sharding) {
                    case Some(sh) => sh.localEntityIds()
                    case None => Array<String>()
                }
        }
    }

    /** 当前用于路由的 Up 成员数（0 = 集群视图尚未就绪） */
    public func ownerCount(): Int64 {
        match (sleeper) {
            case Some(s) => s.ownerCount()
            case None =>
                match (sharding) {
                    case Some(sh) => sh.ownerCount()
                    case None => 0
                }
        }
    }

    /** 本节点托管的会话数 */
    public func localSessionCount(): Int64 {
        match (sleeper) {
            case Some(s) => s.localActiveCount()
            case None =>
                match (sharding) {
                    case Some(sh) => sh.localEntityCount()
                    case None => 0
                }
        }
    }

    /** key 的归属节点（睡醒模式；诊断与迁移断言用） */
    public func ownerOf(sessionId: String): Option<String> {
        match (sleeper) {
            case Some(s) => s.ownerOf(sessionId)
            case None => None<String>
        }
    }
}
