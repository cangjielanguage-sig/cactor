/*
 * CActor Platform.Party - 多方会话（F2 / AR9）
 *
 * Party 是一个有主持者的成员表：加入/离开、广播、成员上限、主持人失联自动转移。
 * 成员可以是本地或远程 ActorRef（跨节点的广播直接 tell），因此 Party 本身放在哪个节点
 * 不影响成员分布；主持人 = 最早加入的存活成员（离开/失联后自动顺延，无需额外选举）。
 *
 * 成员失联检测复用 DeathWatch：加入时 context.watch，Terminated 时移除；显式 leave 时 unwatch。
 * 本原语不做业务语义（群聊/协作/辩论的规则由使用方定义），只维护成员关系与投递。
 */
package cactor_platform.party

import std.collection.*
import std.time.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated}
import cactor_core.dsl.{AskInbox}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_remote.{RemoteActorRef}

/** 派对成员：id 唯一，ref 用于广播与 DeathWatch */
public class PartyMember {
    public let id: String
    public let role: String
    public let ref: ActorRef
    public let joinedAtMillis: Int64

    public init(id: String, role: String, ref: ActorRef, joinedAtMillis: Int64) {
        this.id = id
        this.role = role
        this.ref = ref
        this.joinedAtMillis = joinedAtMillis
    }

    public static func create(id: String, role: String, ref: ActorRef): PartyMember {
        PartyMember(id, role, ref, DateTime.now().toUnixTimeStamp().toMilliseconds())
    }
}

/** 加入结果：上限拒绝是明确结果（accepted=false + reason），不是静默丢弃 */
public struct PartyJoinOutcome {
    public let accepted: Bool
    public let reason: String
    public let hostId: String

    public init(accepted: Bool, reason: String, hostId: String) {
        this.accepted = accepted
        this.reason = reason
        this.hostId = hostId
    }
}

// ---------------- 内部协议（经典消息，只在进程内使用） ----------------

class PartyJoin <: Message {
    let member: PartyMember

    init(member: PartyMember) {
        this.member = member
    }

    public func messageType(): String { "PartyJoin" }
}

class PartyLeave <: Message {
    let memberId: String

    init(memberId: String) {
        this.memberId = memberId
    }

    public func messageType(): String { "PartyLeave" }
}

class PartyBroadcastCmd <: Message {
    let text: String
    let exceptMemberId: Option<String>

    init(text: String, exceptMemberId: Option<String>) {
        this.text = text
        this.exceptMemberId = exceptMemberId
    }

    public func messageType(): String { "PartyBroadcast" }
}

class PartyMembersQuery <: Message {
    public func messageType(): String { "PartyMembersQuery" }
}

class PartyJoinReply <: Message {
    let outcome: PartyJoinOutcome

    init(outcome: PartyJoinOutcome) {
        this.outcome = outcome
    }

    public func messageType(): String { "PartyJoinReply" }
}

class PartyLeaveReply <: Message {
    let removed: Bool

    init(removed: Bool) {
        this.removed = removed
    }

    public func messageType(): String { "PartyLeaveReply" }
}

class PartyBroadcastReply <: Message {
    let delivered: Int64

    init(delivered: Int64) {
        this.delivered = delivered
    }

    public func messageType(): String { "PartyBroadcastReply" }
}

class PartyMembersReply <: Message {
    let members: Array<String>
    let hostId: String

    init(members: Array<String>, hostId: String) {
        this.members = members
        this.hostId = hostId
    }

    public func messageType(): String { "PartyMembersReply" }
}

/** 维护成员表与主持者；所有状态只在本 Actor 线程上修改，无需锁 */
class PartyActor <: Actor {
    private let maxMembers: Int64
    // 按加入顺序保存；主持人 = 第一个存活成员
    private let members = ArrayList<PartyMember>()

    init(maxMembers: Int64) {
        this.maxMembers = maxMembers
    }

    public prop name: String {
        get() { "party" }
    }

    public prop description: String {
        get() { "party member registry" }
    }

    private func hostId(): String {
        if (members.size > 0) {
            members[0].id
        } else {
            ""
        }
    }

    private func indexOf(id: String): Int64 {
        for (i in 0..members.size) {
            if (members[i].id == id) {
                return i
            }
        }
        -1
    }

    private func removeAt(index: Int64): PartyMember {
        let m = members[index]
        members.remove(at: index)
        m
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case j: PartyJoin =>
                if (indexOf(j.member.id) >= 0) {
                    // 幂等加入：已存在视为成功，避免客户端重连时把旧成员顶掉
                    context.reply(PartyJoinReply(PartyJoinOutcome(true, "already joined", hostId())))
                    return MessageResult.Handled
                }
                if (Int64(members.size) >= maxMembers) {
                    context.reply(PartyJoinReply(PartyJoinOutcome(false, "party is full", hostId())))
                    return MessageResult.Handled
                }
                members.add(j.member)
                // 远程成员不能走本地 DeathWatch（watch 只接受本系统 Actor）；
                // 跨节点离开由客户端显式 leave，本地成员才自动清理
                match (j.member.ref) {
                    case _: RemoteActorRef => ()
                    case _ => context.watch(j.member.ref)
                }
                context.reply(PartyJoinReply(PartyJoinOutcome(true, "", hostId())))
            case l: PartyLeave =>
                let idx = indexOf(l.memberId)
                if (idx < 0) {
                    context.reply(PartyLeaveReply(false))
                } else {
                    let m = removeAt(idx)
                    match (m.ref) {
                        case _: RemoteActorRef => ()
                        case _ => context.unwatch(m.ref)
                    }
                    context.reply(PartyLeaveReply(true))
                }
            case b: PartyBroadcastCmd =>
                var delivered: Int64 = 0
                for (m in members) {
                    let skip = match (b.exceptMemberId) {
                        case Some(id) => id == m.id
                        case None => false
                    }
                    if (!skip) {
                        m.ref.tell(StringMessage(b.text))
                        delivered++
                    }
                }
                context.reply(PartyBroadcastReply(delivered))
            case q: PartyMembersQuery =>
                let ids = ArrayList<String>()
                for (m in members) {
                    ids.add(m.id)
                }
                context.reply(PartyMembersReply(ids.toArray(), hostId()))
            case t: Terminated =>
                let path = t.actor.path.toString()
                for (i in 0..members.size) {
                    if (members[i].ref.path.toString() == path) {
                        members.remove(at: i)
                        break
                    }
                }
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

/**
 * Party 的调用端：所有方法在调用线程同步等待 Actor 的回复。
 * 不要在有严格延迟要求的 Actor 处理方法里调用阻塞方法（用 tell 版本或放入 ThreadPerActor）。
 */
public class PartyHost {
    private let ref: ActorRef
    private let system: CActorSystem
    private let timeout: Duration

    private init(system: CActorSystem, ref: ActorRef, timeout: Duration) {
        this.system = system
        this.ref = ref
        this.timeout = timeout
    }

    /** 创建并启动一个 Party Actor */
    public static func start(system: CActorSystem, name: String, maxMembers!: Int64 = 16,
        timeout!: Duration = Duration.second * 5): PartyHost {
        if (maxMembers <= 0) {
            throw IllegalArgumentException("Party 成员上限必须为正数: ${maxMembers}")
        }
        let ref = system.actorOf({ => PartyActor(maxMembers) }, name)
        PartyHost(system, ref, timeout)
    }

    public func join(member: PartyMember): PartyJoinOutcome {
        match (AskInbox.ask(ref, PartyJoin(member)).get(timeout) as PartyJoinReply) {
            case Some(r) => r.outcome
            case None => throw IllegalStateException("Party.join 收到了非预期回复")
        }
    }

    public func leave(memberId: String): Bool {
        match (AskInbox.ask(ref, PartyLeave(memberId)).get(timeout) as PartyLeaveReply) {
            case Some(r) => r.removed
            case None => throw IllegalStateException("Party.leave 收到了非预期回复")
        }
    }

    /** 广播文本给所有成员（可排除一个成员，例如消息发起者） */
    public func broadcast(text: String, exceptMemberId!: Option<String> = None): Int64 {
        match (AskInbox.ask(ref, PartyBroadcastCmd(text, exceptMemberId)).get(timeout) as PartyBroadcastReply) {
            case Some(r) => r.delivered
            case None => throw IllegalStateException("Party.broadcast 收到了非预期回复")
        }
    }

    /** 当前成员 id（按加入顺序） */
    public func memberIds(): Array<String> {
        match (AskInbox.ask(ref, PartyMembersQuery()).get(timeout) as PartyMembersReply) {
            case Some(r) => r.members
            case None => throw IllegalStateException("Party.members 收到了非预期回复")
        }
    }

    /** 当前主持人 id；成员为空时为空串 */
    public func hostId(): String {
        match (AskInbox.ask(ref, PartyMembersQuery()).get(timeout) as PartyMembersReply) {
            case Some(r) => r.hostId
            case None => throw IllegalStateException("Party.host 收到了非预期回复")
        }
    }

    public func memberCount(): Int64 {
        Int64(memberIds().size)
    }

    public func stop(): Unit {
        system.stop(ref)
    }
}
