/*
 * CActor Platform.Party - 多方会话（F2 / AR9）
 *
 * Party 是一个有主持者的成员表：加入/离开、广播、成员上限、主持人失联自动转移。
 * 成员可以是本地或远程 ActorRef（跨节点的广播直接 tell），因此 Party 本身放在哪个节点
 * 不影响成员分布；主持人 = 最早加入的存活成员（离开/失联后自动顺延，无需额外选举）。
 *
 * 成员失联检测复用 DeathWatch：加入时 context.watch，Terminated 时移除；显式 leave 时 unwatch。
 * 本原语不做业务语义（群聊/协作/辩论的规则由使用方定义），只维护成员关系与投递。
 *
 * 邀请（F2 扩展）：由 Party 成员签发、可撤销、可过期的邀请令牌，复用 G1 的 ScopedToken
 * （HMAC 签名、路径绑定到本 Party、scope=call），不引入新密码学。
 */
package cactor_platform.party

import std.collection.*
import std.time.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated}
import cactor_core.dsl.{AskInbox}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_remote.{RemoteActorRef}
import cactor_platform.auth.{AccessOp, ScopedToken, ScopedTokenCodec, ScopedTokenResult}

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

/**
 * 邀请（F2 扩展）：`id` 用于撤销/查询，`token` 是交给客户端的 G1 ScopedToken
 * （绑定本 Party 路径、scope=call、带过期时间）。
 */
public class PartyInvite {
    public let id: String
    public let token: String
    public let partyPath: String
    public let expiresAtMillis: Int64

    public init(id: String, token: String, partyPath: String, expiresAtMillis: Int64) {
        this.id = id
        this.token = token
        this.partyPath = partyPath
        this.expiresAtMillis = expiresAtMillis
    }
}

/** 邀请签发结果：未配置密钥 / 签发者非成员 / TTL 非正数都以明确原因返回（不静默失败） */
public struct PartyInviteOutcome {
    public let invite: Option<PartyInvite>
    public let reason: String

    public init(invite: Option<PartyInvite>, reason: String) {
        this.invite = invite
        this.reason = reason
    }
}

/** 管理操作结果（踢人/转移主持人）：拒绝带明确原因，不静默 */
public struct PartyAdminOutcome {
    public let ok: Bool
    public let reason: String
    public let hostId: String

    public init(ok: Bool, reason: String, hostId: String) {
        this.ok = ok
        this.reason = reason
        this.hostId = hostId
    }
}

/** Party 概要（GetPartySummary）：成员、主持人、公开性与未撤销邀请数 */
public class PartySummary {
    public let memberIds: Array<String>
    public let hostId: String
    public let isPublic: Bool
    public let inviteCount: Int64

    public init(memberIds: Array<String>, hostId: String, isPublic: Bool, inviteCount: Int64) {
        this.memberIds = memberIds
        this.hostId = hostId
        this.isPublic = isPublic
        this.inviteCount = inviteCount
    }

    public func memberCount(): Int64 {
        Int64(memberIds.size)
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

// ---------------- 邀请协议（F2 扩展） ----------------

class PartyCreateInvite <: Message {
    let inviterId: String
    let subject: String
    let ttlMillis: Int64

    init(inviterId: String, subject: String, ttlMillis: Int64) {
        this.inviterId = inviterId
        this.subject = subject
        this.ttlMillis = ttlMillis
    }

    public func messageType(): String { "PartyCreateInvite" }
}

class PartyRevokeInvite <: Message {
    let inviteId: String

    init(inviteId: String) {
        this.inviteId = inviteId
    }

    public func messageType(): String { "PartyRevokeInvite" }
}

class PartyInviteQuery <: Message {
    let inviteId: String

    init(inviteId: String) {
        this.inviteId = inviteId
    }

    public func messageType(): String { "PartyInviteQuery" }
}

class PartyJoinByInvite <: Message {
    let inviteId: String
    let member: PartyMember

    init(inviteId: String, member: PartyMember) {
        this.inviteId = inviteId
        this.member = member
    }

    public func messageType(): String { "PartyJoinByInvite" }
}

class PartyCreateInviteReply <: Message {
    let outcome: PartyInviteOutcome

    init(outcome: PartyInviteOutcome) {
        this.outcome = outcome
    }

    public func messageType(): String { "PartyCreateInviteReply" }
}

class PartyRevokeInviteReply <: Message {
    let revoked: Bool

    init(revoked: Bool) {
        this.revoked = revoked
    }

    public func messageType(): String { "PartyRevokeInviteReply" }
}

class PartyInviteQueryReply <: Message {
    let partyPath: Option<String>
    let reason: String

    init(partyPath: Option<String>, reason: String) {
        this.partyPath = partyPath
        this.reason = reason
    }

    public func messageType(): String { "PartyInviteQueryReply" }
}

// ---------------- 管理协议（F2 扩展：踢人/转移主持人/公开性/概要） ----------------

class PartyKick <: Message {
    let kickerId: String
    let targetId: String

    init(kickerId: String, targetId: String) {
        this.kickerId = kickerId
        this.targetId = targetId
    }

    public func messageType(): String { "PartyKick" }
}

class PartyTransfer <: Message {
    let targetId: String

    init(targetId: String) {
        this.targetId = targetId
    }

    public func messageType(): String { "PartyTransfer" }
}

class PartySetPublicity <: Message {
    let value: Bool

    init(value: Bool) {
        this.value = value
    }

    public func messageType(): String { "PartySetPublicity" }
}

class PartySummaryQuery <: Message {
    public func messageType(): String { "PartySummaryQuery" }
}

class PartyAdminReply <: Message {
    let outcome: PartyAdminOutcome

    init(outcome: PartyAdminOutcome) {
        this.outcome = outcome
    }

    public func messageType(): String { "PartyAdminReply" }
}

class PartyPublicityReply <: Message {
    let value: Bool

    init(value: Bool) {
        this.value = value
    }

    public func messageType(): String { "PartyPublicityReply" }
}

class PartySummaryReply <: Message {
    let summary: PartySummary

    init(summary: PartySummary) {
        this.summary = summary
    }

    public func messageType(): String { "PartySummaryReply" }
}

/** 维护成员表与主持者；所有状态只在本 Actor 线程上修改，无需锁 */
class PartyActor <: Actor {
    private let maxMembers: Int64
    // 邀请签名密钥；空 = 未启用邀请
    private let secret: String
    // 按加入顺序保存；主持人 = 第一个存活成员
    private let members = ArrayList<PartyMember>()
    // 未撤销的邀请：inviteId -> ScopedToken
    private let invites = HashMap<String, String>()
    private var inviteSeq: Int64 = 0
    private var partyPath: String = ""
    // 显式指定的主持人（TransferPartyOwnership）；为空时回退到"最早加入的存活成员"
    private var hostOverride: Option<String> = None<String>
    private var isPublic: Bool = false

    init(maxMembers: Int64, secret: String) {
        this.maxMembers = maxMembers
        this.secret = secret
    }

    public func onStarted(context: ActorContext): Unit {
        partyPath = context.self.path.toString()
    }

    private func nowMillis(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }

    /** 校验邀请：存在、未撤销、签名有效、未过期、绑定本 Party 且允许 call */
    private func verifyInvite(inviteId: String): (Option<ScopedToken>, String) {
        let token = match (invites.get(inviteId)) {
            case Some(t) => t
            case None => return (None<ScopedToken>, "invite not found or revoked")
        }
        if (secret.isEmpty()) {
            return (None<ScopedToken>, "invites disabled: party secret not configured")
        }
        match (ScopedTokenCodec.verify(secret, token, nowMillis: nowMillis())) {
            case ScopedTokenResult.Valid(t) =>
                if (!t.covers(partyPath) || !t.allows(AccessOp.Call)) {
                    (None<ScopedToken>, "invite scope mismatch")
                } else {
                    (Some<ScopedToken>(t), "")
                }
            case ScopedTokenResult.Expired(_) => (None<ScopedToken>, "invite expired")
            case ScopedTokenResult.Invalid(reason) => (None<ScopedToken>, "invite invalid: ${reason}")
        }
    }

    /** 加入的公共逻辑（普通加入与按邀请加入共用） */
    private func joinOutcome(member: PartyMember, context: ActorContext): PartyJoinOutcome {
        if (indexOf(member.id) >= 0) {
            return PartyJoinOutcome(true, "already joined", hostId())
        }
        if (Int64(members.size) >= maxMembers) {
            return PartyJoinOutcome(false, "party is full", hostId())
        }
        members.add(member)
        // 远程成员不能走本地 DeathWatch；跨节点离开由客户端显式 leave
        match (member.ref) {
            case _: RemoteActorRef => ()
            case _ => context.watch(member.ref)
        }
        PartyJoinOutcome(true, "", hostId())
    }

    public prop name: String {
        get() { "party" }
    }

    public prop description: String {
        get() { "party member registry" }
    }

    private func hostId(): String {
        if (let Some(id) <- hostOverride) {
            if (indexOf(id) >= 0) {
                return id
            }
        }
        if (members.size > 0) {
            members[0].id
        } else {
            ""
        }
    }

    /** 移除成员后清理失效的主持人覆盖（回退到"最早加入的存活成员"） */
    private func clearHostIf(removedId: String): Unit {
        match (hostOverride) {
            case Some(h) => if (h == removedId) { hostOverride = None<String> }
            case None => ()
        }
    }

    /** 踢人：只有主持人可踢、不能踢自己、目标必须存在（拒绝带明确原因） */
    private func kickOutcome(kickerId: String, targetId: String, context: ActorContext): PartyAdminOutcome {
        if (indexOf(kickerId) < 0) {
            return PartyAdminOutcome(false, "kicker is not a member", hostId())
        }
        if (kickerId != hostId()) {
            return PartyAdminOutcome(false, "only host can kick", hostId())
        }
        if (targetId == kickerId) {
            return PartyAdminOutcome(false, "cannot kick self", hostId())
        }
        let idx = indexOf(targetId)
        if (idx < 0) {
            return PartyAdminOutcome(false, "member not found", hostId())
        }
        let m = removeAt(idx)
        match (m.ref) {
            case _: RemoteActorRef => ()
            case _ => context.unwatch(m.ref)
        }
        clearHostIf(m.id)
        PartyAdminOutcome(true, "", hostId())
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
                context.reply(PartyJoinReply(joinOutcome(j.member, context)))
            case ji: PartyJoinByInvite =>
                let (token, reason) = verifyInvite(ji.inviteId)
                match (token) {
                    case Some(_) => context.reply(PartyJoinReply(joinOutcome(ji.member, context)))
                    case None => context.reply(PartyJoinReply(PartyJoinOutcome(false, reason, hostId())))
                }
            case ci: PartyCreateInvite =>
                if (secret.isEmpty()) {
                    context.reply(PartyCreateInviteReply(
                        PartyInviteOutcome(None<PartyInvite>, "invites disabled: party secret not configured")))
                } else if (indexOf(ci.inviterId) < 0) {
                    context.reply(PartyCreateInviteReply(
                        PartyInviteOutcome(None<PartyInvite>, "inviter is not a member")))
                } else if (ci.ttlMillis <= 0) {
                    context.reply(PartyCreateInviteReply(
                        PartyInviteOutcome(None<PartyInvite>, "invite ttl must be positive")))
                } else {
                    let now = nowMillis()
                    let token = ScopedTokenCodec.issue(secret, ci.subject, partyPath, ["call"],
                        ttlMillis: ci.ttlMillis, nowMillis: now)
                    inviteSeq++
                    let id = "inv-${inviteSeq}"
                    invites[id] = token
                    context.reply(PartyCreateInviteReply(
                        PartyInviteOutcome(Some<PartyInvite>(PartyInvite(id, token, partyPath, now + ci.ttlMillis)), "")))
                }
            case ri: PartyRevokeInvite =>
                let existed = invites.contains(ri.inviteId)
                if (existed) {
                    invites.remove(ri.inviteId)
                }
                context.reply(PartyRevokeInviteReply(existed))
            case iq: PartyInviteQuery =>
                let (token, reason) = verifyInvite(iq.inviteId)
                match (token) {
                    case Some(_) => context.reply(PartyInviteQueryReply(Some<String>(partyPath), ""))
                    case None => context.reply(PartyInviteQueryReply(None<String>, reason))
                }
            case k: PartyKick =>
                context.reply(PartyAdminReply(kickOutcome(k.kickerId, k.targetId, context)))
            case tr: PartyTransfer =>
                if (indexOf(tr.targetId) < 0) {
                    context.reply(PartyAdminReply(PartyAdminOutcome(false, "target is not a member", hostId())))
                } else {
                    hostOverride = Some<String>(tr.targetId)
                    context.reply(PartyAdminReply(PartyAdminOutcome(true, "", hostId())))
                }
            case sp: PartySetPublicity =>
                isPublic = sp.value
                context.reply(PartyPublicityReply(isPublic))
            case _: PartySummaryQuery =>
                let ids = ArrayList<String>()
                for (m in members) {
                    ids.add(m.id)
                }
                context.reply(PartySummaryReply(PartySummary(ids.toArray(), hostId(), isPublic,
                    Int64(invites.size))))
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
                    clearHostIf(m.id)
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
                        let removed = members[i]
                        members.remove(at: i)
                        clearHostIf(removed.id)
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

    /**
     * 创建并启动一个 Party Actor。
     * @param secret 邀请签名密钥（复用 G1 ScopedToken）；为空时创建邀请会以明确原因失败
     */
    public static func start(system: CActorSystem, name: String, maxMembers!: Int64 = 16,
        secret!: String = "", timeout!: Duration = Duration.second * 5): PartyHost {
        if (maxMembers <= 0) {
            throw IllegalArgumentException("Party 成员上限必须为正数: ${maxMembers}")
        }
        let ref = system.actorOf({ => PartyActor(maxMembers, secret) }, name)
        PartyHost(system, ref, timeout)
    }

    /** 签发邀请（仅 Party 现有成员可签发）；返回的 token 交给客户端 */
    public func createInvite(inviterId: String, subject: String, ttl: Duration): PartyInviteOutcome {
        let cmd = PartyCreateInvite(inviterId, subject, ttl.toMilliseconds())
        match (AskInbox.ask(ref, cmd).get(timeout) as PartyCreateInviteReply) {
            case Some(r) => r.outcome
            case None => throw IllegalStateException("Party.createInvite 收到了非预期回复")
        }
    }

    /** 撤销邀请（幂等：已撤销/不存在返回 false） */
    public func revokeInvite(inviteId: String): Bool {
        match (AskInbox.ask(ref, PartyRevokeInvite(inviteId)).get(timeout) as PartyRevokeInviteReply) {
            case Some(r) => r.revoked
            case None => throw IllegalStateException("Party.revokeInvite 收到了非预期回复")
        }
    }

    /** 按邀请查 Party 路径（GetPartyFromInvite）：有效则返回本 Party 路径 */
    public func partyOfInvite(inviteId: String): Option<String> {
        match (AskInbox.ask(ref, PartyInviteQuery(inviteId)).get(timeout) as PartyInviteQueryReply) {
            case Some(r) => r.partyPath
            case None => throw IllegalStateException("Party.partyOfInvite 收到了非预期回复")
        }
    }

    /** 按邀请加入（SendJoinRequest）：邀请无效/过期/被撤销时 accepted=false 并给出原因 */
    public func joinByInvite(inviteId: String, member: PartyMember): PartyJoinOutcome {
        match (AskInbox.ask(ref, PartyJoinByInvite(inviteId, member)).get(timeout) as PartyJoinReply) {
            case Some(r) => r.outcome
            case None => throw IllegalStateException("Party.joinByInvite 收到了非预期回复")
        }
    }

    /** 踢人（KickMember）：只有主持人可踢，不能踢自己；拒绝带明确原因 */
    public func kick(kickerId: String, targetId: String): PartyAdminOutcome {
        match (AskInbox.ask(ref, PartyKick(kickerId, targetId)).get(timeout) as PartyAdminReply) {
            case Some(r) => r.outcome
            case None => throw IllegalStateException("Party.kick 收到了非预期回复")
        }
    }

    /** 显式转移主持人（TransferPartyOwnership）：目标必须是当前成员 */
    public func transferOwnership(targetId: String): PartyAdminOutcome {
        match (AskInbox.ask(ref, PartyTransfer(targetId)).get(timeout) as PartyAdminReply) {
            case Some(r) => r.outcome
            case None => throw IllegalStateException("Party.transferOwnership 收到了非预期回复")
        }
    }

    /** 设置公开性（SetPartyPublicity），返回设置后的值 */
    public func setPublicity(value: Bool): Bool {
        match (AskInbox.ask(ref, PartySetPublicity(value)).get(timeout) as PartyPublicityReply) {
            case Some(r) => r.value
            case None => throw IllegalStateException("Party.setPublicity 收到了非预期回复")
        }
    }

    /** Party 概要（GetPartySummary）：成员、主持人、公开性、未撤销邀请数 */
    public func summary(): PartySummary {
        match (AskInbox.ask(ref, PartySummaryQuery()).get(timeout) as PartySummaryReply) {
            case Some(r) => r.summary
            case None => throw IllegalStateException("Party.summary 收到了非预期回复")
        }
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
