/*
 * CActor Platform.Party - 多方会话（F2 / AR9）
 *
 * 覆盖：广播到所有成员、成员上限明确拒绝、主持人失联后自动转移（DeathWatch）、
 * 显式离开后主持人顺延。
 */
package cactor_tests.unit.platform.party

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_platform.party.*
import cactor_testkit.*

class PartyProbeActor <: Actor {
    let seen = EventRecorder()

    public prop name: String {
        get() { "party-probe" }
    }

    public prop description: String {
        get() { "party member probe" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage => seen.record(s.getContent())
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

@Test
public class PartyTest {
    @TestCase
    public func partyBroadcastReachesAllMembers(): Unit {
        let kit = ActorTestKit("party-broadcast")
        let party = PartyHost.start(kit.system, "party-1")
        let p1 = PartyProbeActor()
        let p2 = PartyProbeActor()
        let p3 = PartyProbeActor()
        let r1 = kit.actorOf({ => p1 }, "party-m1")
        let r2 = kit.actorOf({ => p2 }, "party-m2")
        let r3 = kit.actorOf({ => p3 }, "party-m3")
        @Expect(party.join(PartyMember.create("a", "host", r1)).accepted, true)
        @Expect(party.join(PartyMember.create("b", "member", r2)).accepted, true)
        @Expect(party.join(PartyMember.create("c", "member", r3)).accepted, true)
        @Expect(party.memberIds().size, 3)
        @Expect(party.hostId(), "a")
        @Expect(party.broadcast("hello"), 3)
        @Expect(awaitCondition(Duration.second * 5, { =>
            p1.seen.count("hello") == 1 && p2.seen.count("hello") == 1 && p3.seen.count("hello") == 1
        }), true)
        // 排除发起者
        @Expect(party.broadcast("again", exceptMemberId: Some("a")), 2)
        @Expect(awaitCondition(Duration.second * 5, { =>
            p1.seen.count("again") == 0 && p2.seen.count("again") == 1 && p3.seen.count("again") == 1
        }), true)
        party.stop()
        kit.shutdown()
    }

    @TestCase
    public func partyRejectsWhenFull(): Unit {
        let kit = ActorTestKit("party-full")
        let party = PartyHost.start(kit.system, "party-full", maxMembers: 2)
        let p1 = kit.actorOf({ => PartyProbeActor() }, "party-f1")
        let p2 = kit.actorOf({ => PartyProbeActor() }, "party-f2")
        let p3 = kit.actorOf({ => PartyProbeActor() }, "party-f3")
        @Expect(party.join(PartyMember.create("a", "", p1)).accepted, true)
        @Expect(party.join(PartyMember.create("b", "", p2)).accepted, true)
        let rejected = party.join(PartyMember.create("c", "", p3))
        @Expect(rejected.accepted, false)
        @Expect(rejected.reason, "party is full")
        @Expect(party.memberCount(), 2)
        party.stop()
        kit.shutdown()
    }

    @TestCase
    public func partyHostTransfersOnHostLeave(): Unit {
        let kit = ActorTestKit("party-host")
        let party = PartyHost.start(kit.system, "party-host")
        let p1 = PartyProbeActor()
        let p2 = PartyProbeActor()
        let p3 = PartyProbeActor()
        let r1 = kit.actorOf({ => p1 }, "party-h1")
        let r2 = kit.actorOf({ => p2 }, "party-h2")
        let r3 = kit.actorOf({ => p3 }, "party-h3")
        @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
        @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)
        @Expect(party.join(PartyMember.create("c", "", r3)).accepted, true)
        @Expect(party.hostId(), "a")
        // 主持人失联（DeathWatch）：顺延到最早加入的存活成员
        kit.system.stop(r1)
        @Expect(awaitCondition(Duration.second * 5, { => party.hostId() == "b" }), true)
        @Expect(party.memberIds().size, 2)
        // 显式离开也让主持人顺延
        @Expect(party.leave("b"), true)
        @Expect(party.hostId(), "c")
        @Expect(party.memberIds().size, 1)
        @Expect(party.leave("nobody"), false)
        party.stop()
        kit.shutdown()
    }
}
