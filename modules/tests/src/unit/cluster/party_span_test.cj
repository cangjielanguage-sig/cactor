/*
 * CActor Platform.Party - 跨节点成员（F2 / AR9）
 *
 * Party 放在 node0，成员分别位于 node0（本地引用）与 node1（经 exposeActor/remoteActorOf
 * 得到远程引用）；广播对远程成员走真实 TCP，验证成员分布不限于 Party 所在节点。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_platform.party.*
import cactor_testkit.*

class PartyRemoteProbe <: Actor {
    let seen = EventRecorder()

    public prop name: String {
        get() { "party-remote-probe" }
    }

    public prop description: String {
        get() { "party remote probe" }
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
public class PartySpanTest {
    @TestCase
    public func partySpansClusterNodes(): Unit {
        let a = CNode("party-span-a")
        let b = CNode("party-span-b")
        let localProbe = PartyRemoteProbe()
        let remoteProbe = PartyRemoteProbe()
        let localRef = a.kit.system.actorOf({ => localProbe }, "party-local")
        let remoteLocalRef = b.kit.system.actorOf({ => remoteProbe }, "party-remote")
        b.remote.exposeActor("party-remote", remoteLocalRef)
        let remoteRef = a.remote.remoteActorOf(b.addr(), "party-remote")

        let party = PartyHost.start(a.kit.system, "party-span")
        @Expect(party.join(PartyMember.create("local", "", localRef)).accepted, true)
        @Expect(party.join(PartyMember.create("remote", "", remoteRef)).accepted, true)
        @Expect(party.hostId(), "local")
        @Expect(party.broadcast("hi"), 2)
        @Expect(awaitCondition(Duration.second * 10, { => localProbe.seen.count("hi") == 1 }), true)
        @Expect(awaitCondition(Duration.second * 10, { => remoteProbe.seen.count("hi") == 1 }), true)

        party.stop()
        a.remote.stop()
        a.kit.shutdown()
        b.remote.stop()
        b.kit.shutdown()
    }
}
