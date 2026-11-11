/* CActor Remote - 对端身份和握手挑战容量回归。 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TransportHandshake}
import cactor_remote.*
import cactor_testkit.*

class PeerBudgetWire <: NetworkTransport {
    var handshake: Option<TransportHandshake> = None<TransportHandshake>
    var active = false
    public func start(): Unit { active = true }
    public func stop(): Unit { active = false }
    public func isActive(): Bool { active }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", 1) }
    public func setDataHandler(_: (Array<UInt8>) -> Unit): Unit { () }
    public func send(_: NetworkAddress, _: Array<UInt8>): Unit { () }
    public func setHandshake(h: TransportHandshake): Unit { handshake = Some(h) }
    func admit(port: Int64, uid: UInt64): Bool {
        handshake.getOrThrow().accept("HELLO|1|127.0.0.1:${port}|${uid}|0|127.0.0.1:1|-".toArray()).isSome()
    }
}

@Test
public class RemotePeerBudgetTest {
    @TestCase
    public func buildersPreserveBudgetAndRejectInvalidLimit(): Unit {
        let s = RemotingSettings.defaults().withPeerLimit(2).withAskLimit(3)
            .withLimits(4, Duration.minute).withChannelLimits(5, 6).withSecureCookie("test")
        @Expect(s.maxKnownPeers, 2)
        @ExpectThrows[IllegalArgumentException](s.withPeerLimit(0))
        @ExpectThrows[IllegalArgumentException](s.withPeerLimit(-1))
    }

    @TestCase
    public func concurrentAdmissionPreservesExistingIdentityAndQuarantine(): Unit {
        let wire = PeerBudgetWire()
        let s = RemotingSettings.defaults().withPeerLimit(2)
        let m = nodeWith(wire, s)
        try {
            @Expect(wire.admit(2, 1), true)
            let admitted = AtomicInt64(0)
            let done = AtomicInt64(0)
            for (i in 0..50) {
                spawn {
                    if (wire.admit(100 + i, 1)) { admitted.fetchAdd(1) }
                    done.fetchAdd(1)
                }
            }
            @Expect(awaitCondition(T5S, { => done.load() == 50 }), true)
            @Expect(admitted.load(), 1)
            @Expect(wire.admit(2, 1), true)
            @Expect(wire.admit(2, 2), true)
            @Expect(wire.admit(2, 1), false)
            @Expect(m.quarantinedCount(), 1)
            @ExpectThrows[IllegalArgumentException](m.configure(s.withPeerLimit(1)))
            @Expect(wire.admit(999, 1), false)
        } finally { m.stop() }
    }

    @TestCase
    public func fullQuarantineTableRejectsRestartWithoutReplacingCurrentIdentity(): Unit {
        let wire = PeerBudgetWire()
        let s = RemotingSettings.defaults().withPeerLimit(1)
        let m = nodeWith(wire, s)
        try {
            @Expect(wire.admit(2, 1), true)
            @Expect(wire.admit(2, 2), true)
            @Expect(m.quarantinedCount(), 1)
            @Expect(wire.admit(2, 3), false)
            @Expect(wire.admit(2, 2), true)
            @Expect(wire.admit(2, 1), false)
            @Expect(m.isPeerAdmissionClosed(), false)
            m.configure(s.withPeerLimit(2))
            @Expect(wire.admit(2, 3), true)
            @Expect(m.quarantinedCount(), 2)
            @ExpectThrows[IllegalArgumentException](m.configure(s))
            @Expect(wire.admit(2, 2), false)
        } finally { m.stop() }
    }

    @TestCase
    public func quarantineOverflowClosesAdmissionWithoutEvictingHistory(): Unit {
        let wire = PeerBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults().withPeerLimit(2))
        let address = NetworkAddress("127.0.0.1", 2)
        try {
            @Expect(wire.admit(2, 1), true)
            m.quarantine(address, 1, "removed")
            m.quarantine(address, 1, "duplicate")
            @Expect(m.isPeerAdmissionClosed(), false)
            @Expect(m.quarantinedCount(), 1)
            @Expect(wire.admit(2, 2), true)
            let done = AtomicInt64(0)
            for (i in 0..50) {
                spawn {
                    m.quarantine(address, UInt64(100 + i), "retired")
                    done.fetchAdd(1)
                }
            }
            @Expect(awaitCondition(T5S, { => done.load() == 50 }), true)
            @Expect(m.quarantinedCount(), 2)
            @Expect(m.isPeerAdmissionClosed(), true)
            @Expect(wire.admit(2, 1), false)
            @Expect(wire.admit(2, 2), false)
            @Expect(wire.admit(3, 1), false)
            m.stop()
            m.configure(RemotingSettings.defaults().withPeerLimit(100))
            m.start()
            @Expect(m.isPeerAdmissionClosed(), true)
            @Expect(wire.admit(2, 3), false)
            @Expect(m.quarantinedCount(), 2)
        } finally { m.stop() }
    }

    @TestCase
    public func challengeBudgetRejectsNewAddressesAndSurvivesRestart(): Unit {
        let wire = PeerBudgetWire()
        let s = RemotingSettings.defaults().withPeerLimit(1)
        let m = nodeWith(wire, s)
        try {
            let h = wire.handshake.getOrThrow()
            h.hello(NetworkAddress("127.0.0.1", 2))
            h.hello(NetworkAddress("127.0.0.1", 2))
            @ExpectThrows[RemotePeerCapacityException](h.hello(NetworkAddress("127.0.0.1", 3)))
            m.stop()
            m.start()
            @ExpectThrows[RemotePeerCapacityException](h.hello(NetworkAddress("127.0.0.1", 3)))
            m.configure(s.withPeerLimit(2))
            h.hello(NetworkAddress("127.0.0.1", 3))
            @ExpectThrows[IllegalArgumentException](m.configure(s))
        } finally { m.stop() }
    }
}
