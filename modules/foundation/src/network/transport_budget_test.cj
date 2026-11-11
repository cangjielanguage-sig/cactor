/* CActor Foundation.Network - 实例级连接预算与空闲回收回归。
 * 真正建立 TCP 连接，并验证未完成握手也占用预算。
 */
package cactor_foundation.network

import std.collection.*
import std.net.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*

@Test
public class TransportBudgetTest {
    @TestCase
    func concurrentNewTargetsCannotExceedOutboundBudget(): Unit {
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(maxOutboundAssociations: 0))
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(maxInboundConnections: -1))
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(idleRetireAfter: Duration.Zero))
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.millisecond * 100,
            limits: TcpTransportLimits(maxOutboundAssociations: 2, idleRetireAfter: Duration.hour))
        client.start()
        try {
            let jobs = ArrayList<Future<Unit>>()
            for (i in 0..100) {
                jobs.add(spawn { => client.send(NetworkAddress("10.255.255.1", Int32(10000 + i)), bytesOf("bounded")) })
            }
            for (job in jobs) { job.get() }
            @Expect(client.getAssociationCount(), 2)
            @Expect(client.getOutboundCapacityRejections(), 98)
            @Expect(client.getFramesSent(), 2)
            @Expect(client.getDroppedFrames() >= 98, true)
        } finally { client.stop() }
        @Expect(client.getAssociationCount(), 0)
    }

    @TestCase
    func fullBudgetPreservesExistingDestinationAndRejectsPriorityToo(): Unit {
        let received = AtomicInt64(0)
        let a = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let b = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0),
            limits: TcpTransportLimits(maxOutboundAssociations: 1, idleRetireAfter: Duration.hour))
        a.setDataHandler({ _ => received.fetchAdd(1) |> ignoreInt })
        a.start(); b.start(); client.start()
        try {
            client.send(a.getLocalAddress(), bytesOf("first"))
            @Expect(waitUntil(Duration.second * 5, { => received.load() == 1 }), true)
            client.send(b.getLocalAddress(), bytesOf("reject"))
            client.sendPriority(b.getLocalAddress(), bytesOf("reject-control"))
            client.sendPriority(a.getLocalAddress(), bytesOf("existing-control"))
            @Expect(waitUntil(Duration.second * 5, { => received.load() == 2 }), true)
            @Expect(client.getAssociationCount(), 1)
            @Expect(client.getOutboundCapacityRejections(), 2)
            @Expect(client.getDroppedFrames(), 2)
            @Expect(b.getInboundConnectionCount(), 0)
        } finally { client.stop(); a.stop(); b.stop() }
    }

    @TestCase
    func idleConnectedAssociationsReleaseSlotsAndReconnectAfterRestart(): Unit {
        let gotA = AtomicInt64(0)
        let gotB = AtomicInt64(0)
        let a = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let b = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxOutboundAssociations: 1, idleRetireAfter: Duration.millisecond * 150))
        a.setDataHandler({ _ => gotA.fetchAdd(1) |> ignoreInt })
        b.setDataHandler({ _ => gotB.fetchAdd(1) |> ignoreInt })
        a.start(); b.start(); client.start()
        try {
            client.send(a.getLocalAddress(), bytesOf("a"))
            @Expect(waitUntil(Duration.second * 5, { => gotA.load() == 1 }), true)
            @Expect(waitUntil(Duration.second * 5, { => client.getAssociationCount() == 0 && a.getInboundConnectionCount() == 0 }), true)
            client.send(b.getLocalAddress(), bytesOf("b"))
            @Expect(waitUntil(Duration.second * 5, { => gotB.load() == 1 }), true)
            @Expect(waitUntil(Duration.second * 5, { => client.getAssociationCount() == 0 }), true)
            client.send(b.getLocalAddress(), bytesOf("reconnect"))
            @Expect(waitUntil(Duration.second * 5, { => gotB.load() == 2 }), true)
            client.stop()
            @Expect(client.getAssociationCount(), 0)
            client.start()
            client.send(a.getLocalAddress(), bytesOf("restart"))
            @Expect(waitUntil(Duration.second * 5, { => gotA.load() == 2 }), true)
            @Expect(client.getOutboundCapacityRejections(), 0)
        } finally { client.stop(); a.stop(); b.stop() }
    }

    @TestCase
    func slowHandshakeOccupiesCapacityAndDisconnectReleasesIt(): Unit {
        let got = AtomicInt64(0)
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(maxInboundConnections: 1))
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        server.setHandshake(TokenHandshake("budget"))
        client.setHandshake(TokenHandshake("budget"))
        server.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        server.start(); client.start()
        let first = TcpSocket("127.0.0.1", UInt16(server.getLocalAddress().port))
        let second = TcpSocket("127.0.0.1", UInt16(server.getLocalAddress().port))
        try {
            first.connect(timeout: Duration.second)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundConnectionCount() == 1 }), true)
            second.connect(timeout: Duration.second)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundCapacityRejections() == 1 }), true)
            @Expect(server.getInboundConnectionCount(), 1)
            first.close()
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundConnectionCount() == 0 }), true)
            client.send(server.getLocalAddress(), bytesOf("authenticated"))
            @Expect(waitUntil(Duration.second * 5, { => got.load() == 1 }), true)
            @Expect(server.getInboundConnectionCount(), 1)
        } finally { first.close(); second.close(); client.stop(); server.stop() }
        @Expect(server.getInboundConnectionCount(), 0)
    }

    @TestCase
    func acceptedConnectionsRemainBoundedAndStopReleasesAllSlots(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(maxInboundConnections: 2))
        let sockets = ArrayList<TcpSocket>()
        server.start()
        try {
            let jobs = ArrayList<Future<TcpSocket>>()
            for (_ in 0..30) {
                jobs.add(spawn { =>
                    let socket = TcpSocket("127.0.0.1", UInt16(server.getLocalAddress().port))
                    socket.connect(timeout: Duration.second * 5)
                    socket
                })
            }
            for (job in jobs) { sockets.add(job.get()) }
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundCapacityRejections() == 28 }), true)
            @Expect(server.getInboundConnectionCount(), 2)
            server.stop()
            @Expect(server.getInboundConnectionCount(), 0)
        } finally {
            for (socket in sockets) { socket.close() }
            server.stop()
        }
    }
}
