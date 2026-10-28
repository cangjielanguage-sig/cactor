/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 远程通信端到端测试
 * 两个节点各自运行 CActorSystem，通过真实 TCP 连接互发消息
 */
package cactor_tests.integration.remote

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage, PingMessage}
import cactor_core.context.{ActorContext}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, RemoteActorRef}

func eventually(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond * 2)
    }
    cond()
}

/** 收到 "ping:<n>" 时通过远程引用回复 "pong:<n>" */
class PongActor <: Actor {
    let replyTo: RemoteActorRef
    let handled: AtomicInt64

    init(replyTo: RemoteActorRef, handled: AtomicInt64) {
        this.replyTo = replyTo
        this.handled = handled
    }

    public prop name: String {
        get() { "PongActor" }
    }

    public prop description: String {
        get() { "远程回复" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: StringMessage =>
                handled.fetchAdd(1)
                replyTo.tell(StringMessage(m.getContent().replace("ping", "pong")), Some("node-b/pong"))
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 记录收到的消息与发送者 */
class CollectorActor <: Actor {
    let items: ArrayList<String>
    let senders: ArrayList<String>
    let lock: Mutex

    init(items: ArrayList<String>, senders: ArrayList<String>, lock: Mutex) {
        this.items = items
        this.senders = senders
        this.lock = lock
    }

    public prop name: String {
        get() { "CollectorActor" }
    }

    public prop description: String {
        get() { "收集" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: StringMessage =>
                synchronized(lock) {
                    items.add(m.getContent())
                    senders.add(context.sender() ?? "")
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class RemoteEndToEndTest {
    @TestCase
    func pingPongBetweenTwoNodes(): Unit {
        let nodeA = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let nodeB = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        nodeA.start()
        nodeB.start()
        let systemA = CActorSystemFactory.create("node-a-${MonoTime.now().hashCode()}")
        let systemB = CActorSystemFactory.create("node-b-${MonoTime.now().hashCode()}")

        let items = ArrayList<String>()
        let senders = ArrayList<String>()
        let lock = Mutex()
        let collector = systemA.actorOf({ => CollectorActor(items, senders, lock) }, "collector")
        nodeA.exposeActor("collector", collector)

        let handled = AtomicInt64(0)
        let replyTo = nodeB.remoteActorOf(nodeA.getLocalAddress(), "collector")
        let pong = systemB.actorOf({ => PongActor(replyTo, handled) }, "pong")
        nodeB.exposeActor("pong", pong)

        let remotePong = nodeA.remoteActorOf(nodeB.getLocalAddress(), "pong")
        let n = 500
        for (i in 0..n) {
            remotePong.tell(StringMessage("ping:${i}"))
        }
        @Expect(eventually(Duration.second * 10, { => synchronized(lock) { items.size == n } }), true)
        @Expect(handled.load(), Int64(n))
        let got = synchronized(lock) { items.toArray() }
        // 单连接 + 单 Actor：顺序保持
        var ordered = true
        for (i in 0..n) {
            if (got[i] != "pong:${i}") {
                ordered = false
                break
            }
        }
        @Expect(ordered, true)
        @Expect(synchronized(lock) { senders[0] }, "node-b/pong")
        @Expect(remotePong.path.toString().contains("/user/pong"), true)

        systemA.terminate()
        systemB.terminate()
        nodeA.stop()
        nodeB.stop()
    }

    @TestCase
    func unknownTargetIsCountedAsUndeliverable(): Unit {
        let nodeA = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let nodeB = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        nodeA.start()
        nodeB.start()
        nodeA.sendMessage(nodeB.getLocalAddress(), "nobody", PingMessage(1))
        @Expect(eventually(Duration.second * 5, { => nodeB.getReceiver().getUndeliverableCount() == 1 }), true)
        @Expect(nodeA.getSender().getSentCount(), 1)
        nodeA.stop()
        nodeB.stop()
    }

    @TestCase
    func sendFailsWhenStopped(): Unit {
        let node = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        node.start()
        let target = node.getLocalAddress()
        node.stop()
        @Expect(node.isActive(), false)
        @ExpectThrows[Exception](node.sendMessage(target, "x", StringMessage("y")))
    }
}
