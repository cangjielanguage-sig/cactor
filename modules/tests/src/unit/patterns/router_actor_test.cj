/*
 * CActor - 路由器 Actor（E37，对标 Akka Routers.pool / group）
 *
 *   - 经路由器转发后，目标的 context.reply 直接回到原发送方（此前 sender 在路由层被丢弃，C12）
 *   - 池目标终止后被摘除，后续流量全部分给存活目标；目标全部终止后路由器停止
 *   - 组路由器 watch 外部 Actor，外部 Actor 停止后同样摘除
 *   - Broadcast / GetRoutees / AddRoutee / RemoveRoutee / AdjustPoolSize
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_patterns.routing.*
import cactor_testkit.*

/** "die" 停止自己；其余消息回复 "<自己的名字>:<内容>"，并记录处理者 */
class RouteeWorker <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String {
        get() { "routee-worker" }
    }

    public prop description: String {
        get() { "routee-worker" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "die") {
                return MessageResult.Stop
            }
            let me = context.self.path.getPath()
            rec.record(me)
            context.reply(StringMessage("${lastSegment(me)}:${s.getContent()}"))
        }
        MessageResult.Handled
    }
}

func lastSegment(path: String): String {
    let parts = path.split("/")
    parts[parts.size - 1]
}

func routeesOf(kit: ActorTestKit, router: ActorRef): Array<ActorRef> {
    let probe = kit.createTestProbe()
    router.tell(GetRoutees(), probe.ref)
    match (probe.expectMsg(Duration.second * 5) as Routees) {
        case Some(r) => r.routees
        case None => Array<ActorRef>()
    }
}

func distinctCount(items: Array<String>): Int64 {
    let s = HashSet<String>()
    for (i in items) {
        s.add(i)
    }
    s.size
}

@Test
public class RouterActorTest {
    @TestCase
    public func poolForwardsWithOriginalSenderRoundRobin(): Unit {
        let kit = ActorTestKit("router-pool")
        let rec = EventRecorder()
        let router = kit.actorOf(Routers.pool(3, PropsFactory.create<Actor>({ => RouteeWorker(rec) })), "workers")
        let probe = kit.createTestProbe()
        for (i in 0..6) {
            router.tell(StringMessage("job${i}"), probe.ref)
        }
        // 回复直接到达原发送方（probe），而不是路由器
        let replies = ArrayList<String>()
        for (_ in 0..6) {
            match (probe.expectMsg(Duration.second * 5) as StringMessage) {
                case Some(m) => replies.add(m.getContent())
                case None => ()
            }
        }
        @Expect(replies.size, 6)
        @Expect(replies.toArray().contains("routee-1:job0") || replies.toArray().contains("routee-2:job0") ||
            replies.toArray().contains("routee-3:job0"), true)
        // 轮询：3 个目标各处理 2 条
        @Expect(rec.awaitSize(6, Duration.second * 5), true)
        @Expect(distinctCount(rec.snapshot()), 3)
        for (p in rec.snapshot()) {
            @Expect(rec.count(p), 2)
        }
        kit.shutdown()
    }

    @TestCase
    public func terminatedRouteeIsRemovedAndTrafficRedistributed(): Unit {
        let kit = ActorTestKit("router-remove")
        let rec = EventRecorder()
        let router = kit.actorOf(Routers.pool(3, PropsFactory.create<Actor>({ => RouteeWorker(rec) })), "workers")
        let before = routeesOf(kit, router)
        @Expect(before.size, 3)
        let victim = before[0]
        victim << StringMessage("die")
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 2 }), true)
        // 之后的 30 条全部由存活目标处理，没有一条投给已终止的目标（此前继续轮询到它，变成死信）
        let probe = kit.createTestProbe()
        for (i in 0..30) {
            router.tell(StringMessage("m${i}"), probe.ref)
        }
        let got = probe.receiveN(30, Duration.second * 5)
        @Expect(got.size, 30)
        @Expect(rec.count(victim.path.getPath()), 0)
        kit.shutdown()
    }

    @TestCase
    public func poolStopsWhenAllRouteesTerminate(): Unit {
        let kit = ActorTestKit("router-empty")
        let rec = EventRecorder()
        let router = kit.actorOf(Routers.pool(2, PropsFactory.create<Actor>({ => RouteeWorker(rec) })), "workers")
        router << Broadcast(StringMessage("die"))
        @Expect(awaitCondition(Duration.second * 5, { => router.isTerminated() }), true)
        kit.shutdown()
    }

    @TestCase
    public func groupRouterDropsStoppedExternalRoutee(): Unit {
        let kit = ActorTestKit("router-group")
        let rec = EventRecorder()
        let a = kit.actorOf({ => RouteeWorker(rec) }, "a")
        let b = kit.actorOf({ => RouteeWorker(rec) }, "b")
        let router = kit.actorOf(Routers.group([a, b]), "group")
        @Expect(routeesOf(kit, router).size, 2)
        kit.system.stop(a)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 1 }), true)
        let probe = kit.createTestProbe()
        for (i in 0..10) {
            router.tell(StringMessage("g${i}"), probe.ref)
        }
        @Expect(probe.receiveN(10, Duration.second * 5).size, 10)
        @Expect(rec.count("/user/b"), 10)
        // 组路由器不因外部目标全部停止而停止
        kit.system.stop(b)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 0 }), true)
        @Expect(router.isTerminated(), false)
        kit.shutdown()
    }

    @TestCase
    public func managementMessages(): Unit {
        let kit = ActorTestKit("router-admin")
        let rec = EventRecorder()
        let router = kit.actorOf(Routers.pool(2, PropsFactory.create<Actor>({ => RouteeWorker(rec) })), "workers")
        // 广播：每个目标各收到一次，各自回复原发送方
        let probe = kit.createTestProbe()
        router.tell(Broadcast(StringMessage("hello")), probe.ref)
        @Expect(probe.receiveN(2, Duration.second * 5).size, 2)
        // 扩缩容
        router << AdjustPoolSize(3)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 5 }), true)
        router << AdjustPoolSize(-4)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 1 }), true)
        // 增加一个外部目标、再移除池目标
        let ext = kit.actorOf({ => RouteeWorker(rec) }, "ext")
        router << AddRoutee(ext)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 2 }), true)
        let pooled = routeesOf(kit, router)[0]
        router << RemoveRoutee(pooled)
        @Expect(awaitCondition(Duration.second * 5, { => routeesOf(kit, router).size == 1 }), true)
        // 池目标被移除时一并停止
        @Expect(awaitCondition(Duration.second * 5, { => pooled.isTerminated() }), true)
        kit.shutdown()
    }

    @TestCase
    public func consistentHashingKeepsKeyOnSameRoutee(): Unit {
        let kit = ActorTestKit("router-hash")
        let rec = EventRecorder()
        let strategy = ConsistentHashRoutingStrategy({ m: Message =>
            match (m as StringMessage) {
                case Some(s) => s.getContent().split("-")[0]
                case None => ""
            }
        })
        let router = kit.actorOf(Routers.pool(4, { => RouteeWorker(rec) }, strategy), "workers")
        let probe = kit.createTestProbe()
        for (i in 0..20) {
            router.tell(StringMessage("user42-${i}"), probe.ref)
        }
        @Expect(probe.receiveN(20, Duration.second * 5).size, 20)
        // 同一个键的 20 条全部由同一个目标处理
        @Expect(distinctCount(rec.snapshot()), 1)
        kit.shutdown()
    }
}
