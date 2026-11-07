/*
 * CActor - 类型化路由器与 Receptionist 退订（E37、E36 的 T12）
 *
 *   - TypedRouters.pool：轮询均分、一致性哈希同键同目标、广播谓词；目标终止后摘除，全部终止后路由器停止
 *   - TypedRouters.group：按 Receptionist 列表路由，实例停止后流量自动转到其余实例；路由器停止时退订
 *   - Receptionist：subscribe 返回订阅句柄可退订；以 Actor 订阅时订阅者停止后自动退订
 */
package cactor_tests.unit.runtime.typed_routing

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_runtime.typed.{TypedActorSystemImpl}
import cactor_testkit.*

enum Work {
    | Job(String, String, TypedActorRef<String>)   // (键, 内容, 回复给谁)
    | Reload(TypedActorRef<String>)
    | Die
}

let T5S: Duration = Duration.second * 5

/** 处理 Job 时回复 "<自己的路径>|<内容>" */
func worker(): TypedBehavior<Work> {
    TypedBehaviors.receive<Work>({ ctx, w =>
        match (w) {
            case Work.Job(_, body, replyTo) =>
                replyTo.tell("${ctx.self.pathString()}|${body}")
                TypedBehaviors.same<Work>()
            case Work.Reload(replyTo) =>
                replyTo.tell("${ctx.self.pathString()}|reload")
                TypedBehaviors.same<Work>()
            case Work.Die => TypedBehaviors.stopped<Work>()
        }
    })
}

class Replies {
    let lock = Mutex()
    let items = ArrayList<String>()

    func add(s: String): Unit {
        synchronized(lock) {
            items.add(s)
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            items.size
        }
    }

    func handlers(): Array<String> {
        synchronized(lock) {
            Array<String>(items.size, { i => items[i].split("|")[0] })
        }
    }

    func behavior(): TypedBehavior<String> {
        TypedBehaviors.receiveMessage<String>({ s =>
            this.add(s)
            TypedBehaviors.same<String>()
        })
    }
}

func countBy(items: Array<String>): HashMap<String, Int64> {
    let m = HashMap<String, Int64>()
    for (i in items) {
        m[i] = (m.get(i) ?? 0) + 1
    }
    m
}

func newSystem(name: String): (ActorTestKit, TypedActorSystemImpl) {
    let kit = ActorTestKit(name)
    (kit, kit.system.typed())
}

@Test
public class TypedRouterTest {
    @TestCase
    public func poolRoundRobinSpreadsEvenly(): Unit {
        let (kit, sys) = newSystem("trouter-rr")
        let replies = Replies()
        let sink = sys.actorOf(replies.behavior(), "sink")
        let router = sys.actorOf(TypedRouters.pool<Work>(4, worker()), "pool")
        for (i in 0..40) {
            router << Work.Job("k${i}", "j${i}", sink)
        }
        @Expect(awaitCondition(T5S, { => replies.size() == 40 }), true)
        let counts = countBy(replies.handlers())
        @Expect(counts.size, 4)
        for ((_, c) in counts) {
            @Expect(c, 10)
        }
        kit.shutdown()
    }

    @TestCase
    public func consistentHashingAndBroadcastPredicate(): Unit {
        let (kit, sys) = newSystem("trouter-hash")
        let replies = Replies()
        let sink = sys.actorOf(replies.behavior(), "sink")
        let routing = TypedRouting<Work>()
            .consistentHashing({ w =>
                match (w) {
                    case Work.Job(k, _, _) => k
                    case _ => ""
                }
            })
            .withBroadcastPredicate({ w =>
                match (w) {
                    case Work.Reload(_) => true
                    case _ => false
                }
            })
        let router = sys.actorOf(TypedRouters.pool<Work>(5, worker(), routing), "pool")
        for (i in 0..30) {
            router << Work.Job("same-user", "j${i}", sink)
        }
        @Expect(awaitCondition(T5S, { => replies.size() == 30 }), true)
        // 同一个键的消息全部落到同一个目标
        @Expect(countBy(replies.handlers()).size, 1)
        // 广播谓词命中的消息发给全部 5 个目标
        router << Work.Reload(sink)
        @Expect(awaitCondition(T5S, { => replies.size() == 35 }), true)
        kit.shutdown()
    }

    @TestCase
    public func terminatedRouteesAreRemovedThenRouterStops(): Unit {
        let (kit, sys) = newSystem("trouter-dead")
        let replies = Replies()
        let sink = sys.actorOf(replies.behavior(), "sink")
        let router = sys.actorOf(TypedRouters.pool<Work>(3, worker()), "pool")
        // 轮询下第 1 条 Die 让一个目标停止
        router << Work.Die
        // 摘除生效后，每一批消息都全部有回复：已终止的目标不会再轮到（此前每 3 条就有 1 条变成死信）
        let deadline = MonoTime.now() + T5S
        var delivered = false
        while (!delivered && MonoTime.now() < deadline) {
            let before = replies.size()
            for (i in 0..12) {
                router << Work.Job("k${i}", "j${i}", sink)
            }
            delivered = awaitCondition(Duration.second, { => replies.size() == before + 12 })
        }
        @Expect(delivered, true)
        @Expect(countBy(replies.handlers()).size <= 2, true)
        // 再停掉其余两个：池路由器随之停止
        router << Work.Die << Work.Die
        @Expect(awaitCondition(T5S, { =>
            match (router.toClassic()) {
                case Some(c) => c.isTerminated()
                case None => false
            }
        }), true)
        kit.shutdown()
    }

    @TestCase
    public func groupRouterFollowsReceptionistAndUnsubscribesOnStop(): Unit {
        let (kit, sys) = newSystem("trouter-group")
        let key = ServiceKey<Work>("workers")
        let rec = sys.receptionist()
        let replies = Replies()
        let sink = sys.actorOf(replies.behavior(), "sink")
        let a = sys.actorOf(worker(), "wa")
        let b = sys.actorOf(worker(), "wb")
        rec.register(key, a)
        rec.register(key, b)
        let router = sys.actorOf(TypedRouters.group<Work>(key), "group")
        @Expect(awaitCondition(T5S, { => rec.subscriberCount(key) == 1 }), true)
        for (i in 0..10) {
            router << Work.Job("k", "j${i}", sink)
        }
        @Expect(awaitCondition(T5S, { => replies.size() == 10 }), true)
        @Expect(countBy(replies.handlers()).size, 2)
        // a 停止 → Receptionist 自动注销 → 路由器列表随之更新，流量全部转到 b
        a << Work.Die
        @Expect(awaitCondition(T5S, { => rec.find(key).size() == 1 }), true)
        for (i in 0..10) {
            router << Work.Job("k", "x${i}", sink)
        }
        @Expect(awaitCondition(T5S, { => replies.size() == 20 }), true)
        let later = replies.handlers()[10..20]
        @Expect(countBy(later).size, 1)
        @Expect(later[0], b.pathString())
        // 路由器停止时退订（此前订阅永不移除）
        match (router.toClassic()) {
            case Some(c) => kit.system.stop(c)
            case None => ()
        }
        @Expect(awaitCondition(T5S, { => rec.subscriberCount(key) == 0 }), true)
        kit.shutdown()
    }

    @TestCase
    public func receptionistUnsubscribeAndDeadSubscriberRemoval(): Unit {
        let (kit, sys) = newSystem("trouter-recep")
        let key = ServiceKey<Work>("svc")
        let rec = sys.receptionist()
        let seen = AtomicInt64(0)
        let sub = rec.subscribe<Work>(key, { _: Listing<Work> =>
            seen.fetchAdd(1)
            ()
        })
        @Expect(seen.load(), 1)
        @Expect(rec.unsubscribe(sub), true)
        @Expect(rec.unsubscribe(sub), false)
        rec.register(key, sys.actorOf(worker(), "w1"))
        // 退订后不再收到通知
        @Expect(seen.load(), 1)
        // 以 Actor 订阅：订阅者停止后自动退订
        let listener = sys.actorOf(TypedBehaviors.receive<Listing<Work>>({ ctx, l =>
            if (l.size() >= 2) {
                TypedBehaviors.stopped<Listing<Work>>()
            } else {
                TypedBehaviors.same<Listing<Work>>()
            }
        }), "listener")
        rec.subscribe<Work>(key, listener) |> ignoreSub
        @Expect(rec.subscriberCount(key), 1)
        rec.register(key, sys.actorOf(worker(), "w2"))
        @Expect(awaitCondition(T5S, { => rec.subscriberCount(key) == 0 }), true)
        kit.shutdown()
    }

    @TestCase
    public func receptionistWatcherDoesNotClashWithUserActorNames(): Unit {
        let (kit, sys) = newSystem("trouter-recep-name")
        // 用户先占用了内部监视者可能用的名字（此前固定叫 "receptionist"，登记时创建它直接抛"名称已存在"）
        sys.actorOf(worker(), "receptionist") |> ignoreTyped
        sys.actorOf(worker(), "$receptionist") |> ignoreTyped
        let key = ServiceKey<Work>("named")
        let rec = sys.receptionist()
        let w = sys.actorOf(worker(), "w")
        rec.register(key, w)
        @Expect(rec.find(key).size(), 1)
        // 内部监视者照常工作：实例停止后自动注销
        w << Work.Die
        @Expect(awaitCondition(T5S, { => rec.find(key).size() == 0 }), true)
        kit.shutdown()
    }
}

func ignoreTyped(_: TypedActorRef<Work>): Unit {}

func ignoreSub(_: ReceptionistSubscription): Unit {}
