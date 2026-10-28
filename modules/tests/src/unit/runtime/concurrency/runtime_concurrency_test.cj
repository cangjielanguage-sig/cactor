/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 运行时并发测试
 * 多线程同时创建 / 停止 Actor，检查 guardian、registry、lifecycle、metrics 的一致性
 */
package cactor_tests.unit.runtime.concurrency

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

class CountingActor <: Actor {
    let counter: AtomicInt64

    init(counter: AtomicInt64) {
        this.counter = counter
    }

    public prop name: String {
        get() { "CountingActor" }
    }

    public prop description: String {
        get() { "计数" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message is StringMessage) {
            counter.fetchAdd(1)
        }
        MessageResult.Handled
    }
}

@Test
public class RuntimeConcurrencyTest {
    @TestCase
    func concurrentActorCreationIsConsistent(): Unit {
        let system = CActorSystemFactory.create("rt-create-${MonoTime.now().hashCode()}")
        let runtime = system.getRuntime()
        let threads: Int64 = 8
        let perThread: Int64 = 150
        let total = threads * perThread
        let received = AtomicInt64(0)
        let failures = AtomicInt64(0)
        let refs = Array<Option<ActorRef>>(total, { _ => None<ActorRef> })
        let futures = ArrayList<Future<Unit>>()
        for (t in 0..threads) {
            futures.add(spawn {
                for (i in 0..perThread) {
                    try {
                        let ref = system.actorOf({ => CountingActor(received) }, "a-${t}-${i}")
                        refs[t * perThread + i] = Some(ref)
                        ref.tell(StringMessage("hi"))
                    } catch (e: Exception) {
                        failures.fetchAdd(1)
                    }
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(failures.load(), 0)
        @Expect(runtime.getUserGuardian().getAllUserActors().size, total)
        @Expect(runtime.getActorRegistry().getTotalActorCount(), total)
        @Expect(runtime.getRuntimeStats().actorCount, total)
        @Expect(awaitCondition(Duration.second * 10, { => received.load() == total }), true)

        // 并发停止全部 Actor
        let stopFutures = ArrayList<Future<Unit>>()
        for (t in 0..threads) {
            stopFutures.add(spawn {
                for (i in 0..perThread) {
                    if (let Some(ref) <- refs[t * perThread + i]) {
                        runtime.stop(ref)
                    }
                }
            })
        }
        for (f in stopFutures) {
            f.get()
        }
        @Expect(runtime.getUserGuardian().getAllUserActors().size, 0)
        @Expect(runtime.getRuntimeStats().actorCount, 0)
        @Expect(runtime.getActorRegistry().getTotalActorCount(), 0)
        // 重复停止不影响计数
        if (let Some(ref) <- refs[0]) {
            runtime.stop(ref)
        }
        @Expect(runtime.getRuntimeStats().actorCount, 0)
        system.terminate()
    }

    @TestCase
    func duplicateNamesAreRejected(): Unit {
        let system = CActorSystemFactory.create("rt-dup-${MonoTime.now().hashCode()}")
        let created = AtomicInt64(0)
        let rejected = AtomicInt64(0)
        let counter = AtomicInt64(0)
        let futures = ArrayList<Future<Unit>>()
        for (_ in 0..8) {
            futures.add(spawn {
                try {
                    system.actorOf({ => CountingActor(counter) }, "same-name")
                    created.fetchAdd(1)
                } catch (e: Exception) {
                    rejected.fetchAdd(1)
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(created.load(), 1)
        @Expect(rejected.load(), 7)
        system.terminate()
    }
}
