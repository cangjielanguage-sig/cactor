/*
 * CActor - 门面入口语义回归
 *
 * CActorFactory.createSystem / CActorSystemBuilder.build 此前恒抛 "Not implemented yet"；
 * cactor.api.extensions 返回的 SimpleActorSystem 在调用方线程同步执行 receive（无串行保证），
 * ActorSelection.ask 把请求原样当作回复返回。现在它们都由真实运行时（RuntimeActorSystem）实现。
 */
package cactor_tests.unit.facade

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.system.{ActorSystem}
import cactor.api.public.{CActorFactory, CActorSystemBuilder}
import cactor.api.extensions.{ActorSystemExtensions}
import cactor_testkit.{awaitCondition}

class Req <: Message {
    let text: String
    init(text: String) { this.text = text }
    public func messageType(): String { "Req(${text})" }
}

class Resp <: Message {
    let text: String
    init(text: String) { this.text = text }
    public func messageType(): String { "Resp(${text})" }
}

/** 回声 + 计数：count 是普通字段，只有串行处理才能保证不丢更新 */
class EchoCounter <: Actor {
    var count: Int64 = 0
    let seen: AtomicInt64
    let stopped: AtomicBool

    init(seen: AtomicInt64, stopped: AtomicBool) {
        this.seen = seen
        this.stopped = stopped
    }

    public prop name: String { get() { "echo" } }
    public prop description: String { get() { "echo counter" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case r: Req where r.text == "count" => context.reply(Resp("${count}"))
            case r: Req => context.reply(Resp("echo:${r.text}"))
            case _ =>
                count++
                seen.fetchAdd(1)
        }
        MessageResult.Handled
    }

    public func postStop(): Unit {
        stopped.store(true)
    }
}

class Tick <: Message {
    public func messageType(): String { "Tick" }
}

let T = Duration.second * 5

/** 4 个线程并发发送，确认消息被串行处理（普通字段计数不丢）、ask 拿到真实回复、terminate 停止 Actor */
func exerciseSystem(system: ActorSystem): Unit {
    let seen = AtomicInt64(0)
    let stopped = AtomicBool(false)
    let ref = system.actorOf(PropsFactory.create<Actor>({ => EchoCounter(seen, stopped) }), "echo")
    let futures = ArrayList<Future<Unit>>()
    for (_ in 0..4) {
        futures.add(spawn {
            for (_ in 0..1000) {
                ref << Tick()
            }
        })
    }
    for (f in futures) {
        f.get()
    }
    @Expect(awaitCondition(T, { => seen.load() == 4000 }), true)
    let selection = system.actorSelection("/user/echo")
    @Expect((selection.ask(Req("hi")).get() as Resp).getOrThrow().text, "echo:hi")
    @Expect((selection.ask(Req("count")).get() as Resp).getOrThrow().text, "4000")
    @Expect(selection.resolveOne().get().path.getPath(), "/user/echo")
    system.terminate()
    @Expect(awaitCondition(T, { => stopped.load() }), true)
}

@Test
public class FacadeSemanticsTest {
    @TestCase
    public func factorySystemIsBackedByRealRuntime(): Unit {
        let system = CActorFactory.createSystem("facade-factory")
        @Expect(system.name, "facade-factory")
        exerciseSystem(system)
    }

    @TestCase
    public func builderBuildsRealSystem(): Unit {
        exerciseSystem(CActorSystemBuilder().withName("facade-builder").build())
    }

    @TestCase
    public func extensionsSystemIsBackedByRealRuntime(): Unit {
        exerciseSystem(ActorSystemExtensions.create("facade-ext"))
    }
}
