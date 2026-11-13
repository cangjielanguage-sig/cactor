/*
 * CActor - 失败处理与停止协议的语义回归（对照 Akka FaultHandling / DeathWatch）
 *
 * 每个用例对应 docs/plan/20261106.md 中的一个缺陷，修复前可稳定或高概率复现：
 *   - 启动失败（onStarted 恒抛）默认停止，不再同栈无限递归到 StackOverflowError（R1）
 *   - 延迟重启（RestartAfter）期间挂起，重启发生在本 Cell 线程；关闭期间不会复活（R2）
 *   - 父 Actor 监视子 Actor 时停止：子 Actor 全部 postStop 之后父才 postStop（R3）
 *   - 并发的子 Actor 失败：每个失败恰好触发一次重启（系统消息不重复处理，R4）
 *   - 消息处理中的 Error（StackOverflowError）按失败交给监督，工作线程存活（R5）
 *   - 停止与监视竞争：监视者总能收到 Terminated（R20）
 */
package cactor_tests.unit.runtime.failure_semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.supervision.{OneForOneStrategy}
import cactor_testkit.*

class Cmd <: Message {
    let text: String
    init(text: String) { this.text = text }
    public func messageType(): String { "Cmd(${text})" }
}

class Num <: Message {
    let value: Int64
    init(value: Int64) { this.value = value }
    public func messageType(): String { "Num(${value})" }
}

class Boom <: Exception {
    init(msg: String) { super(msg) }
}

/** 通用测试 Actor：生命周期写入 recorder，行为由 handler / onStart 决定 */
class Subject <: Actor {
    let label: String
    let rec: EventRecorder
    let handler: (Subject, Message, ActorContext) -> MessageResult
    let onStart: (Subject, ActorContext) -> Unit
    var state: Int64 = 0
    var strategy: Option<SupervisionStrategy> = None<SupervisionStrategy>

    init(label: String, rec: EventRecorder, handler: (Subject, Message, ActorContext) -> MessageResult) {
        this(label, rec, handler, { _, _ => () })
    }

    init(label: String, rec: EventRecorder, handler: (Subject, Message, ActorContext) -> MessageResult,
        onStart: (Subject, ActorContext) -> Unit) {
        this.label = label
        this.rec = rec
        this.handler = handler
        this.onStart = onStart
    }

    public prop name: String { get() { label } }
    public prop description: String { get() { "subject ${label}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        handler(this, message, context)
    }

    public func onStarted(context: ActorContext): Unit {
        rec.record("${label}:onStarted")
        onStart(this, context)
    }

    public func postStop(): Unit { rec.record("${label}:postStop") }
    public func preRestart(reason: Exception): Unit { rec.record("${label}:preRestart") }
    public func postRestart(reason: Exception): Unit { rec.record("${label}:postRestart") }
    public func supervisorStrategy(): Option<SupervisionStrategy> { strategy }
}

func handled(): MessageResult { MessageResult.Handled }

func textOf(m: Message): String {
    match (m) {
        case c: Cmd => c.text
        case _ => m.messageType()
    }
}

/** 递归到栈溢出：用于制造 StackOverflowError */
/**
 * 递归到栈溢出（真正的 StackOverflowError，用户代码无法构造 Error）。
 * 实测溢出耗时随调用层数增长（DA8）：每层只有一个参数时约 75–145 s（负载 50–60），超过单例 120 s 时限；
 * 每层带 16 个参数并在调用后全部使用（栈帧变宽、同样的栈更快用尽）约 15–45 s。
 * 用 VArray 放大栈帧会越过保护页直接 SIGSEGV，不能用
 */
func deepRecursion(n: Int64, a: Int64, b: Int64, c: Int64, d: Int64, e: Int64, f: Int64, g: Int64, h: Int64,
    i: Int64, j: Int64, k: Int64, l: Int64, m: Int64, o: Int64, p: Int64, q: Int64): Int64 {
    if (n == 0) {
        return a + b
    }
    let r = deepRecursion(n - 1, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, a + n)
    // 调用之后还要用到全部参数：它们必须保存在本层栈帧里，防止被优化成窄帧或尾调用
    r + a * 3 + b * 5 + c * 7 + d * 11 + e * 13 + f * 17 + g * 19 + h * 23 + i * 29 + j * 31 + k * 37 + l * 41 +
        m * 43 + o * 47 + p * 53 + q * 59
}

let T = Duration.second * 5

@Test
public class FailureSemanticsTest {

    // ---------- R1：启动失败 ----------

    @TestCase
    public func initFailureStopsTopLevelActorByDefault(): Unit {
        let kit = ActorTestKit("fs-init-top")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        kit.actorOf({ => Subject("bad", rec, { _, _, _ => handled() }, { _, _ => throw Boom("依赖不可用") }) }, "bad")
        @Expect(rec.await("bad:postStop", T), true)
        // 默认策略对 ActorInitializationException 返回 Stop：只启动一次，没有重启循环
        probe.expectNoMsg(Duration.millisecond * 200)
        @Expect(rec.count("bad:onStarted"), 1)
        @Expect(rec.count("bad:preRestart"), 0)
        // 工作线程未被拖垮：其他 Actor 照常工作
        let ok = kit.actorOf({ => Subject("ok", rec, { _, m, _ => pr.tell(Num(1)); handled() }) }, "ok")
        ok << Cmd("ping")
        @Expect(probe.expectMsgType<Num>().value, 1)
        kit.shutdown()
    }

    @TestCase
    public func initFailureStopsChildByDefault(): Unit {
        let kit = ActorTestKit("fs-init-child")
        let rec = EventRecorder()
        let parent = kit.actorOf({ => Subject("p", rec, { _, m, ctx =>
            if (textOf(m) == "spawn") {
                ctx.actorOf({ => Subject("c", rec, { _, _, _ => handled() }, { _, _ => throw Boom("子 Actor 启动失败") }) }, "c")
                ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        @Expect(rec.await("c:postStop", T), true)
        sleep(Duration.millisecond * 200)
        @Expect(rec.count("c:onStarted"), 1)
        @Expect(rec.count("p:postStop"), 0)
        kit.shutdown()
    }

    @TestCase
    public func initFailureRestartIsBoundedByExplicitStrategy(): Unit {
        // 显式策略仍可重启启动失败的 Actor（决策函数看到的是 ActorInitializationException），重启次数受限
        let kit = ActorTestKit("fs-init-limit")
        let rec = EventRecorder()
        let props = PropsFactory.create<Actor>({ => Subject("r", rec, { _, _, _ => handled() }, { _, _ => throw Boom("x") }) })
            .withSupervisorStrategy(OneForOneStrategy(2, Duration.minute, { e =>
                match (e) {
                    case _: ActorInitializationException => SupervisionDirective.Restart
                    case _ => SupervisionDirective.Stop
                }
            }))
        kit.actorOf(props, "r")
        @Expect(rec.await("r:postStop", T), true)
        sleep(Duration.millisecond * 200)
        let starts = rec.count("r:onStarted")
        @Expect(starts >= 2 && starts <= 4, true)
        kit.shutdown()
    }

    // ---------- R2：延迟重启 ----------

    @TestCase
    public func delayedRestartSuspendsThenRestartsOnOwnThread(): Unit {
        let kit = ActorTestKit("fs-restart-after")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let delay = Duration.millisecond * 300
        let props = PropsFactory.create<Actor>({ => Subject("a", rec, { self, m, _ =>
            if (textOf(m) == "fail") {
                throw Boom("fail")
            }
            self.state++
            rec.record("a:got:${textOf(m)}")
            pr.tell(Num(self.state))
            handled()
        }) }).withSupervisorStrategy(OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.RestartAfter(delay) }))
        let a = kit.actorOf(props, "a")
        a << Cmd("x")
        @Expect(probe.expectMsgType<Num>().value, 1)
        a << Cmd("fail") << Cmd("y")
        // 挂起期间旧实例不再处理消息
        probe.expectNoMsg(Duration.millisecond * 150)
        // 到期后由新实例处理：状态清零，且 postRestart 先于该消息
        @Expect(probe.expectMsgType<Num>().value, 1)
        @Expect(rec.indexOf("a:postRestart") < rec.indexOf("a:got:y"), true)
        kit.shutdown()
    }

    @TestCase
    public func delayedRestartDoesNotReviveActorStoppedDuringDelay(): Unit {
        let kit = ActorTestKit("fs-no-revive")
        let rec = EventRecorder()
        let props = PropsFactory.create<Actor>({ => Subject("z", rec, { _, m, _ =>
            if (textOf(m) == "fail") {
                throw Boom("fail")
            }
            handled()
        }) }).withSupervisorStrategy(
            OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.RestartAfter(Duration.millisecond * 300) }))
        let z = kit.actorOf(props, "z")
        z << Cmd("fail")
        @Expect(awaitCondition(T, { => rec.contains("z:onStarted") }), true)
        sleep(Duration.millisecond * 50)
        // 延迟期间关闭系统：Actor 经系统消息停止；到期的 SysRecreate 走死亡后规则，不能复活
        kit.shutdown()
        @Expect(rec.await("z:postStop", T), true)
        sleep(Duration.millisecond * 500)
        @Expect(rec.count("z:postRestart"), 0)
        @Expect(rec.count("z:onStarted"), 1)
    }

    // ---------- R3：停止合批与监视 ----------

    @TestCase
    public func parentWatchingChildrenStopsAfterAllChildren(): Unit {
        for (round in 0..5) {
            let kit = ActorTestKit("fs-batch-watch-${round}")
            let rec = EventRecorder()
            let parent = kit.actorOf({ => Subject("p", rec, { _, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        let a = ctx.actorOf({ => Subject("a", rec, { _, _, _ => handled() }) }, "a")
                        let b = ctx.actorOf({ => Subject("b", rec, { _, bm, _ =>
                            if (textOf(bm) == "busy") {
                                rec.record("b:busy")
                                // 模拟一条耗时的消息：父停止时 b 正忙，只能稍后经系统消息停止
                                sleep(Duration.millisecond * 300)
                            }
                            handled()
                        }) }, "b")
                        ctx.watch(a)
                        ctx.watch(b)
                        rec.record("p:spawned")
                    case _ => ()
                }
                handled()
            }) }, "p")
            parent << Cmd("spawn")
            @Expect(rec.await("p:spawned", T), true)
            kit.system.lookup("/user/p/b").getOrThrow() << Cmd("busy")
            @Expect(rec.await("b:busy", T), true)
            parent << PoisonPill()
            @Expect(rec.await("p:postStop", T), true)
            // 修复前：a 的死亡通知被重复扣减，父在 b 仍在处理消息时就 postStop
            @Expect(rec.contains("b:postStop"), true)
            @Expect(rec.indexOf("a:postStop") < rec.indexOf("p:postStop"), true)
            @Expect(rec.indexOf("b:postStop") < rec.indexOf("p:postStop"), true)
            kit.shutdown()
        }
    }

    // ---------- R4：系统消息恰好处理一次 ----------

    @TestCase
    public func concurrentChildFailuresRestartEachChildExactlyOnce(): Unit {
        let n = 32
        let kit = ActorTestKit("fs-sys-once")
        let rec = EventRecorder()
        let parent = kit.actorOf({ =>
            let p = Subject("p", rec, { _, m, ctx =>
                if (textOf(m) == "spawn") {
                    for (i in 0..n) {
                        ctx.actorOf({ => Subject("c${i}", rec, { _, cm, _ =>
                            if (textOf(cm) == "fail") {
                                throw Boom("fail")
                            }
                            handled()
                        }) }, "c${i}")
                    }
                    rec.record("p:spawned")
                }
                handled()
            })
            p.strategy = Some(OneForOneStrategy(1000, Duration.minute, { _ => SupervisionDirective.Restart }))
            p
        }, "p")
        parent << Cmd("spawn")
        @Expect(rec.await("p:spawned", T), true)
        let children = Array<ActorRef>(n, { i => kit.system.lookup("/user/p/c${i}").getOrThrow() })
        // 多个线程同时让子 Actor 失败：父的系统队列被并发推送 SysChildFailed
        let futures = ArrayList<Future<Unit>>()
        for (t in 0..4) {
            futures.add(spawn {
                var i = t
                while (i < n) {
                    children[i] << Cmd("fail")
                    i += 4
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(awaitCondition(T, { =>
            var all = true
            for (i in 0..n) {
                all = all && rec.count("c${i}:postRestart") >= 1
            }
            all
        }), true)
        sleep(Duration.millisecond * 200)
        for (i in 0..n) {
            @Expect(rec.count("c${i}:preRestart"), 1)
        }
        kit.shutdown()
    }

    // ---------- R5：Error 不杀死工作线程 ----------

    @TestCase
    public func errorInReceiveIsSupervisedAndWorkerSurvives(): Unit {
        let kit = ActorTestKit("fs-error")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Subject("e", rec, { self, m, _ =>
            if (textOf(m) == "overflow") {
                deepRecursion(100000000, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
                ()
            }
            self.state++
            pr.tell(Num(self.state))
            handled()
        }) }, "e")
        a << Cmd("x")
        @Expect(probe.expectMsgType<Num>().value, 1)
        a << Cmd("overflow") << Cmd("y")
        // StackOverflowError 被包成 ActorErrorException 交给默认策略重启：状态清零后继续处理。
        // 仓颉线程栈按需增长，递归到真正溢出耗时数十秒（见 deepRecursion）。等待上限 110 s 留在单例 120 s 时限内，
        // 超时时给出明确的探针失败而不是框架超时。用例必须等到溢出结束才返回：溢出期间占着共享调度器的一个
        // 工作线程，亲和到它的其他 Actor 都不运行——此前 90 s 先超时返回，后续 watchRacing 用例的 Actor
        // 若落到这个仍在递归的线程上，3 s 内收不到 Terminated（DA8 中两例在同一次运行里先后失败，按此推断为连带）
        @Expect(probe.expectMsgType<Num>(Duration.second * 110).value, 1)
        @Expect(rec.count("e:preRestart"), 1)
        kit.shutdown()
    }

    // ---------- R20：停止与监视竞争 ----------

    @TestCase
    public func watchRacingWithParentStopAlwaysDeliversTerminated(): Unit {
        for (round in 0..30) {
            let kit = ActorTestKit("fs-watch-race-${round}")
            let rec = EventRecorder()
            let probe = kit.createTestProbe()
            let pr = probe.ref
            let parent = kit.actorOf({ => Subject("p", rec, { _, m, ctx =>
                if (textOf(m) == "spawn") {
                    ctx.actorOf({ => Subject("c", rec, { _, _, _ => handled() }) }, "c")
                    rec.record("p:spawned")
                }
                handled()
            }) }, "p")
            parent << Cmd("spawn")
            @Expect(rec.await("p:spawned", T), true)
            let child = kit.system.lookup("/user/p/c").getOrThrow()
            let watcher = kit.actorOf({ => Subject("w", rec, { _, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "watch" => ctx.watch(child)
                    case t: Terminated => pr.tell(Cmd("terminated"))
                    case _ => ()
                }
                handled()
            }) }, "w")
            // 监视与停止同时发出：SysWatch 可能与 SysTerminate 落在子 Actor 的同一批系统消息里
            watcher << Cmd("watch")
            parent << PoisonPill()
            // 等待用文件统一的 T（5 s）：丢失 Terminated 仍会失败，只是给负载下的调度多留余量
            @Expect(probe.expectMsgType<Cmd>(T).text, "terminated")
            kit.shutdown()
        }
    }
}
