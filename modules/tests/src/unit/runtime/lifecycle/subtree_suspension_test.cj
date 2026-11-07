/*
 * CActor - 失败时挂起整棵子树（E33，对标 Akka faultSuspend / suspendChildren）
 *
 *   - Actor 失败、等待监督决定期间，它的子孙都暂停处理用户消息（此前子孙照常运行，
 *     可能与即将重启 / 停止的父 Actor 状态不一致）
 *   - 决定为 Resume 后整棵子树继续，暂停期间到达的消息不丢、按序处理
 *   - 子 Actor 自身的失败挂起与"随父暂停"分开记：父恢复不会让尚未得到决定的失败子 Actor 提前继续
 */
package cactor_tests.unit.runtime.lifecycle

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.supervision.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

/** 叶子：记录收到的消息 */
class SsLeaf <: Actor {
    let rec: EventRecorder
    let label: String

    init(rec: EventRecorder, label: String) {
        this.rec = rec
        this.label = label
    }

    public prop name: String {
        get() { label }
    }

    public prop description: String {
        get() { label }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${label}:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

class SsRefs {
    var parent: Option<ActorRef> = None
    var kid: Option<ActorRef> = None
    var grandKid: Option<ActorRef> = None
}

/** 中间层 K：启动时建孙子 G */
class SsKid <: Actor {
    let rec: EventRecorder
    let refs: SsRefs

    init(rec: EventRecorder, refs: SsRefs) {
        this.rec = rec
        this.refs = refs
    }

    public prop name: String {
        get() { "ss-kid" }
    }

    public prop description: String {
        get() { "ss-kid" }
    }

    public func onStarted(context: ActorContext): Unit {
        refs.grandKid = Some(context.actorOf({ => SsLeaf(rec, "g") }, "g"))
        rec.record("k:started")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("k:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

/** P：启动时建子 K；收到 "fail" 抛异常 */
class SsParent <: Actor {
    let rec: EventRecorder
    let refs: SsRefs

    init(rec: EventRecorder, refs: SsRefs) {
        this.rec = rec
        this.refs = refs
    }

    public prop name: String {
        get() { "ss-parent" }
    }

    public prop description: String {
        get() { "ss-parent" }
    }

    public func onStarted(context: ActorContext): Unit {
        refs.parent = Some(context.self)
        refs.kid = Some(context.actorOf({ => SsKid(rec, refs) }, "k"))
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "fail") {
                throw RpBoom("p fail")
            }
            rec.record("p:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

/** 根：监督 P，决定前阻塞到 release 置位（模拟监督者忙），以便观察"等待决定"期间的子树 */
class SsRoot <: Actor {
    let rec: EventRecorder
    let refs: SsRefs
    let deciding: AtomicBool
    let release: AtomicBool

    init(rec: EventRecorder, refs: SsRefs, deciding: AtomicBool, release: AtomicBool) {
        this.rec = rec
        this.refs = refs
        this.deciding = deciding
        this.release = release
    }

    public prop name: String {
        get() { "ss-root" }
    }

    public prop description: String {
        get() { "ss-root" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.actorOf({ => SsParent(rec, refs) }, "p") |> ignoreRef
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }

    public func supervisorStrategy(): Option<SupervisionStrategy> {
        Some(OneForOneStrategy(100, Duration.minute, { _ =>
            deciding.store(true)
            let _ = awaitCondition(Duration.second * 10, { => release.load() })
            SupervisionDirective.Resume
        }))
    }
}

func refOf(o: Option<ActorRef>): ActorRef {
    o.getOrThrow()
}

@Test
public class SubtreeSuspensionTest {
    @TestCase
    public func failingActorPausesItsWholeSubtreeUntilDecision(): Unit {
        let kit = ActorTestKit("subtree-suspend")
        let rec = EventRecorder()
        let refs = SsRefs()
        let deciding = AtomicBool(false)
        let release = AtomicBool(false)
        kit.actorOf({ => SsRoot(rec, refs, deciding, release) }, "root") |> ignoreRef
        @Expect(rec.await("k:started", Duration.second * 5), true)
        let p = refOf(refs.parent)
        let k = refOf(refs.kid)
        let g = refOf(refs.grandKid)
        // 暂停前子孙照常处理
        k << StringMessage("before")
        g << StringMessage("before")
        @Expect(rec.await("k:before", Duration.second * 5), true)
        @Expect(rec.await("g:before", Duration.second * 5), true)

        p << StringMessage("fail")
        // 根开始决策时，P 已挂起并已向子树发出暂停（先于失败上报）
        @Expect(awaitCondition(Duration.second * 5, { => deciding.load() }), true)
        k << StringMessage("during-1") << StringMessage("during-2")
        g << StringMessage("during-1")
        // 等待决定期间子 K、孙 G 都不处理用户消息（此前立即处理）
        @Expect(awaitCondition(Duration.millisecond * 300, { =>
            rec.contains("k:during-1") || rec.contains("g:during-1")
        }), false)

        // 决定 Resume：整棵子树继续，暂停期间的消息按序处理
        release.store(true)
        @Expect(rec.await("k:during-2", Duration.second * 5), true)
        @Expect(rec.await("g:during-1", Duration.second * 5), true)
        @Expect(rec.indexOf("k:during-1") < rec.indexOf("k:during-2"), true)
        p << StringMessage("after")
        @Expect(rec.await("p:after", Duration.second * 5), true)
        kit.shutdown()
    }
}
