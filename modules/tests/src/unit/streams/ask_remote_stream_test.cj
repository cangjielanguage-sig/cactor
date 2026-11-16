/*
 * CActor Streams - ask 阶段跨节点（DA7）
 *
 * 验证 opAsk 对远端 TypedActorRef 自动改走 TypedAsk.askRemote：
 *   - 真实回环 TCP：远端 Actor 收到请求并回复，Source 按序输出；
 *   - 并行度与顺序沿用本地路径（同一 MapAsyncLogic）；
 *   - 流结束时撤销尚未回复的远端 ask：发起方在途 ask 登记与回复入口都归零；
 *   - 本地 ask 仍走 TypedAsk.ask（本地回归由 cactor_streams 的 AsyncAndControlTest 覆盖，
 *     这里额外断言 TypedActorRefs.isRemote 的判定边界）。
 *
 * 等待一律用 awaitCondition / EventRecorder，不写固定 sleep。
 */
package cactor_tests.unit.streams

import std.time.*
import std.sync.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.StringMessage
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_streams.*
import cactor_testkit.*

let ASK_WAIT = Duration.second * 15
// 心跳调快、容忍放长：本目录的用例不做节点故障判定，只验证请求/回复链路
let ASK_REMOTING = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second * 30,
    Duration.millisecond * 50, Duration.millisecond * 10)

class AskNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager

    init(name: String) {
        kit = ActorTestKit(name)
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.configure(ASK_REMOTING)
        remote.start()
    }

    func close(): Unit {
        remote.stop()
        kit.shutdown()
    }
}

/** 收到 StringMessage 就回 "echo:<内容>"，回复经经典上下文的 reply（走信封 sender） */
func askEcho(rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receive<StringMessage>({ ctx, msg =>
        rec.record("echo:${msg.getContent()}")
        match (ctx.toClassic()) {
            case Some(classic) => classic.reply(StringMessage("echo:${msg.getContent()}"))
            case None => ()
        }
        TypedBehaviors.same<StringMessage>()
    })
}

/** 只记录不回复：用于验证流结束/取消时远端 ask 被撤销 */
func askSilent(rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receiveMessage<StringMessage>({ msg =>
        rec.record("silent:${msg.getContent()}")
        TypedBehaviors.same<StringMessage>()
    })
}

@Test
public class AskRemoteStreamTest {
    @TestCase
    public func askStageUsesRemoteAskAcrossTcp(): Unit {
        let a = AskNode("asq-a")
        let b = AskNode("asq-b")
        try {
            let rec = EventRecorder()
            let refB = b.kit.spawnTyped<StringMessage>(askEcho(rec), "ask-echo")
            b.remote.exposeActor("ask-echo", refB.toClassic().getOrThrow())
            let target = a.remote.typedRemoteActorOf<StringMessage>(b.remote.getLocalAddress(), "ask-echo")
            // 判定边界：远端引用为 true；本节点引用为 false（走本地 ask）
            @Expect(TypedActorRefs.isRemote(target), true)
            @Expect(TypedActorRefs.isRemote(refB), false)

            let out = Sources.range(1, 6)
                .ask<StringMessage, StringMessage>(3, target, { x, _ => StringMessage("q${x}") }, ASK_WAIT)
                .map<String>({ m => m.getContent() })
                .runWith(Sinks.seq<String>(), a.kit.system)
            @Expect(out.get(ASK_WAIT),
                ["echo:q1", "echo:q2", "echo:q3", "echo:q4", "echo:q5", "echo:q6"])
            // 请求确实到了远端节点（不是本地某个回环替身）
            @Expect(rec.count("echo:q6"), 1)
            // 全部回复后 ask 入口已注销
            @Expect(awaitCondition(ASK_WAIT, { => a.remote.pendingTypedAskCount() == 0 }), true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 流在 ask 未回复时结束（kill switch 关闭）：发起方的在途 ask 与回复入口都被撤销 */
    @TestCase
    public func streamEndCancelsPendingRemoteAsk(): Unit {
        let a = AskNode("asq-cancel-a")
        let b = AskNode("asq-cancel-b")
        try {
            let rec = EventRecorder()
            let refB = b.kit.spawnTyped<StringMessage>(askSilent(rec), "ask-silent")
            b.remote.exposeActor("ask-silent", refB.toClassic().getOrThrow())
            let target = a.remote.typedRemoteActorOf<StringMessage>(b.remote.getLocalAddress(), "ask-silent")
            let baseAsks = a.remote.pendingTypedAskCount()
            let baseEntries = a.remote.replyEntryCount()

            let (kill, done) = Sources.single(1)
                .ask<StringMessage, StringMessage>(1, target, { x, _ => StringMessage("never${x}") },
                    Duration.second * 60)
                .viaMat<StringMessage, UniqueKillSwitch, UniqueKillSwitch>(KillSwitches.single<StringMessage>(),
                    { _, k => k })
                .toMat(Sinks.ignore<StringMessage>(), { k, d => (k, d) })
                .run(a.kit.system)
            // 请求真的到了远端且尚未回复：发起方有 1 个在途 ask
            @Expect(rec.await("silent:never1", ASK_WAIT), true)
            @Expect(awaitCondition(ASK_WAIT, { => a.remote.pendingTypedAskCount() == baseAsks + 1 }), true)

            kill.shutdown()
            var finished = false
            try {
                done.get(ASK_WAIT)
                finished = true
            } catch (_: Exception) {}
            @Expect(finished, true)
            // 取消令牌 → promise 失败 → 远程层的 pending ask 与回复入口同步注销
            @Expect(awaitCondition(ASK_WAIT, { => a.remote.pendingTypedAskCount() == baseAsks }), true)
            @Expect(awaitCondition(ASK_WAIT, { => a.remote.replyEntryCount() <= baseEntries }), true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 本地 ask 路径不回归：同一算子对本地引用仍走 TypedAsk.ask，replyTo 由请求携带 */
    @TestCase
    public func localAskStillUsesInProcessReplyRef(): Unit {
        let kit = ActorTestKit("asq-local")
        try {
            let doubler = kit.system.typedActorOf<AskLocalCmd>(TypedBehaviors.receive<AskLocalCmd>({ _, m =>
                match (m) {
                    case AskLocalCmd.Double(x, replyTo) => replyTo << x * 2
                }
                TypedBehaviors.same<AskLocalCmd>()
            }), "local-doubler")
            @Expect(TypedActorRefs.isRemote(doubler), false)
            let out = Sources.range(1, 5)
                .ask<AskLocalCmd, Int64>(2, doubler, { x, r => AskLocalCmd.Double(x, r) }, Duration.second * 5)
                .runWith(Sinks.seq<Int64>(), kit.system)
            @Expect(out.get(Duration.second * 5), [2, 4, 6, 8, 10])
        } finally {
            kit.shutdown()
        }
    }
}

enum AskLocalCmd {
    | Double(Int64, TypedActorRef<Int64>)
}
