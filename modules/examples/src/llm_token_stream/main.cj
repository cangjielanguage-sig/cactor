/*
 * CActor Examples - LLM token 流式回复（StreamRef，E61 / DA7）
 *
 * 两个节点（同一进程内两套 Actor 系统 + 两个真实 TCP 远程层）：
 * - 节点 B：mock LLM 服务。收到提示词后建一条流，把 SourceRef 放进 ask 回复，
 *   然后逐个 offer token；被背压时用 whenReady + pipeToSelf 等需求，不阻塞线程；
 *   流被取消时（termination() 为 Cancelled）中止生成——真实场景里就是中止 HTTP/LLM 调用。
 * - 节点 A：请求方。跨节点 ask 拿到 SourceRef，以 Reactive Streams 订阅者按批 request(2)，
 *   收到 5 个 token 后取消。
 *
 * 运行：CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.llm_token_stream
 */
package cactor_examples.llm_token_stream

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.message.{StringMessage}
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.streamref.*
import cactor_testkit.{ActorTestKit, EventRecorder, awaitCondition}

let TOKENS: Array<String> = ["从前", "有", "一个", "仓颉", "Actor", "，", "它", "把", "每个", "token", "都", "按需", "送达", "。"]

/** 生成器协议：枚举即协议 */
enum GenCmd {
    | Next(Int64)
    | Ready(Int64, Bool)
    | Finished(StreamTermination)
}

/** 每个请求一个生成器：自己建流（owner = 自己，生成器意外停止时消费方立即收到失败） */
func generator(refs: StreamRefs, replyTo: TypedActorRef<StringMessage>, log: EventRecorder): TypedBehavior<GenCmd> {
    TypedBehaviors.setup<GenCmd>({ ctx =>
        let (ref, producer) = refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(4),
            owner: ctx.self.toClassic())
        replyTo << StringMessage(ref.toWireString())
        ctx.pipeToSelf<StreamTermination>(producer.termination(),
            { o => GenCmd.Finished(o.toOption() ?? StreamTermination.Failed("未知")) })
        ctx.self << GenCmd.Next(0)
        TypedBehaviors.receive<GenCmd>({ c, cmd =>
            match (cmd) {
                case GenCmd.Next(i) =>
                    if (i >= TOKENS.size) {
                        producer.complete()
                    } else {
                        match (producer.offer(TOKENS[i])) {
                            case OfferResult.Enqueued => c.self << GenCmd.Next(i + 1)
                            // 背压：消费方还没给需求，等有空位再继续（不阻塞 Actor 线程）
                            case OfferResult.Backpressured =>
                                c.pipeToSelf<Bool>(producer.whenReady(), { o => GenCmd.Ready(i, o.toOption() ?? false) })
                            case OfferResult.Closed => ()
                        }
                    }
                    TypedBehaviors.same<GenCmd>()
                case GenCmd.Ready(i, ok) =>
                    if (ok) {
                        c.self << GenCmd.Next(i)
                    }
                    TypedBehaviors.same<GenCmd>()
                case GenCmd.Finished(t) =>
                    if (t.isCancelled()) {
                        log.record("B: 消费方取消（${t.reason()}）→ 中止 LLM 调用")
                    } else {
                        log.record("B: 流结束 ${t}")
                    }
                    TypedBehaviors.stopped<GenCmd>()
            }
        })
    })
}

/** LLM 服务入口：收到提示词就为它起一个生成器，SourceRef 经 ask 回复返回 */
func llmService(refs: StreamRefs, log: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receive<StringMessage>({ ctx, prompt =>
        log.record("B: 收到提示词「${prompt.getContent()}」")
        if (let Some(sender) <- ctx.toClassic().getOrThrow().senderRef()) {
            ctx.actorOf<GenCmd>(generator(refs, ClassicTypedRef<StringMessage>(sender), log))
        }
        TypedBehaviors.same<StringMessage>()
    })
}

/** 请求方：每次要 2 个 token，收满 5 个就取消 */
class TokenPrinter <: StreamRefSubscriber<String> {
    let got = ArrayList<String>()
    let lock = Mutex()
    let done = AtomicBool(false)
    let subs = ArrayList<StreamRefSubscription>()

    public func onSubscribe(s: StreamRefSubscription): Unit {
        subs.add(s)
        s.request(2)
    }

    public func onNext(token: String): Unit {
        let n = synchronized(lock) {
            got.add(token)
            got.size
        }
        if (n == 5) {
            subs[0].cancel("用户点了停止")
            done.store(true)
        } else if (n % 2 == 0) {
            subs[0].request(2)
        }
    }

    public func onComplete(): Unit {
        done.store(true)
    }

    public func onError(e: StreamRefException): Unit {
        println("A: 流失败 ${e.message}")
        done.store(true)
    }

    func tokens(): Array<String> {
        synchronized(lock) {
            got.toArray()
        }
    }
}

main(): Int64 {
    let timeout = Duration.second * 10
    let kitA = ActorTestKit("llm-a")
    let kitB = ActorTestKit("llm-b")
    let remoteA = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    let remoteB = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    remoteA.start()
    remoteB.start()
    let log = EventRecorder()
    let passed = AtomicBool(false)
    try {
        let refsA = StreamRefs(kitA.system, remote: Some(remoteA))
        let refsB = StreamRefs(kitB.system, remote: Some(remoteB))
        let llm = kitB.spawnTyped<StringMessage>(llmService(refsB, log), "llm")
        remoteB.exposeActor("llm", llm.toClassic().getOrThrow())

        let remoteLlm = remoteA.typedRemoteActorOf<StringMessage>(remoteB.getLocalAddress(), "llm")
        let reply = TypedAsk.askRemote<StringMessage, StringMessage>(remoteLlm, StringMessage("讲个故事"), timeout)
            .get(timeout)
        let ref = SourceRef<String>.fromWireString(reply.getContent())
        println("A: 经 ask 回复拿到 SourceRef")
        let printer = TokenPrinter()
        refsA.subscribe<String>(ref, printer, settings: StreamRefSettings().withBufferSize(4))
        if (!awaitCondition(timeout, { => printer.done.load() })) {
            throw IllegalStateException("没有收到 token")
        }
        println("A: 收到 ${printer.tokens().size} 个 token：${String.join(printer.tokens(), delimiter: " ")}")
        println("A: 取消剩余生成")
        if (!awaitCondition(timeout, { => log.size() >= 2 })) {
            throw IllegalStateException("生产方没有收到取消")
        }
        for (e in log.snapshot()) {
            println(e)
        }
        passed.store(printer.tokens().size == 5 && log.snapshot()[1].contains("中止 LLM 调用"))
    } finally {
        remoteA.stop()
        remoteB.stop()
        kitA.shutdown()
        kitB.shutdown()
    }
    let ok = passed.load()
    println("LLM token 流式回复：${if (ok) { "通过" } else { "失败" }}")
    if (ok) { 0 } else { 1 }
}
