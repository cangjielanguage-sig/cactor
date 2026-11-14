/*
 * CActor Examples - Planner / Worker / Reporter 与持久化会话（E61）
 *
 * 第一套系统执行一步并关闭；第二套系统、新 FileDurableStateStore 从同一会话恢复，
 * 跳过已提交步骤。Worker 的真实异常经监督恢复，Planner 用 TypedRetry 重新 ask；
 * 慢任务登记后才取消等待，然后放行迟到回复，验证它没有覆盖已提交的取消结果。
 * 每步 fsync 后经 StreamRef 输出进度，Reporter 每次只 request(1)。
 * 单写者示例；不提供外部工具副作用的 exactly-once 或集群存储复制。
 * 运行：CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.agent_orchestration_demo
 */
package cactor_examples.agent_orchestration_demo

import std.collection.*
import std.fs.*
import std.sync.*
import std.time.*
import cactor_core.typed.*
import cactor_core.supervision.BackoffPolicy
import cactor_persistence.FileDurableStateStore
import cactor_persistence.typed.*
import cactor_cluster.streamref.*
import cactor_testkit.{ActorTestKit, awaitCondition}

let TIMEOUT = Duration.second * 10
let ATTEMPTS = AtomicInt64(0)
let FIRST_CALLS = AtomicInt64(0)
let LATE_REPLIES = AtomicInt64(0)

enum PlannerCmd {
    | Run(TypedActorRef<String>)
    | Show(TypedActorRef<String>)
    | Result(Int64, String, TypedActorRef<String>)
    | CancelReady(AskCancellationToken, TypedActorRef<WorkerCmd>)
}

enum WorkerCmd {
    | Work(Int64, TypedActorRef<String>, TypedActorRef<PlannerCmd>, AskCancellationToken)
    | Release
}

func check(ok: Bool, why: String): Unit {
    if (!ok) {
        throw IllegalStateException(why)
    }
}

func worker(pending!: Option<TypedActorRef<String>> = None<TypedActorRef<String>>): TypedBehavior<WorkerCmd> {
    TypedBehaviors.receive<WorkerCmd>({
        ctx, cmd => match (cmd) {
            case Work(index, replyTo, planner, token) =>
                if (index == 0) {
                    FIRST_CALLS.fetchAdd(1)
                    replyTo << "PLAN:HELLO CANGJIE"
                } else if (index == 1) {
                    let n = ATTEMPTS.fetchAdd(1) + 1
                    if (n <= 2) {
                        throw IllegalStateException("工具暂时不可用 ${n}")
                    }
                    replyTo << "RESEARCH:TYPED ACTORS"
                } else {
                    // 把外部调用表示为未完成请求，Actor 仍能收 Release；取消方等待登记完成。
                    planner << PlannerCmd.CancelReady(token, ctx.self)
                    return worker(pending: Some(replyTo))
                }
                TypedBehaviors.same<WorkerCmd>()
            case Release =>
                if (let Some(reply) <- pending) {
                    reply << "LATE:不应写入会话"
                    LATE_REPLIES.fetchAdd(1)
                }
                worker()
        }
    })
}

func offset(state: String): Int64 {
    if (state.isEmpty()) {
        0
    } else {
        state.split("|").size
    }
}

/** 会话状态就是提交日志；只有持久化完成后才回复和发布进度。 */
func planner(store: FileDurableStateStore, progress: StreamProducer<String>): TypedBehavior<PlannerCmd> {
    TypedBehaviors.setup<PlannerCmd>(
        {
            ctx =>
                let supervised = TypedBehaviors
                    .supervise(TypedBehaviors.setup<WorkerCmd>({_ => worker()}))
                    .onFailure(TypedSupervisorStrategy.restart())
                let planningTool = ctx.actorOf<WorkerCmd>(supervised, "planning-tool")
                let researchTool = ctx.actorOf<WorkerCmd>(supervised, "research-tool")
                DurableStateBehavior<PlannerCmd, String>(PersistenceId.of("AgentSession", "demo"), emptyState: "",
                    commandHandler: {
                        state, cmd, effect => match (cmd) {
                            case Run(replyTo) =>
                                let index = offset(state)
                                if (index >= 3) {
                                    return effect.reply(replyTo, state)
                                }
                                let tool = if (index == 0) {
                                    planningTool
                                } else {
                                    researchTool
                                }
                                let token = AskCancellationToken()
                                let self = ctx.self
                                let request = {
                                    _: Int64 => TypedAsk.ask<WorkerCmd, String>(tool,
                                        {r => WorkerCmd.Work(index, r, self, token)}, Duration.millisecond * 250,
                                        cancellation: Some(token))
                                }
                                let result = if (index == 1) {
                                    TypedRetry.run<String>(request, 3,
                                        BackoffPolicy(Duration.millisecond * 10, Duration.millisecond * 20,
                                            randomFactor: 0.0))
                                } else {
                                    request(1)
                                }
                                ctx.pipeToSelf<String>(
                                    result,
                                    {
                                        outcome =>
                                            let line = match (outcome) {
                                                case Replied(value) => value
                                                case Failed(cause) =>
                                                    if ((cause as TypedAskCancelledException).isSome()) {
                                                        "CANCELLED:slow-tool"
                                                    } else {
                                                        "ERROR:${cause.message}"
                                                    }
                                            }
                                            PlannerCmd.Result(index, line, replyTo)
                                    }
                                )
                                effect.none()
                            case CancelReady(token, target) =>
                                token.cancel()
                                target << WorkerCmd.Release
                                effect.none()
                            case Result(index, line, replyTo) =>
                                // 期望偏移拒绝重复/迟到提交；真实工具若有副作用，仍须另传幂等键。
                                if (index != offset(state)) {
                                    return effect.reply(replyTo, "ERROR:stale result")
                                }
                                let next = if (state.isEmpty()) {
                                    line
                                } else {
                                    state + "|" + line
                                }
                                effect.persist(next).thenRun({
                                    _ => match (progress.offer("${index}:${line}")) {
                                        case Enqueued => ()
                                        case _ => throw IllegalStateException("进度流已关闭或缓冲已满")
                                    }
                                }).thenReply(replyTo, {s => s})
                            case Show(replyTo) => effect.reply(replyTo, state)
                        }
                    }, codec: FunctionStateCodec<String>(encode: {s => s}, decode: {s => s}), store: store).behavior()
        }
    )
}

/** Reporter 按单条信用消费；完成 Promise 是跨线程观察结果的唯一入口。 */
class Reporter <: StreamRefSubscriber<String> {
    let lines = ArrayList<String>()
    var subscription = None<StreamRefSubscription>
    let result = TypedPromise<Array<String>>()

    public func onSubscribe(s: StreamRefSubscription): Unit {
        subscription = Some(s)
        s.request(1)
    }
    public func onNext(line: String): Unit {
        lines.add(line)
        subscription.getOrThrow().request(1)
    }
    public func onComplete(): Unit {
        result.trySuccess(lines.toArray());
        ()
    }
    public func onError(e: StreamRefException): Unit {
        result.tryFailure(e);
        ()
    }
}

func ask(agent: TypedActorRef<PlannerCmd>, run: Bool): String {
    TypedAsk.ask<PlannerCmd, String>(agent, {
        r => if (run) {
            PlannerCmd.Run(r)
        } else {
            PlannerCmd.Show(r)
        }
    }, TIMEOUT).get(TIMEOUT)
}

func phase(dir: String, recovered: Bool): String {
    let kit = ActorTestKit(if (recovered) {
        "agent-recovered"
    } else {
        "agent-first"
    })
    try {
        let refs = StreamRefs(kit.system)
        let (source, producer) = refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(4))
        let reporter = Reporter()
        refs.subscribe<String>(source, reporter)
        let agent = kit.spawnTyped<PlannerCmd>(planner(FileDurableStateStore(dir), producer), "planner")
        let before = ask(agent, false)
        check(before == if (recovered) {
            "PLAN:HELLO CANGJIE"
        } else {
            ""
        }, "会话恢复内容错误")
        var state = before
        let steps = if (recovered) {
            2
        } else {
            1
        }
        for (_ in 0..steps) {
            state = ask(agent, true)
        }
        if (recovered) {
            check(awaitCondition(TIMEOUT, {=> LATE_REPLIES.load() == 1}), "迟到回复没有实际发出")
            check(ask(agent, false) == state, "迟到回复覆盖会话")
        }
        producer.complete()
        let lines = reporter.result.get(TIMEOUT)
        check(lines.size == steps, "进度行数错误")
        for (i in 0..steps) {
            let index = if (recovered) {
                i + 1
            } else {
                i
            }
            check(lines[i].startsWith("${index}:"), "进度顺序错误")
            check(!lines[i].contains("ERROR:"), "工具执行失败 ${lines[i]}")
            println("Reporter ${lines[i]}")
        }
        state
    } finally {
        kit.shutdown()
    }
}

main(): Int64 {
    let dir = "/tmp/cactor-agent-session-${MonoTime.now().hashCode()}"
    try {
        check(phase(dir, false) == "PLAN:HELLO CANGJIE", "首步未提交")
        let finalState = phase(dir, true)
        check(finalState == "PLAN:HELLO CANGJIE|RESEARCH:TYPED ACTORS|CANCELLED:slow-tool", "最终会话错误 ${finalState}")
        let verify = ActorTestKit("agent-verify")
        try {
            let refs = StreamRefs(verify.system)
            let (_, producer) = refs.sourceRef<String>()
            let agent = verify.spawnTyped<PlannerCmd>(planner(FileDurableStateStore(dir), producer), "planner")
            check(ask(agent, false) == finalState, "最终会话未正确持久化")
            check(ask(agent, true) == finalState, "完成后的会话应直接返回结果")
            producer.complete()
        } finally {
            verify.shutdown()
        }
        check(FIRST_CALLS.load() == 1, "恢复后重复执行已提交步骤")
        check(ATTEMPTS.load() == 3, "没有实际执行两次失败和一次成功")
        check(awaitCondition(TIMEOUT, {=> LATE_REPLIES.load() == 1}), "迟到回复没有实际发出")
        println("agent_orchestration_demo：通过（新系统磁盘恢复、真实重试 3 次、取消及迟到隔离、3 条流式进度）")
        0
    } catch (e: Exception) {
        println("agent_orchestration_demo：失败 ${e.message}")
        1
    } finally {
        if (exists(dir)) {
            remove(dir, recursive: true)
        }
    }
}
