/*
 * CActor Platform.Exec - 受监督执行（P1）
 *
 * 把任意可失败调用（工具调用、子任务、同步 IO）提交给一个受监督的临时 Actor：
 *   - 失败按监督策略重启（默认最多 3 次 / 60s 窗口），重启即重新执行 work；
 *   - 超时：取消令牌置位 + 停止工作 Actor + Promise 以 SupervisedCallTimeoutException 失败；
 *   - 隔离：ThreadPerTask 用 DispatcherMode.ThreadPerActor，阻塞调用只占自己的线程；
 *   - 不定义任务语义：本模块不知道"工具""模型"是什么，只负责执行与监督。
 *
 * 结构：每次 submit 起一个协调 Actor（parent）+ 一个工作 Actor（child）。
 * 工作 Actor 在 setup 中执行 work（setup 失败会被监督重启并重跑 setup，见 core/typed/typed_supervision.cj）。
 * 协调 Actor 监视工作 Actor：成功消息 → 完成 Promise；工作 Actor 意外停止（重试耗尽）→ 失败；
 * 超时 → 取消 + 停止 + 失败。两者在成功后都停止，不泄漏 Actor。
 */
package cactor_platform.exec

import std.time.*
import std.sync.*
import cactor_core.actor.{DispatcherMode}
import cactor_core.typed.*
import cactor_runtime.typed.{TypedActorSystemImpl}

/** 隔离级别（P1）。Process 仅预留：本版本不支持，构造时明确拒绝而不是静默降级。 */
public enum IsolationLevel {
    /** 与普通 Actor 共享默认亲和池（适合纯计算、保证不阻塞的调用） */
    | Actor
    /** 每个调用一个专属线程（DispatcherMode.ThreadPerActor）：适合 LLM/IO 等会阻塞的调用 */
    | ThreadPerTask
    /** 预留：独立进程隔离（未实现） */
    | Process
}

/** 工作 Actor 在完成前停止（监督重试耗尽、被外部停止等） */
public class SupervisedCallFailedException <: Exception {
    public init(name: String, cause: String) {
        super("受监督调用 ${name} 失败：${cause}")
    }
}

/** 受监督调用超时（取消令牌已置位，工作 Actor 已请求停止） */
public class SupervisedCallTimeoutException <: Exception {
    public let workName: String
    public let timeout: Duration

    public init(workName: String, timeout: Duration) {
        super("受监督调用 ${workName} 超时（${timeout.toMilliseconds()}ms），已请求取消")
        this.workName = workName
        this.timeout = timeout
    }
}

/** 内部协议：工作 Actor → 协调 Actor；定时器 → 协调 Actor */
enum CallMsg<T> {
    | WorkerDone(T)
    | WorkerGone
    | TimedOut
}

/** 全局序号：避免同名的并发调用互相冲突（Actor 名称在系统内唯一） */
struct CallSeq {
    static let value: AtomicInt64 = AtomicInt64(0)
}

/**
 * 受监督执行器（P1）。
 *
 * ```cangjie
 * let exec = SupervisedCall<String>(system.typed())
 * let token = ... // submit 的另一个重载会把取消令牌交给 work
 * let result = exec.submit("fetch", { token => toolCall(token) }).get(5.seconds)
 * ```
 */
public class SupervisedCall<T> {
    private let system: TypedActorSystemImpl
    private let policy: TypedSupervisorStrategy
    private let timeout: Duration
    private let isolation: IsolationLevel

    public init(system: TypedActorSystemImpl,
        policy!: TypedSupervisorStrategy = SupervisedCall<T>.defaultPolicy(),
        timeout!: Duration = Duration.second * 30,
        isolation!: IsolationLevel = IsolationLevel.ThreadPerTask) {
        match (isolation) {
            case IsolationLevel.Process =>
                throw IllegalArgumentException("IsolationLevel.Process 尚未实现；请用 ThreadPerTask（阻塞调用）或 Actor（纯计算）")
            case _ => ()
        }
        if (timeout <= Duration.Zero) {
            throw IllegalArgumentException("timeout 必须为正数")
        }
        this.system = system
        this.policy = policy
        this.timeout = timeout
        this.isolation = isolation
    }

    /** 默认策略：失败最多重启 3 次（60s 窗口）后停止。重启会重新执行 work。 */
    public static func defaultPolicy(): TypedSupervisorStrategy {
        TypedSupervisorStrategy.restart().withLimit(3, Duration.minute)
    }

    /** 提交不关心取消的调用 */
    public func submit(name: String, work: () -> T): TypedPromise<T> {
        submit(name, { _ => work() })
    }

    /**
     * 提交调用；work 收到取消令牌，应在长循环/可中断点检查 isCancellationRequested()。
     * 超时会置位令牌并请求停止工作 Actor（协作式取消，与仓颉线程模型一致）。
     */
    public func submit(name: String, work: (AskCancellationToken) -> T): TypedPromise<T> {
        let promise = TypedPromise<T>()
        let token = AskCancellationToken()
        let seq = CallSeq.value.fetchAdd(1)
        let actorName = "supervised-call-${name}-${seq}"
        let workerProps = TypedProps().withDispatcherMode(dispatchMode())
        let behavior = coordinator(name, promise, token, work, workerProps)
        let ref = system.actorOf<CallMsg<T>>(behavior, actorName, workerProps)
        // tell 为空：setup 中已经启动工作 Actor；这里只是确保引用被创建后不会被优化掉
        let _ = ref
        promise
    }

    private func dispatchMode(): DispatcherMode {
        match (isolation) {
            case IsolationLevel.ThreadPerTask => DispatcherMode.ThreadPerActor
            case _ => DispatcherMode.Affinity
        }
    }

    private func coordinator(name: String, promise: TypedPromise<T>, token: AskCancellationToken,
        work: (AskCancellationToken) -> T, workerProps: TypedProps): TypedBehavior<CallMsg<T>> {
        TypedBehaviors.setup<CallMsg<T>>({ ctx =>
            let replyTo = ctx.self
            // 工作 Actor：在 setup 中执行 work。setup 抛异常 → 监督按 policy 重启 → 再次执行 work。
            let workerBehavior: TypedBehavior<CallMsg<T>> = TypedBehaviors.supervise(
                TypedBehaviors.setup<CallMsg<T>>({ _ =>
                    let value = work(token)
                    replyTo.tell(CallMsg<T>.WorkerDone(value))
                    TypedBehaviors.same<CallMsg<T>>()
                })
            ).onFailure(policy)
            let worker = ctx.actorOf<CallMsg<T>>(workerBehavior, "worker", workerProps)
            ctx.watchWith<CallMsg<T>>(worker, CallMsg<T>.WorkerGone)
            ctx.scheduleOnce<CallMsg<T>>(timeout, ctx.self, CallMsg<T>.TimedOut)

            TypedBehaviors.receive<CallMsg<T>>({ c, message =>
                match (message) {
                    case WorkerDone(value) =>
                        promise.trySuccess(value)
                        c.stop<CallMsg<T>>(worker)
                        c.stopSelf()
                        TypedBehaviors.stopped<CallMsg<T>>()
                    case WorkerGone =>
                        promise.tryFailure(SupervisedCallFailedException(name, "工作 Actor 在完成前停止（重试耗尽或失败）"))
                        c.stopSelf()
                        TypedBehaviors.stopped<CallMsg<T>>()
                    case TimedOut =>
                        if (!promise.isCompleted()) {
                            token.cancel()
                            promise.tryFailure(SupervisedCallTimeoutException(name, timeout))
                        }
                        c.stop<CallMsg<T>>(worker)
                        c.stopSelf()
                        TypedBehaviors.stopped<CallMsg<T>>()
                }
            })
        })
    }
}
