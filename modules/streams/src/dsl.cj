/*
 * CActor Streams - 流 DSL：Source / Flow / Sink / RunnableGraph（对标 Akka Streams）
 *
 * 蓝图是惰性的、不可变的、可重复物化的：
 *   let graph = Sources.repeat(1).map({ x => x * 2 }).take(10).toMat(Sinks.seq<Int64>(), { _, r => r })
 *   let result = graph.run(system).get(3.seconds)
 *
 * 物化值（Mat）沿着 via / to 组合：via / to 保留左边，runWith 取汇的物化值，
 * viaMat / toMat 用 combine 函数自己决定（例如同时拿到 kill switch 与完成 Promise）。
 *
 * 仓颉的泛型类不能在不给出类型参数时调用静态函数，所以工厂放在 Sources / Flows / Sinks 里。
 */
package cactor_streams

import std.time.*
import cactor_core.typed.*
import cactor_streams.reactive.*
import cactor.api.CActorSystem

// ========== 算子构建器（Source 与 Flow 共用） ==========

func opMap<A, B>(f: (A) -> B): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = MapLogic<A, B>(b.current, c, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opCollect<A, B>(f: (A) -> Option<B>): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = CollectLogic<A, B>(b.current, c, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opMapConcat<A, B>(f: (A) -> Iterable<B>): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = MapConcatLogic<A, B>(b.current, c, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opTake<A>(n: Int64): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = TakeLogic<A>(b.current, c, n)
        b.add(l)
        l.outlet
    }
}

func opTakeWhile<A>(p: (A) -> Bool, inclusive: Bool): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = TakeWhileLogic<A>(b.current, c, p, inclusive, b.decider())
        b.add(l)
        l.outlet
    }
}

func opDrop<A>(n: Int64): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = DropLogic<A>(b.current, c, n)
        b.add(l)
        l.outlet
    }
}

func opDropWhile<A>(p: (A) -> Bool): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = DropWhileLogic<A>(b.current, c, p, b.decider())
        b.add(l)
        l.outlet
    }
}

func opGrouped<A>(n: Int64): (GraphBuilder, Conn<A>) -> Conn<Array<A>> {
    if (n <= 0) {
        throw IllegalArgumentException("grouped 的大小必须为正数，收到 ${n}")
    }
    { b, c =>
        let l = GroupedLogic<A>(b.current, c, n)
        b.add(l)
        l.outlet
    }
}

func opScan<A, B>(zero: B, f: (B, A) -> B): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = ScanLogic<A, B>(b.current, c, zero, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opFold<A, B>(zero: B, f: (B, A) -> B): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = FoldLogic<A, B>(b.current, c, zero, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opMapAsync<A, B>(parallelism: Int64, f: (A) -> TypedPromise<B>, ordered: Bool): (GraphBuilder, Conn<A>) -> Conn<B> {
    if (parallelism <= 0) {
        throw IllegalArgumentException("mapAsync 的 parallelism 必须为正数，收到 ${parallelism}")
    }
    { b, c =>
        let l = MapAsyncLogic<A, B>(b.current, c, parallelism, f, b.decider(), ordered, None<AskCancellationToken>)
        b.add(l)
        l.outlet
    }
}

func opAsk<A, Req, Res>(parallelism: Int64, target: TypedActorRef<Req>, createRequest: (A, TypedActorRef<Res>) -> Req,
    timeout: Duration): (GraphBuilder, Conn<A>) -> Conn<Res> {
    if (parallelism <= 0) {
        throw IllegalArgumentException("ask 的 parallelism 必须为正数，收到 ${parallelism}")
    }
    if (timeout <= Duration.Zero) {
        throw IllegalArgumentException("ask 的 timeout 必须为正数")
    }
    { b, c =>
        // 每次物化一个取消令牌：阶段结束（完成、失败、取消）时撤掉尚未回复的 ask
        let token = AskCancellationToken()
        let f = { e: A =>
            TypedAsk.ask<Req, Res>(target, { r => createRequest(e, r) }, timeout, cancellation: Some(token))
        }
        let l = MapAsyncLogic<A, Res>(b.current, c, parallelism, f, b.decider(), true, Some(token))
        b.add(l)
        l.outlet
    }
}

func opThrottle<A>(elements: Int64, per: Duration, maximumBurst: Int64): (GraphBuilder, Conn<A>) -> Conn<A> {
    if (elements <= 0 || per <= Duration.Zero) {
        throw IllegalArgumentException("throttle 需要正的 elements 与 per")
    }
    if (maximumBurst <= 0) {
        throw IllegalArgumentException("throttle 的 maximumBurst 必须为正数")
    }
    { b, c =>
        let l = ThrottleLogic<A>(b.current, c, elements, per.toNanoseconds(), maximumBurst)
        b.add(l)
        l.outlet
    }
}

func opBuffer<A>(size: Int64, strategy: OverflowStrategy): (GraphBuilder, Conn<A>) -> Conn<A> {
    if (size <= 0) {
        throw IllegalArgumentException("buffer 的大小必须为正数，收到 ${size}")
    }
    { b, c =>
        let l = BufferLogic<A>(b.current, c, size, strategy)
        b.add(l)
        l.outlet
    }
}

func opRecover<A>(f: (Exception) -> Option<A>): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = RecoverLogic<A>(b.current, c, f)
        b.add(l)
        l.outlet
    }
}

func opRecoverWith<A>(attempts: Int64, f: (Exception) -> Option<Source<A, Unit>>): (GraphBuilder, Conn<A>) -> Conn<A> {
    { b, c =>
        let l = RecoverWithLogic<A>(b.current, c, attempts, f)
        b.add(l)
        l.outlet
    }
}

func opFlatMapConcat<A, B>(f: (A) -> Source<B, Unit>): (GraphBuilder, Conn<A>) -> Conn<B> {
    { b, c =>
        let l = FlatMapConcatLogic<A, B>(b.current, c, f, b.decider())
        b.add(l)
        l.outlet
    }
}

func opAsync<A>(bufferSize: Option<Int64>): (GraphBuilder, Conn<A>) -> Conn<A> {
    if (let Some(n) <- bufferSize) {
        if (n <= 0) {
            throw IllegalArgumentException("async 的 bufferSize 必须为正数，收到 ${n}")
        }
    }
    { b, c => b.asyncBoundary<A>(c, bufferSize ?? b.materializer.settings.inputBufferSize) }
}

func watchTerminationOp<A>(b: GraphBuilder, c: Conn<A>): (Conn<A>, TypedPromise<Unit>) {
    let p = TypedPromise<Unit>()
    let l = WatchTerminationLogic<A>(b.current, c, p)
    b.add(l)
    (l.outlet, p)
}

// ========== Source ==========

/**
 * 有一个输出的蓝图。Out 是元素类型，Mat 是物化值类型。
 */
public class Source<Out, Mat> {
    let buildFn: (GraphBuilder) -> (Conn<Out>, Mat)

    init(buildFn: (GraphBuilder) -> (Conn<Out>, Mat)) {
        this.buildFn = buildFn
    }

    func andThen<U>(op: (GraphBuilder, Conn<Out>) -> Conn<U>): Source<U, Mat> {
        let inner = buildFn
        Source<U, Mat>({ b =>
            let (c, m) = inner(b)
            (op(b, c), m)
        })
    }

    // ---------- 组合 ----------

    /** 接上一个 Flow，保留本 Source 的物化值 */
    public func via<U, M2>(flow: Flow<Out, U, M2>): Source<U, Mat> {
        viaMat<U, M2, Mat>(flow, { l, _ => l })
    }

    /** 接上一个 Flow，用 combine 决定物化值 */
    public func viaMat<U, M2, M3>(flow: Flow<Out, U, M2>, combine: (Mat, M2) -> M3): Source<U, M3> {
        let inner = buildFn
        let fb = flow.buildFn
        Source<U, M3>({ b =>
            let (c, m) = inner(b)
            let (c2, m2) = fb(b, c)
            (c2, combine(m, m2))
        })
    }

    /** 接上汇得到可运行的图，保留本 Source 的物化值 */
    public func to<M2>(sink: Sink<Out, M2>): RunnableGraph<Mat> {
        toMat<M2, Mat>(sink, { l, _ => l })
    }

    /** 接上汇得到可运行的图，用 combine 决定物化值 */
    public func toMat<M2, M3>(sink: Sink<Out, M2>, combine: (Mat, M2) -> M3): RunnableGraph<M3> {
        let inner = buildFn
        let sb = sink.buildFn
        RunnableGraph<M3>({ b =>
            let (c, m) = inner(b)
            combine(m, sb(b, c))
        })
    }

    /** 接上汇并运行，返回汇的物化值 */
    public func runWith<M2>(sink: Sink<Out, M2>, materializer: Materializer): M2 {
        toMat<M2, M2>(sink, { _, r => r }).run(materializer)
    }

    public func runWith<M2>(sink: Sink<Out, M2>, system: CActorSystem): M2 {
        runWith<M2>(sink, Materializer.create(system))
    }

    /** 对每个元素执行 f，返回完成 Promise */
    public func runForeach(f: (Out) -> Unit, system: CActorSystem): TypedPromise<Unit> {
        runWith<TypedPromise<Unit>>(Sinks.foreach<Out>(f), system)
    }

    public func runForeach(f: (Out) -> Unit, materializer: Materializer): TypedPromise<Unit> {
        runWith<TypedPromise<Unit>>(Sinks.foreach<Out>(f), materializer)
    }

    public func runFold<U>(zero: U, f: (U, Out) -> U, system: CActorSystem): TypedPromise<U> {
        runWith<TypedPromise<U>>(Sinks.fold<Out, U>(zero, f), system)
    }

    public func runFold<U>(zero: U, f: (U, Out) -> U, materializer: Materializer): TypedPromise<U> {
        runWith<TypedPromise<U>>(Sinks.fold<Out, U>(zero, f), materializer)
    }

    /** 变换物化值 */
    public func mapMaterializedValue<M2>(f: (Mat) -> M2): Source<Out, M2> {
        let inner = buildFn
        Source<Out, M2>({ b =>
            let (c, m) = inner(b)
            (c, f(m))
        })
    }

    // ---------- 属性 ----------

    /**
     * 异步边界：到此为止的阶段在一个 Actor（岛）里执行，之后的阶段在另一个 Actor 里执行。
     * 两侧之间按 StreamSettings.inputBufferSize 批量请求，在途元素不超过该值。
     */
    public func async(): Source<Out, Mat> {
        andThen<Out>(opAsync<Out>(None<Int64>))
    }

    /** 指定边界缓冲区大小的异步边界 */
    public func async(bufferSize: Int64): Source<Out, Mat> {
        andThen<Out>(opAsync<Out>(Some(bufferSize)))
    }

    /** 本蓝图内（尚未单独指定策略的）阶段使用 decider 处理用户函数的异常 */
    public func withSupervision(decider: Decider): Source<Out, Mat> {
        let inner = buildFn
        Source<Out, Mat>({ b =>
            b.pushDecider(decider)
            try {
                inner(b)
            } finally {
                b.popDecider()
            }
        })
    }

    // ---------- 算子 ----------

    public func map<U>(f: (Out) -> U): Source<U, Mat> {
        andThen<U>(opMap<Out, U>(f))
    }

    public func filter(p: (Out) -> Bool): Source<Out, Mat> {
        andThen<Out>(opCollect<Out, Out>({ e => if (p(e)) { Some(e) } else { None<Out> } }))
    }

    public func filterNot(p: (Out) -> Bool): Source<Out, Mat> {
        andThen<Out>(opCollect<Out, Out>({ e => if (p(e)) { None<Out> } else { Some(e) } }))
    }

    /** 过滤并变换：f 返回 None 的元素被跳过 */
    public func collect<U>(f: (Out) -> Option<U>): Source<U, Mat> {
        andThen<U>(opCollect<Out, U>(f))
    }

    /** 一个元素展开为多个，按下游需求逐个发出 */
    public func mapConcat<U>(f: (Out) -> Iterable<U>): Source<U, Mat> {
        andThen<U>(opMapConcat<Out, U>(f))
    }

    /** 只取前 n 个，然后完成下游、取消上游 */
    public func take(n: Int64): Source<Out, Mat> {
        andThen<Out>(opTake<Out>(n))
    }

    public func takeWhile(p: (Out) -> Bool): Source<Out, Mat> {
        andThen<Out>(opTakeWhile<Out>(p, false))
    }

    /** inclusive 为 true 时，第一个不满足条件的元素也会发出 */
    public func takeWhile(p: (Out) -> Bool, inclusive: Bool): Source<Out, Mat> {
        andThen<Out>(opTakeWhile<Out>(p, inclusive))
    }

    public func drop(n: Int64): Source<Out, Mat> {
        andThen<Out>(opDrop<Out>(n))
    }

    public func dropWhile(p: (Out) -> Bool): Source<Out, Mat> {
        andThen<Out>(opDropWhile<Out>(p))
    }

    /** 先发出 zero，再发出每一步的累积值 */
    public func scan<U>(zero: U, f: (U, Out) -> U): Source<U, Mat> {
        andThen<U>(opScan<Out, U>(zero, f))
    }

    /** 上游结束时发出一个累积值 */
    public func fold<U>(zero: U, f: (U, Out) -> U): Source<U, Mat> {
        andThen<U>(opFold<Out, U>(zero, f))
    }

    /** 异步变换，最多 parallelism 个在途，按上游顺序发出 */
    public func mapAsync<U>(parallelism: Int64, f: (Out) -> TypedPromise<U>): Source<U, Mat> {
        andThen<U>(opMapAsync<Out, U>(parallelism, f, true))
    }

    /** 异步变换，最多 parallelism 个在途，按完成顺序发出 */
    public func mapAsyncUnordered<U>(parallelism: Int64, f: (Out) -> TypedPromise<U>): Source<U, Mat> {
        andThen<U>(opMapAsync<Out, U>(parallelism, f, false))
    }

    /**
     * 对每个元素向 target 发起 ask（TypedAsk.ask），最多 parallelism 个在途，按上游顺序发出回复。
     * 超时让流按监督策略处理（默认失败）；流结束时尚未回复的 ask 被取消。
     */
    public func ask<Req, Res>(parallelism: Int64, target: TypedActorRef<Req>,
        createRequest: (Out, TypedActorRef<Res>) -> Req, timeout: Duration): Source<Res, Mat> {
        andThen<Res>(opAsk<Out, Req, Res>(parallelism, target, createRequest, timeout))
    }

    /** 每 per 时间最多 elements 个元素（令牌桶，突发上限 = elements） */
    public func throttle(elements: Int64, per: Duration): Source<Out, Mat> {
        andThen<Out>(opThrottle<Out>(elements, per, elements))
    }

    public func throttle(elements: Int64, per: Duration, maximumBurst: Int64): Source<Out, Mat> {
        andThen<Out>(opThrottle<Out>(elements, per, maximumBurst))
    }

    /** 最多缓存 size 个元素，满时按 strategy 处理 */
    public func buffer(size: Int64, strategy: OverflowStrategy): Source<Out, Mat> {
        andThen<Out>(opBuffer<Out>(size, strategy))
    }

    /** 上游失败时 f 给出最后一个元素并正常完成；f 返回 None 时照常失败 */
    public func recover(f: (Exception) -> Option<Out>): Source<Out, Mat> {
        andThen<Out>(opRecover<Out>(f))
    }

    /** 上游失败时切换到 f 给出的备用 Source；最多切换 attempts 次（负数表示不限） */
    public func recoverWithRetries(attempts: Int64, f: (Exception) -> Option<Source<Out, Unit>>): Source<Out, Mat> {
        andThen<Out>(opRecoverWith<Out>(attempts, f))
    }

    /** 每个元素展开为一个子 Source，依次发出各子流的全部元素 */
    public func flatMapConcat<U>(f: (Out) -> Source<U, Unit>): Source<U, Mat> {
        andThen<U>(opFlatMapConcat<Out, U>(f))
    }

    /** 物化值替换为终止 Promise（完成 / 下游取消时成功，失败时失败）；要保留原物化值用带 combine 的版本 */
    public func watchTermination(): Source<Out, TypedPromise<Unit>> {
        watchTermination<TypedPromise<Unit>>({ _, p => p })
    }

    public func watchTermination<M2>(combine: (Mat, TypedPromise<Unit>) -> M2): Source<Out, M2> {
        let inner = buildFn
        Source<Out, M2>({ b =>
            let (c, m) = inner(b)
            let (c2, p) = watchTerminationOp<Out>(b, c)
            (c2, combine(m, p))
        })
    }
}

// ========== Flow ==========

/**
 * 一进一出的蓝图。
 */
public class Flow<In, Out, Mat> {
    let buildFn: (GraphBuilder, Conn<In>) -> (Conn<Out>, Mat)

    init(buildFn: (GraphBuilder, Conn<In>) -> (Conn<Out>, Mat)) {
        this.buildFn = buildFn
    }

    func andThen<U>(op: (GraphBuilder, Conn<Out>) -> Conn<U>): Flow<In, U, Mat> {
        let inner = buildFn
        Flow<In, U, Mat>({ b, c0 =>
            let (c, m) = inner(b, c0)
            (op(b, c), m)
        })
    }

    // ---------- 组合 ----------

    public func via<U, M2>(flow: Flow<Out, U, M2>): Flow<In, U, Mat> {
        viaMat<U, M2, Mat>(flow, { l, _ => l })
    }

    public func viaMat<U, M2, M3>(flow: Flow<Out, U, M2>, combine: (Mat, M2) -> M3): Flow<In, U, M3> {
        let inner = buildFn
        let fb = flow.buildFn
        Flow<In, U, M3>({ b, c0 =>
            let (c, m) = inner(b, c0)
            let (c2, m2) = fb(b, c)
            (c2, combine(m, m2))
        })
    }

    /** 接上汇得到新的汇，保留本 Flow 的物化值 */
    public func to<M2>(sink: Sink<Out, M2>): Sink<In, Mat> {
        toMat<M2, Mat>(sink, { l, _ => l })
    }

    public func toMat<M2, M3>(sink: Sink<Out, M2>, combine: (Mat, M2) -> M3): Sink<In, M3> {
        let inner = buildFn
        let sb = sink.buildFn
        Sink<In, M3>({ b, c0 =>
            let (c, m) = inner(b, c0)
            combine(m, sb(b, c))
        })
    }

    public func mapMaterializedValue<M2>(f: (Mat) -> M2): Flow<In, Out, M2> {
        let inner = buildFn
        Flow<In, Out, M2>({ b, c0 =>
            let (c, m) = inner(b, c0)
            (c, f(m))
        })
    }

    /**
     * 物化为 Reactive Streams Processor：订阅者一侧接收输入，发布者一侧（只允许一个订阅者）输出。
     */
    public func toProcessor(materializer: Materializer): Processor<In, Out> {
        let graph = Sources.asSubscriber<In>()
            .viaMat<Out, Mat, Subscriber<In>>(this, { s, _ => s })
            .toMat<Publisher<Out>, (Subscriber<In>, Publisher<Out>)>(Sinks.asPublisher<Out>(), { s, p => (s, p) })
        let (sub, pub) = graph.run(materializer)
        FlowProcessor<In, Out>(sub, pub)
    }

    // ---------- 属性 ----------

    public func async(): Flow<In, Out, Mat> {
        andThen<Out>(opAsync<Out>(None<Int64>))
    }

    public func async(bufferSize: Int64): Flow<In, Out, Mat> {
        andThen<Out>(opAsync<Out>(Some(bufferSize)))
    }

    public func withSupervision(decider: Decider): Flow<In, Out, Mat> {
        let inner = buildFn
        Flow<In, Out, Mat>({ b, c0 =>
            b.pushDecider(decider)
            try {
                inner(b, c0)
            } finally {
                b.popDecider()
            }
        })
    }

    // ---------- 算子（语义同 Source） ----------

    public func map<U>(f: (Out) -> U): Flow<In, U, Mat> {
        andThen<U>(opMap<Out, U>(f))
    }

    public func filter(p: (Out) -> Bool): Flow<In, Out, Mat> {
        andThen<Out>(opCollect<Out, Out>({ e => if (p(e)) { Some(e) } else { None<Out> } }))
    }

    public func filterNot(p: (Out) -> Bool): Flow<In, Out, Mat> {
        andThen<Out>(opCollect<Out, Out>({ e => if (p(e)) { None<Out> } else { Some(e) } }))
    }

    public func collect<U>(f: (Out) -> Option<U>): Flow<In, U, Mat> {
        andThen<U>(opCollect<Out, U>(f))
    }

    public func mapConcat<U>(f: (Out) -> Iterable<U>): Flow<In, U, Mat> {
        andThen<U>(opMapConcat<Out, U>(f))
    }

    public func take(n: Int64): Flow<In, Out, Mat> {
        andThen<Out>(opTake<Out>(n))
    }

    public func takeWhile(p: (Out) -> Bool): Flow<In, Out, Mat> {
        andThen<Out>(opTakeWhile<Out>(p, false))
    }

    public func takeWhile(p: (Out) -> Bool, inclusive: Bool): Flow<In, Out, Mat> {
        andThen<Out>(opTakeWhile<Out>(p, inclusive))
    }

    public func drop(n: Int64): Flow<In, Out, Mat> {
        andThen<Out>(opDrop<Out>(n))
    }

    public func dropWhile(p: (Out) -> Bool): Flow<In, Out, Mat> {
        andThen<Out>(opDropWhile<Out>(p))
    }

    public func scan<U>(zero: U, f: (U, Out) -> U): Flow<In, U, Mat> {
        andThen<U>(opScan<Out, U>(zero, f))
    }

    public func fold<U>(zero: U, f: (U, Out) -> U): Flow<In, U, Mat> {
        andThen<U>(opFold<Out, U>(zero, f))
    }

    public func mapAsync<U>(parallelism: Int64, f: (Out) -> TypedPromise<U>): Flow<In, U, Mat> {
        andThen<U>(opMapAsync<Out, U>(parallelism, f, true))
    }

    public func mapAsyncUnordered<U>(parallelism: Int64, f: (Out) -> TypedPromise<U>): Flow<In, U, Mat> {
        andThen<U>(opMapAsync<Out, U>(parallelism, f, false))
    }

    public func ask<Req, Res>(parallelism: Int64, target: TypedActorRef<Req>,
        createRequest: (Out, TypedActorRef<Res>) -> Req, timeout: Duration): Flow<In, Res, Mat> {
        andThen<Res>(opAsk<Out, Req, Res>(parallelism, target, createRequest, timeout))
    }

    public func throttle(elements: Int64, per: Duration): Flow<In, Out, Mat> {
        andThen<Out>(opThrottle<Out>(elements, per, elements))
    }

    public func throttle(elements: Int64, per: Duration, maximumBurst: Int64): Flow<In, Out, Mat> {
        andThen<Out>(opThrottle<Out>(elements, per, maximumBurst))
    }

    public func buffer(size: Int64, strategy: OverflowStrategy): Flow<In, Out, Mat> {
        andThen<Out>(opBuffer<Out>(size, strategy))
    }

    public func recover(f: (Exception) -> Option<Out>): Flow<In, Out, Mat> {
        andThen<Out>(opRecover<Out>(f))
    }

    public func recoverWithRetries(attempts: Int64, f: (Exception) -> Option<Source<Out, Unit>>): Flow<In, Out, Mat> {
        andThen<Out>(opRecoverWith<Out>(attempts, f))
    }

    public func flatMapConcat<U>(f: (Out) -> Source<U, Unit>): Flow<In, U, Mat> {
        andThen<U>(opFlatMapConcat<Out, U>(f))
    }

    public func watchTermination(): Flow<In, Out, TypedPromise<Unit>> {
        watchTermination<TypedPromise<Unit>>({ _, p => p })
    }

    public func watchTermination<M2>(combine: (Mat, TypedPromise<Unit>) -> M2): Flow<In, Out, M2> {
        let inner = buildFn
        Flow<In, Out, M2>({ b, c0 =>
            let (c, m) = inner(b, c0)
            let (c2, p) = watchTerminationOp<Out>(b, c)
            (c2, combine(m, p))
        })
    }
}

// ========== Sink ==========

/**
 * 有一个输入的蓝图。
 */
public class Sink<In, Mat> {
    let buildFn: (GraphBuilder, Conn<In>) -> Mat

    init(buildFn: (GraphBuilder, Conn<In>) -> Mat) {
        this.buildFn = buildFn
    }

    /** 先用 f 变换输入再交给本汇 */
    public func contramap<U>(f: (U) -> In): Sink<U, Mat> {
        Flows.create<U>().map<In>(f).toMat<Mat, Mat>(this, { _, r => r })
    }

    public func mapMaterializedValue<M2>(f: (Mat) -> M2): Sink<In, M2> {
        let inner = buildFn
        Sink<In, M2>({ b, c => f(inner(b, c)) })
    }

    /** 本汇之前插入异步边界：汇在独立的 Actor 里执行 */
    public func async(): Sink<In, Mat> {
        let inner = buildFn
        Sink<In, Mat>({ b, c => inner(b, b.asyncBoundary<In>(c, b.materializer.settings.inputBufferSize)) })
    }

    public func withSupervision(decider: Decider): Sink<In, Mat> {
        let inner = buildFn
        Sink<In, Mat>({ b, c =>
            b.pushDecider(decider)
            try {
                inner(b, c)
            } finally {
                b.popDecider()
            }
        })
    }
}

// ========== RunnableGraph ==========

/**
 * 两端都已封闭、可以运行的图。
 */
public class RunnableGraph<Mat> {
    let buildFn: (GraphBuilder) -> Mat

    init(buildFn: (GraphBuilder) -> Mat) {
        this.buildFn = buildFn
    }

    /** 物化并启动，返回物化值。每次调用都是一条独立的流 */
    public func run(materializer: Materializer): Mat {
        materializer.materialize<Mat>(buildFn)
    }

    public func run(system: CActorSystem): Mat {
        run(Materializer.create(system))
    }

    public func mapMaterializedValue<M2>(f: (Mat) -> M2): RunnableGraph<M2> {
        let inner = buildFn
        RunnableGraph<M2>({ b => f(inner(b)) })
    }
}

// ========== 工厂 ==========

/** Source 工厂 */
public struct Sources {
    /** 一个元素 */
    public static func single<T>(element: T): Source<T, Unit> {
        fromIterator<T>({ => [element].iterator() })
    }

    /** 依次发出集合里的元素（每次物化重新迭代） */
    public static func from<T>(elements: Iterable<T>): Source<T, Unit> {
        fromIterator<T>({ => elements.iterator() })
    }

    /** 每次物化调用 mk 取一个新的迭代器，按需拉取 */
    public static func fromIterator<T>(mk: () -> Iterator<T>): Source<T, Unit> {
        Source<T, Unit>({ b =>
            let l = IteratorSourceLogic<T>(b.current, mk)
            b.add(l)
            (l.outlet, ())
        })
    }

    public static func empty<T>(): Source<T, Unit> {
        Source<T, Unit>({ b =>
            let l = EmptySourceLogic<T>(b.current)
            b.add(l)
            (l.outlet, ())
        })
    }

    /** 物化后立即失败 */
    public static func failed<T>(cause: Exception): Source<T, Unit> {
        Source<T, Unit>({ b =>
            let l = FailedSourceLogic<T>(b.current, cause)
            b.add(l)
            (l.outlet, ())
        })
    }

    /** [start, end] 闭区间 */
    public static func range(start: Int64, end: Int64): Source<Int64, Unit> {
        range(start, end, 1)
    }

    public static func range(start: Int64, end: Int64, step: Int64): Source<Int64, Unit> {
        if (step == 0) {
            throw IllegalArgumentException("range 的 step 不能为 0")
        }
        fromIterator<Int64>({ => RangeIterator(start, end, step) })
    }

    /** 无限重复同一个元素（只在下游需要时才发出） */
    public static func repeat<T>(element: T): Source<T, Unit> {
        fromIterator<T>({ => RepeatIterator<T>(element) })
    }

    /** 从 seed 起展开：f 返回 Some((下一状态, 元素)) 继续，None 结束；可以是无限流 */
    public static func unfold<S, T>(seed: S, f: (S) -> Option<(S, T)>): Source<T, Unit> {
        fromIterator<T>({ => UnfoldIterator<S, T>(seed, f) })
    }

    /** seed, f(seed), f(f(seed)), ... 的无限流 */
    public static func iterate<T>(seed: T, f: (T) -> T): Source<T, Unit> {
        unfold<T, T>(seed, { s => Some((f(s), s)) })
    }

    /** Promise 成功时发出其值后完成，失败时流失败 */
    public static func fromPromise<T>(promise: TypedPromise<T>): Source<T, Unit> {
        Source<T, Unit>({ b =>
            let l = PromiseSourceLogic<T>(b.current, promise)
            b.add(l)
            (l.outlet, ())
        })
    }

    /** 每 interval 发出一次 element；下游没有需求时这次 tick 被丢弃。物化值可停止 tick（流随之完成） */
    public static func tick<T>(initialDelay: Duration, interval: Duration, element: T): Source<T, Cancellable> {
        if (interval <= Duration.Zero) {
            throw IllegalArgumentException("tick 的 interval 必须为正数")
        }
        Source<T, Cancellable>({ b =>
            let c = TickCancellable()
            let l = TickSourceLogic<T>(b.current, initialDelay, interval, element, c)
            c.logic = Some(l)
            b.add(l)
            (l.outlet, c)
        })
    }

    /**
     * 从流外送入元素的源（物化值 SourceQueue）。缓冲区最多 bufferSize 个元素，满时按 overflow 处理；
     * Backpressure 策略下 offer 的 Promise 在腾出位置后才完成，同时未完成的 offer 最多 1 个。
     */
    public static func queue<T>(bufferSize: Int64, overflow: OverflowStrategy): Source<T, SourceQueue<T>> {
        queue<T>(bufferSize, overflow, 1)
    }

    /** maxConcurrentOffers：Backpressure 策略下允许同时挂起的 offer 数（多个生产者时调大） */
    public static func queue<T>(bufferSize: Int64, overflow: OverflowStrategy, maxConcurrentOffers: Int64): Source<T,
        SourceQueue<T>> {
        if (bufferSize <= 0) {
            throw IllegalArgumentException("Source.queue 的 bufferSize 必须为正数，收到 ${bufferSize}")
        }
        if (maxConcurrentOffers <= 0) {
            throw IllegalArgumentException("maxConcurrentOffers 必须为正数，收到 ${maxConcurrentOffers}")
        }
        Source<T, SourceQueue<T>>({ b =>
            let l = QueueSourceLogic<T>(b.current, bufferSize, overflow, maxConcurrentOffers)
            b.add(l)
            (l.outlet, SourceQueue<T>(l))
        })
    }

    /**
     * 物化值是一个 TypedActorRef<T>：tell 进来的消息成为流的元素（对标 Akka Typed ActorSource.actorRef）。
     * completeWhen 为真的消息让流在排空缓冲区后完成；failWhen 返回异常的消息让流立即失败。
     * 不支持 Backpressure（tell 无法等待）——需要背压时用 Sources.queue。
     */
    public static func actorRef<T>(bufferSize: Int64, overflow: OverflowStrategy, completeWhen: (T) -> Bool,
        failWhen: (T) -> Option<Exception>): Source<T, TypedActorRef<T>> {
        if (let Backpressure <- overflow) {
            throw IllegalArgumentException("Sources.actorRef 不支持 Backpressure 策略，请用 Sources.queue")
        }
        if (bufferSize <= 0) {
            throw IllegalArgumentException("Sources.actorRef 的 bufferSize 必须为正数，收到 ${bufferSize}")
        }
        Source<T, TypedActorRef<T>>({ b =>
            let l = QueueSourceLogic<T>(b.current, bufferSize, overflow, 1)
            b.add(l)
            (l.outlet, QueueActorRef<T>(l, completeWhen, failWhen))
        })
    }

    /** 订阅一个 Reactive Streams 发布者；按 inputBufferSize 批量 request */
    public static func fromPublisher<T>(publisher: Publisher<T>): Source<T, Unit> {
        Source<T, Unit>({ b =>
            let l = SubscriberSourceLogic<T>(b.current, b.materializer.settings.inputBufferSize, Some(publisher))
            b.add(l)
            (l.outlet, ())
        })
    }

    /** 物化值是一个订阅者：把它交给任意发布者去 subscribe，发布的元素就流进来 */
    public static func asSubscriber<T>(): Source<T, Subscriber<T>> {
        Source<T, Subscriber<T>>({ b =>
            let l = SubscriberSourceLogic<T>(b.current, b.materializer.settings.inputBufferSize, None<Publisher<T>>)
            b.add(l)
            (l.outlet, l.subscriber)
        })
    }
}

/** Flow 工厂 */
public struct Flows {
    /** 恒等 Flow，在其上链式添加算子 */
    public static func create<T>(): Flow<T, T, Unit> {
        Flow<T, T, Unit>({ _, c => (c, ()) })
    }

    public static func fromFunction<A, B>(f: (A) -> B): Flow<A, B, Unit> {
        create<A>().map<B>(f)
    }

    /**
     * 每 n 个元素打成一组；结束时不足 n 个的余数也会发出。用法：source.via(Flows.grouped<T>(n))
     * （不做成 Source / Flow 的方法：返回 Source<Array<Out>> 的非泛型方法会让仓颉泛型无限实例化）
     */
    public static func grouped<T>(n: Int64): Flow<T, Array<T>, Unit> {
        create<T>().andThen<Array<T>>(opGrouped<T>(n))
    }
}

/** Sink 工厂 */
public struct Sinks {
    /** 对每个元素执行 f；物化值在流完成时成功、失败时失败 */
    public static func foreach<T>(f: (T) -> Unit): Sink<T, TypedPromise<Unit>> {
        Sink<T, TypedPromise<Unit>>({ b, c =>
            let p = TypedPromise<Unit>()
            let l = ForeachLogic<T>(b.current, c, f, b.decider(), p)
            b.add(l)
            p
        })
    }

    /** 丢弃全部元素（仍然逐个请求，保持背压） */
    public static func ignore<T>(): Sink<T, TypedPromise<Unit>> {
        foreach<T>({ _ => () })
    }

    public static func fold<T, U>(zero: U, f: (U, T) -> U): Sink<T, TypedPromise<U>> {
        Sink<T, TypedPromise<U>>({ b, c =>
            let p = TypedPromise<U>()
            let l = FoldSinkLogic<T, U>(b.current, c, zero, f, b.decider(), p)
            b.add(l)
            p
        })
    }

    /** 收集全部元素（无上限：只用于已知有限的流） */
    public static func seq<T>(): Sink<T, TypedPromise<Array<T>>> {
        seq<T>(Int64.Max)
    }

    /** 收集全部元素，超过 maxElements 时以 StreamLimitReachedException 失败 */
    public static func seq<T>(maxElements: Int64): Sink<T, TypedPromise<Array<T>>> {
        if (maxElements < 0) {
            throw IllegalArgumentException("maxElements 不能为负数")
        }
        Sink<T, TypedPromise<Array<T>>>({ b, c =>
            let p = TypedPromise<Array<T>>()
            let l = SeqLogic<T>(b.current, c, maxElements, p)
            b.add(l)
            p
        })
    }

    /** 第一个元素；流为空时以 EmptyStreamException 失败 */
    public static func head<T>(): Sink<T, TypedPromise<T>> {
        headOption<T>().mapMaterializedValue<TypedPromise<T>>({ p =>
            p.map<T>({ o =>
                match (o) {
                    case Some(v) => v
                    case None => throw EmptyStreamException("Sink.head：流为空")
                }
            })
        })
    }

    /** 第一个元素；流为空时为 None */
    public static func headOption<T>(): Sink<T, TypedPromise<Option<T>>> {
        Sink<T, TypedPromise<Option<T>>>({ b, c =>
            let p = TypedPromise<Option<T>>()
            let l = HeadOptionLogic<T>(b.current, c, p)
            b.add(l)
            p
        })
    }

    /** 立即取消上游 */
    public static func cancelled<T>(): Sink<T, Unit> {
        Sink<T, Unit>({ b, c =>
            b.add(CancelledSinkLogic<T>(b.current, c))
            ()
        })
    }

    /**
     * 带确认的投递到类型化 Actor（对标 Akka Typed ActorSink.actorRefWithBackpressure）：
     * 先发 onInitMessage(ackTo)，目标每处理完一条就向 ackTo 回一个 Ack，流才发下一条；
     * 最后一条被确认后发 onCompleteMessage，失败时发 onFailureMessage(e)。目标慢时整条流被背压。
     */
    public static func actorRefWithBackpressure<T, M, Ack>(target: TypedActorRef<M>,
        messageAdapter: (TypedActorRef<Ack>, T) -> M, onInitMessage: (TypedActorRef<Ack>) -> M,
        onCompleteMessage: M, onFailureMessage: (Exception) -> M): Sink<T, Unit> {
        Sink<T, Unit>({ b, c =>
            b.add(ActorRefBackpressureLogic<T, M, Ack>(b.current, c, target, messageAdapter, onInitMessage,
                onCompleteMessage, onFailureMessage))
            ()
        })
    }

    /** 物化值是一个只允许一个订阅者的 Reactive Streams 发布者；订阅者 request 多少才发多少 */
    public static func asPublisher<T>(): Sink<T, Publisher<T>> {
        Sink<T, Publisher<T>>({ b, c =>
            let l = PublisherSinkLogic<T>(b.current, c, None<Subscriber<T>>)
            b.add(l)
            l.publisher
        })
    }

    /** 把元素交给一个 Reactive Streams 订阅者（按它的 request 发送） */
    public static func fromSubscriber<T>(subscriber: Subscriber<T>): Sink<T, Unit> {
        Sink<T, Unit>({ b, c =>
            b.add(PublisherSinkLogic<T>(b.current, c, Some(subscriber)))
            ()
        })
    }

    /** 物化值是一个拉取队列：在流外逐个 pull 元素 */
    public static func queue<T>(): Sink<T, SinkQueue<T>> {
        Sink<T, SinkQueue<T>>({ b, c =>
            let l = SinkQueueLogic<T>(b.current, c)
            b.add(l)
            SinkQueue<T>(l)
        })
    }
}
