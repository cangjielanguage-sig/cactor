/*
 * CActor Streams - 管道写法：source |> via(flow) |> to(sink) |> run(system)
 *
 * 仓颉的 |> 是内建管道运算符：a |> f 等价于 f(a)，右侧必须是可调用的值，且不能重载。
 * 这里的每个函数都返回一个"管道段"——接收上一段蓝图、返回下一段蓝图的函数值，因此：
 *
 *   let result = Sources.range(1, 100)
 *       |> filter { x => x % 2 == 0 }
 *       |> map { x => x * 10 }
 *       |> via(enrich)
 *       |> runWith(Sinks.seq<Int64>(), system)
 *
 * 规则只有一条：a |> f(参数) 与链式写法 a.f(参数) 完全等价（物化值的保留方式也相同），
 * 两种写法可以混用，不另起执行路径。
 *
 * 为什么是自由函数而不是运算符重载（逐项在 cjc 1.2.0 上验证过）：
 * - via / map 等要引入新的类型参数（下游元素类型、Flow 的物化值），而运算符函数不允许是泛型的，
 *   所以 source >> flow、可调用对象的 operator () 都写不出来；
 * - |> 会用左侧的类型反推右侧泛型函数的类型参数（含 Source 的物化值类型 Mat），
 *   尾随 lambda 的参数类型也随之推断：source |> map { x => x * 2 } 不需要写 x 的类型；
 * - 同名函数按"返回的管道段接收 Source 还是 Flow"重载，由左侧选中，所以同一个 via / map
 *   既能接在 Source 后面，也能接在 Flow 后面；与 std.collection 同名的 map / filter / fold / take
 *   同时导入时也按左侧类型区分，集合管道 [1, 2] |> map { ... } |> collectArray 照常可用。
 *
 * 参数不含泛型类型的算子（take、drop、grouped、throttle、buffer、async、withSupervision）只有 Source 版：
 *   它们的 Source 版与 Flow 版参数完全相同（如都是 Int64），仓颉判为重载冲突（即使类型参数个数不同）；
 *   在 Flow 上请用链式写法 flow.take(n)，或 flow |> via(Flows.create<T>().take(n))。
 *
 * 不提供管道版的：返回 Source<Array<_>> 以外需要额外物化值组合的少见算子（recoverWithRetries、
 * watchTermination 带组合函数等）——它们仍可用链式写法，与管道段自由混用。
 */
package cactor_streams

import std.time.*
import cactor_core.typed.*
import cactor.api.CActorSystem

// ========== 组合：via / viaMat ==========

/** 接上一个 Flow，保留左侧的物化值：source |> via(flow) */
public func via<A, B, M, M2>(flow: Flow<A, B, M2>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.via<B, M2>(flow) }
}

/** Flow 之后再接一个 Flow：flow |> via(next) */
public func via<I, A, B, M, M2>(flow: Flow<A, B, M2>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { f: Flow<I, A, M> => f.via<B, M2>(flow) }
}

/** 接上一个 Flow，用 combine 决定物化值 */
public func viaMat<A, B, M, M2, M3>(flow: Flow<A, B, M2>, combine: (M, M2) -> M3): (Source<A, M>) -> Source<B, M3> {
    { s: Source<A, M> => s.viaMat<B, M2, M3>(flow, combine) }
}

public func viaMat<I, A, B, M, M2, M3>(flow: Flow<A, B, M2>, combine: (M, M2) -> M3): (Flow<I, A, M>) -> Flow<I, B, M3> {
    { f: Flow<I, A, M> => f.viaMat<B, M2, M3>(flow, combine) }
}

// ========== 封闭：to / toMat ==========

/** 接上汇得到可运行的图，保留左侧的物化值（与 Source.to 相同）：source |> to(sink) |> run(system) */
public func to<A, M, M2>(sink: Sink<A, M2>): (Source<A, M>) -> RunnableGraph<M> {
    { s: Source<A, M> => s.to<M2>(sink) }
}

/** Flow 接上汇得到新的汇：flow |> to(sink) */
public func to<I, A, M, M2>(sink: Sink<A, M2>): (Flow<I, A, M>) -> Sink<I, M> {
    { f: Flow<I, A, M> => f.to<M2>(sink) }
}

/** 接上汇，用 combine 决定物化值；只要汇的物化值时写 toMat(sink, { _, r => r }) 或直接用 runWith */
public func toMat<A, M, M2, M3>(sink: Sink<A, M2>, combine: (M, M2) -> M3): (Source<A, M>) -> RunnableGraph<M3> {
    { s: Source<A, M> => s.toMat<M2, M3>(sink, combine) }
}

public func toMat<I, A, M, M2, M3>(sink: Sink<A, M2>, combine: (M, M2) -> M3): (Flow<I, A, M>) -> Sink<I, M3> {
    { f: Flow<I, A, M> => f.toMat<M2, M3>(sink, combine) }
}

// ========== 运行 ==========

/** 物化并启动可运行的图，返回物化值：graph |> run(system) */
public func run<M>(system: CActorSystem): (RunnableGraph<M>) -> M {
    { g: RunnableGraph<M> => g.run(system) }
}

public func run<M>(materializer: Materializer): (RunnableGraph<M>) -> M {
    { g: RunnableGraph<M> => g.run(materializer) }
}

/** 接上汇并运行，返回汇的物化值（例如 Sinks.seq 的结果 Promise）：source |> runWith(sink, system) */
public func runWith<A, M, M2>(sink: Sink<A, M2>, system: CActorSystem): (Source<A, M>) -> M2 {
    { s: Source<A, M> => s.runWith<M2>(sink, system) }
}

public func runWith<A, M, M2>(sink: Sink<A, M2>, materializer: Materializer): (Source<A, M>) -> M2 {
    { s: Source<A, M> => s.runWith<M2>(sink, materializer) }
}

/** 对每个元素执行 f 并运行，返回完成 Promise */
public func runForeach<A, M>(system: CActorSystem, f: (A) -> Unit): (Source<A, M>) -> TypedPromise<Unit> {
    { s: Source<A, M> => s.runForeach(f, system) }
}

/** 折叠全部元素并运行，返回结果 Promise */
public func runFold<A, M, U>(system: CActorSystem, zero: U, f: (U, A) -> U): (Source<A, M>) -> TypedPromise<U> {
    { s: Source<A, M> => s.runFold<U>(zero, f, system) }
}

// ========== 变换算子（Source 与 Flow 各一份，由左侧选中） ==========

public func map<A, B, M>(f: (A) -> B): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.map<B>(f) }
}

public func map<I, A, B, M>(f: (A) -> B): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.map<B>(f) }
}

public func filter<A, M>(p: (A) -> Bool): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.filter(p) }
}

public func filter<I, A, M>(p: (A) -> Bool): (Flow<I, A, M>) -> Flow<I, A, M> {
    { fl: Flow<I, A, M> => fl.filter(p) }
}

public func filterNot<A, M>(p: (A) -> Bool): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.filterNot(p) }
}

public func filterNot<I, A, M>(p: (A) -> Bool): (Flow<I, A, M>) -> Flow<I, A, M> {
    { fl: Flow<I, A, M> => fl.filterNot(p) }
}

/** f 返回 Some 的元素才向下游发出 */
public func collect<A, B, M>(f: (A) -> Option<B>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.collect<B>(f) }
}

public func collect<I, A, B, M>(f: (A) -> Option<B>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.collect<B>(f) }
}

public func mapConcat<A, B, M>(f: (A) -> Iterable<B>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.mapConcat<B>(f) }
}

public func mapConcat<I, A, B, M>(f: (A) -> Iterable<B>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.mapConcat<B>(f) }
}

public func take<A, M>(n: Int64): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.take(n) }
}


public func takeWhile<A, M>(p: (A) -> Bool): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.takeWhile(p) }
}

public func takeWhile<I, A, M>(p: (A) -> Bool): (Flow<I, A, M>) -> Flow<I, A, M> {
    { fl: Flow<I, A, M> => fl.takeWhile(p) }
}

public func drop<A, M>(n: Int64): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.drop(n) }
}


public func dropWhile<A, M>(p: (A) -> Bool): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.dropWhile(p) }
}

public func dropWhile<I, A, M>(p: (A) -> Bool): (Flow<I, A, M>) -> Flow<I, A, M> {
    { fl: Flow<I, A, M> => fl.dropWhile(p) }
}

/** 发出每一步的累积值（含初始值） */
public func scan<A, U, M>(zero: U, f: (U, A) -> U): (Source<A, M>) -> Source<U, M> {
    { s: Source<A, M> => s.scan<U>(zero, f) }
}

public func scan<I, A, U, M>(zero: U, f: (U, A) -> U): (Flow<I, A, M>) -> Flow<I, U, M> {
    { fl: Flow<I, A, M> => fl.scan<U>(zero, f) }
}

/** 上游完成时发出一个折叠结果 */
public func fold<A, U, M>(zero: U, f: (U, A) -> U): (Source<A, M>) -> Source<U, M> {
    { s: Source<A, M> => s.fold<U>(zero, f) }
}

public func fold<I, A, U, M>(zero: U, f: (U, A) -> U): (Flow<I, A, M>) -> Flow<I, U, M> {
    { fl: Flow<I, A, M> => fl.fold<U>(zero, f) }
}

/** 每 n 个元素打成一组（结束时不足 n 个的余数也发出） */
public func grouped<A, M>(n: Int64): (Source<A, M>) -> Source<Array<A>, M> {
    { s: Source<A, M> => s.via<Array<A>, Unit>(Flows.grouped<A>(n)) }
}


/** 最多 parallelism 个在途，按上游顺序发出：source |> mapAsync(4) { x => lookup(x) } */
public func mapAsync<A, B, M>(parallelism: Int64, f: (A) -> TypedPromise<B>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.mapAsync<B>(parallelism, f) }
}

public func mapAsync<I, A, B, M>(parallelism: Int64, f: (A) -> TypedPromise<B>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.mapAsync<B>(parallelism, f) }
}

/** 最多 parallelism 个在途，谁先完成先发出谁 */
public func mapAsyncUnordered<A, B, M>(parallelism: Int64, f: (A) -> TypedPromise<B>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.mapAsyncUnordered<B>(parallelism, f) }
}

public func mapAsyncUnordered<I, A, B, M>(parallelism: Int64, f: (A) -> TypedPromise<B>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.mapAsyncUnordered<B>(parallelism, f) }
}

/** 对每个元素向 target 发起 ask，最多 parallelism 个在途，按上游顺序发出回复 */
public func ask<A, Req, Res, M>(parallelism: Int64, target: TypedActorRef<Req>,
    createRequest: (A, TypedActorRef<Res>) -> Req, timeout: Duration): (Source<A, M>) -> Source<Res, M> {
    { s: Source<A, M> => s.ask<Req, Res>(parallelism, target, createRequest, timeout) }
}

public func ask<I, A, Req, Res, M>(parallelism: Int64, target: TypedActorRef<Req>,
    createRequest: (A, TypedActorRef<Res>) -> Req, timeout: Duration): (Flow<I, A, M>) -> Flow<I, Res, M> {
    { fl: Flow<I, A, M> => fl.ask<Req, Res>(parallelism, target, createRequest, timeout) }
}

/** 每 per 时间最多 elements 个元素 */
public func throttle<A, M>(elements: Int64, per: Duration): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.throttle(elements, per) }
}


/** 最多缓存 size 个元素，满时按 strategy 处理 */
public func buffer<A, M>(size: Int64, strategy: OverflowStrategy): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.buffer(size, strategy) }
}


/** 上游失败时 f 给出最后一个元素并正常完成 */
public func recover<A, M>(f: (Exception) -> Option<A>): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.recover(f) }
}

public func recover<I, A, M>(f: (Exception) -> Option<A>): (Flow<I, A, M>) -> Flow<I, A, M> {
    { fl: Flow<I, A, M> => fl.recover(f) }
}

/** 每个元素展开为一个子 Source，依次发出各子流的全部元素 */
public func flatMapConcat<A, B, M>(f: (A) -> Source<B, Unit>): (Source<A, M>) -> Source<B, M> {
    { s: Source<A, M> => s.flatMapConcat<B>(f) }
}

public func flatMapConcat<I, A, B, M>(f: (A) -> Source<B, Unit>): (Flow<I, A, M>) -> Flow<I, B, M> {
    { fl: Flow<I, A, M> => fl.flatMapConcat<B>(f) }
}

// ========== 执行属性（只有 Source 版，见文件头"参数不含泛型的算子"） ==========

/** 在此处切出异步边界：之前与之后的阶段在两个 Actor 里执行 */
public func async<A, M>(): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.async() }
}


/** 为此前（左侧）的全部阶段设置流内监督策略 */
public func withSupervision<A, M>(decider: Decider): (Source<A, M>) -> Source<A, M> {
    { s: Source<A, M> => s.withSupervision(decider) }
}

