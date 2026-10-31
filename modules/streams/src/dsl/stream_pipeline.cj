/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Streams - 管道 API：Source |> Flow |> Sink
 *
 * 仓颉的 |> 是内建管道运算符（右侧必须是函数值，且不可重载），
 * 因此这里把 Flow / Sink 包装成"流 → 结果"的函数值，得到自然的管道写法：
 *
 *   let total = source |> via(flow) |> to(sink)
 *
 * 语义与链式写法完全等价：
 *   source.via(flow).to(sink)  ==  source |> via(flow) |> to(sink)
 */
package cactor_streams.dsl

import std.collection.*

/**
 * 把 Flow 包装成管道中的一段：Source<T> -> Source<U>
 */
public func via<T, U>(flow: StreamFlow<T, U>): (StreamSource<T>) -> StreamSource<U> {
    { s => flow.apply(s) }
}

/**
 * 把 Sink 包装成管道中的一段：Source<T> -> R
 */
public func to<T, R>(sink: StreamSink<T, R>): (StreamSource<T>) -> R {
    { s => sink.consume(s) }
}

/**
 * 只做过滤的 Flow 快捷方式：Source<T> -> Source<T>
 * （不叫 where：where 是仓颉关键字）
 */
public func keep<T>(predicate: (T) -> Bool): StreamFlow<T, T> {
    StreamFlow<T, T>({ elements =>
        let kept = ArrayList<T>()
        for (e in elements) {
            if (predicate(e)) {
                kept.add(e)
            }
        }
        kept.toArray()
    }, "keep")
}
