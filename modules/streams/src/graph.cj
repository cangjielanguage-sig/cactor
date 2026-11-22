/*
 * CActor Streams - GraphDSL（差距 4.8 的扇出编排）
 *
 * 在现有 Source / Flow / Sink 线性组合之上补最小的多出口编排，不重写引擎：
 *   - `broadcast(source, sinks)`：每个元素发给全部汇，最慢的汇决定上游节奏；
 *   - `partitionBy(source, partitions, keyOf, sinks)`：按 key 恒定路由到同一分区。
 * 两者都保持 Reactive Streams 语义：每个出口有独立需求；出口全部取消时取消上游。
 */
package cactor_streams

import std.collection.*

/** 最小的图编排组合子（对标 Akka GraphDSL 的 Broadcast / Partition 子集） */
public struct GraphDSL {
    /**
     * 一个源扇出到多个汇：每个元素广播给全部汇。
     * 物化值为 `(源的物化值, 各汇的物化值)`。
     */
    public static func broadcast<In, Mat, M>(source: Source<In, Mat>,
        sinks: Array<Sink<In, M>>): RunnableGraph<(Mat, Array<M>)> {
        if (sinks.isEmpty()) {
            throw IllegalArgumentException("broadcast 至少需要一个汇")
        }
        let n = sinks.size
        let inner = source.buildFn
        RunnableGraph<(Mat, Array<M>)>({ b =>
            let (c, m) = inner(b)
            let island = b.current
            let indices = Array<Int64>(n, { i => i })
            let logic = FanOutLogic<In>(island, c, n, { _ => indices })
            b.add(logic)
            let mats = ArrayList<M>()
            for (i in 0..n) {
                // 汇里可能带 async 边界；每个汇都必须从 junction 所在的岛开始构建
                b.current = island
                mats.add(sinks[i].buildFn(b, logic.outlets[i]))
            }
            (m, mats.toArray())
        })
    }

    /**
     * 一个源按 key 分区到 n 个汇：同一 key 恒定落到同一分区。
     * `keyOf` 的返回值按 n 取模归一化到 [0, n)，因此负数也安全。
     */
    public static func partitionBy<In, Mat, M>(source: Source<In, Mat>, partitions: Int64,
        keyOf: (In) -> Int64, sinks: Array<Sink<In, M>>): RunnableGraph<(Mat, Array<M>)> {
        if (partitions <= 0) {
            throw IllegalArgumentException("分区数必须为正数: ${partitions}")
        }
        if (sinks.size != partitions) {
            throw IllegalArgumentException("分区数 ${partitions} 与汇的数量 ${sinks.size} 不一致")
        }
        let n = partitions
        let inner = source.buildFn
        RunnableGraph<(Mat, Array<M>)>({ b =>
            let (c, m) = inner(b)
            let island = b.current
            let route: (In) -> Array<Int64> = { x =>
                let k = keyOf(x)
                let idx = ((k % n) + n) % n
                [idx]
            }
            let logic = FanOutLogic<In>(island, c, n, route)
            b.add(logic)
            let mats = ArrayList<M>()
            for (i in 0..n) {
                b.current = island
                mats.add(sinks[i].buildFn(b, logic.outlets[i]))
            }
            (m, mats.toArray())
        })
    }

    /**
     * 一个源按需轮询分发到多个汇（对标 Akka Balance）：只发给当前有需求的出口，
     * 从上次位置继续，因此各汇负载大致均衡；有界（最多缓冲 1 个元素）。
     */
    public static func balance<In, Mat, M>(source: Source<In, Mat>,
        sinks: Array<Sink<In, M>>): RunnableGraph<(Mat, Array<M>)> {
        if (sinks.isEmpty()) {
            throw IllegalArgumentException("balance 至少需要一个汇")
        }
        let n = sinks.size
        let inner = source.buildFn
        RunnableGraph<(Mat, Array<M>)>({ b =>
            let (c, m) = inner(b)
            let island = b.current
            let logic = BalanceLogic<In>(island, c, n)
            b.add(logic)
            let mats = ArrayList<M>()
            for (i in 0..n) {
                b.current = island
                mats.add(sinks[i].buildFn(b, logic.outlets[i]))
            }
            (m, mats.toArray())
        })
    }

    /**
     * 两个源按位置配对（对标 Akka Zip）：任一侧结束即结束。
     * 物化值为 `(左源 Mat, 右源 Mat, 汇 Mat)`。
     */
    public static func zip<A, B, M1, M2, M>(left: Source<A, M1>, right: Source<B, M2>,
        sink: Sink<(A, B), M>): RunnableGraph<(M1, M2, M)> {
        let lb = left.buildFn
        let rb = right.buildFn
        let sb = sink.buildFn
        RunnableGraph<(M1, M2, M)>({ b =>
            let (lc, lm) = lb(b)
            let island = b.current
            b.current = island
            let (rc, rm) = rb(b)
            // 两个源必须在同一个岛里，否则同一 junction 会被两个岛并发调用
            if (!refEq(b.current, island)) {
                throw IllegalArgumentException("zip 的源不能各自以 async 边界结尾")
            }
            let logic = ZipLogic<A, B>(island, lc, rc)
            b.add(logic)
            b.current = island
            let mm = sb(b, logic.outlet)
            (lm, rm, mm)
        })
    }
}
