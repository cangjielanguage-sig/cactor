/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 流 DSL 示例：Source |> Flow |> Sink
 *
 * 演示 P4 的管道 API：
 *   let result = source |> via(flow) |> to(sink)
 * 与链式写法 source.via(flow).to(sink).run() 完全等价。
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_pipeline
 */
package cactor_examples.dsl_pipeline

import std.collection.*
import cactor_streams.dsl.*

main(): Int64 {
    // 数据源
    let source = StreamSource<Int64>([1, 2, 3, 4, 5, 6], "numbers")

    // 转换：只保留偶数，再映射成字符串
    let even = keep<Int64>({ x => x % 2 == 0 })
    let toText = StreamFlow<Int64, String>({ elements =>
        Array<String>(elements.size, { i => "值${elements[i]}" })
    }, "toText")

    // 汇聚：求和
    let sum = StreamSink<String, Int64>({ elements =>
        var total: Int64 = 0
        for (e in elements) {
            total += e.size
        }
        total
    }, "sum")

    // 管道写法
    let piped = source |> via(even) |> via(toText) |> to(sum)
    // 链式写法（同一语义，便于对照）
    let chained = source.via(even).via(toText).to(sum).run()

    println("管道结果：${piped}")
    println("链式结果：${chained}")

    var ok = piped == chained && piped > 0
    ok = ok && piped == 12
    println(if (ok) { "dsl_pipeline：通过" } else { "dsl_pipeline：失败" })
    if (ok) { 0 } else { 1 }
}
