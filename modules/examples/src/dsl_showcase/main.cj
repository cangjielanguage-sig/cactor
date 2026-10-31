/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例入口
 *
 * 同一组场景（Hello World / 计数器 / 请求-应答）各写两遍：
 *   经典写法：dsl_showcase/classic 下的三个文件（手写 receive + match）
 *   DSL 写法：dsl_showcase/dsl 下的三个文件（@actor 宏）
 * 行数对照（A1 验收）由 tools/dsl_line_count.sh 生成，结果见 docs/dsl.md。
 */
package cactor_examples.dsl_showcase

import cactor_examples.dsl_showcase.classic.{runClassicHello, runClassicCounter, runClassicAsk}
import cactor_examples.dsl_showcase.dsl.{runDslHello, runDslCounter, runDslAsk}

main(): Int64 {
    println("=== CActor DSL 对照示例 ===")
    println("--- 经典写法（手写 receive + match）---")
    runClassicHello()
    runClassicCounter()
    runClassicAsk()
    println("--- DSL 写法（@actor 宏）---")
    runDslHello()
    runDslCounter()
    runDslAsk()
    println("=== 完成 ===")
    return 0
}
