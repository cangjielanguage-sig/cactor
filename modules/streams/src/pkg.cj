/*
 * CActor Streams - 包定义
 *
 * 按 Reactive Streams 实现的背压流（对标 Akka Streams）：
 * - cactor_streams：惰性蓝图 Source / Flow / Sink / RunnableGraph、物化器、算子、kill switch、流内监督
 * - cactor_streams.reactive：Publisher / Subscriber / Subscription / Processor 接口（与运行时无关）
 *
 * 执行模型见 engine.cj：同一岛内的阶段融合在一个类型化 Actor 中执行，async 边界把图切成多个岛，
 * 岛之间按信用（request(n)）批量传递元素，因此慢消费者面对无限流时内存有界。
 */
package cactor_streams
