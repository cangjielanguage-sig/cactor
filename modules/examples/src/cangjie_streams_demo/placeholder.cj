/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 仓颉流演示
 * Plan 0.20 Phase 2.1: 占位文件清理 - 仓颉流演示实现
 */
package cactor_examples.cangjie_streams_demo

import std.time.*
import std.collection.*
import cactor_streams.unified.{UnifiedStreamPipeline, StreamBuilder, SimpleStreamElement, UnifiedStreamManager}

/**
 * 仓颉流处理演示类
 */
public class CangjieStreamDemo {
    private let streamManager: UnifiedStreamManager

    public init() {
        this.streamManager = UnifiedStreamManager.getInstance()
    }

    /**
     * 演示基本流操作
     */
    public func demonstrateBasicStreams(): Unit {
        println("🔧 演示1: 基本流操作")

        // 创建数据源
        let sourceData = ArrayList<String>()
        for (i in 1..11) {
            sourceData.add("item-${i}")
        }

        // 创建流管道
        let pipeline = StreamBuilder("basic-stream")
            .map("add-prefix", { data => "processed-${data}" })
            .filter("even-only", { data => data.contains("2") || data.contains("4") || data.contains("6") || data.contains("8") || data.contains("10") })
            .build()

        // 注册管道
        streamManager.registerPipeline("basic-demo", pipeline)

        // 处理数据（简化实现）
        println("开始处理${sourceData.size}个数据项")
        var processedCount: Int64 = 0

        for (data in sourceData) {
            let element = SimpleStreamElement(data)
            match (pipeline.process(element)) {
                case Some(result) =>
                    processedCount += 1
                    println("- ${result.getValue()}")
                case None =>
                    println("- 数据被过滤: ${data}")
            }
        }

        println("输入数据: ${sourceData.size}个")
        println("处理完成: ${processedCount}个")
    }

    /**
     * 演示链式流操作
     */
    public func demonstrateChainedStreams(): Unit {
        println("\n🔗 演示2: 链式流操作")

        // 创建复杂的链式流管道
        let chainedPipeline = StreamBuilder("chained-stream")
            .map("normalize", { data => data + "-normalized" })
            .map("add-timestamp", { data => "${data}-${DateTime.now().toUnixTimeStamp().toNanoseconds()}" })
            .filter("valid-data", { data => data.size > 5 })
            .map("finalize", { data => "FINAL-${data}" })
            .build()

        // 注册管道
        streamManager.registerPipeline("chained-demo", chainedPipeline)

        // 测试数据
        let testData = ArrayList<String>()
        testData.add("Hello")
        testData.add("World")
        testData.add("Cangjie")
        testData.add("Stream")
        testData.add("A")  // 这个会被过滤掉

        // 处理数据（简化实现）
        println("链式处理结果:")
        for (data in testData) {
            let element = SimpleStreamElement(data)
            match (chainedPipeline.process(element)) {
                case Some(result) =>
                    println("- ${result.getValue()}")
                case None =>
                    println("- 数据被过滤: ${data}")
            }
        }
    }

    /**
     * 演示并行流处理
     */
    public func demonstrateParallelStreams(): Unit {
        println("\n⚡ 演示3: 并行流处理")

        // 创建多个并行管道
        let pipeline1 = StreamBuilder("parallel-1")
            .map("branch-1", { data => "BRANCH1-${data}" })
            .build()

        let pipeline2 = StreamBuilder("parallel-2")
            .map("branch-2", { data => "BRANCH2-${data}" })
            .build()

        // 注册管道
        streamManager.registerPipeline("parallel-1", pipeline1)
        streamManager.registerPipeline("parallel-2", pipeline2)

        // 测试数据
        let sharedData = ArrayList<String>()
        for (i in 1..6) {
            sharedData.add("data-${i}")
        }

        // 并行处理（简化实现）
        println("并行处理结果:")
        println("分支1结果:")
        for (data in sharedData) {
            let element = SimpleStreamElement(data)
            match (pipeline1.process(element)) {
                case Some(result) =>
                    println("  - ${result.getValue()}")
                case None =>
                    println("  - 数据被过滤: ${data}")
            }
        }

        println("分支2结果:")
        for (data in sharedData) {
            let element = SimpleStreamElement(data)
            match (pipeline2.process(element)) {
                case Some(result) =>
                    println("  - ${result.getValue()}")
                case None =>
                    println("  - 数据被过滤: ${data}")
            }
        }
    }

    /**
     * 演示流性能测试
     */
    public func demonstrateStreamPerformance(): Unit {
        println("\n⚡ 演示4: 流性能测试")

        // 创建高性能流管道
        let perfPipeline = StreamBuilder("performance-test")
            .map("fast-transform", { data => data + "-FAST" })
            .filter("all-pass", { data => true })
            .build()

        // 性能测试（简化实现）
        let dataSize: Int64 = 1000
        var processedCount: Int64 = 0

        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        for (i in 0..dataSize) {
            let element = SimpleStreamElement("perf-data-${i}")
            match (perfPipeline.process(element)) {
                case Some(result) =>
                    processedCount += 1
                case None =>
                    println("数据被过滤")
            }
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        let throughput = if (duration > 0) { dataSize * 1000000000 / duration } else { 0 }

        println("性能测试结果:")
        println("处理数据量: ${dataSize}个元素")
        println("处理时间: ${duration}ns")
        println("吞吐量: ${throughput} elements/s")
        println("输出结果: ${processedCount}个元素")
    }

    /**
     * 演示流统计和监控
     */
    public func demonstrateStreamMonitoring(): Unit {
        println("\n📊 演示5: 流统计和监控")

        // 显示流管理器统计
        let allPipelines = streamManager.getAllPipelines()
        println("已注册的流管道:")
        for (pipelineName in allPipelines) {
            println("- ${pipelineName}")
        }

        // 显示详细统计
        println("\n流管理器统计:")
        println(streamManager.getStats())

        // 获取特定管道统计
        match (streamManager.getPipeline("basic-demo")) {
            case Some(pipeline) =>
                println("\n基本演示管道统计:")
                println("处理器数量: ${pipeline.getProcessorCount()}")
                println("处理总数: ${pipeline.getProcessedCount()}")
            case None =>
                println("未找到基本演示管道")
        }
    }

    /**
     * 运行所有演示
     */
    public func runAllDemonstrations(): Unit {
        println("🚀 仓颉流处理特性演示开始")
        println("=" * 60)

        demonstrateBasicStreams()
        demonstrateChainedStreams()
        demonstrateParallelStreams()
        demonstrateStreamPerformance()
        demonstrateStreamMonitoring()

        println("\n🎯 仓颉流处理特性演示完成")
        println("展示了基本流操作、链式处理、并行处理、性能测试和监控功能")
    }
}

/**
 * 仓颉流演示主程序
 */
main(): Unit {
    println("🚀 CActor 仓颉流演示开始")
    println("展示仓颉语言在流处理中的强大特性")

    // 创建演示实例
    let demo = CangjieStreamDemo()

    // 运行所有演示
    demo.runAllDemonstrations()

    println("\n💡 仓颉流处理特性总结:")
    println("- 函数式编程风格的流操作")
    println("- 链式调用和流畅接口设计")
    println("- 高性能的数据处理能力")
    println("- 完整的监控和统计功能")
    println("- 类型安全的流处理管道")

    println("\n🎯 仓颉流演示完成")
}
