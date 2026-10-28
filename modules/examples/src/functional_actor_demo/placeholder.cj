/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 函数式Actor示例
 * Plan 0.20 Phase 2.1: 占位文件清理 - 函数式Actor演示实现
 */
package cactor_examples.functional_actor_demo

import std.time.*
import std.collection.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 函数式计算器Actor
 * 展示函数式编程风格的Actor实现
 */
class FunctionalCalculatorActor <: Actor {
    private var operations: ArrayList<String> = ArrayList<String>()

    public prop name: String {
        get() { "FunctionalCalculatorActor" }
    }

    public prop description: String {
        get() { "函数式计算器Actor，展示函数式编程风格" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()

                if (content.startsWith("add:")) {
                    operations.add("加法操作")
                    println("执行加法操作")
                } else if (content.startsWith("multiply:")) {
                    operations.add("乘法操作")
                    println("执行乘法操作")
                } else if (content.startsWith("compose:")) {
                    operations.add("组合操作")
                    println("执行函数组合")
                } else if (content == "get-operations") {
                    println("操作历史: ${operations.size}个操作")
                    for (op in operations) {
                        println("- ${op}")
                    }
                }

                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getOperationsCount(): Int64 {
        operations.size
    }
}

/**
 * 函数式数据处理Actor
 */
class FunctionalDataProcessorActor <: Actor {
    private var processedItems: Int64 = 0

    public prop name: String {
        get() { "FunctionalDataProcessorActor" }
    }

    public prop description: String {
        get() { "函数式数据处理Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()

                if (content.startsWith("map:")) {
                    processedItems += 1
                    println("映射操作: 处理第${processedItems}个项目")
                } else if (content.startsWith("filter:")) {
                    processedItems += 1
                    println("过滤操作: 处理第${processedItems}个项目")
                } else if (content.startsWith("reduce:")) {
                    processedItems += 1
                    println("归约操作: 处理第${processedItems}个项目")
                } else if (content == "get-stats") {
                    println("数据处理统计: 已处理${processedItems}个项目")
                }

                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getProcessedItems(): Int64 {
        processedItems
    }
}

/**
 * 函数式Actor演示主程序
 */
main(): Unit {
    println("🚀 CActor 函数式Actor演示开始")

    // 创建Actor系统
    let system = CActorSystemFactory.create("functional-actor-demo-system")

    // 创建函数式Actor
    let calculator = system.actorOf({ => FunctionalCalculatorActor() }, "functional-calculator")
    let processor = system.actorOf({ => FunctionalDataProcessorActor() }, "functional-processor")

    println("✅ 创建了函数式Actor")

    // 演示函数式计算
    println("\n📊 演示函数式计算操作:")
    calculator.tell(StringMessage("add:10,20"))
    calculator.tell(StringMessage("multiply:5,6"))
    calculator.tell(StringMessage("compose:add,multiply"))

    // 等待处理
    sleep(Duration.millisecond * 200)

    // 获取操作历史
    calculator.tell(StringMessage("get-operations"))

    // 等待输出
    sleep(Duration.millisecond * 200)

    // 演示函数式数据处理
    println("\n🔄 演示函数式数据处理:")
    processor.tell(StringMessage("map:transform"))
    processor.tell(StringMessage("filter:condition"))
    processor.tell(StringMessage("reduce:aggregate"))

    // 等待处理
    sleep(Duration.millisecond * 200)

    // 获取处理统计
    processor.tell(StringMessage("get-stats"))

    // 等待输出
    sleep(Duration.millisecond * 200)

    // 演示函数组合
    println("\n🔗 演示函数组合:")
    for (i in 0..5) {
        calculator.tell(StringMessage("compose:operation-${i}"))
        processor.tell(StringMessage("map:item-${i}"))
        sleep(Duration.millisecond * 50)
    }

    // 等待处理
    sleep(Duration.millisecond * 300)

    // 最终统计
    calculator.tell(StringMessage("get-operations"))
    processor.tell(StringMessage("get-stats"))

    // 等待最终输出
    sleep(Duration.millisecond * 300)

    println("\n🎯 函数式Actor演示完成")

    // 清理系统
    system.terminate()
    sleep(Duration.second * 1)

    println("✅ 系统已清理")
}
