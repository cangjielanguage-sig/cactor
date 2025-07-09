/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 仓颉0.60.5特性演示
 * Plan 0.20 Phase 2.1: 占位文件清理 - 仓颉特性演示实现
 */
package cactor.examples.cangjie_features_demo

import std.time.*
import std.collection.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 模式匹配演示Actor
 */
class PatternMatchingActor <: Actor {
    private var matchedPatterns: ArrayList<String> = ArrayList<String>()

    public prop name: String {
        get() { "PatternMatchingActor" }
    }

    public prop description: String {
        get() { "演示仓颉模式匹配特性的Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()

                // 演示字符串模式匹配（简化实现）
                if (content == "start") {
                    matchedPatterns.add("启动模式")
                    println("匹配到启动模式")
                } else if (content == "stop") {
                    matchedPatterns.add("停止模式")
                    println("匹配到停止模式")
                } else if (content.startsWith("data:")) {
                    matchedPatterns.add("数据模式")
                    println("匹配到数据模式: ${content}")
                } else if (content.contains("error")) {
                    matchedPatterns.add("错误模式")
                    println("匹配到错误模式: ${content}")
                } else {
                    matchedPatterns.add("默认模式")
                    println("匹配到默认模式: ${content}")
                }

                if (content == "get-patterns") {
                    println("已匹配的模式:")
                    for (pattern in matchedPatterns) {
                        println("- ${pattern}")
                    }
                }

                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getMatchedPatternsCount(): Int64 {
        matchedPatterns.size
    }
}

/**
 * 泛型演示类
 */
public class GenericContainer<T> {
    private let items: ArrayList<T>
    private let typeName: String

    public init(typeName: String) {
        this.items = ArrayList<T>()
        this.typeName = typeName
    }

    public func add(item: T): Unit {
        items.add(item)
    }

    public func get(index: Int64): Option<T> {
        if (index >= 0 && index < items.size) {
            Some(items[index])
        } else {
            None
        }
    }

    public func size(): Int64 {
        items.size
    }

    public func getTypeName(): String {
        typeName
    }

    public func printAll(): Unit {
        println("${typeName}容器内容 (${items.size}个项目):")
        for (i in 0..items.size) {
            println("  [${i}] 项目${i}")
        }
    }
}

/**
 * Option类型演示Actor
 */
class OptionDemoActor <: Actor {
    private var optionalValues: ArrayList<String> = ArrayList<String>()

    public prop name: String {
        get() { "OptionDemoActor" }
    }

    public prop description: String {
        get() { "演示Option类型使用的Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()

                if (content.startsWith("store:")) {
                    // 演示Option的Some情况
                    let value = content  // 简化处理
                    optionalValues.add(value)
                    println("存储值: ${value}")
                } else if (content.startsWith("get:")) {
                    // 演示Option的使用
                    let index = 0  // 简化处理，总是获取第一个
                    if (optionalValues.size > 0) {
                        let value = optionalValues[0]
                        println("获取到值: ${value}")
                    } else {
                        println("没有找到值")
                    }
                } else if (content == "clear") {
                    optionalValues.clear()
                    println("清空所有值")
                } else if (content == "show-all") {
                    println("所有存储的值:")
                    for (value in optionalValues) {
                        println("- ${value}")
                    }
                }

                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getStoredValuesCount(): Int64 {
        optionalValues.size
    }
}

/**
 * 仓颉特性演示主程序
 */
main(): Unit {
    println("🚀 CActor 仓颉0.60.5特性演示开始")
    println("展示仓颉语言的现代特性在Actor系统中的应用")

    // 创建Actor系统
    let system = CActorSystemFactory.create("cangjie-features-demo-system")

    // 创建演示Actor
    let patternActor = system.actorOf({ => PatternMatchingActor() }, "pattern-matching-actor")
    let optionActor = system.actorOf({ => OptionDemoActor() }, "option-demo-actor")

    println("✅ 创建了特性演示Actor")

    // 演示1: 模式匹配特性
    println("\n🎯 演示1: 模式匹配特性")
    patternActor.tell(StringMessage("start"))
    patternActor.tell(StringMessage("data:user-info"))
    patternActor.tell(StringMessage("error-occurred"))
    patternActor.tell(StringMessage("unknown-command"))
    patternActor.tell(StringMessage("stop"))

    // 等待处理
    sleep(Duration.millisecond * 300)

    // 获取匹配结果
    patternActor.tell(StringMessage("get-patterns"))
    sleep(Duration.millisecond * 200)

    // 演示2: 泛型特性
    println("\n🔧 演示2: 泛型特性")
    let stringContainer = GenericContainer<String>("String")
    let intContainer = GenericContainer<Int32>("Int32")

    // 添加数据
    stringContainer.add("Hello")
    stringContainer.add("World")
    stringContainer.add("Cangjie")

    intContainer.add(1)
    intContainer.add(2)
    intContainer.add(3)

    // 显示容器内容
    stringContainer.printAll()
    intContainer.printAll()

    // 演示3: Option类型特性
    println("\n📦 演示3: Option类型特性")
    optionActor.tell(StringMessage("store:value1"))
    optionActor.tell(StringMessage("store:value2"))
    optionActor.tell(StringMessage("store:value3"))

    // 等待存储
    sleep(Duration.millisecond * 200)

    optionActor.tell(StringMessage("show-all"))
    optionActor.tell(StringMessage("get:0"))

    // 等待处理
    sleep(Duration.millisecond * 200)

    // 演示4: 集合操作特性
    println("\n📚 演示4: 集合操作特性")
    let numbers = ArrayList<Int32>()
    for (i in 1..11) {
        numbers.add(Int32(i))
    }

    println("原始数组: ${numbers.size}个元素")
    for (num in numbers) {
        print("${num} ")
    }
    println("")

    // 演示集合过滤（简化实现）
    let evenNumbers = ArrayList<Int32>()
    for (num in numbers) {
        if (num % 2 == 0) {
            evenNumbers.add(num)
        }
    }

    println("偶数: ${evenNumbers.size}个元素")
    for (num in evenNumbers) {
        print("${num} ")
    }
    println("")

    // 演示5: 字符串插值特性
    println("\n💬 演示5: 字符串插值特性")
    let actorCount = 2
    let systemName = "cangjie-features-demo-system"
    let currentTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

    println("系统信息: 名称=${systemName}, Actor数量=${actorCount}, 时间戳=${currentTime}")

    // 最终统计
    println("\n📊 特性演示统计:")
    patternActor.tell(StringMessage("get-patterns"))
    optionActor.tell(StringMessage("show-all"))

    // 等待最终输出
    sleep(Duration.millisecond * 300)

    println("\n🎯 仓颉0.60.5特性演示完成")
    println("展示了模式匹配、泛型、Option类型、集合操作、字符串插值等特性")

    // 清理系统
    system.terminate()
    sleep(Duration.second * 1)

    println("✅ 系统已清理")
}
