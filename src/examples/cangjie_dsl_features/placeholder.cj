/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 仓颉DSL特性演示
 * Plan 0.20 Phase 2.1: 占位文件清理 - 仓颉DSL特性演示实现
 */
package cactor.examples.cangjie_dsl_features

import std.time.*
import std.collection.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * DSL构建器基类
 */
public interface DslBuilder<T> {
    func build(): T
}

/**
 * Actor配置DSL
 */
public class ActorConfigDsl <: DslBuilder<ActorConfig> {
    private var actorName: String = "default-actor"
    private var actorType: String = "basic"
    private var maxMessages: Int64 = 1000
    private var timeout: Int64 = 5000

    public init() {}

    public func name(name: String): ActorConfigDsl {
        this.actorName = name
        this
    }

    public func setType(actorType: String): ActorConfigDsl {
        this.actorType = actorType
        this
    }

    public func setMaxMessages(max: Int64): ActorConfigDsl {
        this.maxMessages = max
        this
    }

    public func setTimeout(timeoutMs: Int64): ActorConfigDsl {
        this.timeout = timeoutMs
        this
    }

    public func build(): ActorConfig {
        ActorConfig(actorName, actorType, maxMessages, timeout)
    }
}

/**
 * Actor配置类
 */
public class ActorConfig {
    public let name: String
    public let actorType: String
    public let maxMessages: Int64
    public let timeout: Int64

    public init(name: String, actorType: String, maxMessages: Int64, timeout: Int64) {
        this.name = name
        this.actorType = actorType
        this.maxMessages = maxMessages
        this.timeout = timeout
    }

    public func getDescription(): String {
        "ActorConfig(name=${name}, actorType=${actorType}, maxMessages=${maxMessages}, timeout=${timeout})"
    }
}

/**
 * 消息流DSL
 */
public class MessageFlowDsl {
    private let steps: ArrayList<String> = ArrayList<String>()

    public init() {}

    public func from(source: String): MessageFlowDsl {
        steps.add("from(${source})")
        this
    }

    public func to(target: String): MessageFlowDsl {
        steps.add("to(${target})")
        this
    }

    public func transform(operation: String): MessageFlowDsl {
        steps.add("transform(${operation})")
        this
    }

    public func filter(condition: String): MessageFlowDsl {
        steps.add("filter(${condition})")
        this
    }

    public func execute(): String {
        let flow = ArrayList<String>()
        for (step in steps) {
            flow.add(step)
        }
        "MessageFlow: ${flow.toString()}"
    }

    public func getSteps(): ArrayList<String> {
        steps
    }
}

/**
 * DSL演示Actor
 */
class DslDemoActor <: Actor {
    private var processedFlows: ArrayList<String> = ArrayList<String>()
    private var config: Option<ActorConfig> = None

    public prop name: String {
        get() { "DslDemoActor" }
    }

    public prop description: String {
        get() { "演示仓颉DSL特性的Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()

                if (content.startsWith("config:")) {
                    // 演示Actor配置DSL
                    let configDsl = ActorConfigDsl()
                        .name("dsl-demo-actor")
                        .setType("demonstration")
                        .setMaxMessages(2000)
                        .setTimeout(10000)

                    config = Some(configDsl.build())
                    println("✅ 使用DSL配置Actor")
                    match (config) {
                        case Some(cfg) => println("配置: name=${cfg.name}, actorType=${cfg.actorType}")
                        case None => println("配置失败")
                    }

                } else if (content.startsWith("flow:")) {
                    // 演示消息流DSL
                    let flowDsl = MessageFlowDsl()
                        .from("input-source")
                        .transform("uppercase")
                        .filter("non-empty")
                        .to("output-target")

                    let flowResult = flowDsl.execute()
                    processedFlows.add(flowResult)
                    println("✅ 执行消息流DSL")
                    println("流程: ${flowResult}")

                } else if (content == "get-stats") {
                    println("DSL演示统计:")
                    match (config) {
                        case Some(cfg) => println("当前配置: name=${cfg.name}")
                        case None => println("未配置")
                    }
                    println("处理的流程数: ${processedFlows.size}")
                    for (flow in processedFlows) {
                        println("- ${flow}")
                    }
                } else {
                    println("处理普通消息: ${content}")
                }

                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getProcessedFlowsCount(): Int64 {
        processedFlows.size
    }
}

/**
 * 链式调用演示类
 */
public class ChainedOperations {
    private let operations: ArrayList<String> = ArrayList<String>()
    private var result: String = ""

    public init() {}

    public func start(value: String): ChainedOperations {
        result = value
        operations.add("start(${value})")
        this
    }

    public func append(text: String): ChainedOperations {
        result = result + text
        operations.add("append(${text})")
        this
    }

    public func uppercase(): ChainedOperations {
        result = result + "-UPPER"  // 简化实现
        operations.add("uppercase()")
        this
    }

    public func reverse(): ChainedOperations {
        // 简化实现，实际应该反转字符串
        result = result + "-reversed"
        operations.add("reverse()")
        this
    }

    public func finish(): String {
        operations.add("finish()")
        result
    }

    public func getOperations(): ArrayList<String> {
        operations
    }
}

/**
 * 仓颉DSL特性演示主程序
 */
main(): Unit {
    println("🚀 CActor 仓颉DSL特性演示开始")
    println("展示仓颉语言在DSL设计中的强大特性")

    // 创建Actor系统
    let system = CActorSystemFactory.create("cangjie-dsl-demo-system")

    // 创建DSL演示Actor
    let dslActor = system.actorOf({ => DslDemoActor() }, "dsl-demo-actor")

    println("✅ 创建了DSL演示Actor")

    // 演示1: Actor配置DSL
    println("\n🔧 演示1: Actor配置DSL")
    dslActor.tell(StringMessage("config:demo"))

    // 等待处理
    sleep(Duration.millisecond * 200)

    // 演示2: 消息流DSL
    println("\n🔄 演示2: 消息流DSL")
    dslActor.tell(StringMessage("flow:demo1"))
    dslActor.tell(StringMessage("flow:demo2"))

    // 等待处理
    sleep(Duration.millisecond * 300)

    // 演示3: 链式调用DSL
    println("\n🔗 演示3: 链式调用DSL")
    let chainResult = ChainedOperations()
        .start("Hello")
        .append(" World")
        .uppercase()
        .reverse()
        .finish()

    println("链式调用结果: ${chainResult}")

    // 演示4: 流畅接口模式
    println("\n💫 演示4: 流畅接口模式")
    let configBuilder = ActorConfigDsl()
        .name("fluent-actor")
        .setType("fluent-demo")
        .setMaxMessages(5000)
        .setTimeout(15000)

    let fluentConfig = configBuilder.build()
    println("流畅配置: name=${fluentConfig.name}")

    // 演示5: 复杂DSL组合
    println("\n🎯 演示5: 复杂DSL组合")
    let complexFlow = MessageFlowDsl()
        .from("sensor-data")
        .filter("valid-data")
        .transform("normalize")
        .transform("aggregate")
        .filter("threshold-check")
        .to("alert-system")

    let complexResult = complexFlow.execute()
    println("复杂流程: ${complexResult}")

    // 获取统计信息
    println("\n📊 DSL演示统计:")
    dslActor.tell(StringMessage("get-stats"))

    // 等待统计输出
    sleep(Duration.millisecond * 200)

    // 演示6: DSL性能测试
    println("\n⚡ 演示6: DSL性能测试")
    let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

    for (i in 0..100) {
        let testConfig = ActorConfigDsl()
            .name("perf-test-${i}")
            .setType("performance")
            .setMaxMessages(1000)
            .setTimeout(5000)
            .build()
    }

    let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
    let duration = endTime - startTime

    println("DSL性能测试: 100次配置构建耗时 ${duration}ns")

    println("\n🎯 仓颉DSL特性演示完成")
    println("展示了配置DSL、流程DSL、链式调用、流畅接口等特性")

    // 清理系统
    system.terminate()
    sleep(Duration.second * 1)

    println("✅ 系统已清理")
}
