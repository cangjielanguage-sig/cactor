/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（DSL 写法）：驱动代码
 *
 * 与 classic/drivers.cj 一一对应（只有输出前缀与协议类型不同），
 * 单独成文件是为了让"消息定义 + Actor 定义"能与经典写法做纯粹的行数对照（A1）。
 */
package cactor_examples.dsl_showcase.dsl

import std.time.*
import cactor.api.CActor

/** 运行 DSL 写法的 Hello World */
public func runDslHello(): Unit {
    let system = CActor.system("dsl-showcase-dsl-hello")
    let actor = system.actorOf({ => HelloActor() }, "hello")
    actor.tell(GreetingMsg.Hi("你好，世界"))
    actor.tell(GreetingMsg.Bye("再见"))
    sleep(Duration.millisecond * 200)
    system.terminate()
}

/** 运行 DSL 写法的计数器 */
public func runDslCounter(): Unit {
    let system = CActor.system("dsl-showcase-dsl-counter")
    let counter = system.actorOf({ => CounterActor() }, "counter")
    counter.tell(CounterMsg.Increment(2))
    counter.tell(CounterMsg.Increment(3))
    counter.tell(CounterMsg.GetCount)
    sleep(Duration.millisecond * 200)
    system.terminate()
}

/** 运行 DSL 写法的请求-应答 */
public func runDslAsk(): Unit {
    let system = CActor.system("dsl-showcase-dsl-ask")
    let service = system.actorOf({ => ServiceActor() }, "service")
    let collector = system.actorOf({ => Collector() }, "collector")
    service.tell(QueryMsg.Ask("今天天气如何", collector))
    sleep(Duration.millisecond * 200)
    system.terminate()
}
