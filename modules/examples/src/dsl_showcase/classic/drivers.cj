/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（经典写法）：驱动代码
 *
 * 三个场景的驱动逻辑与 dsl/drivers.cj 一一对应（只有输出前缀不同），
 * 单独成文件是为了让"消息定义 + Actor 定义"能与 DSL 写法做纯粹的行数对照（A1）。
 */
package cactor_examples.dsl_showcase.classic

import std.time.*
import cactor.api.CActor

/** 运行经典写法的 Hello World */
public func runClassicHello(): Unit {
    let system = CActor.system("dsl-showcase-classic-hello")
    let actor = system.actorOf({ => HelloActor() }, "hello")
    actor.tell(Greeting("你好，世界"))
    actor.tell(Farewell("再见"))
    sleep(Duration.millisecond * 200)
    system.terminate()
}

/** 运行经典写法的计数器 */
public func runClassicCounter(): Unit {
    let system = CActor.system("dsl-showcase-classic-counter")
    let counter = system.actorOf({ => CounterActor() }, "counter")
    counter.tell(Increment(2))
    counter.tell(Increment(3))
    counter.tell(GetCount())
    sleep(Duration.millisecond * 200)
    system.terminate()
}

/** 运行经典写法的请求-应答 */
public func runClassicAsk(): Unit {
    let system = CActor.system("dsl-showcase-classic-ask")
    let service = system.actorOf({ => ServiceActor() }, "service")
    let collector = system.actorOf({ => Collector() }, "collector")
    service.tell(Query("今天天气如何", collector))
    sleep(Duration.millisecond * 200)
    system.terminate()
}
