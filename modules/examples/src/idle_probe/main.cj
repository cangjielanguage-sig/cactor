/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 空闲系统能耗探针
 *
 * 创建系统与少量 Actor 后空闲保持 N 秒退出；配合 shell 的 time 观测空闲期间的进程 CPU，
 * 用于验证调度器空闲退避（docs/plan/perf20261101.md P3）。
 * 运行：./target/release/bin/cactor_examples.idle_probe [秒，默认 3]
 */
package cactor_examples.idle_probe

import std.sync.*
import std.time.*
import std.convert.*
import std.collection.*
import std.env.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}

class Idle <: Actor {
    public prop name: String { get() { "idle" } }
    public prop description: String { get() { "空闲占位" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(args: Array<String>): Int64 {
    let seconds = if (args.size > 0) { Int64.parse(args[0]) } else { 3 }
    let threads = if (let Some(v) <- getVariable("CACTOR_DISPATCHER_THREADS")) { v } else { "默认" }
    let system = CActorSystemFactory.create("idle-probe")
    let keep = ArrayList<ActorRef>()
    for (i in 0..64) {
        keep.add(system.actorOf({ => Idle() }, "idle-${i}"))
    }
    println("空闲 ${seconds} 秒（${keep.size} 个 Actor，调度线程 ${threads}）")
    sleep(Duration.second * seconds)
    system.terminate()
    return 0
}
