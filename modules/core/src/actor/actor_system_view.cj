/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Core - Actor 中可见的系统能力（对标 Akka 经典 API 的 context.system）
 *
 * core 不能依赖 cactor（顶层 API 包），因此用接口表达，由运行时实现：
 * 在 Actor 中通过 context.system() 取得，可以创建 / 停止顶层 Actor、按路径查找、查看死信、终止系统。
 */
package cactor_core.actor

public interface ActorSystemView {
    /** 系统名称 */
    prop name: String

    /** 按路径查找存活的 Actor（包括子 Actor），例如 "/user/parent/child" */
    func lookup(path: String): Option<ActorRef>

    /** 创建顶层 Actor（路径 /user/<name>） */
    func actorOf(props: Props<Actor>, name: String): ActorRef

    func actorOf(creator: () -> Actor, name: String): ActorRef {
        actorOf(PropsFactory.create<Actor>(creator), name)
    }

    /** 停止顶层 Actor */
    func stop(ref: ActorRef): Unit

    /** 系统内累计的死信数 */
    func deadLetterCount(): Int64

    /** 终止系统；在 Actor 中调用不会卡住（不等待自身停止） */
    func terminate(): Unit

    func isTerminated(): Bool
}
