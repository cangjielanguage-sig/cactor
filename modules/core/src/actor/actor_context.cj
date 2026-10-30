/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Core - Actor 上下文
 *
 * 对标 Akka 经典 API 的 ActorContext：self / sender / 子 Actor / 死亡监视 / 行为切换 /
 * 接收超时 / 定时器 / Stash。
 *
 * 除 sender() 外，其余方法都有默认实现（抛出 UnsupportedContextException 或返回空值），
 * 因此测试桩等自定义上下文只需实现 sender() 即可编译；运行时的 Actor Cell 提供完整实现。
 *
 * 本接口定义在 cactor_core.actor 包中（需要引用 ActorRef / Props，放在 context 包会成环），
 * cactor_core.context 包重导出它，原有的 `import cactor_core.context.{ActorContext}` 不受影响。
 */
package cactor_core.actor

import std.time.*
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.logging.{Logger, Logging}

/**
 * 行为函数：become 切换后用它代替 Actor.receive 处理消息
 */
public type Receive = (Message, ActorContext) -> MessageResult

/**
 * 当前上下文不支持某项能力时抛出
 */
public class UnsupportedContextException <: Exception {
    public init(operation: String) {
        super("ActorContext.${operation} 在当前上下文中不可用（需要运行在 Actor Cell 中）")
    }
}

public interface ActorContext {
    /**
     * 当前消息发送方的路径（兼容旧接口）
     */
    func sender(): Option<String>

    /** 当前 Actor 自身的引用 */
    prop self: ActorRef {
        get() {
            throw UnsupportedContextException("self")
        }
    }

    /** 当前消息发送方的引用；发送方不是 Actor 或未提供时为 None */
    func senderRef(): Option<ActorRef> {
        None<ActorRef>
    }

    /** 父 Actor；顶层 Actor 为 None */
    func parent(): Option<ActorRef> {
        None<ActorRef>
    }

    /** 回复当前消息的发送方（以 self 作为发送方）；没有发送方时计为死信 */
    func reply(message: Message): Unit {
        throw UnsupportedContextException("reply")
    }

    /** 以 self 作为发送方向目标发送消息，目标可以用 senderRef()/reply 回复 */
    func send(target: ActorRef, message: Message): Unit {
        target.tell(message)
    }

    // ---------- 子 Actor ----------

    /** 创建子 Actor，名称在兄弟之间唯一（spawn 是仓颉关键字，沿用 Akka 经典 API 的 actorOf） */
    func actorOf(props: Props<Actor>, name: String): ActorRef {
        throw UnsupportedContextException("actorOf")
    }

    func actorOf(creator: () -> Actor, name: String): ActorRef {
        actorOf(PropsFactory.create<Actor>(creator), name)
    }

    /** 创建自动命名的子 Actor */
    func actorOf(creator: () -> Actor): ActorRef {
        throw UnsupportedContextException("actorOf")
    }

    /** 当前存活的子 Actor */
    func children(): Array<ActorRef> {
        Array<ActorRef>()
    }

    func child(name: String): Option<ActorRef> {
        None<ActorRef>
    }

    /** 停止自身或某个子 Actor（立即生效，会先停止其全部子 Actor） */
    func stop(ref: ActorRef): Unit {
        throw UnsupportedContextException("stop")
    }

    // ---------- 死亡监视 ----------

    /** 监视目标；目标停止后，本 Actor 会收到 Terminated(target)。监视已停止的目标会立即收到 */
    func watch(ref: ActorRef): Unit {
        throw UnsupportedContextException("watch")
    }

    /** 取消监视；之后不会再收到该目标的 Terminated */
    func unwatch(ref: ActorRef): Unit {
        throw UnsupportedContextException("unwatch")
    }

    // ---------- 行为切换 ----------

    /** 切换消息处理函数；discardOld = false 时压栈，可用 unbecome 恢复 */
    func become(behavior: Receive, discardOld: Bool): Unit {
        throw UnsupportedContextException("become")
    }

    func become(behavior: Receive): Unit {
        become(behavior, true)
    }

    /** 恢复上一个处理函数；栈空时恢复为 Actor.receive */
    func unbecome(): Unit {
        throw UnsupportedContextException("unbecome")
    }

    // ---------- 接收超时 ----------

    /** 空闲超过 timeout 没有收到消息时，向自身投递 ReceiveTimeout（空闲期间会重复投递） */
    func setReceiveTimeout(timeout: Duration): Unit {
        throw UnsupportedContextException("setReceiveTimeout")
    }

    func cancelReceiveTimeout(): Unit {
        throw UnsupportedContextException("cancelReceiveTimeout")
    }

    // ---------- 定时器与 Stash ----------

    /** 本 Actor 的定时器；Actor 停止或重启时自动取消全部定时器 */
    prop timers: TimerScheduler {
        get() {
            throw UnsupportedContextException("timers")
        }
    }

    /** 暂存当前消息，稍后用 unstashAll 放回邮箱队首 */
    func stash(): Unit {
        throw UnsupportedContextException("stash")
    }

    /** 把全部暂存消息按原顺序放回，先于邮箱中的其他消息处理 */
    func unstashAll(): Unit {
        throw UnsupportedContextException("unstashAll")
    }

    // ---------- 日志 ----------

    /** 所在的 Actor 系统（创建顶层 Actor、按路径查找、终止系统等） */
    func system(): ActorSystemView {
        throw UnsupportedContextException("system")
    }

    /** 以 Actor 路径为来源标签的日志记录器 */
    func log(): Logger {
        let source = try {
            self.path.getPath()
        } catch (_: Exception) {
            "actor"
        }
        Logging.logger(source)
    }

    // ---------- 查找 ----------

    /** 按路径查找本系统内存活的 Actor，例如 "/user/parent/child" */
    func lookup(path: String): Option<ActorRef> {
        None<ActorRef>
    }
}

/**
 * Actor 级定时器（对标 Akka TimerScheduler）
 *
 * 同一 key 只保留一个定时器：再次 start 会取消旧的；已取消的定时器即使消息已在途也不会被投递。
 */
public interface TimerScheduler {
    /** delay 之后投递一次 */
    func startSingleTimer(key: String, message: Message, delay: Duration): Unit

    /** 每次投递之间至少间隔 delay（首次在 delay 之后） */
    func startTimerWithFixedDelay(key: String, message: Message, delay: Duration): Unit

    /** 按固定频率投递（首次在 interval 之后） */
    func startTimerAtFixedRate(key: String, message: Message, interval: Duration): Unit

    func isTimerActive(key: String): Bool

    func cancel(key: String): Unit

    func cancelAll(): Unit
}
