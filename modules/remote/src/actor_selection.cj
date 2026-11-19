/*
 * CActor Remote - 远程 ActorSelection（对标 Akka ActorSelection / protoactor 的按路径投递）
 *
 * 与 RemoteActorRef 的区别：ref 要求调用方**已经**知道目标名称；Selection 允许按通配模式
 * （`worker-*`、`worker-?`）寻址一组远程 Actor——这正是"不知道目标叫什么，只知道它在哪类角色里"
 * 的场景（节点上的 worker 池、临时登录用户、某前缀的分片实体）。
 *
 * 两条路径：
 * - `tell`：把目标名写成 `$sel:<pattern>` 直接发一帧，由**对端**在本地展开通配并逐个投递。
 *   零额外往返、不阻塞调用方，保持"tell 只入队"的语义；通配匹配不到时计入对端死信。
 * - `resolveNames` / `resolveOne`：向对端 `$sys` 发 SELECT 请求，取回名称列表。
 *   用于需要先知道"到底有谁"的场景；结果排序后返回。
 *
 * 命名空间只包含对端通过 `exposeActor` 暴露的名称；以 `$` 开头的内部入口（$sys、回复入口、
 * ask 临时入口）永远不参与匹配，避免选择器打到内部协议上。
 */
package cactor_remote

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.message.{Message, StringMessage}
import cactor_core.typed.TypedPromise
import cactor_foundation.network.NetworkAddress

/** 通配选择的目标名前缀：`$sel:worker-*` 由对端展开后各投递一次 */
public const SELECTION_PREFIX: String = "$sel:"

/** 选择解析请求 / 回复的序号（每进程单调递增，避免临时回复入口重名） */
let selectionReplySeq: AtomicInt64 = AtomicInt64(0)

/**
 * 通配匹配：支持 `*`（任意长度）与 `?`（单字节）。
 * 按 UTF-8 字节匹配——被匹配的是暴露名称，两侧编码一致，字节级判断不会误判。
 */
public func selectionGlobMatch(pattern: String, text: String): Bool {
    let p = pattern.toArray()
    let t = text.toArray()
    let star = UInt8(42)
    let question = UInt8(63)
    var pi = 0
    var ti = 0
    var starAt = -1
    var matchAt = 0
    while (ti < t.size) {
        if (pi < p.size && (p[pi] == t[ti] || p[pi] == question)) {
            pi++
            ti++
        } else if (pi < p.size && p[pi] == star) {
            starAt = pi
            matchAt = ti
            pi++
        } else if (starAt >= 0) {
            pi = starAt + 1
            matchAt++
            ti = matchAt
        } else {
            return false
        }
    }
    while (pi < p.size && p[pi] == star) {
        pi++
    }
    pi == p.size
}

/**
 * 远程 Actor 选择器（不可变；由 SimpleRemoteManager.actorSelection 创建）。
 *
 * 路径写法（`/user/` 之后是通配模式）：
 * - `cactor://host:port/user/worker-*`
 * - `/user/worker-*`
 * - `worker-*`（省略前缀时直接当模式）
 */
public class ActorSelection {
    private let manager: SimpleRemoteManager
    private let address: NetworkAddress
    private let pattern: String

    init(manager: SimpleRemoteManager, address: NetworkAddress, pattern: String) {
        this.manager = manager
        this.address = address
        this.pattern = pattern
    }

    public prop targetAddress: NetworkAddress {
        get() { address }
    }

    /** 通配模式（不含 `/user/` 前缀） */
    public prop selectionPattern: String {
        get() { pattern }
    }

    /** 人类可读路径，如 `cactor://127.0.0.1:9000/user/worker-*` */
    public func path(): String {
        "cactor://${address.toString()}/user/${pattern}"
    }

    /**
     * 向所有匹配的远程 Actor 投递（对端展开通配，一次网络帧）。
     * 与 tell 一致：不等待回复、不抛业务异常；对端不可达时由传输层丢弃并计数。
     */
    public func tell(message: Message): Unit {
        manager.sendMessage(address, "${SELECTION_PREFIX}${pattern}", message)
    }

    /** 带发送方投递（对端 Actor 的 context.reply 会经网络回到 sender） */
    public func tell(message: Message, senderPath: Option<String>): Unit {
        manager.sendMessage(address, "${SELECTION_PREFIX}${pattern}", message, senderPath)
    }

    /**
     * 解析当前匹配的名称（排序后返回）。超时抛 TypedAskTimeoutException。
     */
    public func resolveNames(timeout!: Duration = Duration.second * 2): Array<String> {
        let seq = selectionReplySeq.fetchAdd(1)
        let replyName = "$sel-reply-${seq}"
        let receiver = manager.getReceiver()
        let promise = TypedPromise<Array<String>>()
        receiver.registerHandler(replyName, { m: Message =>
            if (let Some(s) <- (m as StringMessage)) {
                let text = s.getContent()
                if (text.startsWith("SELECT-REPLY|")) {
                    let rest = text["SELECT-REPLY|".size..]
                    match (rest.indexOf("|")) {
                        case Some(i) =>
                            let payload = rest[(i + 1)..]
                            let names = if (payload.isEmpty()) {
                                Array<String>()
                            } else {
                                payload.split("\n")
                            }
                            promise.trySuccess(names)
                        case None => ()
                    }
                }
            }
        })
        try {
            let local = manager.getLocalAddress()
            manager.sendControl(address, "$sys",
                StringMessage("SELECT|${replyName}|${pattern}|${local.toString()}"))
            promise.get(timeout)
        } finally {
            receiver.unregisterHandler(replyName)
        }
    }

    /**
     * 解析唯一匹配；0 个或多个都抛 IllegalStateException（与 Akka resolveOne 语义一致：
     * 选择器必须唯一确定一个目标，否则调用方应先 resolveNames 缩小范围）。
     */
    public func resolveOne(timeout!: Duration = Duration.second * 2): RemoteActorRef {
        let names = resolveNames(timeout: timeout)
        if (names.size != 1) {
            throw IllegalStateException(
                "选择器 ${path()} 匹配到 ${names.size} 个 Actor，resolveOne 要求恰好 1 个")
        }
        manager.remoteActorOf(address, names[0])
    }
}

extend SimpleRemoteManager {
    /**
     * 创建远程选择器。`path` 可以是完整路径、`/user/<pattern>` 或裸 pattern。
     * 模式不能包含 `|`（控制协议字段分隔符）或换行。
     */
    public func actorSelection(targetAddress: NetworkAddress, path: String): ActorSelection {
        var rest = path
        let scheme = "cactor://"
        if (rest.startsWith(scheme)) {
            let afterScheme = rest[scheme.size..]
            match (afterScheme.indexOf("/")) {
                case Some(i) => rest = afterScheme[i..]
                case None => throw IllegalArgumentException("选择路径缺少 /user/ 段: ${path}")
            }
        }
        let userPrefix = "/user/"
        if (rest.startsWith(userPrefix)) {
            rest = rest[userPrefix.size..]
        }
        if (rest.isEmpty()) {
            throw IllegalArgumentException("选择路径的模式不能为空: ${path}")
        }
        if (rest.contains("|") || rest.contains("\n") || rest.contains("\r")) {
            throw IllegalArgumentException("选择模式不能包含 '|' 或换行: ${path}")
        }
        ActorSelection(this, targetAddress, rest)
    }
}
