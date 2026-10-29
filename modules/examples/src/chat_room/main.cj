/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 聊天室示例（Akka Typed 官方入门示例 “Chat Room” 的仓颉版本）
 *
 * 演示：
 * - 函数式行为：聊天室的会话列表通过“返回新行为”保存，没有可变字段
 * - 每个客户端一个子 Actor（会话），会话持有客户端引用和聊天室引用
 * - 协议用 enum 表达，match 穷尽检查保证每种消息都被处理
 * - 客户端（Gabbler）收到自己发出的消息后停止；主 Actor 监视全部客户端，全部停止后结束
 *
 * 程序自检：每个客户端都必须收到全部广播，重复的昵称必须被拒绝，否则以退出码 1 结束。
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.chat_room
 */
package cactor_examples.chat_room

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

// ======================= 协议 =======================

/** 聊天室的命令 */
enum RoomCommand {
    | GetSession(String, TypedActorRef<SessionEvent>)
    | PublishSessionMessage(String, String)
}

/** 发给客户端的事件 */
enum SessionEvent {
    | SessionGranted(TypedActorRef<SessionCommand>)
    | SessionDenied(String)
    | MessagePosted(String, String)
}

/** 会话的命令：客户端发言，或聊天室通知客户端 */
enum SessionCommand {
    | PostMessage(String)
    | NotifyClient(String, String)
}

// ======================= 聊天室 =======================

func chatRoom(): TypedBehavior<RoomCommand> {
    TypedBehaviors.setup<RoomCommand>({ ctx => room(ctx.self, []) })
}

func room(self: TypedActorRef<RoomCommand>, sessions: Array<(String, TypedActorRef<SessionCommand>)>): TypedBehavior<RoomCommand> {
    TypedBehaviors.receive<RoomCommand>({ ctx, cmd =>
        match (cmd) {
            case RoomCommand.GetSession(screenName, client) =>
                for ((n, _) in sessions) {
                    if (n == screenName) {
                        client.tell(SessionEvent.SessionDenied("昵称 ${screenName} 已被使用"))
                        return TypedBehaviors.same<RoomCommand>()
                    }
                }
                let ses = ctx.actorOf<SessionCommand>(session(self, screenName, client), "session-${screenName}")
                client.tell(SessionEvent.SessionGranted(ses))
                room(self, sessions.concat([(screenName, ses)]))
            case RoomCommand.PublishSessionMessage(screenName, message) =>
                for ((_, s) in sessions) {
                    s.tell(SessionCommand.NotifyClient(screenName, message))
                }
                TypedBehaviors.same<RoomCommand>()
        }
    })
}

func session(room: TypedActorRef<RoomCommand>, screenName: String, client: TypedActorRef<SessionEvent>): TypedBehavior<SessionCommand> {
    TypedBehaviors.receiveMessage<SessionCommand>({ cmd =>
        match (cmd) {
            // 发言先发到聊天室，由聊天室广播给所有会话
            case SessionCommand.PostMessage(m) => room.tell(RoomCommand.PublishSessionMessage(screenName, m))
            case SessionCommand.NotifyClient(from, m) => client.tell(SessionEvent.MessagePosted(from, m))
        }
        TypedBehaviors.same<SessionCommand>()
    })
}

// ======================= 客户端 =======================

class Transcript {
    let lock = Mutex()
    let lines = ArrayList<String>()
    var denied: Int64 = 0

    func add(line: String): Unit {
        synchronized(lock) {
            lines.add(line)
        }
    }

    func count(prefix: String): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for (l in lines) {
                if (l.startsWith(prefix)) {
                    n++
                }
            }
            n
        }
    }
}

/** 加入后说一句话，收到全部 expected 条广播后停止 */
func gabbler(name: String, expected: Int64, transcript: Transcript): TypedBehavior<SessionEvent> {
    TypedBehaviors.setup<SessionEvent>({ _ =>
        let received = ArrayList<String>()
        TypedBehaviors.receive<SessionEvent>({ ctx, ev =>
            match (ev) {
                case SessionEvent.SessionGranted(handle) =>
                    handle.tell(SessionCommand.PostMessage("${name} 说：大家好"))
                    TypedBehaviors.same<SessionEvent>()
                case SessionEvent.SessionDenied(reason) =>
                    ctx.log().info("被拒绝：${reason}")
                    synchronized(transcript.lock) {
                        transcript.denied++
                    }
                    TypedBehaviors.stopped<SessionEvent>()
                case SessionEvent.MessagePosted(from, m) =>
                    received.add(m)
                    transcript.add("${name}<-${from}: ${m}")
                    if (received.size == expected) {
                        TypedBehaviors.stopped<SessionEvent>()
                    } else {
                        TypedBehaviors.same<SessionEvent>()
                    }
            }
        })
    })
}

// ======================= 主程序 =======================

enum MainCmd {
    | Start
}

main(): Int64 {
    let names = ["alice", "bob", "carol", "dave", "erin"]
    let transcript = Transcript()
    let finished = AtomicBool(false)
    let system = CActorSystemFactory.create("chat-room")

    let root = TypedBehaviors.setup<MainCmd>({ ctx =>
        let chat = ctx.actorOf<RoomCommand>(chatRoom(), "chatroom")
        for (n in names) {
            let g = ctx.actorOf<SessionEvent>(gabbler(n, names.size, transcript), "gabbler-${n}")
            ctx.watch<SessionEvent>(g)
            chat.tell(RoomCommand.GetSession(n, g))
        }
        // 重名客户端：必须被拒绝
        let dup = ctx.actorOf<SessionEvent>(gabbler("alice-dup", names.size, transcript), "gabbler-dup")
        ctx.watch<SessionEvent>(dup)
        chat.tell(RoomCommand.GetSession("alice", dup))
        let alive = AtomicInt64(names.size + 1)
        TypedBehaviors.receiveMessage<MainCmd>({ _ => TypedBehaviors.same<MainCmd>() }).receiveSignal({ _, sig =>
            if (sig is TypedTerminated && alive.fetchSub(1) == 1) {
                finished.store(true)
                TypedBehaviors.stopped<MainCmd>()
            } else {
                TypedBehaviors.same<MainCmd>()
            }
        })
    })
    system.typedActorOf<MainCmd>(root, "main")

    let deadline = MonoTime.now() + Duration.second * 10
    while (!finished.load() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    system.terminate()

    var ok = finished.load()
    for (n in names) {
        let got = transcript.count("${n}<-")
        println("${n} 收到 ${got} 条消息")
        ok = ok && got == names.size
    }
    println("重名被拒绝 ${transcript.denied} 次")
    ok = ok && transcript.denied == 1
    if (ok) {
        println("聊天室示例：通过")
        0
    } else {
        eprintln("聊天室示例：失败")
        1
    }
}
