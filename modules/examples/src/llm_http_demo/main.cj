/*
 * CActor Examples - HTTP SSE、可取消 IO 与持久化 token 会话（E61）
 *
 * 真 TCP mock 按 OpenAI delta.content 发送 chunked SSE；首 token 持久化后才继续响应。
 * HTTP IO 在协程中执行，每个 token 落盘后确认；取消直接关闭原始 TCP，唤醒停滞的读取。
 * 新系统/新文件存储恢复完整与取消的会话，并在保留历史的会话上继续下一轮 HTTP 生成。
 * 运行：CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.llm_http_demo
 */
package cactor_examples.llm_http_demo

import std.collection.*
import std.fs.*
import std.net.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import stdx.encoding.json.*
import cactor_core.typed.*
import cactor_persistence.FileDurableStateStore
import cactor_persistence.typed.*
import cactor_testkit.ActorTestKit

let TIMEOUT = Duration.second * 10
let TOKENS = ["仓颉", " Actor", " 流式", " token"]

enum HttpCmd {
    | Start(TypedActorRef<TokenEvent>, TypedActorRef<String>)
    | Chunk(String, TypedPromise<Bool>)
    | Next
    | Cancel
    | Finished(String)
}

enum TokenEvent {
    | Begin(TypedActorRef<String>)
    | Token(String)
    | Finish(String, TypedActorRef<String>)
    | Read(TypedActorRef<String>)
}

func check(ok: Bool, why: String): Unit {
    if (!ok) {
        throw IllegalStateException(why)
    }
}

/** 基础设施句柄只保护跨线程 IO 资源；业务状态仍由返回新行为保存。 */
class RequestControl {
    let lock = Mutex()
    let cancelled = AtomicBool(false)
    let resumed = TypedPromise<Bool>()
    private var socket = None<TcpSocket>

    func attach(s: TcpSocket): Unit {
        let shouldClose = synchronized(lock) {
            socket = Some(s)
            cancelled.load()
        }
        if (shouldClose) {
            s.close();
            throw IllegalStateException("请求已取消")
        }
    }

    func cancel(): Unit {
        let active = synchronized(lock) {
            cancelled.store(true)
            socket
        }
        // 先关原始 TCP，不能等待 HTTP body 自己读完或超时。
        if (let Some(s) <- active) {
            s.close()
        }
    }
}

func event(token: String): String {
    let content = JsonString(token).toString()
    "data: {\"choices\":[{\"delta\":{\"content\":${content}}}]}\r\n\r\n"
}

func chunkBytes(text: String): Array<UInt8> {
    let wire = ArrayList<UInt8>()
    // 每个 HTTP chunk 只有一个字节，中文 UTF-8 必然跨 chunk；只能在完整行上解码。
    for (b in text.toArray()) {
        wire.add(all: [b'1', 13, 10, b, 13, 10])
    }
    wire.toArray()
}

/** 完整流、token 边界取消、停滞读取取消、恢复后下一轮：共四次真实 HTTP 请求。 */
class MockLlm {
    let listener = TcpServerSocket(bindAt: IPSocketAddress("127.0.0.1", 0))
    let firstSeen = Array<TypedPromise<Bool>>(4, {_ => TypedPromise<Bool>()})
    let served = TypedPromise<Bool>()

    init() {
        listener.bind()
    }

    func url(): String {
        let port = (listener.localAddress as IPSocketAddress).getOrThrow().port
        "http://127.0.0.1:${port}/v1/chat/completions"
    }

    func run(): Unit {
        spawn {
            try {
                for (request in 0..4) {
                    let socket = listener.accept(timeout: Some(TIMEOUT))
                    try {
                        socket.readTimeout = Some(TIMEOUT)
                        let one = Array<UInt8>(1, repeat: 0)
                        let header = ArrayList<UInt8>()
                        while (true) {
                            check(socket.read(one) == 1, "请求头提前结束")
                            header.add(one[0])
                            check(header.size < 16384, "请求头过长")
                            if (header.size >= 4 && String.fromUtf8(header.toArray()).endsWith("\r\n\r\n")) {
                                break
                            }
                        }
                        check(String.fromUtf8(header.toArray()).startsWith("POST /v1/chat/completions "),
                            "错误的 HTTP 请求")
                        for (_ in 0..2) {
                            check(socket.read(one) == 1, "缺少请求体")
                        }
                        socket.write(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                                .toArray())
                        socket.write(chunkBytes(event(TOKENS[0])))
                        check(firstSeen[request].get(TIMEOUT), "首个 token 未在响应完成前持久化")
                        if (request == 0 || request == 3) {
                            let tail = String.join(TOKENS[1..TOKENS.size].map({t => event(t)}), delimiter: "") +
                                "data: [DONE]\r\n\r\n"
                            socket.write(chunkBytes(tail))
                            socket.write("0\r\n\r\n".toArray())
                        } else {
                            if (request == 1) {
                                socket.write(chunkBytes(event(TOKENS[1])))
                            }
                            // 第三次请求保持停滞，不再产生 token，也不发结束标记。
                            check(socket.read(one) == 0, "取消后 HTTP 连接没有关闭")
                        }
                    } finally {
                        socket.close()
                    }
                }
                served.trySuccess(true)
            } catch (e: Exception) {
                served.tryFailure(e)
            }
        }
    }
}

/** IO 边界把网络/解析失败转成协议结果，Actor 线程不等待网络。 */
func readSse(url: String, self: TypedActorRef<HttpCmd>, control: RequestControl): Unit {
    let client = ClientBuilder().noProxy().readTimeout(TIMEOUT).writeTimeout(TIMEOUT).connector(
        {
            address =>
                let socket = TcpSocket(address)
                control.attach(socket)
                socket.connect(timeout: Some(TIMEOUT))
                socket
        }
    ).build()
    var result = "error: SSE 缺少 [DONE]"
    try {
        let response = client.send(HttpRequestBuilder().url(url).post().body("{}").build())
        try {
            check(response.status == 200, "HTTP 状态 ${response.status}")
            let bytes = ArrayList<UInt8>()
            let buffer = Array<UInt8>(7, repeat: 0)
            var completed = false
            var cancelled = false
            // Next 已确认后必须实际尝试读取；取消靠关闭 TCP 唤醒 IO，不能只靠标志跳过读取。
            // 这也使停滞取消自检不依赖 Cancel 恰好落在下一次 read 前还是后。
            while (!completed && !cancelled) {
                let n = response.body.read(buffer)
                if (n == 0) {
                    break
                }
                for (b in buffer[0..n]) {
                    if (completed || cancelled || control.cancelled.load()) {
                        break
                    }
                    if (b != 10) {
                        bytes.add(b)
                        check(bytes.size <= 65536, "SSE 行过长")
                        continue
                    }
                    let line = String.fromUtf8(bytes.toArray()).trimAscii()
                    bytes.clear()
                    if (!line.startsWith("data: ")) {
                        continue
                    }
                    let payload = line[6..line.size]
                    if (payload == "[DONE]") {
                        completed = true
                        break
                    }
                    let choice = JsonValue
                        .fromStr(payload)
                        .asObject()
                        .get("choices")
                        .getOrThrow()
                        .asArray()[0]
                        .asObject()
                    let token = choice
                        .get("delta")
                        .getOrThrow()
                        .asObject()
                        .get("content")
                        .getOrThrow()
                        .asString()
                        .getValue()
                    let ack = TypedPromise<Bool>()
                    self << HttpCmd.Chunk(token, ack)
                    cancelled = !ack.get(TIMEOUT)
                    if (!cancelled) {
                        control.resumed.trySuccess(true)
                    }
                }
            }
            if (completed) {
                result = "completed"
            }
        } finally {
            response.close()
        }
    } catch (e: Exception) {
        result = "error: ${e.message}"
    } finally {
        client.close()
    }
    // 关闭连接会唤醒 read 并抛异常；主动取消的结果由取消状态决定。
    self << HttpCmd.Finished(if (control.cancelled.load()) {
        "cancelled"
    } else {
        result
    })
}

func httpClient(url: String, control: RequestControl): TypedBehavior<HttpCmd> {
    TypedBehaviors.receive<HttpCmd>({
        ctx, cmd => match (cmd) {
            case Start(sink, done) =>
                let self = ctx.self
                spawn {readSse(url, self, control)}
                active(sink, done, control, None<TypedPromise<Bool>>)
            case _ => TypedBehaviors.unhandled<HttpCmd>()
        }
    })
}

func active(sink: TypedActorRef<TokenEvent>, done: TypedActorRef<String>, control: RequestControl,
    pending: Option<TypedPromise<Bool>>): TypedBehavior<HttpCmd> {
    TypedBehaviors.receive<HttpCmd>({
        _, cmd => match (cmd) {
            case Chunk(token, ack) =>
                if (control.cancelled.load()) {
                    ack.trySuccess(false)
                } else {
                    sink << TokenEvent.Token(token)
                }
                active(sink, done, control, Some(ack))
            case Next =>
                if (let Some(ack) <- pending) {
                    ack.trySuccess(!control.cancelled.load())
                }
                active(sink, done, control, None<TypedPromise<Bool>>)
            case Cancel =>
                control.cancel()
                if (let Some(ack) <- pending) {
                    ack.trySuccess(false)
                }
                active(sink, done, control, None<TypedPromise<Bool>>)
            case Finished(result) =>
                done << result
                TypedBehaviors.stopped<HttpCmd>()
            case Start(_, rejected) =>
                rejected << "error: busy"
                TypedBehaviors.same<HttpCmd>()
        }
    })
}

struct TokenSession {
    let tokens: Array<String>
    let status: String
    init(tokens: Array<String>, status: String) {
        this.tokens = tokens
        this.status = status
    }
}

func encodeSession(state: TokenSession): String {
    let tokens = String.join(state.tokens.map({t => JsonString(t).toString()}), delimiter: ",")
    "{\"status\":${JsonString(state.status)},\"tokens\":[${tokens}]}"
}

func decodeSession(text: String): TokenSession {
    let json = JsonValue.fromStr(text).asObject()
    let values = json.get("tokens").getOrThrow().asArray()
    let tokens = Array<String>(values.size(), {i => values[i].asString().getValue()})
    TokenSession(tokens, json.get("status").getOrThrow().asString().getValue())
}

/** token 落盘后才确认继续读取，因此取消终态也包含已确认的完整历史。 */
func session(id: String, store: FileDurableStateStore, client: TypedActorRef<HttpCmd>, gate: TypedPromise<Bool>,
    collected: TypedPromise<Array<String>>, base: Int64, limit: Int64, cancelAtLimit: Bool): TypedBehavior<TokenEvent> {
    DurableStateBehavior<TokenEvent, TokenSession>(PersistenceId.of("HttpSession", id),
        emptyState: TokenSession([], "new"), commandHandler: {
            state, msg, effect => match (msg) {
                case Begin(replyTo) => effect
                    .persist(TokenSession(state.tokens, "running"))
                    .thenReply(replyTo, {s => encodeSession(s)})
                case Token(token) =>
                    if (state.status != "running") {
                        return effect.none()
                    }
                    effect.persist(TokenSession(state.tokens.concat([token]), "running")).thenRun(
                        {
                            s =>
                                let n = s.tokens.size - base
                                if (n == 1) {
                                    gate.trySuccess(true)
                                }
                                if (n == limit) {
                                    client << if (cancelAtLimit) {
                                        HttpCmd.Cancel
                                    } else {
                                        HttpCmd.Next
                                    }
                                    collected.trySuccess(s.tokens)
                                } else {
                                    client << HttpCmd.Next
                                }
                        }
                    )
                case Finish(status, replyTo) => effect
                    .persist(TokenSession(state.tokens, status))
                    .thenReply(replyTo, {s => encodeSession(s)})
                case Read(replyTo) => effect.reply(replyTo, encodeSession(state))
            }
        }, codec: FunctionStateCodec<TokenSession>(encode: encodeSession, decode: decodeSession), store: store)
        .behavior()
}

func askSession(ref: TypedActorRef<TokenEvent>, factory: (TypedActorRef<String>) -> TokenEvent): TokenSession {
    decodeSession(TypedAsk.ask<TokenEvent, String>(ref, factory, TIMEOUT).get(TIMEOUT))
}

func runRequest(kit: ActorTestKit, mock: MockLlm, dir: String, index: Int64, base: Int64): TokenSession {
    let control = RequestControl()
    let agent = kit.spawnTyped<HttpCmd>(httpClient(mock.url(), control), "http-${index}")
    let collected = TypedPromise<Array<String>>()
    let limit = if (index == 1) {
        2
    } else if (index == 2) {
        1
    } else {
        TOKENS.size
    }
    let id = if (index == 3) {
        "0"
    } else {
        "${index}"
    }
    let sink = kit.spawnTyped<TokenEvent>(
        session(id, FileDurableStateStore(dir), agent, mock.firstSeen[index], collected, base, limit, index == 1),
        "session-${index}")
    let before = askSession(sink, {r => TokenEvent.Begin(r)})
    check(before.tokens.size == base, "恢复后历史长度错误")
    let result = TypedAsk.ask<HttpCmd, String>(agent, {r => HttpCmd.Start(sink, r)}, TIMEOUT)
    let tokens = collected.get(TIMEOUT)
    if (index == 2) {
        // IO 协程已经拿到 Next 确认；此时没有待确认的 token，服务端永久停在下一段之前。
        check(control.resumed.get(TIMEOUT), "读取没有继续")
        agent << HttpCmd.Cancel
    }
    let finish = result.get(if (index == 2) {
        Duration.second * 3
    } else {
        TIMEOUT
    })
    check(finish == if (index == 1 || index == 2) {
        "cancelled"
    } else {
        "completed"
    }, "流的结束状态错误 ${finish}")
    let state = askSession(sink, {r => TokenEvent.Finish(finish, r)})
    check(state.tokens == tokens && tokens[base..tokens.size] == TOKENS[0..limit], "token 内容/顺序错误")
    println("HTTP SSE ${index}：${finish}，持久化 ${state.tokens.size} 个 token")
    state
}

main(): Int64 {
    let dir = "/tmp/cactor-http-session-${MonoTime.now().hashCode()}"
    let mock = MockLlm()
    mock.run()
    try {
        let original = ActorTestKit("http-original")
        try {
            for (index in 0..3) {
                runRequest(original, mock, dir, index, 0)
            }
        } finally {
            original.shutdown()
        }
        let recovered = ActorTestKit("http-recovered")
        try {
            let unused = recovered.spawnTyped<HttpCmd>(TypedBehaviors.ignore<HttpCmd>(), "unused")
            for (index in 0..3) {
                let sink = recovered.spawnTyped<TokenEvent>(
                    session("${index}", FileDurableStateStore(dir), unused, TypedPromise<Bool>(),
                        TypedPromise<Array<String>>(), 0, 0, false), "restored-${index}")
                let state = askSession(sink, {r => TokenEvent.Read(r)})
                let count = if (index == 0) {
                    4
                } else if (index == 1) {
                    2
                } else {
                    1
                }
                check(state.tokens == TOKENS[0..count], "恢复的 HTTP token 历史错误")
                check(state.status == if (index == 0) {
                    "completed"
                } else {
                    "cancelled"
                }, "恢复的会话终态错误")
            }
        } finally {
            recovered.shutdown()
        }
        // 单写者：恢复检查系统已停止，下一套系统才能继续写相同持久化 ID。
        let continued = ActorTestKit("http-continued")
        try {
            let finalState = runRequest(continued, mock, dir, 3, 4)
            check(finalState.tokens == TOKENS.concat(TOKENS), "恢复后下一轮没有保留历史")
        } finally {
            continued.shutdown()
        }
        check(mock.served.get(TIMEOUT), "mock 未处理全部请求")
        println("llm_http_demo：通过（真实 TCP SSE、token 持久化恢复续聊、两种取消及连接关闭）")
        0
    } catch (e: Exception) {
        println("llm_http_demo：失败 ${e.message}")
        1
    } finally {
        mock.listener.close()
        if (exists(dir)) {
            remove(dir, recursive: true)
        }
    }
}
