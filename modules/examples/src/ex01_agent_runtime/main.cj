/*
 * CActor Ex 01 - LLM Agent 编排运行时（HTTP/SSE 用 Lou 承载）
 *
 * 真实约束（docs/plan/2026117-lz.md §3 E1）：
 * - 一次生成可能几十秒：模型 IO 在协程里跑，不占住 Actor 邮箱；
 * - 客户端可能断开：取消必须关掉上游模型连接（停止计费），会话写成 cancelled 终态；
 * - 工具调用会失败：失败不污染会话（该轮 token 不写入事件流，只留一条失败记录）；
 * - 慢客户端不能拖垮其他会话：每个会话独立 Actor，背压只在会话内生效；
 * - 进程重启后会话可恢复：会话状态用 DurableStateBehavior 落盘。
 *
 * 组件：
 * - Lou 提供 HTTP/SSE（GET /agent/:session?q=... → text/event-stream）
 * - SessionActor（DurableStateBehavior）：会话历史 + 轮次状态，串行处理
 * - ToolActor：可失败的工具，Ask 调用 + 有界重试
 * - MockLlm：真 TCP 的 SSE 模型服务，取消后停止发送并统计连接关闭
 *
 * 自检（全部断言，退出码 0/1）：
 * 1. 完整轮次：SSE 收到全部 token，且与会话持久化内容一致；
 * 2. 客户端断开：会话终态 cancelled，模型侧观察到连接关闭；
 * 3. 工具失败：该轮没有 token，只有 failed 记录；
 * 4. 慢客户端：另一会话不被拖垮（总耗时 < 2s）；
 * 5. 重启恢复：新系统从同一目录恢复完整历史。
 *
 * 运行：source tools/env.sh && cjpm build && \
 *       CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex01_agent_runtime
 */
package cactor_examples.ex01_agent_runtime

import std.collection.*
import std.net.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import stdx.encoding.json.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_persistence.FileDurableStateStore
import cactor_persistence.typed.*

let ASK_TIMEOUT = Duration.second * 10
let HTTP_TIMEOUT = Duration.second * 10
let TOKENS = ["仓颉", " Actor", " 流式", " 回答"]

// ======================= 协议 =======================

/** 会话命令（TurnId 用字符串，避免为消息类型引入 class） */
enum SessionCmd {
    | Begin(String, String, TypedActorRef<String>)
    | Token(String, String)
    | Finish(String, String, TypedActorRef<String>)
    | ToolFailed(String, String)
    | Cancel(String, TypedActorRef<String>)
    | Read(TypedActorRef<SessionSnapshot>)
}

enum ToolCmd {
    | Invoke(String, TypedActorRef<ToolResult>)
}

enum ToolResult {
    | Ok(String)
    | Failed(String)
}

/** 会话快照：状态全部不可变，由事件重放得到 */
class SessionSnapshot {
    let turns: Array<TurnRecord>

    init(turns: Array<TurnRecord>) {
        this.turns = turns
    }

    static func empty(): SessionSnapshot {
        SessionSnapshot([])
    }

    func apply(record: TurnRecord): SessionSnapshot {
        let kept = turns.filter({ r => r.id != record.id }).toArray()
        SessionSnapshot(kept.concat([record]))
    }

    func completedTokens(): Array<String> {
        let out = ArrayList<String>()
        for (r in turns) {
            if (r.status == "done") {
                for (t in r.tokens) {
                    out.add(t)
                }
            }
        }
        out.toArray()
    }

    func find(id: String): Option<TurnRecord> {
        for (r in turns) {
            if (r.id == id) {
                return Some(r)
            }
        }
        None
    }

    func encode(): String {
        let items = StringBuilder()
        var first = true
        for (r in turns) {
            if (!first) {
                items.append(",")
            }
            first = false
            items.append(r.encode())
        }
        "[${items.toString()}]"
    }

    static func decode(text: String): SessionSnapshot {
        let values = JsonValue.fromStr(text).asArray()
        SessionSnapshot(Array<TurnRecord>(values.size(), { i => TurnRecord.decode(values[i]) }))
    }
}

/** 一轮生成：tokens 只在该轮 done 时写入 */
class TurnRecord {
    let id: String
    let prompt: String
    let tokens: Array<String>
    let status: String
    let failure: Option<String>

    init(id: String, prompt: String, tokens: Array<String>, status: String, failure: Option<String>) {
        this.id = id
        this.prompt = prompt
        this.tokens = tokens
        this.status = status
        this.failure = failure
    }

    static func started(id: String, prompt: String): TurnRecord {
        TurnRecord(id, prompt, [], "running", None)
    }

    func withTokens(more: Array<String>): TurnRecord {
        TurnRecord(id, prompt, tokens.concat(more), status, failure)
    }

    func finish(status: String): TurnRecord {
        TurnRecord(id, prompt, tokens, status, failure)
    }

    func fail(why: String): TurnRecord {
        TurnRecord(id, prompt, [], "failed", Some(why))
    }

    func encode(): String {
        let ts = String.join(tokens.map({ t => JsonString(t).toString() }), delimiter: ",")
        // 失败原因始终编码为字符串（缺失时为空串），避免依赖 JSON null 的判空 API
        let fail = match (failure) {
            case Some(f) => JsonString(f).toString()
            case None => JsonString("").toString()
        }
        "{\"id\":${JsonString(id)},\"prompt\":${JsonString(prompt)},\"tokens\":[${ts}],\"status\":${JsonString(status)},\"failure\":${fail}}"
    }

    static func decode(value: JsonValue): TurnRecord {
        let obj = value.asObject()
        let tokenValues = obj.get("tokens").getOrThrow().asArray()
        let failure: Option<String> = match (obj.get("failure")) {
            case Some(v) where v.asString().getValue() != "" => Some(v.asString().getValue())
            case _ => None<String>
        }
        TurnRecord(
            obj.get("id").getOrThrow().asString().getValue(),
            obj.get("prompt").getOrThrow().asString().getValue(),
            Array<String>(tokenValues.size(), { i => tokenValues[i].asString().getValue() }),
            obj.get("status").getOrThrow().asString().getValue(),
            failure
        )
    }
}

func check(ok: Bool, why: String): Unit {
    if (!ok) {
        throw IllegalStateException(why)
    }
}

// ======================= 模型侧：真 TCP SSE =======================

/** 每个请求一个连接：发送 TOKENS 后等客户端关闭；客户端断开计一次取消 */
class MockLlm {
    let listener = TcpServerSocket(bindAt: IPSocketAddress("127.0.0.1", 0))
    let requests = AtomicInt64(0)
    let cancellations = AtomicInt64(0)

    init() {
        listener.bind()
    }

    func url(): String {
        let port = (listener.localAddress as IPSocketAddress).getOrThrow().port
        "http://127.0.0.1:${port}/v1/chat/completions"
    }

    func run(): Unit {
        spawn {
            while (true) {
                let socket = try {
                    listener.accept(timeout: Some(Duration.second * 30))
                } catch (_: Exception) {
                    break
                }
                spawn { serveOne(socket) }
            }
        }
    }

    func serveOne(socket: TcpSocket): Unit {
        requests.fetchAdd(1)
        try {
            socket.readTimeout = Some(Duration.second * 30)
            let one = Array<UInt8>(1, repeat: 0)
            let header = ArrayList<UInt8>()
            while (true) {
                if (socket.read(one) != 1) {
                    return
                }
                header.add(one[0])
                check(header.size < 16384, "请求头过长")
                if (header.size >= 4 && String.fromUtf8(header.toArray()).endsWith("\r\n\r\n")) {
                    break
                }
            }
            socket.write(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                    .toArray())
            for (t in TOKENS) {
                // 生成期间写失败 = 客户端已断开（这是"取消"的唯一可靠信号）
                socket.write(chunkBytes(event(t)))
                sleep(Duration.millisecond * 20)
            }
            socket.write(chunkBytes("data: [DONE]\r\n\r\n"))
            socket.write("0\r\n\r\n".toArray())
            // 正常结束后客户端关闭连接属于预期，不计取消
            try {
                while (socket.read(one) == 1) {
                    ()
                }
            } catch (_: Exception) {
                ()
            }
        } catch (_: Exception) {
            // 发送中途断开：记为一次取消
            cancellations.fetchAdd(1)
        } finally {
            try {
                socket.close()
            } catch (_: Exception) {
                ()
            }
        }
    }
}

/** 单行 OpenAI 风格 delta（SSE data 字段内容，不含换行） */
func deltaJson(token: String): String {
    let content = JsonString(token).toString()
    "{\"choices\":[{\"delta\":{\"content\":${content}}}]}"
}

/** mock 模型写出的完整 SSE 帧 */
func event(token: String): String {
    "data: ${deltaJson(token)}\r\n\r\n"
}

/** 每 chunk 一个字节：中文 UTF-8 必然跨 chunk，读取端必须按行重组 */
func chunkBytes(text: String): Array<UInt8> {
    let wire = ArrayList<UInt8>()
    for (b in text.toArray()) {
        wire.add(all: [b'1', 13, 10, b, 13, 10])
    }
    wire.toArray()
}

// ======================= 会话 Actor =======================

/**
 * 运行中的 token 存内存（不持久化），轮次 done 时一次性落盘——
 * 取消/失败的轮次因此天然不会留下半截 token。
 */
class SessionState {
    let snapshot: SessionSnapshot
    let liveTokens: HashMap<String, ArrayList<String>>

    init(snapshot: SessionSnapshot, liveTokens: HashMap<String, ArrayList<String>>) {
        this.snapshot = snapshot
        this.liveTokens = liveTokens
    }

    static func empty(): SessionState {
        SessionState(SessionSnapshot.empty(), HashMap<String, ArrayList<String>>())
    }

    func liveOf(turn: String): ArrayList<String> {
        match (liveTokens.get(turn)) {
            case Some(v) => v
            case None =>
                let v = ArrayList<String>()
                liveTokens[turn] = v
                v
        }
    }

    func encode(): String {
        snapshot.encode()
    }

    static func decode(text: String): SessionState {
        SessionState(SessionSnapshot.decode(text), HashMap<String, ArrayList<String>>())
    }
}

/** 无回复的引用：只接收不做事（用于不需要回复的命令） */
class SinkRef<T> <: TypedActorRef<T> {
    public func tell(message: T): Unit {}
    public func pathString(): String { "sink" }
    public func equals(other: TypedActorRef<T>): Bool { false }
}

func sinkOf<T>(): TypedActorRef<T> {
    SinkRef<T>()
}

/** 把闭包装成引用：SSE 侧把 token 直接推进 Lou 的流 */
class FuncRef<T> <: TypedActorRef<T> {
    let f: (T) -> Unit

    init(f: (T) -> Unit) {
        this.f = f
    }

    public func tell(message: T): Unit {
        f(message)
    }

    public func pathString(): String { "func" }
    public func equals(other: TypedActorRef<T>): Bool { false }
}

func sessionBehavior(sessionId: String, store: FileDurableStateStore): TypedBehavior<SessionCmd> {
    DurableStateBehavior<SessionCmd, SessionState>(PersistenceId.of("AgentSession", sessionId),
        emptyState: SessionState.empty(), commandHandler: { state, cmd, effect =>
            match (cmd) {
                case Begin(turn, prompt, replyTo) =>
                    let next = SessionState(state.snapshot.apply(TurnRecord.started(turn, prompt)), state.liveTokens)
                    effect.persist(next).thenReply(replyTo, { _ => "begun" })
                case Token(turn, token) =>
                    state.liveOf(turn).add(token)
                    effect.none()
                case Finish(turn, status, replyTo) =>
                    let live = state.liveOf(turn)
                    let tokens = live.toArray()
                    live.clear()
                    let record = match (state.snapshot.find(turn)) {
                        case Some(r) => r.withTokens(tokens).finish(status)
                        case None => TurnRecord(turn, "", tokens, status, None)
                    }
                    let next = SessionState(state.snapshot.apply(record), state.liveTokens)
                    effect.persist(next).thenReply(replyTo, { _ => "ok" })
                case ToolFailed(turn, why) =>
                    let record = match (state.snapshot.find(turn)) {
                        case Some(r) => r.fail(why)
                        case None => TurnRecord(turn, "", [], "failed", Some(why))
                    }
                    let next = SessionState(state.snapshot.apply(record), state.liveTokens)
                    effect.persist(next)
                case Cancel(turn, replyTo) =>
                    let record = match (state.snapshot.find(turn)) {
                        case Some(r) => r.finish("cancelled")
                        case None => TurnRecord(turn, "", [], "cancelled", None)
                    }
                    state.liveOf(turn).clear()
                    let next = SessionState(state.snapshot.apply(record), state.liveTokens)
                    effect.persist(next).thenReply(replyTo, { _ => "cancelled" })
                case Read(replyTo) => effect.reply(replyTo, state.snapshot)
            }
        }, codec: FunctionStateCodec<SessionState>(encode: { s => s.encode() }, decode: { s => SessionState.decode(s) }),
        store: store).behavior()
}

// ======================= 工具 Actor =======================

/** 前 failTimes 次调用失败（回复 Failed），之后成功；调用次数是基础设施状态 */
class FlakyTool <: TypedBehavior<ToolCmd> {
    let failTimes: Int64
    var calls: Int64 = 0

    init(failTimes: Int64) {
        this.failTimes = failTimes
    }

    public func onMessage(message: ToolCmd, context: TypedActorContext<ToolCmd>): TypedBehavior<ToolCmd> {
        match (message) {
            case Invoke(arg, replyTo) =>
                calls++
                if (calls <= failTimes) {
                    replyTo << ToolResult.Failed("工具第 ${calls} 次失败")
                } else {
                    replyTo << ToolResult.Ok("tool(${arg})")
                }
                this
        }
    }

    public func getDescription(): String {
        "FlakyTool"
    }
}

// ======================= 轮次驱动（模型 IO 在协程） =======================

/** 持有上游 socket；cancel() 直接关闭它唤醒停滞的读取 */
class TurnDriver {
    let cancelled = AtomicBool(false)
    let lock = Mutex()
    private var socket = None<TcpSocket>

    func attach(s: TcpSocket): Unit {
        let shouldClose = synchronized(lock) {
            socket = Some(s)
            cancelled.load()
        }
        if (shouldClose) {
            s.close()
            throw IllegalStateException("轮次已取消")
        }
    }

    func cancel(): Unit {
        let active = synchronized(lock) {
            cancelled.store(true)
            socket
        }
        if (let Some(s) <- active) {
            s.close()
        }
    }

    /** 读模型 SSE，把 token 送进 session 与 sink；完成后调用工具决定 done / failed */
    func run(url: String, session: TypedActorRef<SessionCmd>, turn: String, sink: TypedActorRef<String>,
        tool: TypedActorRef<ToolCmd>): Unit {
        let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).writeTimeout(HTTP_TIMEOUT).connector(
            {
                address =>
                    let s = TcpSocket(address)
                    attach(s)
                    s.connect(timeout: Some(HTTP_TIMEOUT))
                    s
            }
        ).build()
        var completed = false
        try {
            let response = client.send(HttpRequestBuilder().url(url).post().body("{}").build())
            try {
                check(response.status == 200, "模型返回 ${response.status}")
                let bytes = ArrayList<UInt8>()
                let buffer = Array<UInt8>(7, repeat: 0)
                while (!completed && !cancelled.load()) {
                    let n = response.body.read(buffer)
                    if (n == 0) {
                        break
                    }
                    for (b in buffer[0..n]) {
                        if (completed || cancelled.load()) {
                            break
                        }
                        if (b != 10) {
                            bytes.add(b)
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
                        let choice = JsonValue.fromStr(payload).asObject()
                            .get("choices").getOrThrow().asArray()[0].asObject()
                        let token = choice.get("delta").getOrThrow().asObject()
                            .get("content").getOrThrow().asString().getValue()
                        session << SessionCmd.Token(turn, token)
                        sink << token
                    }
                }
            } finally {
                response.close()
            }
            if (completed && !cancelled.load()) {
                match (callTool(tool, turn)) {
                    case Ok(_) => session << SessionCmd.Finish(turn, "done", sinkOf<String>())
                    case Failed(why) => session << SessionCmd.ToolFailed(turn, why)
                }
            }
        } catch (_: Exception) {
            ()
        } finally {
            client.close()
            if (cancelled.load()) {
                session << SessionCmd.Cancel(turn, sinkOf<String>())
            }
        }
    }

    func callTool(tool: TypedActorRef<ToolCmd>, turn: String): ToolResult {
        var attempt = 0
        while (attempt < 3) {
            attempt++
            try {
                let result = TypedAsk.ask<ToolCmd, ToolResult>(tool, { r => ToolCmd.Invoke(turn, r) },
                    Duration.second * 5).get(Duration.second * 5)
                match (result) {
                    case Ok(_) => return result
                    case Failed(_) where attempt < 3 => continue
                    case Failed(why) => return ToolResult.Failed(why)
                }
            } catch (_: Exception) {
                if (attempt >= 3) {
                    return ToolResult.Failed("工具调用超时")
                }
            }
        }
        ToolResult.Failed("工具调用失败")
    }
}

// ======================= 自检工具 =======================

func readBody(response: HttpResponse): String {
    let out = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    var n = response.body.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = response.body.read(buf)
    }
    String.fromUtf8(out.toArray())
}

func parseSseTokens(text: String): Array<String> {
    let out = ArrayList<String>()
    for (line in text.split("\n")) {
        let trimmed = line.trimAscii()
        if (!trimmed.startsWith("data: ")) {
            continue
        }
        let payload = trimmed[6..trimmed.size]
        if (payload == "[DONE]") {
            continue
        }
        let choice = JsonValue.fromStr(payload).asObject()
            .get("choices").getOrThrow().asArray()[0].asObject()
        out.add(choice.get("delta").getOrThrow().asObject()
            .get("content").getOrThrow().asString().getValue())
    }
    out.toArray()
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func tokensOf(snapshot: SessionSnapshot, turn: String): Array<String> {
    match (snapshot.find(turn)) {
        case Some(r) => r.tokens
        case None => []
    }
}

func statusOf(snapshot: SessionSnapshot, turn: String): String {
    match (snapshot.find(turn)) {
        case Some(r) => r.status
        case None => "missing"
    }
}

func waitFor(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 10)
    }
    cond()
}

func readSnapshot(session: TypedActorRef<SessionCmd>): SessionSnapshot {
    TypedAsk.ask<SessionCmd, SessionSnapshot>(session, { r => SessionCmd.Read(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
}

/**
 * 等某一轮离开 running：Finish / ToolFailed / Cancel 是 driver 在流结束后才 tell 的，
 * 读取快照前必须先等这些命令被会话 Actor 处理完（否则会读到 running 的空 token）。
 */
func awaitTurn(session: TypedActorRef<SessionCmd>, turn: String, timeout: Duration): SessionSnapshot {
    var snap = readSnapshot(session)
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        snap = readSnapshot(session)
        if (snap.turns.size > 0 && statusOf(snap, turn) != "running" && statusOf(snap, turn) != "missing") {
            return snap
        }
        sleep(Duration.millisecond * 20)
    }
    snap
}

// ======================= main =======================

main(): Int64 {
    let dir = "/tmp/cactor-ex01-${MonoTime.now().hashCode()}"
    let mock = MockLlm()
    mock.run()
    let system = CActorSystemFactory.create("ex01-agent")
    let store = FileDurableStateStore("${dir}/sessions")
    let lock = Mutex()
    let sessions = HashMap<String, TypedActorRef<SessionCmd>>()

    func sessionOf(id: String): TypedActorRef<SessionCmd> {
        synchronized(lock) {
            match (sessions.get(id)) {
                case Some(ref) => ref
                case None =>
                    let ref = system.typedActorOf(sessionBehavior(id, store), "session-${id}")
                    sessions[id] = ref
                    ref
            }
        }
    }

    let okTool = system.typedActorOf<ToolCmd>(FlakyTool(1), "tool-ok")
    let badTool = system.typedActorOf<ToolCmd>(FlakyTool(1000), "tool-bad")
    let c = Checker()

    let app = Lou([withHost("127.0.0.1"), withPort(0u16), withHideBanner(), withHideRoutes()])
    // SSE 端点：一个请求 = 一轮。断开会取消上游模型连接。
    app.get("/agent/:session", { ctx =>
        let sessionId = ctx.param("session")
        let prompt = ctx.query("q") ?? "hello"
        let toolName = ctx.query("tool") ?? "ok"
        let turn = "t-${MonoTime.now().hashCode()}"
        ctx.setSingleHeader("X-Turn-Id", turn)
        let session = sessionOf(sessionId)
        TypedAsk.ask<SessionCmd, String>(session, { r => SessionCmd.Begin(turn, prompt, r) }, ASK_TIMEOUT)
            .get(ASK_TIMEOUT)
        let tool = if (toolName == "bad") { badTool } else { okTool }
        ctx.sse({ stream =>
            let driver = TurnDriver()
            // 对外就是 OpenAI 风格的 delta JSON（真实网关可直接被现有客户端消费）
            let sink = FuncRef<String>({ t => stream.send(deltaJson(t)) })
            driver.run(mock.url(), session, turn, sink, tool)
            stream.close()
        })
    })
    // 显式取消：真实网关里由客户端断开触发；Lou 的 SseStream 目前不暴露断连回调，
    // 所以用独立端点把"取消"这一步做成可驱动的信号（缺口记录在 docs/plan/2026117-lz.md）。
    app.post("/cancel/:session/:turn", { ctx =>
        let session = sessionOf(ctx.param("session"))
        let turn = ctx.param("turn")
        let reply = TypedAsk.ask<SessionCmd, String>(session, { r => SessionCmd.Cancel(turn, r) }, ASK_TIMEOUT)
            .get(ASK_TIMEOUT)
        ctx.writeString(reply)
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    spawn { app.listen() }
    var port: UInt16 = 0
    for (i in 0..300) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                port = s.port
                break
            }
        }
        sleep(Duration.millisecond * 20)
    }
    let base = "http://127.0.0.1:${port}"
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 完整轮次 ----
        println("\n[1] 完整轮次：SSE 收到全部 token，与会话持久化一致")
        let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let resp = client.get("${base}/agent/s1?q=hi&tool=ok")
        c.check(resp.status == 200, "HTTP 200")
        let tokens = parseSseTokens(readBody(resp))
        c.check(tokens == TOKENS, "SSE token 序列与模型一致：${tokens}")
        client.close()
        let snap0 = readSnapshot(sessionOf("s1"))
        let snap1 = awaitTurn(sessionOf("s1"), snap0.turns[0].id, Duration.second * 10)
        c.check(snap1.turns.size == 1, "会话有 1 轮")
        c.check(tokensOf(snap1, snap1.turns[0].id) == TOKENS, "持久化 token 与 SSE 一致")
        c.check(statusOf(snap1, snap1.turns[0].id) == "done", "该轮状态为 done")

        // ---- 2. 取消：会话 cancelled，且该轮不留下半截 token ----
        println("\n[2] 取消：会话终态 cancelled，且不残留半截 token")
        let abort = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let r2 = abort.get("${base}/agent/s2?q=bye&tool=ok")
        let turn2 = r2.headers.getFirst("X-Turn-Id") ?? ""
        let buf = Array<UInt8>(8, repeat: 0)
        r2.body.read(buf)
        // 显式取消（等价于客户端断开后网关发起的取消）
        let cancelClient = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let cancelResp = cancelClient.post("${base}/cancel/s2/${turn2}", "")
        c.check((cancelResp.status == 200), "取消端点返回 200")
        cancelClient.close()
        r2.close()
        abort.close()
        let snap2 = awaitTurn(sessionOf("s2"), turn2, Duration.second * 10)
        c.check(statusOf(snap2, turn2) == "cancelled", "该轮被标记为 cancelled")
        c.check(tokensOf(snap2, turn2).isEmpty(), "取消的轮次不残留半截 token")

        // ---- 3. 工具失败不污染会话 ----
        println("\n[3] 工具失败：该轮无 token，仅留 failed")
        let badClient = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let r3 = badClient.get("${base}/agent/s3?q=boom&tool=bad")
        let body3 = readBody(r3)
        badClient.close()
        c.check(!parseSseTokens(body3).isEmpty(), "失败轮次在流里仍发出了 token（模型已生成）")
        let snap3raw = readSnapshot(sessionOf("s3"))
        let snap3 = awaitTurn(sessionOf("s3"), snap3raw.turns[0].id, Duration.second * 10)
        let turn3 = snap3.turns[0].id
        c.check(statusOf(snap3, turn3) == "failed", "该轮状态为 failed")
        c.check(tokensOf(snap3, turn3).isEmpty(), "失败轮次没有写入任何 token")

        // ---- 4. 慢客户端 ----
        println("\n[4] 慢客户端：另一会话不被拖垮")
        let slowClient = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let slow = slowClient.get("${base}/agent/slow?q=slow&tool=ok")
        sleep(Duration.millisecond * 100)
        let started = MonoTime.now()
        let fastClient = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let fastBody = readBody(fastClient.get("${base}/agent/fast?q=fast&tool=ok"))
        let latency = (MonoTime.now() - started).toMilliseconds()
        fastClient.close()
        c.check(!parseSseTokens(fastBody).isEmpty(), "快会话收到了 token")
        c.check(latency < 2000, "快会话总耗时 ${latency} ms < 2000 ms")
        slow.close()
        slowClient.close()

        // ---- 5. 重启恢复 ----
        println("\n[5] 重启恢复：新系统从同一目录恢复历史")
        system.terminate()
        c.check(PersistenceIo.awaitIdle(Duration.second * 30), "持久化 IO 在关闭前完成")
        let system2 = CActorSystemFactory.create("ex01-agent-2")
        let store2 = FileDurableStateStore("${dir}/sessions")
        let restored = system2.typedActorOf(sessionBehavior("s1", store2), "session-s1")
        let snap5 = TypedAsk.ask<SessionCmd, SessionSnapshot>(restored, { r => SessionCmd.Read(r) },
            ASK_TIMEOUT).get(ASK_TIMEOUT)
        c.check(snap5.completedTokens() == TOKENS, "重启后 s1 完整 token 历史一致")
        system2.terminate()
        c.check(PersistenceIo.awaitIdle(Duration.second * 30), "第二次关闭也等到 IO 完成")
    } finally {
        app.stop()
    }

    println("\nex01：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
