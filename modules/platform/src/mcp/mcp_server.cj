/*
 * CActor Platform.MCP - MCP server（P5 / AR7）
 *
 * 只实现协议与调度，不做工具语义：
 *   initialize / notifications/initialized / ping / tools/list / tools/call
 *
 * 错误映射（可区分的三种形态，验收 mcpServerMapsErrorsExplicitly）：
 *   1. 未知工具 / 参数不是对象       → JSON-RPC -32602 invalid_params；
 *   2. 执行超时 / provider 抛异常    → JSON-RPC -32603 internal_error；
 *   3. 工具业务失败（McpToolBusinessException）→ 正常 result，content 里 isError=true。
 *
 * 传输：stdx.net.http（不新建 HTTP 栈）。`handle(path, body)` 是纯函数式的单次处理，
 * 便于嵌入式挂载与单测；`start()` 把挂载点注册到真实 HTTP server。
 */
package cactor_platform.mcp

import std.collection.*
import std.io.{StringReader}
import std.sync.*
import std.time.*
import stdx.encoding.json.*
import stdx.log.{LogLevel}
import stdx.net.http.*
import cactor_core.typed.{TypedAskTimeoutException, TypedPromise}

/** 一次 HTTP 响应的状态与体（与框架无关，便于测试与嵌入式路由） */
public struct McpHttpResponse {
    public let status: UInt16
    public let body: String

    public init(status: UInt16, body: String) {
        this.status = status
        this.body = body
    }
}

/** 运行中的 MCP HTTP 服务：close 会关闭监听并等待后台 serve 结束 */
public class RunningMcpServer {
    public let port: UInt16
    private let server: Server
    private let worker: Future<Unit>

    init(server: Server, worker: Future<Unit>) {
        this.server = server
        this.worker = worker
        // port(0) 由系统分配：绑定发生在后台 serve() 里，这里等到实际端口可用
        let deadline = MonoTime.now() + Duration.second * 5
        while (server.port == 0 && MonoTime.now() < deadline) {
            sleep(Duration.millisecond)
        }
        this.port = server.port
    }

    public func close(): Unit {
        try {
            server.close()
        } catch (e: Exception) {
            ()
        }
        try {
            worker.get(Duration.second * 5)
        } catch (e: Exception) {
            ()
        }
    }
}

/**
 * MCP server：一个实例可挂多个路径，每个路径一个 `McpToolProvider`。
 *
 * ```cangjie
 * McpServer().mount("/mcp", provider).start()   // 阻塞由 stdx 内部管理
 * ```
 */
public class McpServer {
    private let lock = Mutex()
    private let providers = HashMap<String, McpToolProvider>()
    private let serverName: String
    private let serverVersion: String
    private let defaultTimeout: Duration

    public init(serverName!: String = "cactor-mcp", serverVersion!: String = "1.2.1",
        defaultTimeout!: Duration = Duration.second * 30) {
        this.serverName = serverName
        this.serverVersion = serverVersion
        this.defaultTimeout = defaultTimeout
    }

    /** 挂载工具提供方；path 必须以 '/' 开头（例如 "/mcp"） */
    public func mount(path: String, provider: McpToolProvider): McpServer {
        if (!path.startsWith("/")) {
            throw IllegalArgumentException("MCP 挂载路径必须以 '/' 开头: ${path}")
        }
        synchronized(lock) {
            providers[path] = provider
        }
        this
    }

    /** 单次 JSON-RPC 处理；未知路径返回 404（不是 JSON-RPC 错误） */
    public func handle(path: String, body: String): McpHttpResponse {
        let provider = synchronized(lock) {
            providers.get(path)
        }
        match (provider) {
            case Some(p) => dispatch(p, body)
            case None => McpHttpResponse(404u16, "")
        }
    }

    /**
     * 启动真实 HTTP 服务（后台线程 serve），返回可关闭的句柄。
     * port 为 0 时由系统分配，句柄的 `port` 是实际端口。
     */
    public func start(address!: String = "127.0.0.1", port!: UInt16 = 0): RunningMcpServer {
        let server = ServerBuilder().addr(address).port(port).build()
        server.logger.level = LogLevel.OFF
        let me = this
        synchronized(lock) {
            for ((path, _) in providers) {
                server.distributor.register(path, { ctx => me.serveHttp(ctx, path) })
            }
        }
        if (providers.size == 0) {
            throw IllegalStateException("MCP server 还没有挂载任何 provider")
        }
        let worker = spawn {
            server.serve()
        }
        RunningMcpServer(server, worker)
    }

    private func serveHttp(ctx: HttpContext, path: String): Unit {
        var body = ""
        try {
            body = StringReader(ctx.request.body).readToEnd()
        } catch (e: Exception) {
            body = ""
        }
        let resp = handle(path, body)
        ctx.responseBuilder
            .status(resp.status)
            .header("Content-Type", "application/json")
            .body(resp.body)
    }

    private func dispatch(provider: McpToolProvider, body: String): McpHttpResponse {
        let parsed = parseObject(body)
        if (parsed.isNone()) {
            return McpHttpResponse(200u16, rpcError(None<JsonValue>, MCP_PARSE_ERROR, "JSON 解析失败"))
        }
        let request = parsed.getOrThrow()
        let id = request.get("id")
        // JSON-RPC 通知（无 id）不产生响应；MCP 的 notifications/initialized 走这里
        if (id.isNone()) {
            return McpHttpResponse(202u16, "")
        }
        let method = match (request.get("method")) {
            case Some(m: JsonString) => m.getValue()
            case _ => return McpHttpResponse(200u16, rpcError(id, MCP_INVALID_REQUEST, "缺少 method"))
        }
        var params = JsonObject()
        match (request.get("params")) {
            case Some(v) =>
                try {
                    params = v.asObject()
                } catch (e: JsonException) {
                    return McpHttpResponse(200u16, rpcError(id, MCP_INVALID_PARAMS, "params 必须是对象"))
                }
            case None => ()
        }
        match (method) {
            case "initialize" => McpHttpResponse(200u16, rpcResult(id, initializeResult()))
            case "ping" => McpHttpResponse(200u16, rpcResult(id, JsonObject()))
            case "tools/list" => handleList(provider, id)
            case "tools/call" => handleCall(provider, id, params)
            case _ => McpHttpResponse(200u16, rpcError(id, MCP_METHOD_NOT_FOUND, "未知方法: ${method}"))
        }
    }

    private func initializeResult(): JsonObject {
        let result = JsonObject()
        result.put("protocolVersion", JsonString(MCP_PROTOCOL_VERSION))
        let capabilities = JsonObject()
        capabilities.put("tools", JsonObject())
        result.put("capabilities", capabilities)
        let info = JsonObject()
        info.put("name", JsonString(serverName))
        info.put("version", JsonString(serverVersion))
        result.put("serverInfo", info)
        result
    }

    private func handleList(provider: McpToolProvider, id: Option<JsonValue>): McpHttpResponse {
        let tools = try {
            provider.list()
        } catch (e: Exception) {
            return McpHttpResponse(200u16, rpcError(id, MCP_INTERNAL_ERROR, "tools/list 失败: ${e.message}"))
        }
        var arr = JsonArray()
        for (t in tools) {
            arr = arr.add(descriptorJson(t))
        }
        let result = JsonObject()
        result.put("tools", arr)
        McpHttpResponse(200u16, rpcResult(id, result))
    }

    private func handleCall(provider: McpToolProvider, id: Option<JsonValue>, params: JsonObject): McpHttpResponse {
        var name = ""
        match (params.get("name")) {
            case Some(v) =>
                match (v) {
                    case s: JsonString => name = s.getValue()
                    case _ => return McpHttpResponse(200u16, rpcError(id, MCP_INVALID_PARAMS, "tools/call 的 name 必须是字符串"))
                }
            case None => return McpHttpResponse(200u16, rpcError(id, MCP_INVALID_PARAMS, "tools/call 缺少 name"))
        }
        // 未知工具必须明确拒绝，而不是交给 provider 猜
        var known = false
        let descriptors = try {
            provider.list()
        } catch (e: Exception) {
            return McpHttpResponse(200u16, rpcError(id, MCP_INTERNAL_ERROR, "tools/list 失败: ${e.message}"))
        }
        for (t in descriptors) {
            if (t.name == name) {
                known = true
            }
        }
        if (!known) {
            return McpHttpResponse(200u16, rpcError(id, MCP_INVALID_PARAMS, "未知工具: ${name}"))
        }
        let argsText = match (params.get("arguments")) {
            case Some(v) => v.toString()
            case None => "{}"
        }
        let ctx = McpCallContext(name, defaultTimeout)
        let promise = try {
            provider.call(name, argsText, ctx)
        } catch (e: Exception) {
            return McpHttpResponse(200u16, rpcError(id, MCP_INTERNAL_ERROR, "工具 ${name} 调用失败: ${e.message}"))
        }
        let text = try {
            promise.get(defaultTimeout)
        } catch (e: TypedAskTimeoutException) {
            // 超时是执行层错误，不是工具业务失败
            return McpHttpResponse(200u16,
                rpcError(id, MCP_INTERNAL_ERROR, "工具 ${name} 超时（${defaultTimeout.toMilliseconds()}ms）"))
        } catch (e: McpToolBusinessException) {
            // 业务失败作为内容返回：协议成功、内容表示失败
            return McpHttpResponse(200u16, rpcResult(id, contentResult(e.detail, true)))
        } catch (e: Exception) {
            return McpHttpResponse(200u16, rpcError(id, MCP_INTERNAL_ERROR, "工具 ${name} 失败: ${e.message}"))
        }
        McpHttpResponse(200u16, rpcResult(id, contentResult(text, false)))
    }
}

func parseObject(body: String): Option<JsonObject> {
    try {
        Some<JsonObject>(JsonValue.fromStr(body).asObject())
    } catch (e: JsonException) {
        None<JsonObject>
    }
}

func descriptorJson(t: ToolDescriptor): JsonObject {
    let obj = JsonObject()
    obj.put("name", JsonString(t.name))
    obj.put("description", JsonString(t.description))
    // schema 是使用方给的 JSON 文本；单个坏的 schema 不应让整个 tools/list 失败
    var schema: JsonValue = JsonObject()
    try {
        schema = JsonValue.fromStr(t.inputSchemaJson)
    } catch (e: JsonException) {
        schema = JsonObject()
    }
    obj.put("inputSchema", schema)
    obj
}

func contentResult(text: String, isError: Bool): JsonObject {
    let item = JsonObject()
    item.put("type", JsonString("text"))
    item.put("text", JsonString(text))
    var content = JsonArray()
    content = content.add(item)
    let result = JsonObject()
    result.put("content", content)
    if (isError) {
        result.put("isError", JsonBool(true))
    }
    result
}

func rpcResult(id: Option<JsonValue>, result: JsonValue): String {
    let obj = JsonObject()
    obj.put("jsonrpc", JsonString("2.0"))
    match (id) {
        case Some(v) => obj.put("id", v)
        case None => obj.put("id", JsonNull())
    }
    obj.put("result", result)
    obj.toString()
}

func rpcError(id: Option<JsonValue>, code: Int64, message: String): String {
    let error = JsonObject()
    error.put("code", JsonInt(code))
    error.put("message", JsonString(message))
    let obj = JsonObject()
    obj.put("jsonrpc", JsonString("2.0"))
    match (id) {
        case Some(v) => obj.put("id", v)
        case None => obj.put("id", JsonNull())
    }
    obj.put("error", error)
    obj.toString()
}
