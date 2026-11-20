/*
 * CActor Platform.MCP - MCP server（P5 / AR7）
 *
 * 覆盖：
 *   1. `mcpServerServesToolList`：真实 stdx HTTP 客户端完成 initialize 握手与 tools/list，
 *      schema 原样返回；通知（无 id）得到 202；
 *   2. `mcpServerDispatchesToolCall`：tools/call 经 AskInbox 触发真实 Actor，结果写回 content；
 *   3. `mcpServerMapsErrorsExplicitly`：未知工具 -32602、超时 -32603、业务失败 isError=true、
 *      未知方法 -32601、坏 JSON -32700。
 */
package cactor_tests.unit.platform.mcp

import std.collection.*
import std.io.{StringReader}
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import stdx.encoding.json.*
import stdx.net.http.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.dsl.{AskInbox}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedPromise}
import cactor_platform.mcp.*
import cactor_testkit.*

let TOOL_SCHEMA = #"{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}"#

class McpEchoActor <: Actor {
    private let lock = Mutex()
    private var count: Int64 = 0

    public prop name: String {
        get() { "mcp-echo" }
    }

    public prop description: String {
        get() { "mcp echo actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                let n = synchronized(lock) {
                    count += 1
                    count
                }
                context.reply(StringMessage("echo:${n}:${s.getContent()}"))
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

/** 用闭包描述行为的 provider，避免每个用例都写一个类 */
class ScriptedToolProvider <: McpToolProvider {
    private let tools: Array<ToolDescriptor>
    private let handler: (String, String) -> TypedPromise<String>

    init(tools: Array<ToolDescriptor>, handler: (String, String) -> TypedPromise<String>) {
        this.tools = tools
        this.handler = handler
    }

    public func list(): Array<ToolDescriptor> {
        tools
    }

    public func call(name: String, argsJson: String, ctx: McpCallContext): TypedPromise<String> {
        handler(name, argsJson)
    }
}

func mcpRpcBody(id: Int64, method: String, params: JsonObject): String {
    let obj = JsonObject()
    obj.put("jsonrpc", JsonString("2.0"))
    obj.put("id", JsonInt(id))
    obj.put("method", JsonString(method))
    obj.put("params", params)
    obj.toString()
}

func mcpRpcNotification(method: String): String {
    let obj = JsonObject()
    obj.put("jsonrpc", JsonString("2.0"))
    obj.put("method", JsonString(method))
    obj.toString()
}

func mcpCallParams(name: String, args: JsonObject): JsonObject {
    let params = JsonObject()
    params.put("name", JsonString(name))
    params.put("arguments", args)
    params
}

func mcpParse(text: String): JsonObject {
    JsonValue.fromStr(text).asObject()
}

func mcpErrorCode(resp: JsonObject): Int64 {
    match (resp.get("error")) {
        case Some(v) => v.asObject().get("code").getOrThrow().asInt().getValue()
        case None => -1
    }
}

func mcpResult(resp: JsonObject): JsonObject {
    resp.get("result").getOrThrow().asObject()
}

func mcpPost(client: Client, port: UInt16, body: String): String {
    let resp = client.post("http://127.0.0.1:${port}/mcp", body)
    @Expect(resp.status, 200)
    StringReader(resp.body).readToEnd()
}

@Test
public class McpServerTest {
    @TestCase
    public func mcpServerServesToolList(): Unit {
        let provider = ScriptedToolProvider([ToolDescriptor("echo", "回显", TOOL_SCHEMA)],
            { name: String, args: String => TypedPromise<String>() })
        let server = McpServer().mount("/mcp", provider)
        let running = server.start()
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 5).build()
        try {
            let initBody = mcpRpcBody(1, "initialize", JsonObject())
            let initResp = mcpParse(mcpPost(client, running.port, initBody))
            let initResult = mcpResult(initResp)
            @Expect(initResult.get("protocolVersion").getOrThrow().asString().getValue(), MCP_PROTOCOL_VERSION)
            @Expect(initResult.get("capabilities").getOrThrow().asObject().containsKey("tools"), true)

            let listBody = mcpRpcBody(2, "tools/list", JsonObject())
            let listed = mcpResult(mcpParse(mcpPost(client, running.port, listBody)))
            let tools = listed.get("tools").getOrThrow().asArray()
            @Expect(tools.size(), 1)
            let tool = tools[0].asObject()
            @Expect(tool.get("name").getOrThrow().asString().getValue(), "echo")
            @Expect(tool.get("inputSchema").getOrThrow().asObject().get("type").getOrThrow().asString().getValue(), "object")

            // 通知（无 id）不产生 JSON-RPC 响应
            let noteResp = client.post("http://127.0.0.1:${running.port}/mcp", mcpRpcNotification("notifications/initialized"))
            @Expect(noteResp.status, 202)
        } finally {
            client.close()
            running.close()
        }
    }

    @TestCase
    public func mcpServerDispatchesToolCall(): Unit {
        let kit = ActorTestKit("mcp-dispatch")
        let actor = McpEchoActor()
        let ref = kit.actorOf({ => actor }, "mcp-echo-actor")
        let provider = ScriptedToolProvider([ToolDescriptor("echo", "回显", TOOL_SCHEMA)], { name: String, args: String =>
            let promise = TypedPromise<String>()
            let parsed = JsonValue.fromStr(args).asObject()
            let text = match (parsed.get("text")) {
                case Some(v) => v.asString().getValue()
                case None => ""
            }
            let reply = AskInbox.ask(ref, StringMessage(text)).get(Duration.second * 3)
            match (reply) {
                case s: StringMessage => promise.trySuccess(s.getContent())
                case _ => promise.trySuccess(reply.messageType())
            }
            promise
        })
        let server = McpServer().mount("/mcp", provider)
        let running = server.start()
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 5).build()
        try {
            let args = JsonObject()
            args.put("text", JsonString("hi"))
            let first = mcpResult(mcpParse(mcpPost(client, running.port,
                mcpRpcBody(1, "tools/call", mcpCallParams("echo", args)))))
            let content = first.get("content").getOrThrow().asArray()
            @Expect(content.size(), 1)
            @Expect(content[0].asObject().get("text").getOrThrow().asString().getValue(), "echo:1:hi")

            args.put("text", JsonString("again"))
            let second = mcpResult(mcpParse(mcpPost(client, running.port,
                mcpRpcBody(2, "tools/call", mcpCallParams("echo", args)))))
            let content2 = second.get("content").getOrThrow().asArray()
            @Expect(content2[0].asObject().get("text").getOrThrow().asString().getValue(), "echo:2:again")
        } finally {
            client.close()
            running.close()
            kit.shutdown()
        }
    }

    @TestCase
    public func mcpServerMapsErrorsExplicitly(): Unit {
        // 未知工具：provider 声明 echo，调用 nope
        let known = ScriptedToolProvider([ToolDescriptor("echo", "回显", TOOL_SCHEMA)], { name: String, args: String =>
            let p = TypedPromise<String>()
            p.trySuccess("ok")
            p
        })
        let server = McpServer().mount("/mcp", known)
        let unknown = mcpParse(server.handle("/mcp",
            mcpRpcBody(1, "tools/call", mcpCallParams("nope", JsonObject()))).body)
        @Expect(mcpErrorCode(unknown), MCP_INVALID_PARAMS)
        // 未知方法
        let badMethod = mcpParse(server.handle("/mcp", mcpRpcBody(2, "resources/list", JsonObject())).body)
        @Expect(mcpErrorCode(badMethod), MCP_METHOD_NOT_FOUND)
        // 坏 JSON / 未知路径
        @Expect(mcpErrorCode(mcpParse(server.handle("/mcp", "{not json").body)), MCP_PARSE_ERROR)
        @Expect(server.handle("/other", "{}").status, 404)

        // 超时：promise 永不完成，默认 150ms 兜底
        let slow = ScriptedToolProvider([ToolDescriptor("slow", "慢", TOOL_SCHEMA)], { name: String, args: String =>
            TypedPromise<String>()
        })
        let slowServer = McpServer(defaultTimeout: Duration.millisecond * 150).mount("/mcp", slow)
        let timedOut = mcpParse(slowServer.handle("/mcp",
            mcpRpcBody(3, "tools/call", mcpCallParams("slow", JsonObject()))).body)
        @Expect(mcpErrorCode(timedOut), MCP_INTERNAL_ERROR)

        // 业务失败：正常 result + isError=true，而不是协议错误
        let failing = ScriptedToolProvider([ToolDescriptor("fail", "失败", TOOL_SCHEMA)], { name: String, args: String =>
            let p = TypedPromise<String>()
            p.tryFailure(McpToolBusinessException("参数不合法"))
            p
        })
        let failServer = McpServer().mount("/mcp", failing)
        let business = mcpParse(failServer.handle("/mcp",
            mcpRpcBody(4, "tools/call", mcpCallParams("fail", JsonObject()))).body)
        let result = mcpResult(business)
        @Expect(result.get("isError").getOrThrow().asBool().getValue(), true)
        let content = result.get("content").getOrThrow().asArray()
        @Expect(content[0].asObject().get("text").getOrThrow().asString().getValue(), "参数不合法")
    }
}
