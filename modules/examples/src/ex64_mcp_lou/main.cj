/*
 * CActor Ex 64 - MCP 工具服务挂载在 Lou 上（对标 protoactor-go 的 web/MCP 组合方式）
 *
 * 真实约束：
 * - AI Agent 的工具调用走 MCP（JSON-RPC over HTTP），但 HTTP 栈用项目已有的 Lou；
 * - 工具业务失败（参数不合法）应是"结果里 isError=true"，而不是协议错误；
 * - 未知工具/坏 JSON 必须明确报 JSON-RPC 错误码，不能静默。
 *
 * 组件：
 * - `cactor_platform.mcp.McpServer.handle(path, body)`（纯函数式单次处理，不重复建 HTTP 服务）
 * - `McpToolProvider` / `ToolDescriptor` / `McpToolBusinessException`
 * - Lou：POST /mcp、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. initialize 返回 serverInfo；tools/list 含 add_item / list_items；
 * 2. add_item 两次（2 + 3）→ list_items 得到 5；
 * 3. 非法 qty → result.isError=true（业务失败）；未知工具 → -32602；
 * 4. Lou POST /mcp 与直接 handle 一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex64_mcp_lou
 *   ./target/release/bin/cactor_examples.ex64_mcp_lou serve 18208
 */
package cactor_examples.ex64_mcp_lou

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.encoding.json.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.{TypedPromise, TypedPromises}
import cactor_platform.mcp.*

let WAIT = Duration.second * 20
let ADD_SCHEMA = #"{"type":"object","properties":{"sku":{"type":"string"},"qty":{"type":"integer"}},"required":["sku","qty"]}"#

/** 一个最小库存工具集：add_item / list_items，状态在进程内加锁维护 */
class InventoryProvider <: McpToolProvider {
    private let lock = Mutex()
    private let stock = HashMap<String, Int64>()

    public func list(): Array<ToolDescriptor> {
        [
            ToolDescriptor("add_item", "增加库存", ADD_SCHEMA),
            ToolDescriptor("list_items", "列出库存", #"{"type":"object"}"#)
        ]
    }

    public func call(name: String, argsJson: String, ctx: McpCallContext): TypedPromise<String> {
        match (name) {
            case "add_item" => addItem(argsJson)
            case "list_items" => TypedPromises.successful<String>(itemsJson())
            case _ => TypedPromises.failed<String>(McpToolBusinessException("未知工具: ${name}"))
        }
    }

    private func addItem(argsJson: String): TypedPromise<String> {
        let obj = try {
            JsonValue.fromStr(argsJson).asObject()
        } catch (e: JsonException) {
            return TypedPromises.failed<String>(McpToolBusinessException("arguments 不是 JSON 对象"))
        }
        let sku = match (obj.get("sku")) {
            case Some(s: JsonString) => s.getValue()
            case _ => ""
        }
        let qty = match (obj.get("qty")) {
            case Some(q: JsonInt) => q.getValue()
            case _ => 0
        }
        if (sku.isEmpty() || qty <= 0) {
            return TypedPromises.failed<String>(McpToolBusinessException("sku 必填且 qty 必须为正"))
        }
        let total = synchronized(lock) {
            let t = (stock.get(sku) ?? 0) + qty
            stock[sku] = t
            t
        }
        TypedPromises.successful<String>("{\"sku\":\"${sku}\",\"total\":${total}}")
    }

    private func itemsJson(): String {
        let sb = StringBuilder()
        sb.append("{")
        synchronized(lock) {
            var first = true
            for ((k, v) in stock) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${k}\":${v}")
            }
        }
        sb.append("}")
        sb.toString()
    }
}

func rpcBody(id: Int64, method: String, params: JsonObject): String {
    let obj = JsonObject()
    obj.put("jsonrpc", JsonString("2.0"))
    obj.put("id", JsonInt(id))
    obj.put("method", JsonString(method))
    obj.put("params", params)
    obj.toString()
}

func callParams(name: String, args: JsonObject): JsonObject {
    let params = JsonObject()
    params.put("name", JsonString(name))
    params.put("arguments", args)
    params
}

func criteria(args: Array<(String, Int64)>): JsonObject {
    let obj = JsonObject()
    for ((k, v) in args) {
        obj.put(k, JsonInt(v))
    }
    obj
}

func json(text: String): JsonObject {
    JsonValue.fromStr(text).asObject()
}

func resultOf(text: String): JsonObject {
    json(text).get("result").getOrThrow().asObject()
}

/** 取出 content[0].text */
func contentText(result: JsonObject): String {
    let content = result.get("content").getOrThrow().asArray()
    let first = content[0].asObject()
    first.get("text").getOrThrow().asString().getValue()
}

func isError(result: JsonObject): Bool {
    match (result.get("isError")) {
        case Some(b: JsonBool) => b.getValue()
        case _ => false
    }
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

func buildApp(mcp: McpServer, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/mcp", { ctx =>
        let body = String.fromUtf8(ctx.bodyRaw()).trimAscii()
        let resp = mcp.handle("/mcp", body)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(resp.body)
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func startServer(app: Lou): UInt16 {
    spawn { app.listen() }
    for (_ in 0..500) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                return s.port
            }
        }
        sleep(Duration.millisecond * 20)
    }
    0u16
}

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

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex64-mcp")
    let provider = InventoryProvider()
    let mcp = McpServer(serverName: "ex64-mcp", serverVersion: "1.0.0").mount("/mcp", provider)
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(mcp, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex64：端口绑定失败")
                return 1
            }
            println("ex64 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 握手与工具清单")
        let initResult = resultOf(mcp.handle("/mcp", rpcBody(1, "initialize", JsonObject())).body)
        let info = initResult.get("serverInfo").getOrThrow().asObject()
        c.check(info.get("name").getOrThrow().asString().getValue() == "ex64-mcp", "initialize 返回 serverInfo")
        let tools = resultOf(mcp.handle("/mcp", rpcBody(2, "tools/list", JsonObject())).body)
            .get("tools").getOrThrow().asArray()
        c.check(tools.size() == 2, "tools/list 返回 2 个工具")

        println("\n[2] 业务调用：add_item 累加")
        let add1 = resultOf(mcp.handle("/mcp", rpcBody(3, "tools/call",
            callParams("add_item", criteria([("qty", 2)])))).body)
        // 上面故意缺 sku：业务失败，但协议成功
        c.check(isError(add1), "缺 sku → isError=true：${contentText(add1)}")
        let args1 = JsonObject()
        args1.put("sku", JsonString("apple"))
        args1.put("qty", JsonInt(2))
        let add2 = resultOf(mcp.handle("/mcp", rpcBody(4, "tools/call", callParams("add_item", args1))).body)
        c.check(!isError(add2) && contentText(add2).contains("\"total\":2"), "第一次入库：${contentText(add2)}")
        let args2 = JsonObject()
        args2.put("sku", JsonString("apple"))
        args2.put("qty", JsonInt(3))
        let add3 = resultOf(mcp.handle("/mcp", rpcBody(5, "tools/call", callParams("add_item", args2))).body)
        c.check(contentText(add3).contains("\"total\":5"), "第二次入库累计 5：${contentText(add3)}")
        let list = resultOf(mcp.handle("/mcp", rpcBody(6, "tools/call",
            callParams("list_items", JsonObject()))).body)
        c.check(contentText(list).contains("\"apple\":5"), "list_items：${contentText(list)}")

        println("\n[3] 协议错误：未知工具")
        let unknown = json(mcp.handle("/mcp", rpcBody(7, "tools/call",
            callParams("nope", JsonObject()))).body)
        let code = unknown.get("error").getOrThrow().asObject().get("code").getOrThrow().asInt().getValue()
        c.check(code == MCP_INVALID_PARAMS, "未知工具 → -32602（实际 ${code}）")

        println("\n[4] Lou 挂载")
        let server = buildApp(mcp, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let args3 = JsonObject()
            args3.put("sku", JsonString("pear"))
            args3.put("qty", JsonInt(7))
            let resp = readBody(client.post("http://127.0.0.1:${port}/mcp",
                rpcBody(8, "tools/call", callParams("add_item", args3))))
            let louResult = resultOf(resp)
            c.check(contentText(louResult).contains("\"total\":7") && !isError(louResult),
                "POST /mcp：${contentText(louResult)}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex64：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        system.terminate()
    }
}
