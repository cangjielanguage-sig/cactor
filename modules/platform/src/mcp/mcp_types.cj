/*
 * CActor Platform.MCP - 工具协议原语（P5 / AR7）
 *
 * CActor 只做 MCP **server** 的协议与调度，不定义"工具是什么"：
 *   - `McpToolProvider` 由使用方实现：列工具（name/description/JSON Schema）与执行调用；
 *   - `McpServer` 把 `tools/list` / `tools/call` 映射到 provider，错误形态显式区分：
 *       未知工具 → JSON-RPC -32602（invalid_params）
 *       执行超时 / provider 抛异常 → -32603（internal_error）
 *       工具业务失败（McpToolBusinessException）→ 正常 result，`isError: true` 的内容
 *   - 传输用 stdx.net.http，不新建 HTTP 栈；JSON 用 stdx.encoding.json。
 *
 * 分工：Agent 语义（LLM 调用、agent loop、工具实现）在 ~/pix/pic；这里只接收 pic 的
 * MCP client 调用（stdio 之外最常用的 streamable HTTP 形态）。
 */
package cactor_platform.mcp

import std.time.*
import cactor_core.typed.{TypedPromise}

/** MCP 协议版本（与主流客户端握手的稳定版本） */
public let MCP_PROTOCOL_VERSION: String = "2024-11-05"

/** JSON-RPC 错误码（MCP 沿用 JSON-RPC 2.0） */
public let MCP_PARSE_ERROR: Int64 = -32700
public let MCP_INVALID_REQUEST: Int64 = -32600
public let MCP_METHOD_NOT_FOUND: Int64 = -32601
public let MCP_INVALID_PARAMS: Int64 = -32602
public let MCP_INTERNAL_ERROR: Int64 = -32603

/** 工具描述：schema 由使用方给 JSON 文本，CActor 不解析工具语义 */
public struct ToolDescriptor {
    public let name: String
    public let description: String
    public let inputSchemaJson: String

    public init(name: String, description: String, inputSchemaJson: String) {
        this.name = name
        this.description = description
        this.inputSchemaJson = inputSchemaJson
    }
}

/**
 * 一次工具调用的上下文：工具实现可据此判断剩余预算。
 * 超时由 server 侧统一兜底（provider 即使不检查也不会拖住连接）。
 */
public class McpCallContext {
    private let toolNameValue: String
    private let deadlineValue: MonoTime

    public init(toolName: String, timeout: Duration) {
        this.toolNameValue = toolName
        this.deadlineValue = MonoTime.now() + timeout
    }

    public prop toolName: String {
        get() { toolNameValue }
    }

    /** 剩余预算（可能为负，表示已超时） */
    public func remaining(): Duration {
        deadlineValue - MonoTime.now()
    }

    public func expired(): Bool {
        remaining() <= Duration.Zero
    }
}

/**
 * 工具业务失败：作为内容（`isError: true`）返回给客户端，而不是协议错误。
 * 例如"参数校验不通过""目标资源不存在"——调用本身成功了，只是结果表示失败。
 */
public class McpToolBusinessException <: Exception {
    public let detail: String

    public init(detail: String) {
        super(detail)
        this.detail = detail
    }
}

/**
 * 工具提供方（由使用方实现）。
 *
 * `call` 返回 `TypedPromise<String>`：实现可以在 Actor 内异步完成，也可以用
 * `cactor_platform.exec.SupervisedCall` 执行可能阻塞的工具；server 只在该 Promise
 * 上等待到超时，并在超时后向调用方返回 -32603。
 */
public interface McpToolProvider {
    func list(): Array<ToolDescriptor>
    func call(name: String, argsJson: String, ctx: McpCallContext): TypedPromise<String>
}
