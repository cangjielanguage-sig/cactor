/*
 * CActor Platform.Edge - 传输无关的协议适配（P3 / F4）
 *
 * Edge 的语义在 `edge_router.cj`；这里只把"帧"和"头"映射到语义，不绑定具体服务端框架：
 *   - SSE：每条记录一个 `id: <offset>` + 多行 `data:`，客户端按 id 续连；
 *     单条 SSE 长连接有 60s 驻留上限（`MAX_SSE_RESIDENCE`），到期由服务端主动结束，
 *     客户端带令牌重连——避免代理层把长连接当成僵死连接；
 *   - WebSocket：一条文本帧一条记录（首行 offset，其余为载荷），双向各用各的帧；
 *   - 握手：`GET /v1/edge/{sessionId}` + `edge-resume-token` 头 → 连接或明确拒绝。
 *
 * 真实服务端（stdx.net.http / 任意框架）只需把请求填入 `EdgeRequest`、把返回的头写回响应，
 * 与 Durable Streams 的协议适配保持一致的做法。
 */
package cactor_platform.edge

import std.collection.*
import std.convert.*
import std.time.*
import cactor_platform.durable.{StreamRecord}

/** SSE 单连接驻留上限：超过后服务端主动结束，让客户端走重连令牌续传 */
public let MAX_SSE_RESIDENCE: Duration = Duration.second * 60

public let HEADER_RESUME_TOKEN: String = "edge-resume-token"
public let HEADER_SESSION_ID: String = "edge-session-id"
public let HEADER_LAST_ACK: String = "edge-last-ack"
public let HEADER_EDGE_NEXT_OFFSET: String = "edge-next-offset"

/** 一条 WebSocket 出站帧：offset 用作客户端确认与去重 */
public struct EdgeWsFrame {
    public let offset: Int64
    public let payload: String

    public init(offset: Int64, payload: String) {
        this.offset = offset
        this.payload = payload
    }
}

/** SSE 编码：`id` 携带 offset，`data` 按行拆分（SSE 规范要求每行一个 data 字段） */
public class EdgeSse {
    public static func encode(records: Array<StreamRecord>): String {
        let sb = StringBuilder()
        for (r in records) {
            sb.append("id: ${r.offset}\n")
            for (line in r.payload.split("\n")) {
                sb.append("data: ${line}\n")
            }
            sb.append("\n")
        }
        sb.toString()
    }

    /** 长轮询没有新消息时的保活注释（SSE 注释行客户端会忽略） */
    public static func keepAlive(): String {
        ": keep-alive\n\n"
    }

    /** 连接是否已到 60s 驻留上限，应当结束并让客户端重连 */
    public static func shouldRecycle(connectedAt: MonoTime, now!: MonoTime = MonoTime.now()): Bool {
        now - connectedAt >= MAX_SSE_RESIDENCE
    }
}

/** WebSocket 文本帧编解码：首行 offset，余下全部是载荷（允许载荷含换行） */
public class EdgeWs {
    public static func encode(records: Array<StreamRecord>): Array<String> {
        let out = ArrayList<String>()
        for (r in records) {
            out.add("${r.offset}\n${r.payload}")
        }
        out.toArray()
    }

    public static func encodeOne(record: StreamRecord): String {
        "${record.offset}\n${record.payload}"
    }

    public static func decode(text: String): Option<EdgeWsFrame> {
        match (text.indexOf("\n")) {
            case Some(i) =>
                match (Int64.tryParse(text[..i])) {
                    case Some(offset) => Some<EdgeWsFrame>(EdgeWsFrame(offset, text[(i + 1)..]))
                    case None => None<EdgeWsFrame>
                }
            case None => None<EdgeWsFrame>
        }
    }
}

/** 与框架无关的接入请求（只需要 method/path/headers） */
public struct EdgeRequest {
    public let method: String
    public let path: String
    public let headers: HashMap<String, String>

    public init(method: String, path: String, headers!: HashMap<String, String> = HashMap<String, String>()) {
        this.method = method
        this.path = path
        this.headers = headers
    }
}

public struct EdgeUpgrade {
    public let connection: EdgeConnection
    public let headers: HashMap<String, String>

    public init(connection: EdgeConnection, headers: HashMap<String, String>) {
        this.connection = connection
        this.headers = headers
    }
}

/** 升级结果：拒绝带明确状态码（401 过期/缺凭证、403 令牌不匹配、404 会话不存在） */
public enum EdgeUpgradeResult {
    | Upgraded(EdgeUpgrade)
    | Rejected(Int64, String)
}

/**
 * 握手：`GET /v1/edge/{sessionId}`，可选 `edge-resume-token` 头。
 *
 * 成功时返回连接与响应头（含新的重连令牌与 next-offset）；客户端把响应头原样保留，
 * 断线后带令牌重连即可从 lastAck + 1 继续。
 */
public class EdgeGateway {
    private let router: EdgeRouter
    private let basePath: String

    public init(router: EdgeRouter, basePath!: String = "/v1/edge") {
        this.router = router
        this.basePath = basePath
    }

    public func upgrade(request: EdgeRequest): EdgeUpgradeResult {
        if (request.method != "GET") {
            return EdgeUpgradeResult.Rejected(405, "method not allowed")
        }
        let sessionId = pathTail(request.path)
        if (sessionId.isEmpty()) {
            return EdgeUpgradeResult.Rejected(404, "missing session id")
        }
        let token = request.headers.get(HEADER_RESUME_TOKEN)
        match (router.connect(sessionId, resumeToken: token)) {
            case Connected(conn) =>
                let headers = HashMap<String, String>()
                headers[HEADER_SESSION_ID] = sessionId
                headers[HEADER_LAST_ACK] = conn.lastAckedOffset().toString()
                headers[HEADER_EDGE_NEXT_OFFSET] = (conn.lastAckedOffset() + 1).toString()
                headers[HEADER_RESUME_TOKEN] = conn.recoveryToken()
                EdgeUpgradeResult.Upgraded(EdgeUpgrade(conn, headers))
            case Unauthorized(status, reason) =>
                EdgeUpgradeResult.Rejected(status, reason)
        }
    }

    private func pathTail(fullPath: String): String {
        let prefix = if (basePath.endsWith("/")) { basePath } else { basePath + "/" }
        if (!fullPath.startsWith(prefix)) {
            return ""
        }
        fullPath[prefix.size..]
    }
}
