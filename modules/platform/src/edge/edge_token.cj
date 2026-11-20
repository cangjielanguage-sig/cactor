/*
 * CActor Platform.Edge - 断线重连的恢复令牌（P3 / F4）
 *
 * Edge 节点必须是**无状态**的：连接本身不保存会话状态，重连时只凭一个自包含令牌
 * 就能恢复到"上一条已确认消息之后"。令牌因此要携带三样东西：
 *   - sessionId：防止 A 的令牌被拿去续读 B 的流；
 *   - lastAck：已确认的最大 offset，重连从 lastAck + 1 继续；
 *   - expiresAt：令牌有效期（离线太久重连需要重新走鉴权）。
 *
 * 签名直接复用 foundation 的 HMAC-SHA256（与远程握手同一套实现），不引入新密码学；
 * 校验用常数时间比较，避免令牌泄露时序。这里不是 G1 的 Scoped JWT——令牌只证明
 * "可以续读这条流"，真正的按 actor 授权在 AR8 的 `cactor_platform/auth` 里做。
 */
package cactor_platform.edge

import std.convert.*
import std.time.*
import cactor_foundation.hashing.{constantTimeEquals, fromHex, hmacSha256, toHex}

/** 默认令牌有效期：24 小时（客户端应在此期间内重连） */
public let DEFAULT_EDGE_TOKEN_TTL: Duration = Duration.hour * 24

/** 令牌版本前缀；格式变化时换版本而不是猜 */
public let EDGE_TOKEN_VERSION: String = "edge-v1"

/** 令牌校验结果：三种失败要能区分（过期可让客户端重新鉴权，篡改只能拒绝） */
public enum EdgeTokenResult {
    | Valid(String, Int64)
    | Expired(String)
    | Invalid(String)
}

func edgeNowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

func signEdgeToken(secret: String, payload: String): String {
    toHex(hmacSha256(secret.toArray(), payload.toArray()))
}

/**
 * 恢复令牌：`edge-v1|<hex(sessionId)>|<lastAck>|<expiresAtMillis>|<hmac>`
 *
 * sessionId 做十六进制编码，避免 '|'、换行等字符破坏格式；HMAC 覆盖前四段，
 * 改动任何一段（包括把 lastAck 改小重放）都会校验失败。
 */
public struct EdgeRecoveryToken {
    /** 签发令牌；ttlMillis <= 0 时立即过期（测试与主动失效用） */
    public static func issue(secret: String, sessionId: String, lastAck: Int64, ttlMillis!: Int64,
        nowMillis!: Int64): String {
        if (secret.isEmpty()) {
            throw IllegalArgumentException("恢复令牌密钥不能为空")
        }
        if (lastAck < 0) {
            throw IllegalArgumentException("lastAck 不能为负: ${lastAck}")
        }
        let payload = "${EDGE_TOKEN_VERSION}|${toHex(sessionId.toArray())}|${lastAck}|${nowMillis + ttlMillis}"
        "${payload}|${signEdgeToken(secret, payload)}"
    }

    /** 校验令牌：先验签再看过期，篡改的令牌不会因为"恰好过期"被误判 */
    public static func verify(secret: String, token: String, nowMillis!: Int64): EdgeTokenResult {
        if (secret.isEmpty()) {
            return Invalid("恢复令牌密钥不能为空")
        }
        let parts = token.split("|")
        if (parts.size != 5) {
            return Invalid("令牌格式不正确")
        }
        if (parts[0] != EDGE_TOKEN_VERSION) {
            return Invalid("不支持的令牌版本: ${parts[0]}")
        }
        let sidBytes = match (fromHex(parts[1])) {
            case Some(bytes) => bytes
            case None => return Invalid("令牌会话编码无效")
        }
        var sessionId = ""
        try {
            sessionId = String.fromUtf8(sidBytes)
        } catch (e: Exception) {
            return Invalid("令牌会话编码无效")
        }
        let lastAck = match (Int64.tryParse(parts[2])) {
            case Some(v) => v
            case None => return Invalid("令牌 lastAck 无效")
        }
        let expires = match (Int64.tryParse(parts[3])) {
            case Some(v) => v
            case None => return Invalid("令牌过期时间无效")
        }
        if (lastAck < 0) {
            return Invalid("令牌 lastAck 无效")
        }
        let expected = signEdgeToken(secret, "${parts[0]}|${parts[1]}|${parts[2]}|${parts[3]}")
        let actual = match (fromHex(parts[4])) {
            case Some(bytes) => bytes
            case None => return Invalid("令牌签名无效")
        }
        let expectedBytes = match (fromHex(expected)) {
            case Some(bytes) => bytes
            case None => return Invalid("令牌签名无效")
        }
        if (!constantTimeEquals(expectedBytes, actual)) {
            return Invalid("令牌签名不匹配")
        }
        if (nowMillis > expires) {
            return Expired(sessionId)
        }
        Valid(sessionId, lastAck)
    }
}
