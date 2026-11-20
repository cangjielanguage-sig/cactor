/*
 * CActor Platform.Auth - Scoped JWT（按 actor 授权的令牌，P6 / G1）
 *
 * 远程层解决的是"节点之间能不能连"（secureCookie HMAC 握手、TLS 双向认证）；
 * 这里解决"客户端能碰哪个 actor"：令牌载荷含 `{ subject, actorPath, scopes, exp }`，
 * 只能访问 actorPath（含子路径）且操作在 scopes 内。
 *
 * 不引入新密码学：签名复用 foundation 的 HMAC-SHA256，密钥可用远程的 `secureCookie`，
 * 校验用常数时间比较。格式是紧凑的自定义 HMAC 令牌（不是 RFC 7519 的 alg/kid 头）：
 *   `scim-v1|hex(subject)|hex(actorPath)|hex(scopes)|expiresAtMillis|hex(hmac)`
 * scopes 用逗号分隔（`call,read,write`，`*` 表示全部）。
 *
 * 接入点：Edge 握手（主入口，`EdgeRouter` 可选开启）；其他入口按需调用 `AuthGate`，
 * 默认不开启时既有远程/集群行为不变。
 */
package cactor_platform.auth

import std.collection.*
import std.convert.*
import std.time.*
import cactor_foundation.hashing.{constantTimeEquals, fromHex, hmacSha256, toHex}

/** 访问操作；令牌 scopes 里的名字分别是 call / read / write */
public enum AccessOp {
    | Call
    | Read
    | Write
}

public func accessOpName(op: AccessOp): String {
    match (op) {
        case Call => "call"
        case Read => "read"
        case Write => "write"
    }
}

func authNowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

/** 令牌允许的公共场所（令牌一旦签发不可修改） */
public class ScopedToken {
    public let subject: String
    public let actorPath: String
    public let scopes: Array<String>
    public let expiresAtMillis: Int64

    public init(subject: String, actorPath: String, scopes: Array<String>, expiresAtMillis: Int64) {
        this.subject = subject
        this.actorPath = actorPath
        this.scopes = scopes
        this.expiresAtMillis = expiresAtMillis
    }

    /** 是否允许该操作；`*` 表示全部 */
    public func allows(op: AccessOp): Bool {
        let name = accessOpName(op)
        for (s in scopes) {
            if (s == "*" || s == name) {
                return true
            }
        }
        false
    }

    /** 路径覆盖：精确匹配或位于该路径之下（带边界，`/user/a` 不覆盖 `/user/ab`） */
    public func covers(targetPath: String): Bool {
        if (targetPath == actorPath) {
            return true
        }
        targetPath.startsWith(actorPath + "/")
    }

    public func expired(nowMillis: Int64): Bool {
        nowMillis > expiresAtMillis
    }
}

/** 校验结果：过期与非法要可区分（客户端对过期可以重新鉴权，篡改只能拒绝） */
public enum ScopedTokenResult {
    | Valid(ScopedToken)
    | Expired(ScopedToken)
    | Invalid(String)
}

/** 授权判定（可插拔）：默认实现 = 路径覆盖 + scope 匹配 */
public interface TokenAuthorizer {
    func authorize(token: ScopedToken, targetPath: String, op: AccessOp): Bool
}

public class ScopedTokenAuthorizer <: TokenAuthorizer {
    public init() {}

    public func authorize(token: ScopedToken, targetPath: String, op: AccessOp): Bool {
        token.covers(targetPath) && token.allows(op)
    }
}

/** 鉴权/授权结果：拒绝必须带状态码（401 身份问题、403 权限问题），不能是 500 */
public enum AuthDecision {
    | Allowed(ScopedToken)
    | Rejected(Int64, String)
}

const SCOPED_TOKEN_VERSION = "scim-v1"

func signScoped(secret: String, payload: String): String {
    toHex(hmacSha256(secret.toArray(), payload.toArray()))
}

func decodeHexText(text: String): Option<String> {
    match (fromHex(text)) {
        case Some(bytes) =>
            try {
                Some<String>(String.fromUtf8(bytes))
            } catch (e: Exception) {
                None<String>
            }
        case None => None<String>
    }
}

func joinScopes(scopes: Array<String>): String {
    let sb = StringBuilder()
    for (i in 0..scopes.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append(scopes[i])
    }
    sb.toString()
}

/**
 * Scoped 令牌的签发与校验。
 *
 * 令牌自包含，服务端无需存储；`verify` 只做签名与过期，授权由 `TokenAuthorizer` 决定。
 */
public struct ScopedTokenCodec {
    /** 签发；ttlMillis <= 0 表示立即过期（主动失效/测试） */
    public static func issue(secret: String, subject: String, actorPath: String, scopes: Array<String>,
        ttlMillis!: Int64, nowMillis!: Int64): String {
        if (secret.isEmpty()) {
            throw IllegalArgumentException("Scoped 令牌密钥不能为空")
        }
        if (subject.isEmpty()) {
            throw IllegalArgumentException("Scoped 令牌 subject 不能为空")
        }
        if (actorPath.isEmpty()) {
            throw IllegalArgumentException("Scoped 令牌 actorPath 不能为空")
        }
        for (s in scopes) {
            if (s.isEmpty() || s.contains(",") || s.contains("|")) {
                throw IllegalArgumentException("scope 不能为空，也不能包含 ',' 或 '|': ${s}")
            }
        }
        let payload = "${SCOPED_TOKEN_VERSION}|${toHex(subject.toArray())}|${toHex(actorPath.toArray())}|" +
            "${toHex(joinScopes(scopes).toArray())}|${nowMillis + ttlMillis}"
        "${payload}|${signScoped(secret, payload)}"
    }

    /** 校验：先验签再判过期，篡改的令牌不会因为"恰好过期"被误判 */
    public static func verify(secret: String, token: String, nowMillis!: Int64): ScopedTokenResult {
        if (secret.isEmpty()) {
            return Invalid("Scoped 令牌密钥不能为空")
        }
        let parts = token.split("|")
        if (parts.size != 6 || parts[0] != SCOPED_TOKEN_VERSION) {
            return Invalid("令牌格式不正确")
        }
        let subject = match (decodeHexText(parts[1])) {
            case Some(v) => v
            case None => return Invalid("令牌 subject 编码无效")
        }
        let actorPath = match (decodeHexText(parts[2])) {
            case Some(v) => v
            case None => return Invalid("令牌 actorPath 编码无效")
        }
        let scopesText = match (decodeHexText(parts[3])) {
            case Some(v) => v
            case None => return Invalid("令牌 scopes 编码无效")
        }
        let expires = match (Int64.tryParse(parts[4])) {
            case Some(v) => v
            case None => return Invalid("令牌过期时间无效")
        }
        let expected = signScoped(secret, "${parts[0]}|${parts[1]}|${parts[2]}|${parts[3]}|${parts[4]}")
        let expectedBytes = match (fromHex(expected)) {
            case Some(b) => b
            case None => return Invalid("令牌签名无效")
        }
        let actualBytes = match (fromHex(parts[5])) {
            case Some(b) => b
            case None => return Invalid("令牌签名无效")
        }
        if (!constantTimeEquals(expectedBytes, actualBytes)) {
            return Invalid("令牌签名不匹配")
        }
        let scopes = if (scopesText.isEmpty()) { Array<String>() } else { scopesText.split(",") }
        let tokenValue = ScopedToken(subject, actorPath, scopes, expires)
        if (tokenValue.expired(nowMillis)) {
            return Expired(tokenValue)
        }
        Valid(tokenValue)
    }
}

/**
 * 鉴权与授权的组合门面：先校验令牌（401），再判断路径与 scopes（403）。
 *
 * 默认实现不开启时完全不参与任何路径；Edge 等入口按需持有 `AuthGate`。
 */
public class AuthGate {
    private let secret: String
    private let authorizer: TokenAuthorizer

    public init(secret: String, authorizer!: TokenAuthorizer = ScopedTokenAuthorizer()) {
        if (secret.isEmpty()) {
            throw IllegalArgumentException("AuthGate 密钥不能为空")
        }
        this.secret = secret
        this.authorizer = authorizer
    }

    /** 身份校验：缺失/非法/过期 → 401 */
    public func authenticate(tokenText: Option<String>): AuthDecision {
        match (tokenText) {
            case None => Rejected(401, "缺少访问令牌")
            case Some(text) =>
                match (ScopedTokenCodec.verify(secret, text, nowMillis: authNowMillis())) {
                    case Valid(t) => Allowed(t)
                    case Expired(_) => Rejected(401, "访问令牌已过期")
                    case Invalid(reason) => Rejected(401, "访问令牌无效: ${reason}")
                }
        }
    }

    /** 授权：路径前缀 + scope */
    public func authorizeToken(token: ScopedToken, targetPath: String, op: AccessOp): Bool {
        authorizer.authorize(token, targetPath, op)
    }

    /** 校验 + 授权一步完成 */
    public func check(tokenText: Option<String>, targetPath: String, op: AccessOp): AuthDecision {
        match (authenticate(tokenText)) {
            case Allowed(t) =>
                if (authorizer.authorize(t, targetPath, op)) {
                    Allowed(t)
                } else {
                    Rejected(403, "访问令牌无权访问 ${targetPath}")
                }
            case Rejected(status, reason) => Rejected(status, reason)
        }
    }
}
