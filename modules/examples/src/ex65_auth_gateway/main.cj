/*
 * CActor Ex 65 - Scoped Token 鉴权网关（对标平台 auth 能力 + Lou）
 *
 * 真实约束：
 * - 令牌必须自包含、可校验、可过期，服务端不存会话；
 * - 过期（401，客户端可重新鉴权）与篡改（401，只能拒绝）要能区分；
 * - 有身份但无权限是 403，不能一律 500 或 401；
 * - 路径覆盖要有边界：/data 不覆盖 /database。
 *
 * 组件：
 * - `ScopedTokenCodec.issue/verify`、`ScopedToken.allows/covers`、`AuthGate.check`
 * - Lou：POST /token、GET /data、POST /data、GET /admin、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 无令牌 → 401；read 令牌 GET /data → ok；同一令牌 POST /data → 403；
 * 2. `*` scope 可访问 /admin；过期令牌 → 401 且原因含"过期"；篡改 → 401"无效"；
 * 3. /data 令牌不能访问 /database（路径边界）；
 * 4. Lou 端点与直接调用结果一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex65_auth_gateway
 *   ./target/release/bin/cactor_examples.ex65_auth_gateway serve 18209
 */
package cactor_examples.ex65_auth_gateway

import std.collection.*
import std.convert.*
import std.time.*
import stdx.net.http.*
import stdx.net.http.{HttpRequestBuilder}
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_platform.auth.*

let SECRET = "ex65-demo-secret"
let GATE = AuthGate(SECRET)

func nowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

func tokenOf(subject: String, path: String, scopes: Array<String>, ttlMillis: Int64): String {
    ScopedTokenCodec.issue(SECRET, subject, path, scopes, ttlMillis: ttlMillis, nowMillis: nowMillis())
}

func decisionJson(d: AuthDecision): String {
    match (d) {
        case Allowed(t) => "{\"ok\":true,\"subject\":\"${t.subject}\"}"
        case Rejected(status, reason) => "{\"ok\":false,\"status\":${status},\"reason\":\"${reason}\"}"
    }
}

func gate(token: Option<String>, path: String, op: AccessOp): String {
    decisionJson(GATE.check(token, path, op))
}

/** 从 Authorization 头取令牌；接受 "Bearer xxx" 与裸令牌两种写法 */
func tokenFrom(header: Option<String>): Option<String> {
    match (header) {
        case Some(h) =>
            let text = h.trimAscii()
            if (text.startsWith("Bearer ")) {
                Some<String>(text[7..].trimAscii())
            } else {
                Some<String>(text)
            }
        case None => None<String>
    }
}

func buildApp(port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    // 演示用签发端点：真实系统由身份服务签发，业务服务只校验
    app.post("/token", { ctx =>
        let subject = ctx.query("subject") ?? "user"
        let path = ctx.query("path") ?? "/data"
        let scope = ctx.query("scope") ?? "read"
        let ttl = ctx.queryInt("ttl") ?? 600000
        let token = tokenOf(subject, path, [scope], ttl)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"token\":\"${token}\"}")
    })
    app.get("/data", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        let token = match (tokenFrom(ctx.getHeader("Authorization"))) {
            case Some(t) => Some<String>(t)
            case None => ctx.query("token")
        }
        ctx.writeString(gate(token, "/data", AccessOp.Read))
    })
    app.post("/data", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        let token = match (tokenFrom(ctx.getHeader("Authorization"))) {
            case Some(t) => Some<String>(t)
            case None => ctx.query("token")
        }
        ctx.writeString(gate(token, "/data", AccessOp.Write))
    })
    app.get("/admin", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        let token = match (tokenFrom(ctx.getHeader("Authorization"))) {
            case Some(t) => Some<String>(t)
            case None => ctx.query("token")
        }
        ctx.writeString(gate(token, "/admin", AccessOp.Call))
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

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex65-auth")
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex65：端口绑定失败")
                return 1
            }
            println("ex65 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        let readToken = tokenOf("alice", "/data", ["read"], 600000)
        let writeToken = tokenOf("bob", "/data", ["write"], 600000)
        let adminToken = tokenOf("root", "/admin", ["*"], 600000)
        let expired = tokenOf("alice", "/data", ["read"], -1)
        let tampered = readToken + "x"

        println("\n[1] 身份与权限")
        c.check(gate(None<String>, "/data", AccessOp.Read).contains("\"status\":401"), "无令牌 → 401")
        c.check(gate(Some<String>(readToken), "/data", AccessOp.Read).contains("\"ok\":true"), "read 令牌 GET /data")
        c.check(gate(Some<String>(writeToken), "/data", AccessOp.Read).contains("\"status\":403"),
            "write 令牌不能 GET /data → 403")
        c.check(gate(Some<String>(readToken), "/data", AccessOp.Write).contains("\"status\":403"),
            "read 令牌不能 POST /data → 403")

        println("\n[2] 通配、过期与篡改")
        c.check(gate(Some<String>(adminToken), "/admin", AccessOp.Call).contains("\"ok\":true"), "* scope 可访问 /admin")
        let exp = gate(Some<String>(expired), "/data", AccessOp.Read)
        c.check(exp.contains("\"status\":401") && exp.contains("过期"), "过期 → 401: ${exp}")
        let bad = gate(Some<String>(tampered), "/data", AccessOp.Read)
        c.check(bad.contains("\"status\":401") && bad.contains("无效"), "篡改 → 401 无效: ${bad}")

        println("\n[3] 路径覆盖有边界")
        c.check(gate(Some<String>(readToken), "/database", AccessOp.Read).contains("\"status\":403"),
            "/data 不覆盖 /database")
        let subtree = tokenOf("alice", "/data", ["read"], 600000)
        c.check(gate(Some<String>(subtree), "/data/sub", AccessOp.Read).contains("\"ok\":true"), "/data 覆盖 /data/sub")

        println("\n[4] 编解码器：Expired 与 Invalid 可区分")
        match (ScopedTokenCodec.verify(SECRET, expired, nowMillis: nowMillis())) {
            case Expired(_) => c.check(true, "verify 返回 Expired")
            case _ => c.check(false, "verify 返回 Expired")
        }
        match (ScopedTokenCodec.verify(SECRET, tampered, nowMillis: nowMillis())) {
            case Invalid(_) => c.check(true, "verify 返回 Invalid")
            case _ => c.check(false, "verify 返回 Invalid")
        }

        println("\n[5] Lou 端点")
        let server = buildApp(0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let unauth = readBody(client.get("http://127.0.0.1:${port}/data"))
            c.check(unauth.contains("\"status\":401"), "GET /data 无令牌 → ${unauth}")
            let issued = readBody(client.post("http://127.0.0.1:${port}/token?subject=carol&path=/data&scope=read&ttl=600000", ""))
            let token = issued.split("\"token\":\"")[1].split("\"")[0]
            // 令牌含 '|'，不能直接放 query（stdx URL 会拒绝），用 Authorization 头
            let getReq = HttpRequestBuilder().method("GET").url("http://127.0.0.1:${port}/data")
                .header("Authorization", "Bearer ${token}").build()
            let ok = readBody(client.send(getReq))
            c.check(ok.contains("\"ok\":true"), "GET /data 带令牌 → ${ok}")
            let denied = readBody(client.post("http://127.0.0.1:${port}/data", ""))
            c.check(denied.contains("\"status\":401"), "POST /data 无令牌 → ${denied}")
            let postReq = HttpRequestBuilder().method("POST").url("http://127.0.0.1:${port}/data")
                .header("Authorization", token).build()
            let denied2 = readBody(client.send(postReq))
            c.check(denied2.contains("\"status\":403"), "POST /data 仅 read → ${denied2}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex65：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        system.terminate()
    }
}
