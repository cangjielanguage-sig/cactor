/*
 * CActor Platform.Auth - Scoped JWT（P6 / G1）
 *
 * 覆盖：
 *   1. scopedTokenAllowsOnlyItsActor：令牌只覆盖指定 actor 路径（含子路径，不含兄弟路径），
 *      越权是 403 而不是静默丢弃；
 *   2. scopedTokenExpires：过期令牌 → 401（与签名不符的 401 可区分 message）；
 *   3. edgeRejectsUnscopedToken：Edge 开启 AuthGate 后，无令牌 401、路径不匹配 403；
 *   4. 篡改/越 scope 的令牌被拒绝。
 */
package cactor_tests.unit.platform.auth

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.auth.*
import cactor_platform.edge.*
import cactor_testkit.*

func authNow(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

class AuthProbeActor <: Actor {
    private let lock = Mutex()
    private let seen = ArrayList<String>()

    public prop name: String {
        get() { "auth-probe" }
    }

    public prop description: String {
        get() { "auth probe actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                synchronized(lock) {
                    seen.add(s.getContent())
                }
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    func count(): Int64 {
        synchronized(lock) {
            seen.size
        }
    }
}

@Test
public class ScopedTokenTest {
    @TestCase
    public func scopedTokenAllowsOnlyItsActor(): Unit {
        let now = 1_700_000_000_000
        let token = ScopedTokenCodec.issue("auth-secret", "alice", "/user/session-a", ["call", "read"],
            ttlMillis: 60_000, nowMillis: now)
        match (ScopedTokenCodec.verify("auth-secret", token, nowMillis: now + 1000)) {
            case Valid(t) =>
                @Expect(t.covers("/user/session-a"), true)
                @Expect(t.covers("/user/session-a/child"), true)
                @Expect(t.covers("/user/session-b"), false)
                @Expect(t.covers("/user/session-ab"), false)
                @Expect(t.allows(AccessOp.Call), true)
                @Expect(t.allows(AccessOp.Write), false)
            case Expired(_) => @Fail("令牌不应过期")
            case Invalid(reason) => @Fail("令牌应有效: ${reason}")
        }
        let gate = AuthGate("auth-secret")
        // 固定时间的令牌只用于确定性校验；gate 用真实时钟，单独签发一个当前有效的
        let live = ScopedTokenCodec.issue("auth-secret", "alice", "/user/session-a", ["call", "read"],
            ttlMillis: 60_000, nowMillis: authNow())
        match (gate.check(Some(live), "/user/session-a", AccessOp.Call)) {
            case Allowed(_) => ()
            case Rejected(_, reason) => @Fail("自己的 actor 应允许: ${reason}")
        }
        match (gate.check(Some(live), "/user/session-b", AccessOp.Call)) {
            case Allowed(_) => @Fail("越权访问必须被拒绝")
            case Rejected(status, _) => @Expect(status, 403)
        }
        match (gate.check(Some(live), "/user/session-a", AccessOp.Write)) {
            case Allowed(_) => @Fail("未授予的 scope 必须被拒绝")
            case Rejected(status, _) => @Expect(status, 403)
        }
    }

    @TestCase
    public func scopedTokenExpires(): Unit {
        let expired = ScopedTokenCodec.issue("auth-secret", "alice", "/user/session-a", ["call"],
            ttlMillis: -1, nowMillis: 1000)
        match (ScopedTokenCodec.verify("auth-secret", expired, nowMillis: 1001)) {
            case Expired(t) => @Expect(t.actorPath, "/user/session-a")
            case Valid(_) => @Fail("过期令牌不应有效")
            case Invalid(reason) => @Fail("过期不应被判为非法: ${reason}")
        }
        let gate = AuthGate("auth-secret")
        match (gate.check(Some(expired), "/user/session-a", AccessOp.Call)) {
            case Rejected(status, _) => @Expect(status, 401)
            case Allowed(_) => @Fail("过期令牌必须被拒绝")
        }
        // 篡改过期时间会破坏签名
        let parts = expired.split("|")
        let tampered = "${parts[0]}|${parts[1]}|${parts[2]}|${parts[3]}|9999999999999|${parts[5]}"
        match (ScopedTokenCodec.verify("auth-secret", tampered, nowMillis: 1001)) {
            case Invalid(_) => ()
            case Valid(_) => @Fail("篡改的令牌不应有效")
            case Expired(_) => @Fail("篡改应判为非法而不是过期")
        }
    }

    @TestCase
    public func edgeRejectsUnscopedToken(): Unit {
        let kit = ActorTestKit("auth-edge")
        let probe = AuthProbeActor()
        let ref = kit.actorOf({ => probe }, "auth-session-a")
        let resolver = LocalEdgeResolver()
        resolver.bind("a", ref)
        let gate = AuthGate("auth-secret")
        let router = EdgeRouter(kit.system, MemoryDurableStateStore(), resolver, "edge-secret", auth: Some(gate))

        // 无令牌 → 401
        match (router.connect("a")) {
            case Unauthorized(status, _) => @Expect(status, 401)
            case Connected(_) => @Fail("开启 AuthGate 后无令牌不应建立连接")
        }
        // 路径不匹配 → 握手即 403
        let otherPath = ScopedTokenCodec.issue("auth-secret", "mallory", "/user/session-b", ["call"],
            ttlMillis: 60_000, nowMillis: authNow())
        match (router.connect("a", accessToken: Some(otherPath))) {
            case Unauthorized(status, _) => @Expect(status, 403)
            case Connected(_) => @Fail("令牌与目标路径不匹配不应建立连接")
        }
        // 正确令牌 → 可连接且可投递
        let good = ScopedTokenCodec.issue("auth-secret", "alice", ref.path.toString(), ["call"],
            ttlMillis: 60_000, nowMillis: authNow())
        match (router.connect("a", accessToken: Some(good))) {
            case Connected(conn) =>
                match (conn.deliver("hello")) {
                    case Delivered => ()
                    case NoRoute(_) => @Fail("会话应可路由")
                    case Forbidden(_) => @Fail("已授权令牌不应被拒绝")
                }
                @Expect(awaitCondition(Duration.second * 5, { => probe.count() == 1 }), true)
            case Unauthorized(_, reason) => @Fail("合法令牌被拒绝: ${reason}")
        }
        kit.shutdown()
    }

    @TestCase
    public func edgeGatewayMapsAccessTokenHeader(): Unit {
        let kit = ActorTestKit("auth-gateway")
        let probe = AuthProbeActor()
        let ref = kit.actorOf({ => probe }, "auth-session-g")
        let resolver = LocalEdgeResolver()
        resolver.bind("g", ref)
        let router = EdgeRouter(kit.system, MemoryDurableStateStore(), resolver, "edge-secret",
            auth: Some(AuthGate("auth-secret")))
        let gateway = EdgeGateway(router)

        match (gateway.upgrade(EdgeRequest("GET", "/v1/edge/g"))) {
            case Rejected(status, _) => @Expect(status, 401)
            case Upgraded(_) => @Fail("无访问令牌不应升级")
        }
        let headers = HashMap<String, String>()
        headers[HEADER_ACCESS_TOKEN] = ScopedTokenCodec.issue("auth-secret", "alice", ref.path.toString(),
            ["call"], ttlMillis: 60_000, nowMillis: authNow())
        match (gateway.upgrade(EdgeRequest("GET", "/v1/edge/g", headers: headers))) {
            case Upgraded(_) => ()
            case Rejected(status, reason) => @Fail("合法令牌应升级(${status}): ${reason}")
        }
        kit.shutdown()
    }
}
