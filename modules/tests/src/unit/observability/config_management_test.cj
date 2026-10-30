/*
 * CActor - R8 测试：分层配置、管理端点（健康检查与 Prometheus 指标，经真实 HTTP）
 */
package cactor_tests.unit.observability

import std.fs.*
import std.net.*
import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory, ManagementServer}
import cactor_foundation.config.{LayeredConfig, ConfigException, EnvironmentConfigSource}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, DowningStrategy}
import cactor_testkit.*

let SAMPLE = """
# 示例配置
app.name = "订单服务"
[cluster]
gossip-interval = 200ms
phi-threshold = 10.5
downing = keep-majority
stable-after = 3 s
// 注释
[dispatcher]
threads = 16
enabled = on
"""

func configWith(env: HashMap<String, String>): LayeredConfig {
    LayeredConfig(Some("CACTOR_"), { name => env.get(name) })
}

/** 最小 HTTP 客户端：发送请求并读到连接关闭，返回 (状态码, 响应体) */
func httpRequest(port: UInt16, method: String, path: String): (Int64, String) {
    let s = TcpSocket("127.0.0.1", port)
    s.connect(timeout: Duration.second * 3)
    s.readTimeout = Duration.second * 5
    s.write("${method} ${path} HTTP/1.1\r\nHost: localhost\r\n\r\n".toArray())
    let acc = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    while (true) {
        let n = try {
            s.read(buf)
        } catch (_: Exception) {
            0
        }
        if (n <= 0) {
            break
        }
        acc.add(all: buf[0..n])
    }
    s.close()
    let text = String.fromUtf8(acc.toArray())
    let status = Int64.parse(text[9..12])
    let body = match (text.indexOf("\r\n\r\n")) {
        case Some(i) => text[(i + 4)..]
        case None => ""
    }
    (status, body)
}

@Test
public class LayeredConfigTest {
    @TestCase
    public func testPrecedenceTypesAndOrigins(): Unit {
        let env = HashMap<String, String>()
        env["CACTOR_DISPATCHER_THREADS"] = "32"
        let cfg = configWith(env)
            .withDefault("dispatcher.threads", "8")
            .withDefault("dispatcher.throughput", "64")
            .withText(SAMPLE, "app.conf")
            .withText("[cluster]\ngossip-interval = 1s\n", "override.conf")
            .withOverride("app.name", "覆盖后的名字")
        // 环境变量 > 文件 > 默认值；后加载的文件覆盖先加载的；代码覆盖最高
        @Expect(cfg.getInt("dispatcher.threads"), 32)
        @Expect(cfg.origin("dispatcher.threads"), Some("环境变量 CACTOR_DISPATCHER_THREADS"))
        @Expect(cfg.getInt("dispatcher.throughput"), 64)
        @Expect(cfg.origin("dispatcher.throughput"), Some("默认值"))
        @Expect(cfg.getDuration("cluster.gossip-interval"), Duration.second)
        @Expect(cfg.origin("cluster.gossip-interval"), Some("override.conf:2"))
        @Expect(cfg.getDuration("cluster.stable-after"), Duration.second * 3)
        @Expect(cfg.getFloat("cluster.phi-threshold"), 10.5)
        @Expect(cfg.getBool("dispatcher.enabled"), true)
        @Expect(cfg.getString("app.name"), "覆盖后的名字")
        @Expect(cfg.getInt("missing.key", 7), 7)
        @Expect(cfg.keysUnder("cluster").size, 4)
        @Expect(LayeredConfig.envName("CACTOR_", "cluster.gossip-interval"), "CACTOR_CLUSTER_GOSSIP_INTERVAL")
        for ((text, d) in [("250us", Duration.microsecond * 250), ("2m", Duration.minute * 2), ("1h", Duration.hour)]) {
            @Expect(LayeredConfig.parseDuration(text), Some(d))
        }
        @Expect(LayeredConfig.parseDuration("10"), None<Duration>)
        @Expect(LayeredConfig.parseDuration("ms"), None<Duration>)
    }

    @TestCase
    public func testErrorsNameKeyValueAndOrigin(): Unit {
        let cfg = configWith(HashMap<String, String>()).withText("[a]\nn = abc\nflag = maybe\nt = 5 parsecs\n", "bad.conf")
        var msg = ""
        try {
            cfg.getInt("a.n")
        } catch (e: ConfigException) {
            msg = e.message
        }
        @Expect(msg.contains("a.n") && msg.contains("abc") && msg.contains("bad.conf:2"), true)
        var errors = 0
        for (f in [{ => cfg.getBool("a.flag") ; () }, { => cfg.getDuration("a.t") ; () }, { => cfg.getString("nope") ; () }]) {
            try {
                f()
            } catch (_: ConfigException) {
                errors++
            }
        }
        @Expect(errors, 3)
        var missing = ""
        try {
            cfg.requireKeys(["a.n", "x.y", "z"])
        } catch (e: ConfigException) {
            missing = e.message
        }
        @Expect(missing.contains("x.y, z"), true)
        var malformed = 0
        for (text in ["no equals sign", "[unclosed", " = value"]) {
            try {
                configWith(HashMap<String, String>()).withText(text, "m.conf")
            } catch (e: ConfigException) {
                if (e.message.contains("m.conf:1")) {
                    malformed++
                }
            }
        }
        @Expect(malformed, 3)
    }

    @TestCase
    public func testFileLoadingAndClusterSettings(): Unit {
        let dir = "/tmp/cactor-config-test-${MonoTime.now().hashCode()}"
        Directory.create(dir, recursive: true)
        File.writeTo("${dir}/app.conf", SAMPLE.toArray())
        let cfg = configWith(HashMap<String, String>()).withFile("${dir}/app.conf").withOptionalFile("${dir}/absent.conf")
        let cs = ClusterSettings.fromConfig(cfg)
        @Expect(cs.gossipInterval, Duration.millisecond * 200)
        @Expect(cs.heartbeatInterval, Duration.second)
        @Expect(cs.phiThreshold, 10.5)
        @Expect(cs.downing.stableAfter(), Some(Duration.second * 3))
        var missingFile = false
        try {
            cfg.withFile("${dir}/absent.conf")
        } catch (_: ConfigException) {
            missingFile = true
        }
        @Expect(missingFile, true)
        var badDowning = ""
        try {
            ClusterSettings.fromConfig(configWith(HashMap<String, String>()).withText("cluster.downing = random", "c.conf"))
        } catch (e: ConfigException) {
            badDowning = e.message
        }
        @Expect(badDowning.contains("c.conf:1"), true)
        remove(dir, recursive: true)
        // 旧的环境变量配置源不再返回写死的模拟值：没有设置的变量不出现
        let src = EnvironmentConfigSource("env", 10, "CACTOR_TEST_UNSET_${MonoTime.now().hashCode()}_")
        @Expect(src.loadConfig().size, 0)
    }
}

@Test
public class ManagementServerTest {
    @TestCase
    public func testHealthReadinessAndMetricsOverHttp(): Unit {
        let system = CActorSystemFactory.create("mgmt-${MonoTime.now().hashCode()}")
        let ready = AtomicBool(false)
        let mgmt = ManagementServer(system, 0)
            .addReadinessCheck("warmup", { => ready.load() })
            .addReadinessCheck("broken", { => throw Exception("检查失败") })
            .start()
        let port = mgmt.port()
        @Expect(port > 0, true)
        let (liveStatus, liveBody) = httpRequest(port, "GET", "/health/live")
        @Expect(liveStatus, 200)
        @Expect(liveBody, "{\"status\":\"UP\"}")
        let (s1, b1) = httpRequest(port, "GET", "/health/ready")
        @Expect(s1, 503)
        @Expect(b1, "{\"checks\":{\"warmup\":\"DOWN\",\"broken\":\"DOWN\"},\"status\":\"DOWN\"}")
        let (metricsStatus, metrics) = httpRequest(port, "GET", "/metrics")
        @Expect(metricsStatus, 200)
        @Expect(metrics.contains("# TYPE cactor_actors_live gauge"), true)
        @Expect(httpRequest(port, "GET", "/nope")[0], 404)
        @Expect(httpRequest(port, "POST", "/health/live")[0], 405)
        // 慢客户端（连上但不发请求）不影响其他请求
        let idle = TcpSocket("127.0.0.1", port)
        idle.connect(timeout: Duration.second * 3)
        @Expect(httpRequest(port, "GET", "/health/live?verbose=1")[0], 200)
        idle.close()
        system.terminate()
        @Expect(httpRequest(port, "GET", "/health/live")[0], 503)
        @Expect(mgmt.requestsServed() >= 7, true)
        mgmt.stop()
    }

    @TestCase
    public func testReadinessFollowsClusterMembership(): Unit {
        let kit = ActorTestKit("mgmt-cluster")
        let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
        let settings = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
            DowningStrategy.NoDowning)
        let cluster = Cluster(kit.system, remote, [remote.getLocalAddress()], settings)
        let mgmt = ManagementServer(kit.system, 0)
            .addReadinessCheck("cluster", { => cluster.isUp() && cluster.state().converged })
            .start()
        @Expect(awaitCondition(Duration.second * 10, { => httpRequest(mgmt.port(), "GET", "/health/ready")[0] == 200 }), true)
        cluster.leave()
        @Expect(awaitCondition(Duration.second * 10, { => httpRequest(mgmt.port(), "GET", "/health/ready")[0] == 503 }), true)
        mgmt.stop()
        cluster.shutdown()
        remote.stop()
        kit.shutdown()
    }
}
