/*
 * Copyright (c) 2024. All rights reserved.
 * CActor API - 管理端点（健康检查与指标），供 Kubernetes 探针和 Prometheus 抓取
 *
 *   GET /health/live   存活：系统未终止时 200，否则 503
 *   GET /health/ready  就绪：系统未终止且全部就绪检查通过时 200，否则 503；响应体列出每项检查的结果（JSON）
 *   GET /metrics       Prometheus 文本格式（CActorSystem.metricsText）
 * 只支持 GET；每个连接处理一个请求后关闭（Connection: close）。端口为 0 时由系统分配，用 port() 读取实际端口。
 *
 * 防护（E7′，T14）：
 *   - 绑定地址可配置：`ManagementServer(system, "127.0.0.1", port)` 只在本机可达；两参构造仍绑定全部地址（K8s 探针需要）
 *   - 并发连接上限（默认 64）：超出的连接直接回 503 并关闭，不读请求
 *   - 每个请求的总时限（默认 10 秒）：从接受连接起计，读请求头的每次 read 都按剩余时间设超时。
 *     此前只有单次 read 超时 5 秒，每 4.9 秒发 1 字节的慢速连接可以一直占着协程
 *   - 请求头限制 8 KB
 *
 * ```
 * let mgmt = ManagementServer(system, "0.0.0.0", 8558)
 *     .withMaxConnections(32)
 *     .withRequestTimeout(5.seconds)
 *     .addReadinessCheck("cluster", { => cluster.isUp() && cluster.state().converged })
 *     .start()
 * ```
 */
package cactor.api

import std.net.*
import std.sync.*
import std.time.*
import std.collection.*

public class ManagementServer {
    private let system: CActorSystem
    private let requestedPort: UInt16
    private let lock = Mutex()
    private let checks = ArrayList<(String, () -> Bool)>()
    private var server: Option<TcpServerSocket> = None
    private let running = AtomicBool(false)
    private let served = AtomicInt64(0)
    private var boundPort: UInt16 = 0
    // None：绑定全部地址
    private let bindHost: Option<String>
    private var maxConnections: Int64 = 64
    private var requestTimeout: Duration = Duration.second * 10
    private let active = AtomicInt64(0)
    private let rejected = AtomicInt64(0)

    /** 绑定全部地址 */
    public init(system: CActorSystem, port: UInt16) {
        this.system = system
        this.requestedPort = port
        this.bindHost = None<String>
    }

    /** 绑定指定地址（如 "127.0.0.1" 只允许本机访问） */
    public init(system: CActorSystem, host: String, port: UInt16) {
        this.system = system
        this.requestedPort = port
        this.bindHost = Some(host)
    }

    /** 并发连接上限（start 之前设置）；超出的连接回 503 */
    public func withMaxConnections(n: Int64): ManagementServer {
        if (n <= 0) {
            throw IllegalArgumentException("maxConnections 必须为正数: ${n}")
        }
        maxConnections = n
        this
    }

    /** 每个请求从接受连接到写完响应的总时限（start 之前设置） */
    public func withRequestTimeout(timeout: Duration): ManagementServer {
        if (timeout <= Duration.Zero) {
            throw IllegalArgumentException("requestTimeout 必须为正数")
        }
        requestTimeout = timeout
        this
    }

    /** 因超过并发上限被拒绝的连接数 */
    public func rejectedConnections(): Int64 {
        rejected.load()
    }

    /** 注册就绪检查；检查抛异常视为不通过 */
    public func addReadinessCheck(name: String, check: () -> Bool): ManagementServer {
        synchronized(lock) {
            checks.add((name, check))
        }
        this
    }

    public func start(): ManagementServer {
        if (!running.compareAndSwap(false, true)) {
            return this
        }
        let s = match (bindHost) {
            case Some(h) => TcpServerSocket(bindAt: IPSocketAddress(h, requestedPort))
            case None => TcpServerSocket(bindAt: requestedPort)
        }
        s.reuseAddress = true
        s.bind()
        if (let Some(addr) <- (s.localAddress as IPSocketAddress)) {
            boundPort = addr.port
        }
        synchronized(lock) {
            server = Some(s)
        }
        spawn {
            while (running.load()) {
                let client = try {
                    s.accept()
                } catch (_: Exception) {
                    break
                }
                if (active.fetchAdd(1) >= maxConnections) {
                    active.fetchSub(1)
                    rejected.fetchAdd(1)
                    rejectBusy(client)
                    continue
                }
                spawn {
                    try {
                        handle(client)
                    } finally {
                        active.fetchSub(1)
                    }
                }
            }
        }
        this
    }

    public func port(): UInt16 {
        boundPort
    }

    /** 已处理的请求数 */
    public func requestsServed(): Int64 {
        served.load()
    }

    public func stop(): Unit {
        if (!running.compareAndSwap(true, false)) {
            return
        }
        if (let Some(s) <- synchronized(lock) { server }) {
            try {
                s.close()
            } catch (_: Exception) {}
        }
    }

    /** 超过并发上限：不读请求，直接回 503（响应很短，写入不会阻塞） */
    private static func rejectBusy(client: TcpSocket): Unit {
        try {
            client.writeTimeout = Duration.second
            client.write("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toArray())
        } catch (_: Exception) {
            ()
        } finally {
            try {
                client.close()
            } catch (_: Exception) {}
        }
    }

    private func handle(client: TcpSocket): Unit {
        try {
            let deadline = MonoTime.now() + requestTimeout
            client.writeTimeout = requestTimeout
            let head = readHead(client, deadline)
            let (status, contentType, body) = respond(head)
            let bytes = body.toArray()
            let header = "HTTP/1.1 ${status}\r\nContent-Type: ${contentType}\r\nContent-Length: ${bytes.size}\r\nConnection: close\r\n\r\n"
            client.write(header.toArray())
            client.write(bytes)
            served.fetchAdd(1)
        } catch (_: Exception) {
            // 客户端断开或超时：忽略
        } finally {
            try {
                client.close()
            } catch (_: Exception) {}
        }
    }

    /** 读取请求头（到空行为止，最多 8 KB）；每次 read 的超时取剩余时间，超过总时限抛异常 */
    private static func readHead(client: TcpSocket, deadline: MonoTime): String {
        let buf = Array<UInt8>(1024, repeat: 0)
        let acc = ArrayList<UInt8>()
        while (acc.size < 8192) {
            let remaining = deadline - MonoTime.now()
            if (remaining <= Duration.Zero) {
                throw TimeoutException("请求头超过总时限")
            }
            client.readTimeout = remaining
            let n = client.read(buf)
            if (n <= 0) {
                break
            }
            acc.add(all: buf[0..n])
            let text = String.fromUtf8(acc.toArray())
            if (text.contains("\r\n\r\n")) {
                return text
            }
        }
        String.fromUtf8(acc.toArray())
    }

    private func respond(head: String): (String, String, String) {
        let firstLine = match (head.indexOf("\r\n")) {
            case Some(i) => head[0..i]
            case None => head
        }
        let parts = firstLine.split(" ")
        if (parts.size < 2) {
            return ("400 Bad Request", "text/plain", "bad request\n")
        }
        if (parts[0] != "GET") {
            return ("405 Method Not Allowed", "text/plain", "only GET\n")
        }
        let path = match (parts[1].indexOf("?")) {
            case Some(i) => parts[1][0..i]
            case None => parts[1]
        }
        match (path) {
            case "/health/live" =>
                if (system.isTerminated()) {
                    ("503 Service Unavailable", "application/json", "{\"status\":\"DOWN\"}")
                } else {
                    ("200 OK", "application/json", "{\"status\":\"UP\"}")
                }
            case "/health/ready" => readiness()
            case "/metrics" => ("200 OK", "text/plain; version=0.0.4", system.metricsText(false))
            case _ => ("404 Not Found", "text/plain", "not found\n")
        }
    }

    private func readiness(): (String, String, String) {
        let list = synchronized(lock) { checks.toArray() }
        var allOk = !system.isTerminated()
        let sb = StringBuilder("{\"checks\":{")
        for (i in 0..list.size) {
            let (name, check) = list[i]
            let ok = try {
                check()
            } catch (_: Exception) {
                false
            }
            allOk = allOk && ok
            if (i > 0) {
                sb.append(",")
            }
            sb.append("\"${jsonEscape(name)}\":\"${if (ok) { "UP" } else { "DOWN" }}\"")
        }
        sb.append("},\"status\":\"${if (allOk) { "UP" } else { "DOWN" }}\"}")
        if (allOk) {
            ("200 OK", "application/json", sb.toString())
        } else {
            ("503 Service Unavailable", "application/json", sb.toString())
        }
    }

    private static func jsonEscape(s: String): String {
        s.replace("\\", "\\\\").replace("\"", "\\\"")
    }
}
