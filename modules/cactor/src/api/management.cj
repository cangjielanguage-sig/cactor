/*
 * Copyright (c) 2024. All rights reserved.
 * CActor API - 管理端点（健康检查与指标），供 Kubernetes 探针和 Prometheus 抓取
 *
 *   GET /health/live   存活：系统未终止时 200，否则 503
 *   GET /health/ready  就绪：系统未终止且全部就绪检查通过时 200，否则 503；响应体列出每项检查的结果（JSON）
 *   GET /metrics       Prometheus 文本格式（CActorSystem.metricsText）
 * 只支持 GET；每个连接处理一个请求后关闭（Connection: close）。请求头限制 8 KB、读超时 5 秒，
 * 防止慢速或恶意客户端占住线程。端口为 0 时由系统分配，用 port() 读取实际端口。
 *
 * ```
 * let mgmt = ManagementServer(system, 8558)
 * mgmt.addReadinessCheck("cluster", { => cluster.isUp() && cluster.state().converged })
 * mgmt.start()
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

    public init(system: CActorSystem, port: UInt16) {
        this.system = system
        this.requestedPort = port
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
        let s = TcpServerSocket(bindAt: requestedPort)
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
                spawn {
                    handle(client)
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

    private func handle(client: TcpSocket): Unit {
        try {
            client.readTimeout = Duration.second * 5
            let head = readHead(client)
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

    /** 读取请求头（到空行为止，最多 8 KB） */
    private static func readHead(client: TcpSocket): String {
        let buf = Array<UInt8>(1024, repeat: 0)
        let acc = ArrayList<UInt8>()
        while (acc.size < 8192) {
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
