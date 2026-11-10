/*
 * CActor Testkit - 跨进程 barrier
 *
 * 进程内的 barrier 只需要一个 Mutex + 计数（见 tests 里的 ClusterBarrier），但它证明不了
 * "另一个操作系统进程真的到了这一步"。跨进程同步唯一不需要额外服务的会合点就是文件系统：
 * 每个参与者写一个 `<name>.ready`，所有人只轮询"目录里有几个 .ready"。
 *
 * 选择文件而不是 socket/共享内存的理由：崩溃时不会留下半开的连接，也不需要谁先启动；
 * 失败是看得见的（目录里少一个文件，`parties()` 直接列出缺谁）。代价是轮询间隔（默认 20 ms）
 * 与文件系统的可见性延迟——这是测试工具，不是生产同步原语。
 *
 * 目录布局（shell 脚本也能直接参与）：
 *   <directory>/<name>.ready   参与者到位
 *   <directory>/go             所有人都到位（由最后一个看到 count >= expected 的参与者写）
 */
package cactor_testkit

import std.collection.*
import std.fs.*
import std.random.*
import std.sort.*
import std.time.*

public class ProcessBarrier {
    /** 会合目录（每个 round 一个，避免上一轮的残留文件干扰） */
    public let directory: String

    private let defaultTimeout: Duration
    private let pollInterval: Duration

    public init(directory: String, timeout!: Duration = Duration.second * 30) {
        if (directory.isEmpty()) {
            throw IllegalArgumentException("barrier 目录不能为空")
        }
        this.directory = directory
        this.defaultTimeout = timeout
        this.pollInterval = Duration.millisecond * 20
        if (!exists(directory)) {
            Directory.create(directory, recursive: true)
        }
    }

    /** 每轮一个新的临时目录：`/tmp/cactor-barrier-<prefix>-<随机数>` */
    public static func tempDirectory(prefix: String): String {
        "/tmp/cactor-barrier-${prefix}-${Random().nextUInt64()}"
    }

    /** 登记自己（写 `<name>.ready`，可重复调用） */
    public func markReady(name: String): Unit {
        validate(name)
        let path = readyPath(name)
        if (exists(path)) {
            return
        }
        let f = File(path, OpenMode.Write)
        f.write(Array<UInt8>(1, repeat: 0))
        f.close()
    }

    /** 登记自己并等到齐 */
    public func arrive(name: String, expected: Int64): Bool {
        arrive(name, expected, defaultTimeout)
    }

    public func arrive(name: String, expected: Int64, timeout: Duration): Bool {
        if (expected <= 0) {
            throw IllegalArgumentException("expected 必须大于 0")
        }
        markReady(name)
        await(expected, timeout)
    }

    /** 只等待（不登记）：到齐后写 `go` 标记并返回 true */
    public func await(expected: Int64, timeout: Duration): Bool {
        if (expected <= 0) {
            throw IllegalArgumentException("expected 必须大于 0")
        }
        let deadline = DateTime.now() + timeout
        var opened = false
        while (!opened && DateTime.now() < deadline) {
            if (count() >= expected) {
                openIfAbsent()
                opened = true
            } else {
                sleep(pollInterval)
            }
        }
        opened
    }

    public func await(expected: Int64): Bool {
        await(expected, defaultTimeout)
    }

    public func count(): Int64 {
        var n = 0
        for (info in Directory.readFrom(directory)) {
            if (info.name.endsWith(".ready")) {
                n += 1
            }
        }
        n
    }

    /** 已到位的参与者名（排序，便于断言与读日志） */
    public func parties(): Array<String> {
        let names = ArrayList<String>()
        for (info in Directory.readFrom(directory)) {
            if (info.name.endsWith(".ready")) {
                names.add(info.name[0..info.name.size - 6])
            }
        }
        let arr = names.toArray()
        sort(arr)
        arr
    }

    /** 是否已到齐过（`go` 标记存在） */
    public func isOpen(): Bool {
        exists("${directory}/go")
    }

    /** 清空本轮：删除所有 `.ready` 与 `go`（同一目录复用多轮时调用） */
    public func reset(): Unit {
        for (info in Directory.readFrom(directory)) {
            let path = "${directory}/${info.name}"
            if (info.name.endsWith(".ready") || info.name == "go") {
                remove(path)
            }
        }
    }

    /** 某位参与者的到位文件路径：shell 侧可以 `touch` 它来参与同一个 barrier */
    public func readyPath(name: String): String {
        "${directory}/${name}.ready"
    }

    private func openIfAbsent(): Unit {
        let path = "${directory}/go"
        if (exists(path)) {
            return
        }
        let f = File(path, OpenMode.Write)
        f.write(Array<UInt8>(1, repeat: 0))
        f.close()
    }

    private func validate(name: String): Unit {
        if (name.isEmpty() || name.contains("/") || name.contains("..")) {
            throw IllegalArgumentException("barrier 参与者名不合法：${name}")
        }
    }
}
