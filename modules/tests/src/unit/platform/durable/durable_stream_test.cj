/*
 * CActor Platform.Durable - Durable Streams（P4 / G3）
 *
 * 覆盖协议语义（与具体 HTTP 框架无关）：
 *   1. 游标续读：offset 从 1 开始，nextOffset 可续；
 *   2. 长轮询：无消息时等待，新消息到达后立即返回；
 *   3. fork：从第 N 条分叉，前缀复制、之后两条流独立；
 *   4. producer-id：多生产者追加时序号仍连续；
 *   5. TTL：到期后读/写明确失败（协议返回 410）；
 *   6. 协议头：POST 追加、GET 带游标续读、fork 头。
 */
package cactor_tests.unit.platform.durable

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.durable.*
import cactor_testkit.*

@Test
public class DurableStreamTest {
    @TestCase
    public func durableStreamSupportsCursorResume(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("chat/room-1")
        for (i in 1..=10) {
            s.append("m${i}")
        }
        let first = s.read(maxRows: 4)
        @Expect(first.records.size, 4)
        @Expect(first.records[0].offset, 1)
        @Expect(first.records[3].offset, 4)
        @Expect(first.nextOffset, 5)

        let rest = s.read(fromOffset: first.nextOffset)
        @Expect(rest.records.size, 6)
        @Expect(rest.records[0].offset, 5)
        @Expect(rest.nextOffset, 11)

        let empty = s.read(fromOffset: 11)
        @Expect(empty.records.size, 0)
        @Expect(empty.nextOffset, 11)
    }

    @TestCase
    public func durableStreamLongPollReturnsOnNewMessage(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("lp")
        let writer = spawn {
            sleep(Duration.millisecond * 120)
            s.append("late")
        }
        let t0 = MonoTime.now()
        let r = s.readLongPoll(timeout: Duration.second * 2)
        let elapsed = MonoTime.now() - t0
        @Expect(r.records.size, 1)
        @Expect(r.records[0].payload, "late")
        @Expect(elapsed >= Duration.millisecond * 80, true)
        @Expect(elapsed < Duration.second * 2, true)
        writer.get()
    }

    @TestCase
    public func durableStreamForkCreatesIndependentBranch(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("orig")
        for (i in 1..=6) {
            s.append("o${i}")
        }
        let branch = reg.fork("orig", "branch", 3)
        let prefix = branch.read()
        @Expect(prefix.records.size, 3)
        @Expect(prefix.records[2].payload, "o3")
        @Expect(prefix.forkedFrom ?? "", "orig")
        @Expect(prefix.forkOffset, 3)

        s.append("o7")
        branch.append("b1")
        @Expect(s.nextOffset(), 8)
        @Expect(branch.nextOffset(), 5)

        let srcTail = s.read(fromOffset: 7)
        @Expect(srcTail.records.size, 1)
        @Expect(srcTail.records[0].payload, "o7")
        let branchTail = branch.read(fromOffset: 4)
        @Expect(branchTail.records.size, 1)
        @Expect(branchTail.records[0].payload, "b1")
    }

    @TestCase
    public func durableStreamMultipleProducersShareStream(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("multi")
        @Expect(s.append("a", producerId: "p1"), 1)
        @Expect(s.append("b", producerId: "p2"), 2)
        @Expect(s.append("c", producerId: "p1"), 3)
        let r = s.read()
        @Expect(r.records.size, 3)
        @Expect(r.records[0].producerId, "p1")
        @Expect(r.records[1].producerId, "p2")
        @Expect(r.records[2].producerId, "p1")
        @Expect(r.nextOffset, 4)
    }

    @TestCase
    public func durableStreamHonorsTtl(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("ttl/room", ttlSeconds: 1)
        s.append("x")
        @Expect(s.read().records.size, 1)
        sleep(Duration.millisecond * 1100)
        var expired = false
        try {
            let _ = s.read()
        } catch (e: DurableStreamExpiredException) {
            expired = true
        }
        @Expect(expired, true)
        var writeExpired = false
        try {
            s.append("y")
        } catch (e: DurableStreamExpiredException) {
            writeExpired = true
        }
        @Expect(writeExpired, true)

        // 协议层：过期返回 410 而不是 500
        let proto = DurableStreamProtocol(reg)
        let resp = proto.handle(DurableRequest("GET", "/v1/stream/ttl/room"))
        @Expect(resp.status, 410)
    }

    @TestCase
    public func durableStreamProtocolMapsHeaders(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let proto = DurableStreamProtocol(reg)

        let h1 = HashMap<String, String>()
        h1[HEADER_PRODUCER_ID] = "agent-1"
        let w1 = proto.handle(DurableRequest("POST", "/v1/stream/api/room", headers: h1, body: "hello"))
        @Expect(w1.status, 200)
        @Expect(w1.headers[HEADER_SEQ], "1")
        @Expect(w1.headers[HEADER_NEXT_OFFSET], "2")
        @Expect(w1.headers[HEADER_CURSOR], "2")

        let w2 = proto.handle(DurableRequest("POST", "/v1/stream/api/room", body: "world"))
        @Expect(w2.headers[HEADER_SEQ], "2")

        let h3 = HashMap<String, String>()
        h3[HEADER_NEXT_OFFSET] = "2"
        let r = proto.handle(DurableRequest("GET", "/v1/stream/api/room", headers: h3))
        @Expect(r.status, 200)
        @Expect(r.body.contains("world"), true)
        @Expect(r.body.contains("hello"), false)
        @Expect(r.headers[HEADER_NEXT_OFFSET], "3")
        @Expect(r.headers[HEADER_SEQ], "2")

        // fork 头：从第 1 条分叉
        let hf = HashMap<String, String>()
        hf[HEADER_FORKED_FROM] = "api/room"
        hf[HEADER_FORK_OFFSET] = "1"
        let f = proto.handle(DurableRequest("POST", "/v1/stream/api/branch", headers: hf))
        @Expect(f.status, 201)
        @Expect(f.headers[HEADER_FORKED_FROM], "api/room")
        let hb = HashMap<String, String>()
        hb[HEADER_NEXT_OFFSET] = "0"
        let fb = proto.handle(DurableRequest("GET", "/v1/stream/api/branch", headers: hb))
        @Expect(fb.body.contains("hello"), true)
        @Expect(fb.headers[HEADER_FORK_OFFSET], "1")

        // 未知路径前缀 / 未知方法
        @Expect(proto.handle(DurableRequest("GET", "/other/x")).status, 404)
        @Expect(proto.handle(DurableRequest("DELETE", "/v1/stream/api/room")).status, 405)
    }
}
