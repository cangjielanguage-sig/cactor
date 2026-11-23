/*
 * CActor Platform.Durable - 协议上限（MAX_BODY_BYTES / MAX_READ_BYTES / MAX_TTL_SECONDS）
 *
 * 覆盖：单条消息体上限、单次读的字节上限（游标留给下一次续读）、TTL 上限；
 * 上限值本身允许，超出则明确抛 IllegalArgumentException（不静默截断）。
 */
package cactor_tests.unit.platform.durable

import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.durable.*

func repeatedBytes(size: Int64, value: UInt8): Array<UInt8> {
    Array<UInt8>(size, repeat: value)
}

@Test
public class DurableStreamLimitsTest {
    @TestCase
    public func appendRejectsBodyAboveMax(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("limits/body")

        let tooBig = String.fromUtf8(repeatedBytes(MAX_BODY_BYTES + 1, 97u8))
        var threw = false
        try {
            s.append(tooBig)
        } catch (e: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)
        // 失败不消耗 offset
        @Expect(s.nextOffset(), 1)

        // 恰好等于上限可以写入
        let atLimit = String.fromUtf8(repeatedBytes(MAX_BODY_BYTES, 97u8))
        @Expect(s.append(atLimit), 1)
        @Expect(s.read().records.size, 1)
    }

    @TestCase
    public func readRespectsMaxReadBytes(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let s = reg.stream("limits/read")
        // 每条 1,000,000 字节：4 条共 4,000,000 <= 4MB，第 5 条会超过
        let chunk = String.fromUtf8(repeatedBytes(MAX_BODY_BYTES, 98u8))
        for (_ in 0..5) {
            s.append(chunk)
        }

        let first = s.read(maxRows: MAX_READ_ROWS)
        @Expect(first.records.size, 4)
        @Expect(first.records[3].offset, 4)
        // 游标停在第 5 条，下一次续读从这里开始
        @Expect(first.nextOffset, 5)

        let rest = s.read(fromOffset: first.nextOffset)
        @Expect(rest.records.size, 1)
        @Expect(rest.records[0].offset, 5)
    }

    @TestCase
    public func streamRejectsTtlAboveMax(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        var threw = false
        try {
            reg.stream("limits/ttl", ttlSeconds: MAX_TTL_SECONDS + 1)
        } catch (e: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)

        // 上限本身允许
        let s = reg.stream("limits/ttl-ok", ttlSeconds: MAX_TTL_SECONDS)
        @Expect(s.ttlSeconds(), MAX_TTL_SECONDS)

        // 负 TTL 也明确拒绝
        var negative = false
        try {
            reg.stream("limits/ttl-neg", ttlSeconds: -1)
        } catch (e: IllegalArgumentException) {
            negative = true
        }
        @Expect(negative, true)
    }
}
