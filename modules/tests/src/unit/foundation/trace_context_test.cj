/*
 * CActor Tests - W3C traceparent 编解码与 Mdc 绑定（差距 4.10 的传播原语）
 *
 * 覆盖：
 *   - `toTraceParent` / `parseTraceParent` 往返；
 *   - `continueTrace` 生成子 span（同 traceId、parentSpanId 指向上游、spanId 不同）；
 *   - 非法输入（版本、长度、大写/非十六进制）返回 None，不抛异常；
 *   - `withTraceMdc` 在块内注入 traceId/spanId，块结束（含异常）恢复线程原有字段。
 */
package cactor_tests.unit.foundation

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.logging.{Mdc}
import cactor_foundation.monitoring.*

func isHexExact(s: String, len: Int64): Bool {
    if (s.size != len) {
        return false
    }
    for (b in s.toArray()) {
        if (!((b >= UInt8(48) && b <= UInt8(57)) || (b >= UInt8(97) && b <= UInt8(102)))) {
            return false
        }
    }
    true
}

@Test
public class TraceContextTest {
    @TestCase
    public func traceParentRoundTripAndChildSpan(): Unit {
        let root = TraceContext.createRoot(generateTraceId())
        @Expect(isHexExact(root.traceId, 32), true)
        @Expect(isHexExact(root.spanId, 16), true)
        let parent = toTraceParent(root)
        @Expect(parent.startsWith("00-"), true)
        @Expect(parent.endsWith("-01"), true)

        let parsed = parseTraceParent(parent).getOrThrow()
        @Expect(parsed.traceId, root.traceId)
        @Expect(parsed.spanId, root.spanId)

        // 接收方接续：同 traceId、新 spanId、parent 指向上游
        let child = continueTrace(parent).getOrThrow()
        @Expect(child.traceId, root.traceId)
        @Expect(child.spanId != root.spanId, true)
        @Expect(child.parentSpanId, Some(root.spanId))
        // 子 span 再发出时的 traceparent 携带的是子 span
        let grand = continueTrace(toTraceParent(child)).getOrThrow()
        @Expect(grand.traceId, root.traceId)
        @Expect(grand.parentSpanId, Some(child.spanId))
    }

    @TestCase
    public func invalidTraceParentsAreRejected(): Unit {
        @Expect(parseTraceParent("").isNone(), true)
        @Expect(parseTraceParent("garbage").isNone(), true)
        // 版本不是 00
        @Expect(parseTraceParent("01-0123456789abcdef0123456789abcdef-0123456789abcdef-01").isNone(), true)
        // trace-id 长度不足
        @Expect(parseTraceParent("00-0123-0123456789abcdef-01").isNone(), true)
        // span-id 长度不足
        @Expect(parseTraceParent("00-0123456789abcdef0123456789abcdef-0123-01").isNone(), true)
        // 大写十六进制（W3C 要求小写）
        @Expect(parseTraceParent("00-0123456789ABCDEF0123456789abcdef-0123456789abcdef-01").isNone(), true)
        // 非十六进制字符
        @Expect(parseTraceParent("00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-0123456789abcdef-01").isNone(), true)
        @Expect(continueTrace("00-bad").isNone(), true)
    }

    @TestCase
    public func traceMdcIsScopedAndRestored(): Unit {
        let ctx = TraceContext.createRoot(generateTraceId()).createChild()
        Mdc.put("tenant", "acme")
        withTraceMdc(ctx, { =>
            @Expect(Mdc.get("traceId"), Some(ctx.traceId))
            @Expect(Mdc.get("spanId"), Some(ctx.spanId))
            @Expect(Mdc.get("parentSpanId"), ctx.parentSpanId)
            @Expect(Mdc.get("tenant"), Some("acme"))
        })
        // 块结束后追踪字段清除，块外的其它字段保留
        @Expect(Mdc.get("traceId").isNone(), true)
        @Expect(Mdc.get("spanId").isNone(), true)
        @Expect(Mdc.get("tenant"), Some("acme"))
        Mdc.remove("tenant")

        // 抛异常也要恢复，不能把 traceId 泄漏给同一线程上的下一个任务
        try {
            withTraceMdc(ctx, { =>
                throw IllegalStateException("boom")
            })
        } catch (_: IllegalStateException) {
        }
        @Expect(Mdc.get("traceId").isNone(), true)

        // 字段内容
        let fields = traceMdcFields(ctx)
        var hasTrace = false
        var hasSpan = false
        var hasParent = false
        for ((k, _) in fields) {
            if (k == "traceId") { hasTrace = true }
            if (k == "spanId") { hasSpan = true }
            if (k == "parentSpanId") { hasParent = true }
        }
        @Expect(hasTrace && hasSpan && hasParent, true)
    }
}
