/*
 * CActor - 事件元数据（E45，P8）
 *
 * withMetadata 此前构建了新表却随后丢弃：返回的事件没有任何元数据，时间戳也被换成调用时刻。
 */
package cactor_tests.unit.persistence

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{PersistentEvent, SimpleEventPayload}

@Test
public class EventMetadataTest {
    @TestCase
    public func withMetadataKeepsExistingFieldsAndAddsEntry(): Unit {
        let e = PersistentEvent("order-1", 7, SimpleEventPayload("Created", "{}"), Some("cmd-1"), Some("corr-1"))
        let e1 = e.withMetadata("tenant", "acme")
        let e2 = e1.withMetadata("source", "api")
        @Expect(e2.metadata.get("tenant"), Some("acme"))
        @Expect(e2.metadata.get("source"), Some("api"))
        @Expect(e2.metadata.size, 2)
        // 原事件不变
        @Expect(e.metadata.size, 0)
        @Expect(e1.metadata.size, 1)
        // 其余字段与时间戳原样保留
        @Expect(e2.persistenceId, "order-1")
        @Expect(e2.sequenceNr, 7)
        @Expect(e2.causationId, Some("cmd-1"))
        @Expect(e2.correlationId, Some("corr-1"))
        @Expect(e2.timestamp == e.timestamp, true)
    }
}
