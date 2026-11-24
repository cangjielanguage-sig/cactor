/*
 * CActor Platform.Session - 会话级事件存储隔离（ScopedEventStore）
 *
 * 覆盖：同名 persistenceId 在不同作用域互不可见、replay 还原原标识、旧接口（saveEvent/getEvents）
 * 同样隔离、persistenceIds 去前缀、deleteScope 只删本作用域、含分隔符的 scope/id 不发生对撞。
 */
package cactor_tests.unit.platform.session

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{BasicEvent, Event, MemoryEventStore}
import cactor_platform.session.*

func scopedEvent(id: String, actorId: String): Event {
    BasicEvent(id, actorId, "Test", "{}")
}

func oneEvent(id: String, actorId: String): Array<Event> {
    let out = ArrayList<Event>()
    out.add(scopedEvent(id, actorId))
    out.toArray()
}

@Test
public class ScopedEventStoreTest {
    @TestCase
    public func scopesAreIsolatedAndReplayRestoresIds(): Unit {
        let store = MemoryEventStore()
        let a = ScopedEventStore(store, "session-a")
        let b = ScopedEventStore(store, "session-b")

        @Expect(a.append("counter", oneEvent("e1", "counter"), 0), 1)
        @Expect(a.highestSequenceNr("counter"), 1)
        // 同名标识在另一个作用域不可见
        @Expect(b.highestSequenceNr("counter"), 0)

        let replay = a.replay("counter", 1, 10)
        @Expect(replay.size, 1)
        // 对调用方还原成原标识，而不是带前缀的键
        @Expect(replay[0].persistenceId, "counter")
        @Expect(replay[0].event.getActorId(), "counter")

        @Expect(b.append("counter", oneEvent("e2", "counter"), 0), 1)
        @Expect(a.highestSequenceNr("counter"), 1)
        @Expect(b.highestSequenceNr("counter"), 1)

        // persistenceIds 只列本作用域，且去前缀
        @Expect(a.persistenceIds().size, 1)
        @Expect(a.persistenceIds()[0], "counter")
        @Expect(a.scopedIds()[0], "counter")
    }

    @TestCase
    public func legacyApiIsScoped(): Unit {
        let store = MemoryEventStore()
        let a = ScopedEventStore(store, "s1")
        let b = ScopedEventStore(store, "s2")

        a.saveEvent(scopedEvent("e1", "actor-1"))
        let seen = a.getEvents("actor-1")
        @Expect(seen.size, 1)
        @Expect(seen[0].getActorId(), "actor-1")
        @Expect(b.getEvents("actor-1").size, 0)

        @Expect(a.getEventsAfter("actor-1", 0).size, 1)
        @Expect(b.getEventsAfter("actor-1", 0).size, 0)

        a.deleteEvents("actor-1")
        @Expect(a.getEvents("actor-1").size, 0)
    }

    @TestCase
    public func deleteScopeRemovesOnlyOwnData(): Unit {
        let store = MemoryEventStore()
        let a = ScopedEventStore(store, "a")
        let b = ScopedEventStore(store, "b")
        a.append("p", oneEvent("e1", "p"), 0)
        b.append("p", oneEvent("e2", "p"), 0)

        @Expect(a.scopedIds().size, 1)
        a.deleteScope()
        @Expect(a.scopedIds().size, 0)
        @Expect(a.highestSequenceNr("p"), 0)
        @Expect(b.highestSequenceNr("p"), 1)
    }

    @TestCase
    public func separatorLikeScopeAndIdDoNotCollide(): Unit {
        let store = MemoryEventStore()
        let x = ScopedEventStore(store, "a")
        let y = ScopedEventStore(store, "a:1")
        // id 里也带分隔符，验证长度前缀编码不产生歧义
        x.append("b/2", oneEvent("e1", "b/2"), 0)
        y.append("b", oneEvent("e2", "b"), 0)

        @Expect(x.highestSequenceNr("b/2"), 1)
        @Expect(y.highestSequenceNr("b"), 1)
        @Expect(x.highestSequenceNr("b"), 0)
        @Expect(y.highestSequenceNr("b/2"), 0)
        @Expect(x.persistenceIds().size, 1)
        @Expect(x.persistenceIds()[0], "b/2")
        @Expect(y.persistenceIds()[0], "b")

        var emptyScope = false
        try {
            ScopedEventStore(store, "")
        } catch (e: IllegalArgumentException) {
            emptyScope = true
        }
        @Expect(emptyScope, true)
    }
}
