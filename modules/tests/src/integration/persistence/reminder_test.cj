/* CActor Persistence.Reminders - 真磁盘恢复、确认重发和周期边界。 */
package cactor_tests.integration.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_persistence.*
import cactor_persistence.typed.*
import cactor_persistence.reminders.*
import cactor_testkit.*

func reminderList(ref: TypedActorRef<ReminderCommand>): Array<ReminderDelivery> {
    TypedAsk.ask<ReminderCommand, Array<ReminderDelivery>>(ref, { r => ReminderCommand.Get(r) }, TP_T).get(TP_T)
}

func reminderRegister(ref: TypedActorRef<ReminderCommand>, d: ReminderDefinition): Int64 {
    TypedAsk.ask<ReminderCommand, Int64>(ref, { r => ReminderCommand.Register(d, r) }, TP_T).get(TP_T)
}

func reminderAck(ref: TypedActorRef<ReminderCommand>, d: ReminderDelivery): Bool {
    TypedAsk.ask<ReminderCommand, Bool>(ref, { r => ReminderCommand.Acknowledge(d.definition.name,
        d.generation, d.definition.dueAtMillis, r) }, TP_T).get(TP_T)
}

@Test
public class DurableReminderTest {
    @TestCase
    public func diskRecoveryRedeliversUntilAcknowledged(): Unit {
        let dir = tpDir("reminder-recovery")
        let pid = PersistenceId.of("reminder", "disk")
        let sink = TestInbox<ReminderDelivery>("disk-deliveries")
        let first = ActorTestKit("reminder-write")
        try {
            let store = FileDurableStateStore(dir)
            let ref = first.system.typed().actorOf(DurableReminders.behavior(pid, store,
                { d => sink.tell(d) }, pollInterval: Duration.hour, nowMillis: { => 100 }), "reminders")
            let generation = reminderRegister(ref, ReminderDefinition("名|字\n", "", 50))
            @Expect(generation, 1)
            @Expect(store.get(pid.id).isSome(), true)
            ref.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(TP_T, { => sink.hasMessages() }), true)
            @Expect(sink.receiveMessage().generation, generation)
        } finally { first.shutdown() }
        let second = ActorTestKit("reminder-read")
        try {
            let ref = second.system.typed().actorOf(DurableReminders.behavior(pid, FileDurableStateStore(dir),
                { d => sink.tell(d) }, pollInterval: Duration.hour, nowMillis: { => 100 }), "reminders")
            @Expect(reminderList(ref).size, 1)
            ref.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(TP_T, { => sink.hasMessages() }), true)
            let redelivered = sink.receiveMessage()
            @Expect(redelivered.generation, 1)
            @Expect(redelivered.definition.name, "名|字\n")
            @Expect(redelivered.definition.payload, "")
            @Expect(reminderAck(ref, redelivered), true)
            @Expect(reminderAck(ref, redelivered), false)
            @Expect(reminderList(ref).size, 0)
            @Expect(reminderRegister(ref, ReminderDefinition("next", "new", 200)), 2)
        } finally {
            second.shutdown()
            remove(dir, recursive: true)
        }
    }

    @TestCase
    public func periodicAckCoalescesMissedTicksAndRejectsStaleGeneration(): Unit {
        let kit = ActorTestKit("reminder-period")
        let now = AtomicInt64(100)
        let sink = TestInbox<ReminderDelivery>("period-deliveries")
        try {
            let ref = kit.system.typed().actorOf(DurableReminders.behavior(PersistenceId("period"),
                MemoryDurableStateStore(), { d => sink.tell(d) }, pollInterval: Duration.hour,
                nowMillis: { => now.load() }), "reminders")
            reminderRegister(ref, ReminderDefinition("periodic", "payload", 110, periodMillis: 10))
            ref.tell(ReminderCommand.Tick)
            @Expect(reminderList(ref).size, 1)
            @Expect(sink.hasMessages(), false)
            now.store(135)
            ref.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(TP_T, { => sink.hasMessages() }), true)
            let old = sink.receiveMessage()
            ref.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(TP_T, { => sink.hasMessages() }), true)
            @Expect(sink.receiveMessage().definition.dueAtMillis, old.definition.dueAtMillis)
            @Expect(reminderAck(ref, old), true)
            @Expect(reminderList(ref)[0].definition.dueAtMillis, 140)
            @Expect(reminderAck(ref, old), false)
            let current = reminderList(ref)[0]
            @Expect(reminderAck(ref, current), false)
            // 保持旧到期时刻，确保拒绝旧确认依赖代次，而非碰巧因时间不同。
            @Expect(reminderRegister(ref, ReminderDefinition("periodic", "replacement", old.definition.dueAtMillis)), 2)
            @Expect(reminderAck(ref, old), false)
            @Expect(reminderList(ref)[0].definition.payload, "replacement")
        } finally { kit.shutdown() }
    }

    @TestCase
    public func removalPersistsAndGenerationDoesNotResetAfterRecovery(): Unit {
        let dir = tpDir("reminder-remove")
        let pid = PersistenceId("removed")
        let kit = ActorTestKit("reminder-remove")
        try {
            let ref = kit.system.typed().actorOf(DurableReminders.behavior(pid, FileDurableStateStore(dir),
                { _ => }, pollInterval: Duration.hour), "reminders")
            reminderRegister(ref, ReminderDefinition("x", "x", 1))
            let removed = TypedAsk.ask<ReminderCommand, Bool>(ref, { r => ReminderCommand.Remove("x", r) }, TP_T)
            @Expect(removed.get(TP_T), true)
            @Expect(reminderList(ref).size, 0)
        } finally { kit.shutdown() }
        let recovered = ActorTestKit("reminder-removed-recover")
        try {
            let ref = recovered.system.typed().actorOf(DurableReminders.behavior(pid, FileDurableStateStore(dir),
                { _ => }, pollInterval: Duration.hour), "reminders")
            @Expect(reminderList(ref).size, 0)
            @Expect(reminderRegister(ref, ReminderDefinition("x", "x", 1)), 2)
            let removed = TypedAsk.ask<ReminderCommand, Bool>(ref, { r => ReminderCommand.Remove("absent", r) }, TP_T)
            @Expect(removed.get(TP_T), false)
        } finally {
            recovered.shutdown()
            remove(dir, recursive: true)
        }
    }

    @TestCase
    public func realTimerDeliversAndInvalidDefinitionsFailEarly(): Unit {
        @ExpectThrows[IllegalArgumentException](ReminderDefinition("", "", 0))
        @ExpectThrows[IllegalArgumentException](ReminderDefinition("x", "", -1))
        @ExpectThrows[IllegalArgumentException](ReminderDefinition("x", "", 0, periodMillis: -1))
        @ExpectThrows[IllegalArgumentException](DurableReminders.behavior(PersistenceId("invalid"),
            MemoryDurableStateStore(), { _ => }, pollInterval: Duration.Zero))
        let kit = ActorTestKit("reminder-timer")
        let sink = TestInbox<ReminderDelivery>("timer-deliveries")
        try {
            let ref = kit.system.typed().actorOf(DurableReminders.behavior(PersistenceId("timer"),
                MemoryDurableStateStore(), { d => sink.tell(d) }, pollInterval: Duration.millisecond * 10), "reminders")
            reminderRegister(ref, ReminderDefinition("timer", "payload", 0))
            @Expect(awaitCondition(TP_T, { => sink.hasMessages() }), true)
            @Expect(reminderAck(ref, sink.receiveMessage()), true)
        } finally { kit.shutdown() }
    }
}
