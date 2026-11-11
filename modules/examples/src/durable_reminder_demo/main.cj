/* CActor Examples - 三进程持久化提醒恢复自检。 */
package cactor_examples.durable_reminder_demo

import std.time.*
import cactor_core.typed.*
import cactor_persistence.{FileDurableStateStore}
import cactor_persistence.typed.{PersistenceId}
import cactor_persistence.reminders.*
import cactor_testkit.*

main(args: Array<String>): Int64 {
    if (args.size != 2) { throw IllegalArgumentException("用法：durable_reminder_demo write|recover|verify <目录>") }
    let mode = args[0]
    let kit = ActorTestKit("reminder-${mode}")
    let timeout = Duration.second * 10
    let sink = TestInbox<ReminderDelivery>("deliveries")
    try {
        let ref = kit.system.typed().actorOf(DurableReminders.behavior(PersistenceId("demo-reminders"),
            FileDurableStateStore(args[1]), { d => sink.tell(d) }, pollInterval: Duration.millisecond * 20), "reminders")
        match (mode) {
            case "write" =>
                let gen = TypedAsk.ask<ReminderCommand, Int64>(ref, { r =>
                    ReminderCommand.Register(ReminderDefinition("recover-me", "durable-payload", 0), r)
                }, timeout).get(timeout)
                if (gen != 1) { throw IllegalStateException("首次登记代次错误") }
            case "recover" =>
                if (!awaitCondition(timeout, { => sink.hasMessages() })) { throw IllegalStateException("未恢复到期投递") }
                let d = sink.receiveMessage()
                if (d.generation != 1 || d.definition.payload != "durable-payload") {
                    throw IllegalStateException("恢复内容不一致")
                }
                let ack = TypedAsk.ask<ReminderCommand, Bool>(ref, { r => ReminderCommand.Acknowledge(
                    d.definition.name, d.generation, d.definition.dueAtMillis, r) }, timeout).get(timeout)
                if (!ack) { throw IllegalStateException("恢复投递确认失败") }
            case "verify" =>
                let entries = TypedAsk.ask<ReminderCommand, Array<ReminderDelivery>>(ref,
                    { r => ReminderCommand.Get(r) }, timeout).get(timeout)
                if (!entries.isEmpty()) { throw IllegalStateException("已确认提醒再次恢复") }
            case _ => throw IllegalArgumentException("未知模式 ${mode}")
        }
    } finally { kit.shutdown() }
    println("持久化提醒 ${mode}：通过")
    0
}
