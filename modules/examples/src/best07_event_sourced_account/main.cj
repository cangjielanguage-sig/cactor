/*
 * CActor Best 07 - 事件溯源账户：EventSourcedActor + 文件事件日志 + 快照
 *
 * 演示（原型 API，接口稳定）：
 * - EventSourcedActor：persist 先写日志再更新状态
 * - FileEventStore / FileSnapshotStore：进程重启后 恢复快照 → 重放增量事件
 * - 监督重启后自动恢复
 *
 * 自检：两轮存取后余额正确；重启后新系统从日志恢复到同一余额。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best07_event_sourced_account
 */
package cactor_examples.best07_event_sourced_account

import std.fs.*
import std.sync.*
import std.convert.*
import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_persistence.{EventSourcedActor, Event, EventStore, SnapshotStore, FileEventStore, FileSnapshotStore}

class AccountActor <: EventSourcedActor {
    private let id: String
    private var balance: Int64 = 0
    let observed: AtomicInt64
    let rejected: AtomicInt64

    init(id: String, events: EventStore, snaps: SnapshotStore, observed: AtomicInt64, rejected: AtomicInt64) {
        super(events, snaps, 10)   // 每 10 个事件自动保存一次快照
        this.id = id
        this.observed = observed
        this.rejected = rejected
    }

    public prop name: String { get() { "AccountActor" } }
    public prop description: String { get() { "事件溯源账户" } }

    public func persistenceId(): String {
        "account-${id}"
    }

    public func handleCommand(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: StringMessage =>
                let parts = m.getContent().split(":")
                match (parts[0]) {
                    case "deposit" => persist("Deposited", parts[1])
                    case "withdraw" =>
                        let amount = Int64.parse(parts[1])
                        if (amount > balance) {
                            rejected.fetchAdd(1)
                        } else {
                            persist("Withdrawn", parts[1])
                        }
                    case _ => ()
                }
                observed.store(balance)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }

    public func applyEvent(event: Event): Unit {
        let amount = Int64.parse(event.getEventData())
        match (event.getEventType()) {
            case "Deposited" => balance += amount
            case "Withdrawn" => balance -= amount
            case _ => ()
        }
    }

    public func snapshotState(): Option<String> {
        Some("${balance}")
    }

    public func restoreSnapshot(state: String): Unit {
        balance = Int64.parse(state)
    }

    public func onRecoveryCompleted(): Unit {
        observed.store(balance)
    }
}

func waitBalance(observed: AtomicInt64, target: Int64, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (observed.load() != target && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    observed.load() == target
}

main(): Int64 {
    let base = "/tmp/cactor-best07-${MonoTime.now().hashCode()}"
    if (exists(base)) {
        remove(base, recursive: true)
    }
    var ok = true

    // ---------- 第一轮：存款、取款 ----------
    let observed = AtomicInt64(-1)
    let rejected = AtomicInt64(0)
    let system1 = CActorSystemFactory.create("best07-1")
    let events1 = FileEventStore("${base}/journal")
    let snaps1 = FileSnapshotStore("${base}/snapshots")
    let acc1 = system1.actorOf({ => AccountActor("42", events1, snaps1, observed, rejected) }, "account")
    for (i in 1..=25) {
        acc1.tell(StringMessage("deposit:${i}"))   // 累计 325
    }
    acc1.tell(StringMessage("withdraw:1000"))      // 余额不足，拒绝
    acc1.tell(StringMessage("withdraw:25"))        // 余额 300
    ok = ok && waitBalance(observed, 300, Duration.second * 5)
    println("第一轮结束：余额 ${observed.load()}，拒绝 ${rejected.load()} 次")
    ok = ok && rejected.load() == 1
    system1.terminate()

    // ---------- 第二轮：全新系统从文件日志恢复 ----------
    let observed2 = AtomicInt64(-1)
    let system2 = CActorSystemFactory.create("best07-2")
    let events2 = FileEventStore("${base}/journal")
    let snaps2 = FileSnapshotStore("${base}/snapshots")
    println("事件总数：${events2.getEvents("account-42").size}（26 条，其中第 10、20 条后各有一次快照）")
    ok = ok && events2.getEvents("account-42").size == 26
    let acc2 = system2.actorOf({ => AccountActor("42", events2, snaps2, observed2, AtomicInt64(0)) }, "account")
    ok = ok && waitBalance(observed2, 300, Duration.second * 5)
    println("重启后恢复：余额 ${observed2.load()}")
    acc2.tell(StringMessage("deposit:5"))
    ok = ok && waitBalance(observed2, 305, Duration.second * 5)
    println("恢复后再存款：余额 ${observed2.load()}")
    system2.terminate()

    remove(base, recursive: true)
    println(if (ok) { "best07：通过" } else { "best07：失败" })
    if (ok) { 0 } else { 1 }
}
