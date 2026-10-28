/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 事件溯源端到端测试
 * 在真实 Actor 系统中运行事件溯源 Actor，验证：系统重建后恢复、快照 + 增量重放、失败重启后恢复
 */
package cactor_tests.integration.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_persistence.{EventSourcedActor, Event, EventStore, SnapshotStore, FileEventStore, FileSnapshotStore}

func eventually(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond * 2)
    }
    cond()
}

/**
 * 银行账户：命令 "deposit:<n>" / "withdraw:<n>" / "boom"
 * 余额不足时拒绝取款（不产生事件）
 */
class AccountActor <: EventSourcedActor {
    private let id: String
    private var balance: Int64 = 0
    let observedBalance: AtomicInt64
    let rejected: AtomicInt64

    init(id: String, events: EventStore, snapshots: SnapshotStore, observedBalance: AtomicInt64, rejected: AtomicInt64) {
        super(events, snapshots, 10)
        this.id = id
        this.observedBalance = observedBalance
        this.rejected = rejected
    }

    public prop name: String {
        get() { "AccountActor" }
    }

    public prop description: String {
        get() { "事件溯源账户" }
    }

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
                    case "boom" => throw Exception("模拟故障")
                    case _ => ()
                }
                observedBalance.store(balance)
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
        observedBalance.store(balance)
    }
}

@Test
public class EventSourcedActorTest {
    private func dir(tag: String): String {
        let d = "/tmp/cactor-es-test/${tag}-${MonoTime.now().hashCode()}"
        if (exists(d)) {
            remove(d, recursive: true)
        }
        d
    }

    @TestCase
    func stateIsRecoveredInNewSystem(): Unit {
        let base = dir("recover")
        let balance = AtomicInt64(-1)
        let rejected = AtomicInt64(0)

        // 第一次运行
        let system1 = CActorSystemFactory.create("es-1-${MonoTime.now().hashCode()}")
        let events1 = FileEventStore("${base}/journal")
        let snaps1 = FileSnapshotStore("${base}/snapshots")
        let acc1 = system1.actorOf({ => AccountActor("42", events1, snaps1, balance, rejected) }, "account")
        for (i in 1..=25) {
            acc1.tell(StringMessage("deposit:${i}"))
        }
        acc1.tell(StringMessage("withdraw:1000"))   // 余额不足，被拒绝
        acc1.tell(StringMessage("withdraw:25"))
        @Expect(eventually(Duration.second * 5, { => balance.load() == 300 && rejected.load() == 1 }), true)
        system1.terminate()

        // 模拟进程重启：全新的存储实例与 Actor 系统
        let recovered = AtomicInt64(-1)
        let system2 = CActorSystemFactory.create("es-2-${MonoTime.now().hashCode()}")
        let events2 = FileEventStore("${base}/journal")
        let snaps2 = FileSnapshotStore("${base}/snapshots")
        // 26 个事件：第 10、20 个事件后各保存一次快照
        @Expect(events2.getEvents("account-42").size, 26)
        match (snaps2.getLatestSnapshot("account-42")) {
            case Some(sn) => @Expect(sn.getSequenceNumber(), 20)
            case None => @Fail("快照缺失")
        }
        let acc2 = system2.actorOf({ => AccountActor("42", events2, snaps2, recovered, AtomicInt64(0)) }, "account")
        @Expect(eventually(Duration.second * 5, { => recovered.load() == 300 }), true)
        acc2.tell(StringMessage("deposit:5"))
        @Expect(eventually(Duration.second * 5, { => recovered.load() == 305 }), true)
        system2.terminate()
        remove(base, recursive: true)
    }

    @TestCase
    func stateIsRecoveredAfterSupervisedRestart(): Unit {
        let base = dir("restart")
        let balance = AtomicInt64(-1)
        let system = CActorSystemFactory.create("es-restart-${MonoTime.now().hashCode()}")
        let events = FileEventStore("${base}/journal")
        let snaps = FileSnapshotStore("${base}/snapshots")
        let acc = system.actorOf({ => AccountActor("7", events, snaps, balance, AtomicInt64(0)) }, "account")
        acc.tell(StringMessage("deposit:100"))
        acc.tell(StringMessage("deposit:50"))
        acc.tell(StringMessage("boom"))          // 失败 → 监督重启 → 新实例从日志恢复
        acc.tell(StringMessage("withdraw:30"))
        @Expect(eventually(Duration.second * 5, { => balance.load() == 120 }), true)
        @Expect(events.getEvents("account-7").size, 3)
        system.terminate()
        remove(base, recursive: true)
    }
}
