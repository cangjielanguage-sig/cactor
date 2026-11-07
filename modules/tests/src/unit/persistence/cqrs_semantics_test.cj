/*
 * CActor - CQRS 命令路径语义回归
 *
 * 此前 CQRSBus.sendCommand 的聚合恒为 None、序号恒从 1 开始、期望版本恒为 0，且不等待写入结果：
 * 同一聚合的第二条命令必被存储以版本冲突拒绝，但读模型照样更新、调用方拿到"成功"（静默丢写）。
 */
package cactor_tests.unit.persistence

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{
    CQRSBus, MemoryEnhancedEventStore, CommandHandler, Command, BaseCommand, AggregateRoot, BaseAggregateRoot,
    EventPayload, SimpleEventPayload, ReadModel, PersistentEvent, QuerySpec, CompletedFuture
}
import cactor_persistence.{Future as PFuture}

/** 记录处理器看到的聚合版本，每条命令产生一个事件 */
class DepositHandler <: CommandHandler {
    let seenVersions = ArrayList<Int64>()

    public func handle(command: Command, aggregate: Option<AggregateRoot>): Array<EventPayload> {
        seenVersions.add(aggregate?.getVersion() ?? -1)
        [SimpleEventPayload("Deposited", command.getCommandId())]
    }

    public func canHandle(command: Command): Bool {
        command.getCommandType() == "Deposit"
    }
}

class RecordingReadModel <: ReadModel {
    let seqs = ArrayList<Int64>()

    public func getModelId(): String { "rm" }

    public func update(event: PersistentEvent): Unit {
        seqs.add(event.sequenceNr)
    }

    public func query(querySpec: QuerySpec): PFuture<String> {
        CompletedFuture<String>("${seqs.size}")
    }
}

@Test
public class CqrsSemanticsTest {
    @TestCase
    public func consecutiveCommandsOnSameAggregateAllPersist(): Unit {
        let store = MemoryEnhancedEventStore()
        let bus = CQRSBus(store)
        let handler = DepositHandler()
        let readModel = RecordingReadModel()
        bus.registerCommandHandler("Deposit", handler)
        bus.registerReadModel("rm", readModel)
        bus.registerAggregateFactory({ id => BaseAggregateRoot(id) })

        for (i in 0..3) {
            // get() 在写入失败时抛异常：三条命令都必须真正落库
            bus.sendCommand(BaseCommand("cmd-${i}", "acct-1", "Deposit")).get()
        }
        @Expect(store.getHighestSequenceNr("acct-1").get(), 3)
        @Expect(readModel.seqs.toArray(), [1, 2, 3])
        // 处理器拿到的是重放后的当前聚合：版本依次为 0、1、2
        @Expect(handler.seenVersions.toArray(), [0, 1, 2])
    }

    @TestCase
    public func separateAggregatesHaveIndependentSequences(): Unit {
        let store = MemoryEnhancedEventStore()
        let bus = CQRSBus(store)
        bus.registerCommandHandler("Deposit", DepositHandler())
        bus.sendCommand(BaseCommand("a1", "acct-a", "Deposit")).get()
        bus.sendCommand(BaseCommand("b1", "acct-b", "Deposit")).get()
        bus.sendCommand(BaseCommand("a2", "acct-a", "Deposit")).get()
        @Expect(store.getHighestSequenceNr("acct-a").get(), 2)
        @Expect(store.getHighestSequenceNr("acct-b").get(), 1)
    }
}
