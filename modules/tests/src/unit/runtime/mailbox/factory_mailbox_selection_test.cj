package cactor_tests.unit.runtime.mailbox

import std.unittest.*
import std.unittest.testmacro.*
import cactor_runtime.mailbox.*
import cactor_core.actor.*
import cactor_core.context.*
import cactor_core.message.*
import cactor_runtime.mailbox.FoundationMailbox
import cactor_runtime.mailbox.QueueBasedPriorityMailbox

class HighPerfActor <: Actor {
  public prop name: String { get() { "high-performance-foundation-test" } }
  public prop description: String { get() { "" } }
  public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

class NeutralActor <: Actor {
  public prop name: String { get() { "prio-actor" } }
  public prop description: String { get() { "" } }
  public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

@Test
public class FactoryMailboxSelectionTestSuite {
  @TestCase
  public func testFactoryCreatesFoundationMailbox(): Unit {
    let factory = MailboxFactory(MailboxConfig.createFoundation())
    factory.start()

    let actor = HighPerfActor()
    // 将具体类型上行到 Actor，以匹配 Props<Actor> 的签名
    let base: Actor = actor
    let props = PropsFactory.create<Actor>({ => base })

    let mb = factory.createMailbox(base, props)
    // 断言类型为 FoundationMailbox（通过 is 运算）
    @Assert((mb is FoundationMailbox), true)
  }

  @TestCase
  public func testFactoryCreatesPriorityQueueMailbox(): Unit {
    let factory = MailboxFactory(MailboxConfig(MailboxType.Priority, 32, true, false, "prio"))
    factory.start()

    let actor = NeutralActor()
    let base: Actor = actor
    let props = PropsFactory.create<Actor>({ => base })

    let mb = factory.createMailbox(base, props)
    // 工厂现在返回 QueueBasedPriorityMailbox（统一到queue实现）
    @Assert((mb is QueueBasedPriorityMailbox), true)
  }
}
