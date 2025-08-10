package cactor.tests.unit.runtime.mailbox

import std.unittest.*
import std.unittest.testmacro.*
import cactor.runtime.mailbox.*
import cactor.runtime.mailbox.advanced.*
import cactor.core.message.*

class LowMessage <: UserMessage {
  public func messageType(): String { "LowMessage" }
  public func priority(): Int32 { -1 }
}

class NormalMessage <: UserMessage {
  public func messageType(): String { "NormalMessage" }
  public func priority(): Int32 { 0 }
}

class UrgentMessage <: SystemMessage {
  public func messageType(): String { "StopMessage" }
  public func priority(): Int32 { 100 }
}

@Test
public class PriorityMailboxTestSuite {
  @TestCase
  public func testPriorityOrdering(): Unit {
    let config = MailboxConfig(MailboxType.Priority, 32, true, false, "prio")
    let mb = PriorityMailbox()

    // Enqueue不同优先级
    let low = Envelope(LowMessage(), None<String>)
    let normal = Envelope(NormalMessage(), None<String>)
    let urgent = Envelope(UrgentMessage(), None<String>)

    @Assert(mb.enqueue(normal), true)
    @Assert(mb.enqueue(low), true)
    @Assert(mb.enqueue(urgent), true)

    // Dequeue顺序：urgent -> normal -> low
    match (mb.dequeue()) { case Some(e) => @Assert(e.getMessage().messageType(), "StopMessage") case None => @Assert(true, false) }
    match (mb.dequeue()) { case Some(e) => @Assert(e.getMessage().messageType(), "NormalMessage") case None => @Assert(true, false) }
    match (mb.dequeue()) { case Some(e) => @Assert(e.getMessage().messageType(), "LowMessage") case None => @Assert(true, false) }
  }
}

