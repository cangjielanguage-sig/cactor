package cactor.tests.unit.runtime.mailbox

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.runtime.mailbox.batching.*
import cactor.core.message.*

@Test
public class BatchingMailboxTestSuite {
  private func mkEnv(txt: String): Envelope {
    Envelope(StringMessage(txt), None)
  }

  @TestCase
  public func testDequeueBatchAndSingleFallback(): Unit {
    let mb = BatchingMailbox(3, Duration.millisecond * 100)
    @Assert(mb.enqueue(mkEnv("a")), true)
    @Assert(mb.enqueue(mkEnv("b")), true)
    @Assert(mb.enqueue(mkEnv("c")), true)

    // 批量拉取2条
    let arr = mb.dequeueBatch(2)
    @Assert(Int64(arr.size), 2)

    // 剩余1条，单条回退应可取出
    match (mb.dequeue()) {
      case Some(env) => @Assert(env.getMessage().messageType().contains("c") || env.getMessage().messageType().contains("a") || env.getMessage().messageType().contains("b"), true)
      case None => @Assert(false, true)
    }
  }
}

