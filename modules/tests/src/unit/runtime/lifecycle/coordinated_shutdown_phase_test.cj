package cactor_tests.unit.runtime.lifecycle

import std.unittest.*
import std.unittest.testmacro.*
import cactor_runtime.lifecycle.{CoordinatedShutdown}

class SeqHolder {
  private var seq: String = ""
  public func append(s: String): Unit { seq = seq + s }
  public func value(): String { seq }
}

@Test
public class CoordinatedShutdownPhaseTestSuite {
  @TestCase
  public func testPhaseOrderAndFailureIsolation(): Unit {
    let cs = CoordinatedShutdown.get("test-system")
    let holder = SeqHolder()
    // 注册任务，包含一个抛异常的任务
    cs.registerTask("before-service-unbind", { => holder.append("A") })
    cs.registerTask("service-unbind", { => holder.append("B") })
    cs.registerTask("service-stop", { => throw Exception("boom") })
    cs.registerTask("service-stop", { => holder.append("C") })
    cs.registerTask("actor-system-terminate", { => holder.append("D") })
    cs.registerTask("system-terminate", { => holder.append("E") })

    cs.run()

    // 验证顺序与失败隔离（C 仍然执行）
    @Assert(holder.value(), "ABCDE")
  }
}

