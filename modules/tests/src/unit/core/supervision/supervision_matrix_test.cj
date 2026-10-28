package cactor_tests.unit.core.supervision

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor_core.supervision.{
    SupervisionDirective,
    OneForOneStrategy,
    OneForAllStrategy,
    DefaultSupervisionStrategy,
    EscalateStrategy,
    BackoffSupervisorStrategy,
    ActorFailure
}
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_core.message.{Message}
import cactor_core.context.{ActorContext}

class DummyActorRef <: ActorRef {
    private let p: ActorPath
    public init(pth: String) { this.p = ActorPath("sys", pth) }
    public prop path: ActorPath { get() { p } }
    public func tell(message: Message): Unit {}
    public func tell(message: Message, sender: Option<String>): Unit {}
    public func equals(other: ActorRef): Bool { false }
    public func hashCode(): Int32 { 0 }
}

class Ex1 <: Exception { public init() { super("boom") } }

// Helpers to test enum directives without using '=='
private func isRestart(d: SupervisionDirective): Bool {
    match (d) { case SupervisionDirective.Restart => true; case _ => false }
}
private func isStop(d: SupervisionDirective): Bool {
    match (d) { case SupervisionDirective.Stop => true; case _ => false }
}
private func isResume(d: SupervisionDirective): Bool {
    match (d) { case SupervisionDirective.Resume => true; case _ => false }
}
private func isEscalate(d: SupervisionDirective): Bool {
    match (d) { case SupervisionDirective.Escalate => true; case _ => false }
}

@Test
public class SupervisionMatrixTestSuite {
    @TestCase
    public func testOneForOne_RestartWithinMaxThenStop(): Unit {
        let strat = OneForOneStrategy(2, Duration.minute, { _ => SupervisionDirective.Restart })
        let ref = DummyActorRef("/user/a")
        let f = { => ActorFailure(ref, Ex1(), 1) }
        @Assert(isRestart(strat.decide(f())), true)
        @Assert(isRestart(strat.decide(f())), true)
        // 第三次应超过上限 -> Stop
        @Assert(isStop(strat.decide(f())), true)
    }

    @TestCase
    public func testOneForAll_RestartWithinMaxThenStop(): Unit {
        let strat = OneForAllStrategy(1, Duration.minute, { _ => SupervisionDirective.Restart })
        let ref = DummyActorRef("/user/b")
        let f = { => ActorFailure(ref, Ex1(), 1) }
        @Assert(isRestart(strat.decide(f())), true)
        // 第二次应超过上限 -> Stop
        @Assert(isStop(strat.decide(f())), true)
    }

    @TestCase
    public func testDefaultResumingStrategyAlwaysResume(): Unit {
        let strat = DefaultSupervisionStrategy.resumingStrategy()
        let ref = DummyActorRef("/user/c")
        @Assert(isResume(strat.decide(ActorFailure(ref, Ex1(), 1))), true)
    }

    @TestCase
    public func testEscalateAfterThreshold(): Unit {
        let strat = EscalateStrategy() // 默认阈值3
        let ref = DummyActorRef("/user/d")
        let f = { => ActorFailure(ref, Ex1(), 1) }
        // 前三次Restart，第四次Escalate
        @Assert(isRestart(strat.decide(f())), true)
        @Assert(isRestart(strat.decide(f())), true)
        @Assert(isRestart(strat.decide(f())), true)
        @Assert(isEscalate(strat.decide(f())), true)
    }

    @TestCase
    public func testBackoffSupervisorStrategyStopAtMaxRetries(): Unit {
        let strat = BackoffSupervisorStrategy(Duration.millisecond * 1, Duration.millisecond * 8, 0.0, 2)
        let ref = DummyActorRef("/user/e")
        let f = { => ActorFailure(ref, Ex1(), 1) }
        // 0->1
        let d1 = strat.decide(f())
        // 1->2
        let d2 = strat.decide(f())
        // 达到2后应Stop
        let d3 = strat.decide(f())
        @Assert(isRestart(d1) || isStop(d1), true)
        @Assert(isRestart(d2) || isStop(d2), true)
        @Assert(isStop(d3), true)
    }
}

