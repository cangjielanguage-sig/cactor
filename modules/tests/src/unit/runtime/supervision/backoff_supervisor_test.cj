package cactor_tests.unit.runtime.supervision

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor_runtime.supervision.{BackoffSupervisor}
import cactor_core.supervision.{BackoffSupervisorStrategy}
import cactor_core.guardian.{GuardianActor, GuardianType, GuardianStats, GuardianHealthStatus}
import cactor_core.supervision.{SupervisionStrategy, ActorFailure}
import cactor_core.actor.{Actor, ActorRef, ActorPath, Props}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

class DummyGuardian <: GuardianActor {
    public prop guardianType: GuardianType { get() { GuardianType.User } }
    public prop parent: Option<ActorRef> { get() { None } }
    public func createChild(props: Props<Actor>, name: String): ActorRef { DummyActorRef("/user/" + name) }
    public func supervise(child: ActorRef, strategy: SupervisionStrategy): Unit {}
    public func getChildren(): Array<ActorRef> { [] }
    public func findChild(name: String): Option<ActorRef> { None }
    public func stopChild(child: ActorRef): Unit { stopped = true; last = child.path.toString() }
    public func restartChild(child: ActorRef, reason: Exception): Unit { restarted += 1 }
    public func handleFailure(failure: ActorFailure): Unit {}
    public func getStats(): GuardianStats { GuardianStats.empty() }
    public func getHealthStatus(): GuardianHealthStatus { GuardianHealthStatus.Healthy }
    public var restarted: Int32 = 0
    public var stopped: Bool = false
    public var last: String = ""
    public prop name: String { get() { "guardian" } }
    public prop description: String { get() { "" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Unhandled }
}

class DummyActorRef <: ActorRef {
    private let p: ActorPath
    public init(pth: String) { this.p = ActorPath("sys", pth) }
    public prop path: ActorPath { get() { p } }
    public func tell(message: Message): Unit {}
    public func tell(message: Message, sender: Option<String>): Unit {}
    public func equals(other: ActorRef): Bool { false }
    public func hashCode(): Int32 { 0 }
}

class ExFail <: Exception { public init() { super("x") } }

@Test
public class BackoffSupervisorTestSuite {
    @TestCase
    public func testMaxRetriesStops(): Unit {
        let strat = BackoffSupervisorStrategy(Duration.millisecond * 1, Duration.millisecond * 2, 0.0, 3)
        let g = DummyGuardian()
        let sup = BackoffSupervisor(strat, g)
        let ref = DummyActorRef("/user/a")
        let failure = { =>
            ActorFailure(ref, ExFail(), 1)
        }
        sup.handleFailure(failure())
        sup.handleFailure(failure())
        sup.handleFailure(failure())
        sup.handleFailure(failure())
        @Assert(g.stopped, true)
    }
}

