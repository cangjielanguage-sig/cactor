package cactor.tests.unit.runtime.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.typed.{TypedBehavior, TypedActorRef, TypedActorSelection}
import cactor.core.typed.{TypedBehaviors}

@Test
public class TypedSmokeTest {
    @TestCase
    public func createAndTellTypedActor(): Unit {
        let system = CActorSystemFactory.create("typed-smoke-test")

        let behavior = TypedBehaviors.receive<String>({ ctx, msg =>
            ctx.log().info("received: ${msg}")
            TypedBehaviors.same<String>()
        })

        let actor = system.typedActorOf<String>(behavior, "typed-hello")
        actor.tell("ping")

        // basic assertions
        @Assert(system.getName().contains("typed-smoke-test"), true)

        system.terminate()
        sleep(Duration.millisecond * 100)
    }
}

