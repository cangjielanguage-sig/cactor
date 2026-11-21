/*
 * CActor - D1 门面收敛：ActorSystemSettings 往返与预设
 *
 * 验收 settingsRoundTrip：不可变设置经门面 / RuntimeActorSystem 创建后，settings() 读回无丢失；
 * 扩展工厂在系统启动时被实例化并 initialize（作为插件 / AI 接入宿主）。
 */
package cactor_tests.unit.facade

import std.sync.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{DispatcherMode}
import cactor_core.system.{ActorSystem, ActorSystemExtension}
import cactor.api.{CActorSystem, ActorSystemSettings}
import cactor_runtime.{RuntimeActorSystem}

/** 探针扩展：只记录是否被初始化，避免引入任何业务语义 */
class ProbeExtension <: ActorSystemExtension {
    private let initialized: AtomicBool
    private let extensionName: String

    init(initialized: AtomicBool, name: String) {
        this.initialized = initialized
        this.extensionName = name
    }

    public func initialize(system: ActorSystem): Unit {
        initialized.store(true)
    }

    public func shutdown(): Unit {}

    public func getName(): String {
        extensionName
    }

    public func getVersion(): String {
        "1.0.0"
    }
}

@Test
public class ActorSystemSettingsTest {
    @TestCase
    public func settingsRoundTrip(): Unit {
        let initialized = AtomicBool(false)
        let probeFactory: () -> ActorSystemExtension = { => ProbeExtension(initialized, "probe") }

        let configured = ActorSystemSettings.default("settings-rt")
            .withDispatcherMode(DispatcherMode.ThreadPerActor)
            .withMaxActors(321)
            .withActorMemoryBudget(456789)
            .withDefaultMailbox("bounded")
            .withExtension("probe", probeFactory)

        let system = CActorSystem(configured)
        let readBack = system.settings()
        @Expect(readBack.name, "settings-rt")
        @Expect(readBack.maxActors, 321)
        @Expect(readBack.actorMemoryBudgetBytes, 456789)
        @Expect(readBack.defaultMailbox, "bounded")
        let modeIsThreadPerActor = match (readBack.dispatcherMode) {
            case DispatcherMode.ThreadPerActor => true
            case _ => false
        }
        @Expect(modeIsThreadPerActor, true)
        @Expect(readBack.extensionNames().size, 1)
        @Expect(readBack.extensionFactory("probe").isSome(), true)
        @Expect(initialized.load(), true)
        // D1：启动时间返回真实启动时刻，而不是调用时的当前时间
        @Expect(system.getStartTime() > 0, true)
        system.terminate()

        // RuntimeActorSystem 使用同一设置类型，也应无丢失读回
        let runtimeSystem = RuntimeActorSystem(configured.withName("settings-rt-runtime"))
        let runtimeRead = runtimeSystem.settings()
        @Expect(runtimeRead.name, "settings-rt-runtime")
        @Expect(runtimeRead.maxActors, 321)
        @Expect(runtimeRead.actorMemoryBudgetBytes, 456789)
        @Expect(runtimeRead.defaultMailbox, "bounded")
        runtimeSystem.terminate()
    }

    @TestCase
    public func threeNamedPresets(): Unit {
        let base = ActorSystemSettings.default("preset-default")
        let high = ActorSystemSettings.highThroughput("preset-high")
        let low = ActorSystemSettings.lowLatency("preset-low")

        @Expect(base.maxActors, 0)
        @Expect(base.actorMemoryBudgetBytes, 0)
        @Expect(base.name, "preset-default")
        @Expect(high.name, "preset-high")
        @Expect(low.name, "preset-low")
        @Expect(high.maxActors > low.maxActors, true)
        @Expect(low.maxActors > 0, true)
    }
}
