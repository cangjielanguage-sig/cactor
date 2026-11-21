/*
 * CActor Core.System - Actor 系统设置（D1 门面收敛）
 *
 * 单一、不可变的系统级配置对象，风格对齐 ShardingSettings：所有 withXxx 返回新对象，
 * 线程安全（无锁），可安全地在创建系统前后传递与读回。
 *
 * 为什么放在 core 而不是门面 cactor.api：RuntimeActorSystem 位于 runtime 模块，
 * 而 runtime 不能反向依赖门面（分层检查会报红）。core 是 runtime 与 cactor 共同的下游，
 * 因此把设置类型放在这里，由门面通过 public import 重导出为 cactor.api.ActorSystemSettings。
 *
 * 字段覆盖门面收敛要求的五项：
 *   name                   系统名称
 *   dispatcherMode         默认调度模式（复用 core 的 DispatcherMode，A1）
 *   maxActors              全局 Actor 数上限（C1，0 = 不限）
 *   actorMemoryBudgetBytes 全局 Actor 内存预算（C1，0 = 不限）
 *   defaultMailbox         默认邮箱类型名（"unbounded" / "bounded" / "priority"）
 *
 * withExtension 是未来插件 / AI 接入的宿主编排点：系统启动时按名称实例化并 initialize。
 */
package cactor_core.system

import std.collection.*
import cactor_core.actor.{DispatcherMode}

/** Actor 系统设置：不可变、链式，覆盖调度模式、预算、默认邮箱与扩展点。 */
public struct ActorSystemSettings {
    /** 系统名称 */
    public let name: String

    /** 默认调度模式（Affinity / ThreadPerActor / Blocking） */
    public let dispatcherMode: DispatcherMode

    /** 全局 Actor 数上限；0 表示不限 */
    public let maxActors: Int64

    /** 全局 Actor 内存预算（字节）；0 表示不限 */
    public let actorMemoryBudgetBytes: Int64

    /** 默认邮箱类型名；Actor 的 Props 未显式指定时使用 */
    public let defaultMailbox: String

    /** 扩展名 -> 工厂。私有，只通过 withExtension / extensionNames / extensionFactory 访问 */
    private let extensionFactories: HashMap<String, () -> ActorSystemExtension>

    /** 以默认值创建：Affinity、不限预算、无界邮箱、无扩展 */
    public init(name: String) {
        this.name = name
        this.dispatcherMode = DispatcherMode.Affinity
        this.maxActors = 0
        this.actorMemoryBudgetBytes = 0
        this.defaultMailbox = "unbounded"
        this.extensionFactories = HashMap<String, () -> ActorSystemExtension>()
    }

    private init(name: String, dispatcherMode: DispatcherMode, maxActors: Int64,
                 actorMemoryBudgetBytes: Int64, defaultMailbox: String,
                 extensionFactories: HashMap<String, () -> ActorSystemExtension>) {
        this.name = name
        this.dispatcherMode = dispatcherMode
        // 负值统一归一化为 0（不限），避免下游出现"负上限"这种无意义状态
        this.maxActors = if (maxActors < 0) { 0 } else { maxActors }
        this.actorMemoryBudgetBytes = if (actorMemoryBudgetBytes < 0) { 0 } else { actorMemoryBudgetBytes }
        this.defaultMailbox = defaultMailbox
        this.extensionFactories = extensionFactories
    }

    /** 改名（保留其余字段） */
    public func withName(name: String): ActorSystemSettings {
        ActorSystemSettings(name, dispatcherMode, maxActors, actorMemoryBudgetBytes, defaultMailbox, extensionFactories)
    }

    /** 设置默认调度模式 */
    public func withDispatcherMode(mode: DispatcherMode): ActorSystemSettings {
        ActorSystemSettings(name, mode, maxActors, actorMemoryBudgetBytes, defaultMailbox, extensionFactories)
    }

    /** 设置全局 Actor 数上限；0 = 不限，负值按 0 处理 */
    public func withMaxActors(n: Int64): ActorSystemSettings {
        ActorSystemSettings(name, dispatcherMode, n, actorMemoryBudgetBytes, defaultMailbox, extensionFactories)
    }

    /** 设置全局 Actor 内存预算（字节）；0 = 不限，负值按 0 处理 */
    public func withActorMemoryBudget(bytes: Int64): ActorSystemSettings {
        ActorSystemSettings(name, dispatcherMode, maxActors, bytes, defaultMailbox, extensionFactories)
    }

    /** 设置默认邮箱类型名 */
    public func withDefaultMailbox(mailbox: String): ActorSystemSettings {
        ActorSystemSettings(name, dispatcherMode, maxActors, actorMemoryBudgetBytes, mailbox, extensionFactories)
    }

    /**
     * 注册扩展工厂（同名后注册的覆盖先注册的）。
     * 系统启动时按注册顺序之外的名字实例化并 initialize，作为插件 / AI 能力的宿主。
     */
    public func withExtension(name: String, factory: () -> ActorSystemExtension): ActorSystemSettings {
        let next = HashMap<String, () -> ActorSystemExtension>()
        for ((k, v) in extensionFactories) {
            next[k] = v
        }
        next[name] = factory
        ActorSystemSettings(this.name, dispatcherMode, maxActors, actorMemoryBudgetBytes, defaultMailbox, next)
    }

    /** 已注册的扩展名（顺序不保证） */
    public func extensionNames(): Array<String> {
        let names = ArrayList<String>()
        for ((k, _) in extensionFactories) {
            names.add(k)
        }
        names.toArray()
    }

    /** 按名字取扩展工厂 */
    public func extensionFactory(name: String): Option<() -> ActorSystemExtension> {
        extensionFactories.get(name)
    }

    /** 预设：默认（Affinity、无界、不限预算） */
    public static func default(name: String): ActorSystemSettings {
        ActorSystemSettings(name)
    }

    /** 预设：高吞吐（大上限与较大内存预算、无界邮箱、Affinity） */
    public static func highThroughput(name: String): ActorSystemSettings {
        ActorSystemSettings(name)
            .withMaxActors(5_000_000)
            .withActorMemoryBudget(8_000_000_000)
    }

    /** 预设：低延迟（较小上限、无界邮箱、Affinity；适合延迟敏感但 Actor 数可控的负载） */
    public static func lowLatency(name: String): ActorSystemSettings {
        ActorSystemSettings(name)
            .withMaxActors(1_000_000)
            .withActorMemoryBudget(2_000_000_000)
    }
}
