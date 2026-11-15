# @Deprecated API 清单与 2.0.0 移除计划

> 生成方式：静态扫描 `modules/*/src` 中全部 `@Deprecated[...]` 注解（排除 tests / examples 与 `_test.cj`），
> 数据与 `tools/check_deprecations.py`（E51 门禁，CI 已接入）一致：该脚本要求每处注解带 `since` 并在
> 消息或紧邻注释中注明移除版本（Akka 惯例：since 的下一个 semver 大版本，当前即 **2.0.0**）。
>
> **盘点时点：2026-11-14**，12 个发布库 manifest 均为 `1.2.1`。仓颉的 `@Deprecated` 注解只有
> 消息与 `since` 两个参数、没有 `remove` 参数，因此移除版本统一写在注解消息文本里（形如「2.0.0 移除」「2.0 删除」）。
>
> **约定**（与 docs/RELEASE.md 的 semver 章节配套）：
> - 标记 `@Deprecated` 即承诺：至少保留到声明的移除版本之前，期间只修缺陷、不做行为变化；
> - 移除只在**主版本**（2.0.0）执行；2.x 系列内不移除任何公开符号；
> - 每个 API 必须在消息里给出可操作的替代入口（下表「替代」列）；替代入口缺失时不得标记 @Deprecated；
> - 2.0.0 发布前，对照本清单逐项确认：替代入口已在 1.x 充分可用、迁移文档（docs/MIGRATION.md）覆盖、
>   api_snapshot 中对应符号确实删除、CHANGELOG 记录破坏性变更。

## 汇总

| 模块 | 数量 | since 分布 | 计划移除版本 |
|---|---|---|---|
| persistence | 39 | 全部 1.2.0 | 2.0.0 |
| patterns | 3 | 2×1.3.0 + 1×1.2.0 | 2.0.0 |
| cactor | 2 | 1.2.0 | 2.0.0 |
| core | 1 | 1.2.0 | 2.0.0 |
| runtime | 1 | 1.2.0 | 2.0.0 |
| **合计** | **46** | | 全部 **2.0.0**（46/46 已声明移除版本） |

## 明细

### persistence（39 处）

E8′ 收敛前的旧持久化 API（插件式 Enhanced / CQRS / PersistentActor 三套），统一替代入口为
`EventSourcedActor + EventStore`（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节），全部 2.0 删除：


`modules/persistence/src/persistence_plugin.cj`（持久化插件，E8′ 弃用，7 处，since 全部 1.2.0）:

| 行 | 符号 | since | 移除版本 |
|---|---|---|---|
| 11 | `public class PersistenceConfig` | 1.2.0 | 2.0.0 |
| 58 | `public interface PersistencePlugin` | 1.2.0 | 2.0.0 |
| 69 | `public class DefaultPersistencePlugin` | 1.2.0 | 2.0.0 |
| 202 | `public class PersistencePluginFactory` | 1.2.0 | 2.0.0 |
| 222 | `public class PersistEventMessage` | 1.2.0 | 2.0.0 |
| 241 | `public class TakeSnapshotMessage` | 1.2.0 | 2.0.0 |
| 257 | `public class RecoveryCompleteMessage` | 1.2.0 | 2.0.0 |

`modules/persistence/src/persistent_actor_impl.cj`（PersistentActor 旧实现，E8′ 弃用，4 处，since 全部 1.2.0）:

| 行 | 符号 | since | 移除版本 |
|---|---|---|---|
| 62 | `public open class AbstractPersistentActor` | 1.2.0 | 2.0.0 |
| 329 | `public class PersistentCounterActor` | 1.2.0 | 2.0.0 |
| 478 | `public class CounterIncrementedEvent` | 1.2.0 | 2.0.0 |
| 499 | `public class CounterDecrementedEvent` | 1.2.0 | 2.0.0 |

`modules/persistence/src/persistent_actor_manager.cj`（PersistentActor 管理器，E8′ 弃用，3 处，since 全部 1.2.0）:

| 行 | 符号 | since | 移除版本 |
|---|---|---|---|
| 11 | `public class PersistentActorManager` | 1.2.0 | 2.0.0 |
| 87 | `public class PersistentActorFactory` | 1.2.0 | 2.0.0 |
| 111 | `public class PersistenceConfigBuilder` | 1.2.0 | 2.0.0 |

`modules/persistence/src/event_sourcing.cj`（旧事件溯源 API，E8′ 弃用，10 处，since 全部 1.2.0）:

| 行 | 符号 | since | 移除版本 |
|---|---|---|---|
| 17 | `public interface EventPayload` | 1.2.0 | 2.0.0 |
| 27 | `public struct PersistentEvent` | 1.2.0 | 2.0.0 |
| 93 | `public interface EnhancedEventStore` | 1.2.0 | 2.0.0 |
| 124 | `public class MemoryEnhancedEventStore` | 1.2.0 | 2.0.0 |
| 265 | `public interface PersistentActor` | 1.2.0 | 2.0.0 |
| 306 | `public class SimpleEventPayload` | 1.2.0 | 2.0.0 |
| 330 | `public interface Future` | 1.2.0 | 2.0.0 |
| 337 | `public class CompletedFuture` | 1.2.0 | 2.0.0 |
| 352 | `public class FailedFuture` | 1.2.0 | 2.0.0 |
| 373 | `public class FutureFactory` | 1.2.0 | 2.0.0 |

`modules/persistence/src/cqrs_system.cj`（CQRS，E8′ 弃用，15 处，since 全部 1.2.0）:

| 行 | 符号 | since | 移除版本 |
|---|---|---|---|
| 18 | `public interface Command` | 1.2.0 | 2.0.0 |
| 29 | `public interface Query` | 1.2.0 | 2.0.0 |
| 39 | `public interface QueryResult` | 1.2.0 | 2.0.0 |
| 48 | `public interface AggregateRoot` | 1.2.0 | 2.0.0 |
| 60 | `public interface CommandHandler` | 1.2.0 | 2.0.0 |
| 69 | `public interface QueryHandler` | 1.2.0 | 2.0.0 |
| 78 | `public interface ReadModel` | 1.2.0 | 2.0.0 |
| 88 | `public interface QuerySpec` | 1.2.0 | 2.0.0 |
| 97 | `public class BaseCommand` | 1.2.0 | 2.0.0 |
| 124 | `public class BaseQuery` | 1.2.0 | 2.0.0 |
| 153 | `public class BaseQueryResult` | 1.2.0 | 2.0.0 |
| 174 | `public class BaseAggregateRoot` | 1.2.0 | 2.0.0 |
| 216 | `public class CQRSBus` | 1.2.0 | 2.0.0 |
| 344 | `public class SimpleReadModel` | 1.2.0 | 2.0.0 |
| 401 | `public class SimpleQuerySpec` | 1.2.0 | 2.0.0 |

### patterns（3 处）

| 文件:行 | 符号 | since | 移除版本 | 替代 |
|---|---|---|---|---|
| `patterns/src/ask/ask_pattern.cj:183` | `AskPatternManager` | 1.3.0 | 2.0.0 | `cactor_core.dsl.AskInbox.ask`（经典）/ `TypedAsk.ask`、`ctx.ask`（类型化） |
| `patterns/src/ask/ask_api.cj:28` | `Ask` | 1.3.0 | 2.0.0 | 同上 |
| `patterns/src/ask/ask_api.cj:75` | `Ask.askTyped` | 1.2.0 | 2.0.0 | `cactor_core.typed.TypedAsk.ask` / `ctx.ask` |

### cactor（2 处）

| 文件:行 | 符号 | since | 移除版本 | 替代 |
|---|---|---|---|---|
| `cactor/src/api/public/cactor_factory.cj:112` | `CActorSystemBuilder.withDispatcher` | 1.2.0 | 2.0.0 | 在 `Props` 上用 `withDispatcher`（系统级调度器名未接入运行时，计划 E49） |
| `cactor/src/api/public/cactor_factory.cj:124` | `CActorSystemBuilder.withMailbox` | 1.2.0 | 2.0.0 | 在 `Props` 上用 `withMailbox`（同上） |

### core（1 处）

| 文件:行 | 符号 | since | 移除版本 | 替代 |
|---|---|---|---|---|
| `core/src/supervision/supervision_strategy.cj:308` | `BackoffSupervisorStrategy` | 1.2.0 | 2.0.0 | `BackoffSupervisionStrategy(BackoffPolicy(...))`（类型化 `restartWithBackoff` 共用同一算法） |

### runtime（1 处）

| 文件:行 | 符号 | since | 移除版本 | 替代 |
|---|---|---|---|---|
| `runtime/src/lifecycle/coordinated_shutdown.cj:61` | `CoordinatedShutdown.get`（静态） | 1.2.0 | 2.0.0 | `CActorRuntime.getCoordinatedShutdown()`（实例持有，不泄漏、不跨系统共享任务） |

## 维护

- 新增 `@Deprecated` 时必须同步更新本清单（或重跑扫描命令登记）；
- `tools/check_deprecations.py`（CI）只校验"since + 移除版本"齐全，本清单负责"移除版本 = 2.0.0 且替代入口真实存在"的人工审查；
- 数据源差异说明：本清单为 2026-11-14 的静态快照；e51 成员的 `tools/check_deprecations.py` 是持续门禁，
  两者扫描同一数据源（行首 `@Deprecated[` 注解），口径一致，清单落后于门禁时以门禁为准。

