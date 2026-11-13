# CActor 包与模块结构

CActor 是 cjpm workspace，根 `cjpm.toml` 定义 **14 个模块**。模块包含根包及全部子包，根包名与模块名一致。依赖必须无环；实际依赖以各模块的 `cjpm.toml` 为准，`tools/check_module_deps.py` 检查层次约束。

## 模块与直接依赖

表中依赖省略 `cactor_` 前缀；门面模块名为 `cactor`。

| 目录 | 模块 | 直接依赖 | 职责 |
|---|---|---|---|
| foundation | `cactor_foundation` | 无 | 队列、网络抽象、配置、日志、监控、断路器 |
| macros | `cactor_macros` | 无 | Actor、消息与状态机宏 |
| core | `cactor_core` | foundation | Actor、消息、监督、类型化行为与协议 |
| runtime | `cactor_runtime` | foundation, core | Actor Cell、邮箱、调度与生命周期 |
| patterns | `cactor_patterns` | foundation, core | Ask、路由、背压、SpawnProtocol |
| remote | `cactor_remote` | foundation, core | TCP、握手、隔离、可靠投递、TLS |
| persistence | `cactor_persistence` | foundation, core | 日志、快照、经典及类型化持久化 |
| cactor | `cactor` | core, runtime | 系统门面与管理 HTTP API |
| streams | `cactor_streams` | macros, foundation, core, cactor | 背压流（E60）：Reactive Streams 接口、Source/Flow/Sink 蓝图、融合岛 + async 边界、取消与流内监督 |
| cluster | `cactor_cluster` | foundation, core, remote, cactor | 成员协议、分片、单例、服务发现、分布式数据 |
| xstate | `cactor_xstate` | foundation, core, cactor | 实验性状态机，不依赖 cluster |
| testkit | `cactor_testkit` | foundation, core, runtime, cactor | TestProbe、BehaviorTestKit、ManualTime、ProcessBarrier 等 |
| tests | `cactor_tests` | foundation, core, runtime, patterns, remote, persistence, cactor, testkit, macros, cluster | 单元与集成测试 |
| examples | `cactor_examples` | 全部库模块及 macros（不依赖 tests） | 可执行示例与基准 |

## 依赖与包路径

`modules/<目录>/src/` 对应模块的包树。依赖关系中的关键路径为：

```text
foundation → core → runtime → cactor
                  → patterns
                  → remote → cluster
                  → persistence
cactor → streams / xstate / testkit
以上模块 → tests / examples
```

箭头表示能力从下层组合到上层；宏为独立模块。stdx 只允许远程层及其上层使用，底层通过 `TransportSecurity` 等接口注入能力。

常用入口：

- `cactor_core.typed`：行为、引用、ask、协议编解码；普通 enum 通过 `TypedPayloadCodecs` 过线。
- `cactor_cluster.membership`：真实成员协议、`ClusterSharding`、`ClusterSingleton`、`ClusterReceptionist`。
- `cactor_cluster.ddata`：CRDT 与 Replicator；`cactor_cluster.ops`：管理与故障注入路由。
- `cactor_persistence.typed`：`EventSourcedBehavior`、`DurableStateBehavior`。
- `cactor_foundation.logging` / `cactor_foundation.monitoring`：日志与监控，包含迁移后的 `Histogram` / `calculatePercentile`。
- `cactor_testkit`：真实运行时测试与同步行为测试工具。`TestActorContext` 兼容工具不模拟 watch、行为切换、定时器；这些操作应使用 `ActorTestKit` 或相应行为测试工具。

## 删除模块的迁移

`cactor_distribution` 与 `cactor_integration` 已删除。分布式代码直接依赖 remote / persistence / cluster；原聚合层的 streaming 原型不再提供。原 integration 的日志、配置、监控重复实现已删除；直方图保留到 foundation，测试辅助类型保留到 testkit，测试归入 tests。详见 [CHANGELOG](../../CHANGELOG.md)。`modules/tests/src/integration/` 是测试目录，仍保留。

## 可见性

对外声明位于公开包；`protected package` 仅在所属模块内可见。模块可以通过 `public import` 重导出公开 API。同包 `*_test.cj` 可访问内部成员，仅由测试构建编译。

## 构建与验证

在 workspace 根目录执行，确保继承优化与 stdx 设置：

```bash
source tools/env.sh
cjpm build
cjpm test
python3 tools/check_module_deps.py
tools/ci.sh
```

新增 import 时先检查模块方向；不要通过 testkit 或门面把运行时依赖反向引入 core。
