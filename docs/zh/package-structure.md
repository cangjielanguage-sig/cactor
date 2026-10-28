# CActor 包与模块结构

CActor 是一个 cjpm **workspace（工作区）**，由 12 个仓颉 **module（模块）**组成。仓颉中，一个模块就是“一个根包加上它的全部子包”构成的树，模块的根包名必须与模块名相同；包之间、模块之间都不允许出现循环依赖。

## 目录布局

```
cactor/
├── cjpm.toml                 # [workspace]：成员列表 + 公共编译选项
└── modules/
    ├── foundation/           # 模块 cactor_foundation
    │   ├── cjpm.toml         # [package] name = "cactor_foundation"
    │   └── src/
    │       ├── pkg.cj        # 根包 cactor_foundation
    │       └── queue/ ...    # 子包 cactor_foundation.queue
    ├── core/                 # cactor_core
    ├── patterns/             # cactor_patterns
    ├── runtime/              # cactor_runtime
    ├── cactor/               # cactor（门面：cactor / cactor.api.* / cactor.config.*）
    ├── macros/               # cactor_macros（macro package）
    ├── streams/              # cactor_streams
    ├── distribution/         # cactor_distribution
    ├── xstate/               # cactor_xstate
    ├── integration/          # cactor_integration
    ├── tests/                # cactor_tests
    └── examples/             # cactor_examples
```

## 模块一览与依赖

| 模块 | 文件/行数 | 直接依赖 | 子包 |
|---|---|---|---|
| `cactor_foundation` | 35 / 9,350 | — | abstractions, circuit_breaker, config, contracts, di, memory, monitoring, network, queue, serialization, simple_config |
| `cactor_core` | 31 / 6,733 | foundation | actor, config, context, error, guardian, memory, message, supervision, system, typed |
| `cactor_patterns` | 11 / 2,990 | foundation, core | ask, backpressure, routing(.advanced/.factory) |
| `cactor_runtime` | 63 / 16,185 | foundation, core, patterns | actor, context, dispatcher.*, events, execution, guardian, lifecycle, mailbox.*, message, monitoring, registry, scheduler, supervision, system, typed, wrappers |
| `cactor` | 16 / 2,140 | core, runtime | api, api.config, api.extensions, api.public, config, config.file |
| `cactor_macros` | 1 / 695 | — | （宏包本身） |
| `cactor_streams` | 6 / 2,551 | core, cactor | core, dsl, unified |
| `cactor_distribution` | 20 / 4,993 | foundation, core, cactor | cluster, persistence, remote, streaming |
| `cactor_xstate` | 17 / 4,867 | core, cactor, distribution | core.*, features.*, enterprise.*, lang.macros |
| `cactor_integration` | 28 / 8,138 | foundation, core, runtime, cactor | configuration, logging, monitoring, testing(.*可执行) |
| `cactor_tests` | 37 / 4,812 | core, runtime, cactor | unit.*, integration |
| `cactor_examples` | 61 / 14,015 | 以上全部 | 38 个可执行示例 |

依赖方向（自底向上，无环）：

```
foundation ─► core ─► patterns ─► runtime ─► cactor ─┬─► streams
                                                     ├─► distribution ─► xstate
                                                     └─► integration
tests / examples 位于最上层；macros 独立，只被 examples 依赖
```

## 可见性约定

- **模块对外 API**：放在 `public package`（默认）中，声明为 `public`。
- **模块内部实现**：声明为 `protected package`，只在本模块内可见，其他模块 `import` 会直接编译失败。目前 `cactor_runtime` 下的 actor、context、guardian、registry、message、monitoring、execution、scheduler、mailbox.lockfree、mailbox.ringbuffer、dispatcher.batch_processing / optimized / work_stealing 均为 protected。
- **重导出**：模块根包通过 `public import x.y.{A, B}` 对外提供精选 API。例如 `cactor_patterns` 重导出 `cactor_foundation.circuit_breaker.*`，用户只需 `import cactor_patterns.*`。

## 常用命令

```bash
cjpm build                                        # 构建整个 workspace
cjpm test                                         # 运行所有模块的 @Test
cjpm run --name cactor_examples.hello_world_demo  # 在 workspace 根目录运行示例
```
