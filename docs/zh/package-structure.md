# CActor 包与模块结构

CActor 是一个 cjpm **workspace（工作区）**，由 15 个仓颉 **module（模块）** 组成。仓颉中，一个模块就是“根包加上它的全部子包”构成的树，模块的根包名必须与模块名相同；包之间、模块之间都不允许出现循环依赖。

## 目录布局

```
cactor/
├── cjpm.toml                 # [workspace]：成员列表 + 公共编译选项
├── tools/check_module_deps.py  # 模块分层检查
└── modules/
    ├── foundation/           # 模块 cactor_foundation
    │   ├── cjpm.toml         # [package] name = "cactor_foundation"
    │   └── src/
    │       ├── pkg.cj        # 根包 cactor_foundation
    │       └── queue/ ...    # 子包 cactor_foundation.queue
    ├── core/  runtime/  patterns/  cactor/  macros/  streams/
    ├── remote/  persistence/  cluster/  distribution/  xstate/
    └── integration/  tests/  examples/
```

## 模块一览与依赖

| 层 | 模块 | 文件/行数 | 直接依赖 | 说明 |
|---|---|---|---|---|
| L0 | `cactor_foundation` | 35 / 9,323 | — | 队列、内存、序列化、网络、配置、DI、监控、断路器 |
| L0 | `cactor_macros` | 1 / 695 | — | 宏包（macro package） |
| L1 | `cactor_core` | 31 / 6,756 | foundation | Actor / ActorRef / Message / Supervision / Typed 抽象，`DeadLetterRef` |
| L2 | `cactor_runtime` | 63 / 16,238 | foundation, core | Actor Cell（独立邮箱 + 串行调度）、mailbox、dispatcher、guardian、lifecycle |
| L2 | `cactor_remote` | 2 / 285 | foundation, core | 远程管理（实验性） |
| L2 | `cactor_persistence` | 8 / 2,021 | core | 事件溯源、快照、CQRS（实验性） |
| L3 | `cactor_patterns` | 11 / 2,984 | foundation, core | Ask、路由、背压；重导出断路器 |
| L4 | `cactor` | 16 / 2,132 | core, runtime | 门面：`cactor` / `cactor.api.*` / `cactor.config.*` |
| L5 | `cactor_streams` | 6 / 2,541 | core, cactor | 流 DSL |
| L5 | `cactor_cluster` | 6 / 2,004 | foundation, core, remote, cactor | 集群、分片、路由、故障转移（实验性） |
| L6 | `cactor_distribution` | 4 / 654 | core, remote, persistence, cluster | 聚合模块：重导出三者 + streaming |
| L6 | `cactor_xstate` | 17 / 4,852 | core, cactor, cluster | 状态机 |
| L6 | `cactor_integration` | 28 / 8,107 | foundation, core, runtime, cactor | 日志、指标、配置 + 11 个集成测试程序 |
| L7 | `cactor_tests` | 38 / 5,021 | core, runtime, cactor | @Test 用例 |
| L7 | `cactor_examples` | 61 / 13,891 | 多个 | 38 个可执行示例 |

依赖方向（自底向上，无环，由 `cjpm` 与 `tools/check_module_deps.py` 共同保证）：

```
┌─────────────────────────────────────────────────────────────────────────┐
│ L7   cactor_tests          cactor_examples                              │
├─────────────────────────────────────────────────────────────────────────┤
│ L6   cactor_distribution   cactor_xstate         cactor_integration     │
├─────────────────────────────────────────────────────────────────────────┤
│ L5   cactor_cluster        cactor_streams                               │
├─────────────────────────────────────────────────────────────────────────┤
│ L4   cactor（门面）                                                     │
├─────────────────────────────────────────────────────────────────────────┤
│ L3   cactor_patterns                                                    │
├─────────────────────────────────────────────────────────────────────────┤
│ L2   cactor_runtime        cactor_remote         cactor_persistence     │
├─────────────────────────────────────────────────────────────────────────┤
│ L1   cactor_core                                                        │
├─────────────────────────────────────────────────────────────────────────┤
│ L0   cactor_foundation     cactor_macros                                │
└─────────────────────────────────────────────────────────────────────────┘
```

每个模块只依赖比自己低的层，具体依赖见上表。

## 可见性约定

- **模块对外 API**：放在 `public package`（默认）中，声明为 `public`。
- **模块内部实现**：声明为 `protected package`，只在本模块内可见；从其他模块 `import` 会编译失败。目前 `cactor_runtime` 下的 actor、context、guardian、registry、message、monitoring、execution、scheduler、mailbox.lockfree、mailbox.ringbuffer、dispatcher.batch_processing / optimized / work_stealing 均为 protected。
- **重导出**：模块根包通过 `public import` 对外提供 API。例如 `cactor_patterns` 重导出 `cactor_foundation.circuit_breaker.*`，`cactor_distribution` 重导出 `cactor_remote.*`、`cactor_persistence.*`、`cactor_cluster.*`。
- **测试文件**：同包的 `xxx_test.cj` 可以访问 `internal` 成员，且只在 `cjpm test` 时编译（已在 1.2.0 上验证）。

## 常用命令

```bash
cjpm build                                        # 构建整个 workspace
cjpm build -m modules/core                        # 只构建某个成员模块（及其依赖）
cjpm check                                        # 检查依赖并输出包的编译顺序
cjpm tree --depth 1                               # 查看包依赖树
cjpm test                                         # 运行所有模块的 @Test
python3 tools/check_module_deps.py                # 模块分层检查
cjpm run --name cactor_examples.hello_world_demo  # 在 workspace 根目录运行示例
```

## 真实包目录树

由 `modules/*/src` 实际目录生成，括号内为该包的 `.cj` 文件数：

```
modules/foundation/src/            # 模块 cactor_foundation
├── abstractions/  (2)
├── circuit_breaker/  (2)
├── config/  (3)
├── contracts/  (2)
├── di/  (2)
├── memory/  (5)
├── monitoring/  (4)
├── network/  (2)
├── queue/  (6)
├── serialization/  (5)
└── simple_config/  (1)

modules/core/src/            # 模块 cactor_core
├── actor/  (3)
├── config/  (2)
├── context/  (2)
├── error/  (2)
├── guardian/  (2)
├── memory/  (1)
├── message/  (6)
├── supervision/  (5)
├── system/  (3)
└── typed/  (4)

modules/runtime/src/            # 模块 cactor_runtime
├── actor/  (2)
├── context/  (2)
├── dispatcher/  (3)
│   ├── adaptive/  (1)
│   ├── advanced/  (1)
│   ├── batch_processing/  (2)
│   ├── dynamic/  (1)
│   ├── factory/  (2)
│   ├── monitoring/  (3)
│   ├── optimized/  (2)
│   └── work_stealing/  (2)
├── events/  (2)
├── execution/  (1)
├── guardian/  (4)
├── lifecycle/  (2)
├── mailbox/  (5)
│   ├── advanced/  (1)
│   ├── batching/  (2)
│   ├── factory/  (1)
│   ├── lockfree/  (1)
│   ├── queue/  (2)
│   └── ringbuffer/  (2)
├── message/  (2)
├── monitoring/  (1)
├── registry/  (2)
├── scheduler/  (1)
├── supervision/  (3)
├── system/  (3)
├── typed/  (4)
└── wrappers/  (1)

modules/patterns/src/            # 模块 cactor_patterns
├── ask/  (4)
├── backpressure/  (2)
└── routing/  (2)
    ├── advanced/  (1)
    └── factory/  (1)

modules/cactor/src/            # 模块 cactor
├── api/  (5)
│   ├── config/  (2)
│   ├── extensions/  (4)
│   └── public/  (2)
└── config/  (1)
    └── file/  (1)

modules/macros/src/            # 模块 cactor_macros

modules/streams/src/            # 模块 cactor_streams
├── core/  (3)
├── dsl/  (1)
└── unified/  (1)

modules/remote/src/            # 模块 cactor_remote

modules/persistence/src/            # 模块 cactor_persistence

modules/cluster/src/            # 模块 cactor_cluster

modules/distribution/src/            # 模块 cactor_distribution
└── streaming/  (3)

modules/xstate/src/            # 模块 cactor_xstate
├── core/  (1)
│   ├── actor/  (1)
│   ├── engine/  (1)
│   ├── factory/  (1)
│   ├── machine/  (1)
│   └── types/  (3)
├── enterprise/  (1)
│   ├── distributed/  (1)
│   └── event_sourcing/  (1)
├── features/  (1)
│   ├── hierarchical/  (1)
│   └── parallel/  (1)
└── lang/  (1)
    └── macros/  (1)

modules/integration/src/            # 模块 cactor_integration
├── configuration/  (2)
├── logging/  (4)
├── monitoring/  (6)
└── testing/  (4)
    ├── enhanced_dispatcher_test/  (1)
    ├── plan10_actorcontext_test/  (1)
    ├── plan10_advanced_supervision_test/  (1)
    ├── plan10_api_test/  (1)
    ├── plan10_cactor_runtime_test/  (1)
    ├── plan10_comprehensive_test/  (1)
    ├── plan10_router_test/  (1)
    ├── simple_benchmark_test/  (1)
    ├── simple_plan10_test/  (1)
    ├── simple_realistic_benchmark/  (1)
    └── unified_cactor_system_test/  (1)

modules/tests/src/            # 模块 cactor_tests
├── integration/  (4)
└── unit/  (1)
    ├── core/  (1)
    └── runtime/  (1)

modules/examples/src/            # 模块 cactor_examples
└── <45 个示例包>/
```

## 附录：原分层设计图

以下为模块化之前的原设计图，保留作为架构参考。其中 `src/<层>/` 对应现在的 `modules/<层>/src/`，`cactor.<层>` 对应 `cactor_<层>`；部分目录（如 `foundation/concurrency/`）是设计稿中的规划，尚未按此实现，以上面的真实目录树为准。

### 📦 总体包结构

CActor 采用6层模块化架构，每层都有明确的职责和边界：

```
src/
├── cactor.cj                    # 主包导出文件
├── foundation/                  # 基础设施层 (Foundation Layer)
├── core/                       # 核心层 (Core Layer)
├── runtime/                    # 运行时层 (Runtime Layer)
├── patterns/                   # 模式层 (Patterns Layer)
├── distribution/               # 分布式层 (Distribution Layer)
├── integration/                # 集成层 (Integration Layer)
├── api/                        # API层 (API Layer)
├── macros/                     # 宏系统
├── config/                     # 配置文件
└── examples/                   # 示例代码
```

### 🏗️ 详细包结构

#### 1. Foundation Layer (基础设施层)

```
foundation/
├── pkg.cj                      # 包导出文件
├── concurrency/                # 并发原语
│   ├── pkg.cj
│   ├── lockfree_queue.cj      # 无锁队列实现
│   ├── spsc_queue.cj          # 单生产者单消费者队列
│   ├── mpsc_queue.cj          # 多生产者单消费者队列
│   ├── atomic_operations.cj    # 原子操作封装
│   └── thread_pool.cj         # 线程池实现
├── serialization/              # 序列化框架
│   ├── pkg.cj
│   ├── serializer.cj          # 序列化接口
│   ├── json_serializer.cj     # JSON序列化器
│   ├── binary_serializer.cj   # 二进制序列化器
│   └── serialization_manager.cj # 序列化管理器
├── network/                    # 网络通信
│   ├── pkg.cj
│   ├── network_transport.cj    # 网络传输接口
│   ├── tcp_transport.cj       # TCP传输实现
│   ├── udp_transport.cj       # UDP传输实现
│   └── connection_pool.cj     # 连接池
└── memory/                     # 内存管理
    ├── pkg.cj
    ├── object_pool.cj         # 对象池
    ├── memory_pool.cj         # 内存池
    ├── numa_memory_pool.cj    # NUMA感知内存池
    └── smart_gc.cj            # 智能垃圾回收
```

#### 2. Core Layer (核心层)

```
core/
├── pkg.cj                      # 包导出文件
├── actor/                      # Actor抽象
│   ├── pkg.cj
│   ├── actor.cj               # Actor接口
│   ├── actor_ref.cj           # Actor引用
│   ├── actor_path.cj          # Actor路径
│   ├── props.cj               # Actor属性
│   └── actor_lifecycle.cj     # Actor生命周期
├── message/                    # 消息抽象
│   ├── pkg.cj
│   ├── message.cj             # 消息接口
│   ├── envelope.cj            # 消息信封
│   ├── string_message.cj      # 字符串消息
│   ├── json_message.cj        # JSON消息
│   ├── network_message.cj     # 网络消息
│   ├── zerocopy_message.cj    # 零拷贝消息
│   ├── enhanced_message.cj    # 增强消息
│   └── message_serializer.cj  # 消息序列化器
├── system/                     # 系统抽象
│   ├── pkg.cj
│   ├── actor_system.cj        # Actor系统接口
│   ├── system_guardian.cj     # 系统守护者
│   └── dead_letters.cj        # 死信处理
├── context/                    # 上下文
│   ├── pkg.cj
│   ├── actor_context.cj       # Actor上下文接口
│   └── context_impl.cj        # 上下文实现
├── supervision/                # 监督策略
│   ├── pkg.cj
│   ├── supervision_strategy.cj # 监督策略
│   ├── supervision_directive.cj # 监督指令
│   └── fault_handling.cj      # 故障处理
└── config/                     # 核心配置
    ├── pkg.cj
    ├── actor_config.cj        # Actor配置
    ├── mailbox_config.cj      # 邮箱配置
    ├── dispatcher_config.cj   # 调度器配置
    └── supervision_config.cj  # 监督配置
```

#### 3. Runtime Layer (运行时层)

```
runtime/
├── pkg.cj                      # 包导出文件
├── cactor_runtime.cj          # 主运行时管理器
├── plan10_cactor_runtime.cj   # Plan10运行时实现
├── dispatcher/                 # 调度器实现
│   ├── pkg.cj
│   ├── message_dispatcher.cj   # 调度器接口
│   ├── thread_pool_dispatcher.cj # 线程池调度器
│   ├── work_stealing_dispatcher.cj # 工作窃取调度器
│   ├── pinned_dispatcher.cj    # 固定线程调度器
│   ├── numa_dispatcher.cj     # NUMA感知调度器
│   ├── batch_processing/      # 批处理
│   │   ├── batch_processor.cj
│   │   └── batch_config.cj
│   └── advanced/              # 高级调度器
│       ├── foundation_based_dispatcher.cj
│       └── adaptive_dispatcher.cj
├── mailbox/                    # 邮箱实现
│   ├── pkg.cj
│   ├── mailbox.cj             # 邮箱接口
│   ├── unbounded_mailbox.cj   # 无界邮箱
│   ├── bounded_mailbox.cj     # 有界邮箱
│   ├── priority_mailbox.cj    # 优先级邮箱
│   ├── foundation_mailbox.cj  # Foundation邮箱
│   └── mailbox_factory.cj     # 邮箱工厂
├── actor/                      # 运行时Actor
│   ├── pkg.cj
│   ├── high_performance_actor.cj # 高性能Actor
│   └── actor_system_impl.cj   # Actor系统实现
├── context/                    # 运行时上下文
│   ├── pkg.cj
│   ├── pooled_actor_context.cj # 池化Actor上下文
│   └── context_factory.cj     # 上下文工厂
├── lifecycle/                  # 生命周期管理
│   ├── pkg.cj
│   ├── lifecycle_manager.cj   # 生命周期管理器
│   └── actor_lifecycle_state.cj # Actor生命周期状态
├── monitoring/                 # 运行时监控
│   ├── pkg.cj
│   ├── actor_system_metrics.cj # 系统指标
│   └── performance_monitor.cj  # 性能监控器
├── guardian/                   # 守护者实现
│   ├── pkg.cj
│   ├── runtime_system_guardian.cj # 系统守护者
│   └── runtime_user_guardian.cj # 用户守护者
├── registry/                   # Actor注册表
│   ├── pkg.cj
│   └── simple_actor_registry.cj # 简单注册表
├── supervision/                # 监督实现
│   ├── pkg.cj
│   └── supervision_manager.cj  # 监督管理器
├── events/                     # 事件系统
│   ├── pkg.cj
│   ├── actor_event_bus.cj     # Actor事件总线
│   └── simple_actor_event_bus.cj # 简单事件总线
└── message/                    # 运行时消息
    ├── pkg.cj
    └── batch_message_processor.cj # 批量消息处理器
```

#### 4. Patterns Layer (模式层)

```
patterns/
├── pkg.cj                      # 包导出文件
├── ask/                        # Ask模式
│   ├── pkg.cj
│   ├── ask_pattern.cj         # Ask模式实现
│   ├── ask_future.cj          # Ask Future
│   └── ask_timeout.cj         # Ask超时处理
├── routing/                    # 路由模式
│   ├── pkg.cj
│   ├── router.cj              # 路由器接口
│   ├── round_robin_router.cj  # 轮询路由器
│   ├── random_router.cj       # 随机路由器
│   ├── consistent_hash_router.cj # 一致性哈希路由器
│   └── advanced/              # 高级路由
│       ├── advanced_routing.cj
│       └── load_balancer.cj
├── circuit_breaker/            # 断路器模式
│   ├── pkg.cj
│   ├── circuit_breaker.cj     # 断路器实现
│   └── circuit_breaker_config.cj # 断路器配置
├── supervision/                # 监督模式
│   ├── pkg.cj
│   ├── supervision_patterns.cj # 监督模式
│   └── fault_tolerance.cj     # 容错处理
└── backpressure/              # 背压模式
    ├── pkg.cj
    ├── backpressure_strategy.cj # 背压策略
    └── flow_control.cj        # 流量控制
```

#### 5. Distribution Layer (分布式层)

```
distribution/
├── pkg.cj                      # 包导出文件
├── remote/                     # 远程通信
│   ├── pkg.cj
│   ├── remote_actor_ref.cj    # 远程Actor引用
│   ├── remote_transport.cj    # 远程传输
│   └── remote_deployment.cj   # 远程部署
├── cluster/                    # 集群管理
│   ├── pkg.cj
│   ├── cluster_manager.cj     # 集群管理器
│   ├── node_discovery.cj      # 节点发现
│   ├── cluster_state.cj       # 集群状态
│   ├── failover.cj            # 故障转移
│   └── sharding.cj            # 分片支持
├── persistence/                # 持久化
│   ├── pkg.cj
│   ├── persistent_actor.cj    # 持久化Actor
│   ├── event_store.cj         # 事件存储
│   ├── snapshot.cj            # 快照机制
│   └── recovery.cj            # 恢复机制
└── streaming/                  # 流处理
    ├── pkg.cj
    ├── stream_processing.cj   # 流处理
    ├── stream_builder.cj      # 流构建器
    └── reactive_streams.cj    # 反应式流
```

#### 6. Integration Layer (集成层)

```
integration/
├── pkg.cj                      # 包导出文件
├── monitoring/                 # 监控集成
│   ├── pkg.cj
│   ├── metrics.cj             # 指标收集
│   ├── distributed_tracing.cj # 分布式追踪
│   ├── memory_monitor.cj      # 内存监控
│   └── performance_analyzer.cj # 性能分析器
├── logging/                    # 日志集成
│   ├── pkg.cj
│   ├── actor_logger.cj        # Actor日志器
│   └── structured_logging.cj  # 结构化日志
├── configuration/              # 配置管理
│   ├── pkg.cj
│   ├── config_manager.cj      # 配置管理器
│   └── hot_reload.cj          # 热重载
└── testing/                    # 测试框架
    ├── pkg.cj
    ├── test_actor_system.cj   # 测试Actor系统
    ├── test_probe.cj          # 测试探针
    ├── simple_benchmark.cj    # 简单基准测试
    ├── simple_benchmark_test/ # 基准测试
    ├── plan10_comprehensive_test/ # 综合测试
    ├── plan10_cactor_runtime_test/ # 运行时测试
    └── plan10_api_test/       # API测试
```

#### 7. API Layer (API层)

```
api/
├── pkg.cj                      # 包导出文件
├── cactor.cj                  # 统一API入口
├── cactor_system_api.cj       # Actor系统API
├── config/                     # API配置
│   ├── pkg.cj
│   ├── mailbox_config_api.cj  # 邮箱配置API
│   ├── dispatcher_config_api.cj # 调度器配置API
│   └── supervision_config_api.cj # 监督配置API
└── public/                     # 公共API
    ├── pkg.cj
    ├── cactor_factory.cj      # CActor工厂
    └── cactor_system_builder.cj # 系统构建器
```

#### 8. 其他模块

```
macros/                         # 宏系统
├── actor_dsl_macros.cj        # Actor DSL宏

config/                         # 配置文件
└── file/
    ├── config_loader.cj       # 配置加载器
    └── actor_system_config.cj # 系统配置

examples/                       # 示例代码
├── hello_world/               # 基础示例
├── api_demo/                  # API演示
├── benchmark_demo/            # 性能示例
├── advanced_dispatcher_examples/ # 高级调度器示例
├── advanced_mailbox_examples/ # 高级邮箱示例
└── configuration_usage_test/  # 配置使用测试
```

### 🔗 包依赖关系

#### 依赖层次图

```
API Layer
    ↓ (depends on)
Integration Layer
    ↓ (depends on)
Distribution Layer
    ↓ (depends on)
Patterns Layer
    ↓ (depends on)
Runtime Layer
    ↓ (depends on)
Core Layer
    ↓ (depends on)
Foundation Layer
```

#### 包导入规则

1. **向下依赖**: 上层可以导入下层包
2. **禁止向上依赖**: 下层不能导入上层包
3. **同层依赖**: 同层包之间可以相互导入
4. **API隔离**: 用户只导入API层，不直接使用Core层

### 📋 包职责说明

#### Foundation Layer
- **职责**: 提供基础设施和工具
- **特点**: 零依赖，可独立使用
- **包含**: 并发原语、序列化、网络、内存管理

#### Core Layer
- **职责**: 定义核心抽象和接口
- **特点**: 纯抽象，不包含具体实现
- **包含**: Actor、消息、系统、上下文抽象

#### Runtime Layer
- **职责**: 提供高性能执行引擎
- **特点**: 具体实现，性能优化
- **包含**: 调度器、邮箱、生命周期管理

#### Patterns Layer
- **职责**: 实现常用Actor模式
- **特点**: 可选功能，按需使用
- **包含**: Ask、路由、断路器、背压

#### Distribution Layer
- **职责**: 提供分布式能力
- **特点**: 企业级功能
- **包含**: 远程通信、集群、持久化、流处理

#### Integration Layer
- **职责**: 提供集成和工具
- **特点**: 开发和运维支持
- **包含**: 监控、日志、配置、测试

#### API Layer
- **职责**: 提供统一对外接口
- **特点**: 简洁易用，隐藏复杂性
- **包含**: 统一API、配置API、公共API

---

**CActor包结构设计确保了清晰的职责分离和高度的模块化！** 🚀

