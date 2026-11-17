# CActor Examples 示例集合 (v1.2.0)

## 📚 示例概览

这里收集了 CActor 的示例程序，基于仓颉 1.2.0。标注为“原型”的示例演示的是接口设计，还没有真实的网络传输或存储，详见根目录 README 的“当前限制”。

## 🎉 功能示例

- **Ask模式示例** - 请求-响应模式
- **集群分片示例** - 集群分片接口演示（原型）
- **持久化示例** - 事件溯源和CQRS接口演示（原型）
- **流处理示例** - CActor 背压流（Reactive Streams）
- **状态机示例** - XState 风格状态机（实验性）

## 🎯 入门示例

### 1. Hello World Demo
**文件**: `hello_world_demo/`  
**运行**: `cjpm run --name cactor_examples.hello_world_demo`  
**难度**: ⭐ 入门级

最简单的CActor入门示例，展示：
- 基本的Actor创建和消息传递
- Actor生命周期管理
- 系统启动和关闭
- 适合初学者的完整注释

### 2. API Demo
**文件**: `api_demo/`  
**运行**: `cjpm run --name cactor_examples.api_demo`  
**难度**: ⭐⭐ 初级

展示CActor API的使用方式：
- 简化的Actor系统创建
- 扩展方法的使用
- 批量操作演示

## 🚀 性能示例

### 3. Benchmark Demo
**文件**: `benchmark_demo/`  
**运行**: `cjpm run --name cactor_examples.benchmark_demo`  
**难度**: ⭐⭐ 初级

性能基准测试示例：
- 高吞吐量消息处理
- 性能指标测量
- 延迟统计分析

## 🔧 高级功能示例

### 🎯 Ask模式示例
**文件**: `ask_demo/`
**运行**: `cjpm run --name cactor_examples.ask_demo`
**难度**: ⭐⭐⭐ 中级

展示 Ask 模式的用法：
- 请求-响应模式
- 异步回调处理
- 批量请求处理
- 超时管理机制

### 🏢 集群分片示例
**文件**: `best09_cluster_sharding/`
**运行**: `cjpm run --name cactor_examples.best09_cluster_sharding`
**难度**: ⭐⭐⭐⭐ 高级

演示真实的集群分片语义（单进程内起三个远程节点 + 三个 `ClusterSharding` region，带自检）：
- gossip 成员协议与 Leader 收敛
- 按 entityId 路由到唯一实体，同一实体的消息落在同一份状态上
- 实体分布在各节点；生产环境用 `tools/cluster_demo.sh` 起多进程

### 💾 持久化示例
**文件**: `persistence_demo/`
**运行**: `cjpm run --name cactor_examples.persistence_demo`
**难度**: ⭐⭐⭐⭐ 高级

展示事件溯源和CQRS：
- 事件存储和重放
- 状态快照管理
- CQRS架构
- 版本控制
- 多存储后端

### 🌊 流处理示例
**文件**: `streams_demo/`
**运行**: `cjpm run --name cactor_examples.streams_demo`
**难度**: ⭐⭐⭐ 中级

展示 CActor 背压流（E60，对标 Akka Streams）：
- 惰性蓝图与物化（同一张图运行两次）
- 无限源 + 慢消费者 + async 边界：已生产 − 已消费不超过边界缓冲
- `ask` 有界并行、`Sources.queue` 溢出策略、kill switch、`actorRefWithBackpressure`

### 🔄 状态机示例
**文件**: `xstate_demo/`
**运行**: `cjpm run --name cactor_examples.xstate_demo`
**难度**: ⭐⭐⭐⭐ 高级

展示XState状态机系统：
- 强类型状态机
- 模式匹配引擎
- 与 CActor 集成

### 4. Advanced Dispatcher Examples
**文件**: `advanced_dispatcher_examples/`  
**运行**: `cjpm run --name cactor_examples.advanced_dispatcher_examples`  
**难度**: ⭐⭐⭐ 中级

高级调度器功能演示：
- 线程池调度器（CPU密集型任务）
- 工作窃取调度器（负载均衡）
- 固定线程调度器（实时性能）

### 5. Advanced Mailbox Examples
**文件**: `advanced_mailbox_examples/`  
**运行**: `cjpm run --name cactor_examples.advanced_mailbox_examples`  
**难度**: ⭐⭐⭐ 中级

高级邮箱功能演示：
- SPSC邮箱（高频交易场景）
- MPSC邮箱（多生产者场景）
- 有界邮箱（背压控制）

### 6. Configuration Usage Test
**文件**: `configuration_usage_test/`  
**运行**: `cjpm run --name cactor_examples.configuration_usage_test`  
**难度**: ⭐⭐ 初级

配置系统使用示例：
- 邮箱配置
- 调度器配置
- 监督策略配置

## 🎭 类型安全示例

### 7. Typed Actor Demo
**文件**: `typed_actor_demo/`  
**运行**: `cjpm run --name cactor_examples.typed_actor_demo`  
**难度**: ⭐⭐⭐ 中级

类型安全Actor系统：
- TypedActor使用
- 编译时类型检查
- 类型安全消息处理

### 8. Enhanced Typed Actor Demo
**文件**: `enhanced_typed_actor_demo/`  
**运行**: `cjpm run --name cactor_examples.enhanced_typed_actor_demo`  
**难度**: ⭐⭐⭐⭐ 高级

增强的类型安全Actor：
- 高级类型安全特性
- 复杂消息类型处理

## 🔮 宏系统示例

### 9. Enhanced Actor Test
**文件**: `enhanced_actor_test/`  
**运行**: `cjpm run --name cactor_examples.enhanced_actor_test`  
**难度**: ⭐⭐⭐ 中级

宏增强的Actor：
- @actor宏使用
- @handler宏标记
- 自动消息路由

### 10. True Actor Macro Test
**文件**: `true_actor_macro_test/`  
**运行**: `cjpm run --name cactor_examples.true_actor_macro_test`  
**难度**: ⭐⭐⭐⭐ 高级

真实宏功能测试：
- 编译时代码生成
- 性能监控注入
- 类型安全检查

### 11. Comprehensive Macro Test
**文件**: `comprehensive_macro_test/`  
**运行**: `cjpm run --name cactor_examples.comprehensive_macro_test`  
**难度**: ⭐⭐⭐⭐ 高级

综合宏功能演示：
- 多种宏的组合使用
- 复杂代码生成
- 宏系统集成测试

## 🧪 综合测试示例

### 12. Comprehensive Improvement Test
**文件**: `comprehensive_improvement_test/`  
**运行**: `cjpm run --name cactor_examples.comprehensive_improvement_test`  
**难度**: ⭐⭐⭐⭐⭐ 专家级

综合功能改进测试：
- 类型安全Actor系统
- Ask模式实现
- 监督策略完善
- 宏系统集成

## 📋 学习路径推荐

### 初学者路径
1. **Hello World Demo** - 了解基本概念
2. **API Demo** - 学习API使用
3. **Benchmark Demo** - 体验性能特性
4. **Configuration Usage Test** - 掌握配置系统

### 进阶路径
1. **Advanced Dispatcher Examples** - 深入调度器
2. **Advanced Mailbox Examples** - 掌握邮箱系统
3. **Typed Actor Demo** - 学习类型安全
4. **Enhanced Actor Test** - 使用宏系统

### 专家路径
1. **True Actor Macro Test** - 深入宏功能
2. **Comprehensive Macro Test** - 宏系统集成
3. **Comprehensive Improvement Test** - 综合应用

## 🚀 快速开始

```bash
# 1. 构建所有示例
cjpm build

# 2. 从Hello World开始
cjpm run --name cactor_examples.hello_world_demo

# 3. 尝试性能测试
cjpm run --name cactor_examples.benchmark_demo

# 4. 探索高级功能
cjpm run --name cactor_examples.advanced_dispatcher_examples
```

## 💡 提示

- 每个示例都有详细的注释和说明
- 建议按照难度等级逐步学习
- 观察控制台输出了解运行过程
- 可以修改示例代码进行实验

## 🔗 相关文档

- [CActor API参考](../../../docs/zh/api-reference.md)
- [快速开始指南](../../../docs/zh/QUICK_START.md)
- [架构设计文档](../../../docs/zh/architecture.md)

<!-- BEGIN:example-index（由 tools/check_examples.py --update-readme 生成，勿手改） -->

## 示例清单（自动生成）

| 示例 | 是否在门禁运行 | 说明 |
|---|---|---|
| `advanced_dispatcher_examples` | ✅ 运行 | 自检退出码 0 |
| `advanced_mailbox_examples` | ✅ 运行 | 自检退出码 0 |
| `agent_orchestration_demo` | ✅ 运行 | 自检退出码 0 |
| `api_demo` | — | not-built：cjpm.toml 未声明 executable（API 用法演示，代码在 main.cj） |
| `ask_demo` | ✅ 运行 | 自检退出码 0 |
| `ask_pattern_demo` | ✅ 运行 | 自检退出码 0 |
| `benchmark_demo` | — | not-built：cjpm.toml 未声明 executable |
| `best01_hello_typed` | ✅ 运行 | 自检退出码 0 |
| `best02_ask_roundtrip` | ✅ 运行 | 自检退出码 0 |
| `best03_supervision_tree` | ✅ 运行 | 自检退出码 0 |
| `best04_state_machine` | ✅ 运行 | 自检退出码 0 |
| `best05_backpressure` | ✅ 运行 | 自检退出码 0 |
| `best06_receptionist_pool` | ✅ 运行 | 自检退出码 0 |
| `best07_event_sourced_account` | ✅ 运行 | 自检退出码 0 |
| `best08_remote_nodes` | ✅ 运行 | 自检退出码 0 |
| `best09_cluster_sharding` | ✅ 运行 | 自检退出码 0 |
| `best10_agent_workflow` | ✅ 运行 | 自检退出码 0 |
| `cangjie_dsl_features` | ✅ 运行 | 自检退出码 0 |
| `cangjie_features_demo` | ✅ 运行 | 自检退出码 0 |
| `chat_room` | ✅ 运行 | 自检退出码 0 |
| `cluster_crash_node` | ✅ 运行 | 自检退出码 0 |
| `cluster_node` | ✅ 运行 | 自检退出码 0 |
| `config_system_demo` | — | not-built：cjpm.toml 未声明 executable |
| `configuration_usage_test` | ✅ 运行 | 自检退出码 0 |
| `ddata_stress` | ✅ 运行 | 自检退出码 0 |
| `dsl_agent` | ✅ 运行 | 自检退出码 0 |
| `dsl_bench` | — | measurement：DSL 零开销对比，需重复采样才能判读，不进每次门禁 |
| `dsl_showcase` | ✅ 运行 | 自检退出码 0 |
| `dsl_state_machine` | ✅ 运行 | 自检退出码 0 |
| `durable_reminder_demo` | ✅ 运行 | 自检退出码 0 |
| `enhanced_serialization_demo` | — | not-built：cjpm.toml 未声明 executable（enhanced 序列化未收敛） |
| `enhanced_typed_actor_demo` | ✅ 运行 | 自检退出码 0 |
| `error_handling_demo` | — | not-built：目录内只有 pkg.cj，没有 main |
| `functional_actor_demo` | ✅ 运行 | 自检退出码 0 |
| `hello_world_demo` | ✅ 运行 | 自检退出码 0 |
| `idle_probe` | — | measurement：空闲调度探测（3 秒采样），用于调参而非门禁 |
| `journal_crash` | ✅ 运行 | 自检退出码 0 |
| `lease_contend` | ✅ 运行 | 自检退出码 0 |
| `llm_http_demo` | ✅ 运行 | 自检退出码 0 |
| `llm_token_stream` | ✅ 运行 | 自检退出码 0 |
| `mem_profile` | — | measurement：内存剖析，输出为参考值 |
| `micro_benchmark` | — | measurement：仓颉原语微基准，供 bench/compare.sh 使用 |
| `monitoring_demo` | ✅ 运行 | 自检退出码 0 |
| `objectpool_performance_comparison` | ✅ 运行 | 自检退出码 0 |
| `persistence_demo` | ✅ 运行 | 自检退出码 0 |
| `real01_agent_runtime` | ✅ 运行 | 自检退出码 0 |
| `real02_tenant_orders` | ✅ 运行 | 自检退出码 0 |
| `real03_event_backbone` | ✅ 运行 | 自检退出码 0 |
| `real04_edge_config` | ✅ 运行 | 自检退出码 0 |
| `real05_durable_workflow` | ✅ 运行 | 自检退出码 0 |
| `real06_http_actor_api` | ✅ 运行 | 自检退出码 0 |
| `real07_cluster_pubsub_chat` | ✅ 运行 | 自检退出码 0 |
| `real08_cluster_singleton_job` | ✅ 运行 | 自检退出码 0 |
| `real09_grains_cart` | ✅ 运行 | 自检退出码 0 |
| `real10_fsm_order` | ✅ 运行 | 自检退出码 0 |
| `real11_router_pool` | ✅ 运行 | 自检退出码 0 |
| `real12_streams_sse` | ✅ 运行 | 自检退出码 0 |
| `real13_projection_read_model` | ✅ 运行 | 自检退出码 0 |
| `real14_circuit_breaker` | ✅ 运行 | 自检退出码 0 |
| `real_benchmark` | ✅ 运行 | 自检退出码 0 |
| `scale_latency` | ✅ 运行 | 自检退出码 0 |
| `scale_test` | ✅ 运行 | 自检退出码 0 |
| `simple_objectpool_test` | ✅ 运行 | 自检退出码 0 |
| `simple_serialization_demo` | — | known-failing：性能段除零（总耗时 0ms 时 totalTime 作除数），待修后接入 |
| `soak_test` | ✅ 运行 | 自检退出码 0 |
| `stdlib_objectpool_test` | ✅ 运行 | 自检退出码 0 |
| `streams_demo` | ✅ 运行 | 自检退出码 0 |
| `test_framework_demo` | ✅ 运行 | 自检退出码 0 |
| `test_framework_validation` | — | known-failing：TestActorRefFactory 用毫秒时间戳命名，同毫秒内两次 create 撞名（testkit 缺陷） |
| `typed_actor_demo` | ✅ 运行 | 自检退出码 0 |
| `typed_persistence_demo` | ✅ 运行 | 自检退出码 0 |
| `typed_spawn_bench` | — | measurement：类型化 spawn 微基准，数字随负载抖动，单跑不判定 |
| `xstate_demo` | ✅ 运行 | 自检退出码 0 |

<!-- END:example-index -->
