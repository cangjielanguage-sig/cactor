# CActor v0.22 生产级改造任务清单（task0.20）

## 说明
- 目标：按照 plan0.22.md，将 CActor 对齐 Akka 生产能力，构建高内聚、低耦合、高扩展的架构与实现，彻底移除 mock/placeholder。
- 优先级：P0（必须，先做）/ P1（重要）/ P2（次要）
- 估时单位：人日（D），仅用于相对排序与排期参考
- 验收标准：除功能正确外，需满足文末“产线SLO/SLA、CI矩阵与基准线”要求

---

## Milestones（阶段目标）
- M1（Week 1-3）：P0 完成（Typed/Behavior、监督、调度/邮箱、Cluster基座、Mock清理）
- M2（Week 4-6）：P1 完成（Sharding/Singleton/SBR、Persistence SPI、Streams Materializer）
- M3（Week 7-8）：P2 完成（配置中心/热更新、可观测性标准化、多节点/Chaos 测试）
- M4（Week 9-10）：性能/扩展性基线与优化收敛；产线SLO/SLA机制与Runbook就绪

---

## P0（必须优先完成）

### [P0-1] Core/Runtime Typed & Behavior 能力补齐（Akka Typed 对齐）
- 路径：
  - src/core/typed/typed_behavior.cj（Behavior/MessageAdapter/变换）
  - src/core/typed/typed_actor_ref.cj（TypedActorRef/Selection）
  - src/core/typed/typed_protocol.cj（Typed消息契约）
  - src/runtime/typed/typed_actor_cell.cj（执行单元）
  - src/runtime/typed/timers.cj（Timers/Backoff）
  - src/runtime/typed/stash_buffer.cj（Stash）
- 依赖：Foundation.memory → Core.typed → Runtime.typed
- 交付：类型安全 API、定时器、消息适配、暂存箱、单测/集成测
- 验收：
  - 编译通过；Typed 示例编译与运行通过
  - 单测：Typed 行为/适配/暂存/定时器 ≥ 30 用例，覆盖率 ≥ 90%
  - 基准：本地 Tell ≥ 10M msg/s；Ask ≥ 1M req/s
- 估时：12D
- 状态：[x] 部分完成（本次增量已完成基础连通、Stash/Timers/Cell骨架与冒烟测试）
- 本次提交范围：
  - API：CActorSystem.typedActorOf 路由至 TypedActorSystemImpl，避免 Dummy 包装
  - Runtime：TypedActorSystemImpl.createActor 使用泛型重定向到真实创建逻辑
  - Runtime：新增 runtime/typed/{stash_buffer.cj, timers.cj, typed_actor_cell.cj} 最小真实实现
  - Tests：新增/修复 src/tests/unit/runtime/typed/{typed_smoke_test.cj, stash_buffer_test.cj, timers_test.cj} 并通过
- 待完成：
  - core/typed/{typed_actor_ref.cj, typed_protocol.cj} 的能力增强（MessageAdapter/Behavior变换）
  - 单测扩充 ≥ 30 用例与性能基准


### [P0-2] 监督与生命周期（BackoffSupervisor、CoordinatedShutdown）
- 路径：
  - src/core/supervision/supervision_strategy.cj（补齐 Resume/Restart/Stop/Escalate）
  - src/runtime/supervision/backoff_supervisor.cj
  - src/runtime/lifecycle/coordinated_shutdown.cj（分阶段关停）
- 依赖：Core.supervision → Runtime.supervision → Runtime.lifecycle
- 交付：完整监督矩阵、指数回退重启、协调关停 API
- 验收：
  - 单测：策略矩阵/回退重启/阶段化关停 ≥ 20 用例
  - 集成测：异常注入与自动恢复通过
- 估时：8D

### [P0-3] 调度与邮箱能力（吞吐、优先级、批处理、背压）
- 路径：
  - src/runtime/dispatcher/work_stealing_dispatcher.cj（throughput/throughput-deadline）
  - src/runtime/dispatcher/priority_dispatcher.cj
  - src/runtime/mailbox/priority_mailbox.cj
  - src/runtime/mailbox/bounded_mailbox.cj
  - src/runtime/mailbox/batching_mailbox.cj
  - src/runtime/mailbox/backpressure_mailbox.cj
  - 修复：src/runtime/mailbox/lockfree/lockfree_mailbox.cj 移除 createPlaceholderEnvelope，使用真实 Envelope
- 依赖：Runtime.dispatcher/mailbox
- 交付：吞吐/优先级/批处理/背压可配；优先级消息正确性
- 验收：
  - 基准：在高负载下吞吐/延迟优于现状 ≥ 20%
  - 单测：邮箱/调度策略 ≥ 25 用例；优先级与背压正确性
- 估时：10D

### [P0-4] 分布式 Cluster 基座（Gossip + Phi + 成员/协调）
- 路径：
  - src/foundation/network/failure_detector_phi.cj
  - src/distribution/cluster/gossip_protocol.cj
  - src/distribution/cluster/cluster_membership.cj
  - src/distribution/cluster/cluster_coordinator.cj
- 依赖：Foundation.network → Distribution.cluster
- 交付：节点发现/心跳/Phi 算法/状态传播/成员一致视图
- 验收：
  - 多节点集成测：3/5/7 节点加入/离开、视图一致性
  - 误报率与收敛时间在CI矩阵阈值内
- 估时：12D

### [P0-5] 移除 Mock 与占位符（系统性清理）
- 路径与动作：
  - 删除：src/tests/unit/core/mock/mock_support_test.cj；src/tests/unit/core/mock/pkg.cj（如存在）
  - 替代：使用 integration/testing/test_kit.cj、test_probe.cj
  - 替换/重命名：
    - src/xstate/enterprise/placeholder.cj（拆分增强 enterprise 功能）
    - src/xstate/features/placeholder.cj（增强 hierarchical/parallel 等）
    - src/examples/*/placeholder.cj → main.cj（详见 plan0.22.md 列表）
    - src/core/memory/placeholder.cj → simple_memory_manager.cj（或迁移至 foundation/memory/）
    - src/streams/unified/placeholder.cj → unified_stream_pipeline.cj
    - src/examples/functional_actor_demo/placeholder.cj → main.cj
  - 修复：src/runtime/mailbox/lockfree/lockfree_mailbox.cj 的占位 Envelope 逻辑
- 交付：不存在 mock/placeholder；所有示例可运行
- 验收：
  - 全仓编译通过；单/集成测试通过
  - grep 无“placeholder/mock/简化实现/TODO/FIXME/HACK”（白名单除外）
- 估时：6D

---

## P1（重要）

### [P1-1] Cluster Sharding / Singleton / Split-Brain Resolver
- 路径：
  - src/distribution/cluster/sharding/{shard_coordinator.cj, shard_region.cj, entity_extractor.cj}
  - src/distribution/cluster/singleton/cluster_singleton_manager.cj
  - src/distribution/cluster/sbr/split_brain_resolver.cj
- 依赖：P0 Cluster 基座
- 交付：分片再均衡/实体迁移/单例主从切换/脑裂策略
- 验收：
  - 多节点CI用例：再均衡正确性、单例唯一性、SBR 决策正确
- 估时：14D

### [P1-2] 持久化 SPI + 参考实现 + 投影
- 路径：
  - src/distribution/persistence/spi/persistence_spi.cj（Journal/Snapshot 接口）
  - src/distribution/persistence/journal/journal_inmemory.cj
  - src/distribution/persistence/snapshot/snapshot_inmemory.cj
  - src/distribution/persistence/projection/projection_runner.cj
- 依赖：Foundation.serialization → SPI
- 交付：插件化持久化与投影；最小可用参考实现
- 验收：
  - 单/集成测试覆盖事件溯源/快照/重放/投影
- 估时：10D

### [P1-3] Streams Materializer 与端到端背压/监督
- 路径：
  - src/streams/core/materializer.cj
  - src/streams/core/stages/{map_stage.cj, filter_stage.cj, merge_stage.cj, balance_stage.cj}
- 依赖：Runtime.mailbox 背压/监督；Patterns.backpressure
- 交付：可组合流图、统一背压、异常监督
- 验收：
  - 性能：≥ 10M 元素/秒（单机）
  - 单/集成测试覆盖常见算子与背压传播
- 估时：9D

---

## P2（次要）

### [P2-1] 配置中心（分层/热更新/回滚/审计）
- 路径：
  - src/foundation/config/unified_configuration.cj（增强层级覆盖/动态变更/审计）
  - src/foundation/config/rollback_manager.cj
- 交付：多环境/多租户隔离，热更新与回滚
- 验收：
  - 单/集成测试覆盖动态变更、冲突与回滚
- 估时：6D

### [P2-2] 可观测性标准化（Tracing/Metrics/Structured Logging）
- 路径：
  - src/integration/monitoring/tracing.cj（traceId/spanId 上下文传播）
  - src/integration/monitoring/metrics.cj（RED/USE 指标）
  - src/integration/logging/structured_logging.cj（MDC/Markers）
- 交付：OpenTelemetry/Prometheus 适配，结构化日志
- 验收：
  - 仪表盘/告警规则生效；端到端 trace 贯穿
- 估时：7D

### [P2-3] 多节点测试基建与 Chaos 注入
- 路径：
  - src/integration/testing/multi_node_test_harness.cj
  - src/integration/testing/chaos_injector.cj
- 交付：CI 支持 3/5/7 节点矩阵，网络分区/时钟漂移/节点故障注入
- 验收：
  - CI 稳定运行，基线生成，失败阻断合并
- 估时：8D

---

## 横切任务
- [XT-1] 基准测试套件更新：吞吐/延迟/内存/扩展性（对齐 SLO）
- [XT-2] 文档：API/架构/Runbook/部署/故障排除（同步进度）
- [XT-3] 兼容性与序列化演进策略：Schema 版本与兼容检查
- [XT-4] 安全：TLS/mTLS、RBAC、密钥管理与审计

---

## 依赖拓扑（摘要）
Foundation.memory/network/serialization → Core.actor/message/typed → Runtime.*（dispatcher/mailbox/lifecycle/supervision/typed） → Patterns.*（ask/routing/backpressure） → Distribution.cluster/persistence/remote/streaming → Integration.monitoring/logging/testing → API

---

## 产线 SLO/SLA、Runbook 与多节点CI矩阵（验收网）
- SLO/SLA：按 plan0.22.md 对应章节
- Runbook：按模板提纲落地（故障分类/诊断/缓解/RCA）
- CI矩阵：3/5/7 节点 × 低/中/高负载 × 故障注入 × 调度/邮箱策略组合；失败阻断合并；输出趋势/基线报告

---

## 风险与回滚
- 渐进式特性开关/灰度；严格回滚策略与隔离机制；高优先级路径保持向后兼容



---

## P0 任务 → PR 变更与验证步骤

### [P0-1] Core/Runtime Typed & Behavior（Akka Typed 对齐）
- PR-1: core/typed 基础设施落地
  - 变更：新增 src/core/typed/{typed_behavior.cj, typed_actor_ref.cj, typed_protocol.cj}
  - 要点：Behavior/MessageAdapter/行为变换；TypedActorRef/Selection；协议与类型契约
  - 测试：新增 tests/unit/typed/{behavior_test.cj, adapter_test.cj, selection_test.cj}
  - 验证：
    - cjpm build && cjpm test -p cactor.tests
    - 运行 examples/typed_actor_demo/main.cj 验证编译与运行
- PR-2: runtime/typed 执行单元 + Timers + Stash
  - 变更：新增 src/runtime/typed/{typed_actor_cell.cj, timers.cj, stash_buffer.cj}
  - 要点：定时器（一次/周期/Backoff）、暂存箱（容量/溢出策略）
  - 测试：tests/unit/runtime/typed/{timers_test.cj, stash_test.cj}
  - 验证：Typed Ask/Tell 延迟与吞吐基准（与现状对比）
- 风险/回滚：通过feature flag启用（api层注入 typed.enabled=true）；PR-1可独立合入，PR-2按需回滚不影响Core

### [P0-2] 监督与生命周期（BackoffSupervisor、CoordinatedShutdown）
- PR-1: 扩展 Supervision 策略矩阵
  - 变更：src/core/supervision/supervision_strategy.cj 完整化矩阵与策略DSL
  - 测试：tests/unit/core/supervision/supervision_matrix_test.cj
- PR-2: BackoffSupervisor 实现
  - 变更：新增 src/runtime/supervision/backoff_supervisor.cj
  - 测试：tests/integration/supervision/backoff_supervisor_it.cj（注入失败/重启抖动窗口）
- PR-3: CoordinatedShutdown 分阶段关停
  - 变更：新增 src/runtime/lifecycle/coordinated_shutdown.cj；api 绑定钩子
  - 测试：tests/integration/lifecycle/coordinated_shutdown_it.cj
- 验证：运行 examples/error_handling_demo/main.cj；观察日志与恢复
- 风险/回滚：按阶段特性开关 lifecycle.coordinated.enabled，独立回滚不影响Actor处理

### [P0-3] 调度与邮箱（吞吐/优先级/批处理/背压）
- PR-1: 调度器吞吐与优先级
  - 变更：src/runtime/dispatcher/{work_stealing_dispatcher.cj, priority_dispatcher.cj}
  - 测试：tests/unit/runtime/dispatcher/{throughput_test.cj, priority_fairness_test.cj}
- PR-2: 邮箱能力集
  - 变更：src/runtime/mailbox/{priority_mailbox.cj, bounded_mailbox.cj, batching_mailbox.cj, backpressure_mailbox.cj}
  - 测试：tests/unit/runtime/mailbox/{priority_mailbox_test.cj, backpressure_mailbox_test.cj}
- PR-3: 移除占位信封
  - 变更：编辑 src/runtime/mailbox/lockfree/lockfree_mailbox.cj 移除 createPlaceholderEnvelope，使用 Envelope(Message, sender)
  - 测试：tests/unit/runtime/mailbox/lockfree_mailbox_test.cj（覆盖无锁路径）
- 验证：examples/advanced_dispatcher_examples/main.cj 与 advanced_mailbox_examples/main.cj
- 基准：go_protoactor_benchmark/main.go 对齐方法学，补仓颉基准；期望提升≥20%
- 风险/回滚：保持默认“现状”配置，新增策略通过配置启用

### [P0-4] Cluster 基座（Gossip + Phi + 成员/协调）
- PR-1: Phi Accrual Failure Detector
  - 变更：新增 src/foundation/network/failure_detector_phi.cj
  - 测试：tests/unit/distribution/phi_detector_test.cj（误报率/灵敏度曲线）
- PR-2: Gossip 与成员视图
  - 变更：新增 src/distribution/cluster/{gossip_protocol.cj, cluster_membership.cj}
  - 测试：tests/integration/cluster/membership_it.cj（3/5/7节点）
- PR-3: Cluster Coordinator
  - 变更：新增 src/distribution/cluster/cluster_coordinator.cj（心跳/入群/离群/角色）
  - 测试：tests/integration/cluster/coordinator_it.cj
- 验证：examples/cluster_sharding_demo/main.cj 可启动（先不启用分片）
- 风险/回滚：分布式特性全面 behind feature flags（cluster.enabled=false 为默认）

### [P0-5] 移除 Mock 与占位符（系统清理）
- PR-1: 移除测试 mock 目录
  - 操作：git rm -r src/tests/unit/core/mock
  - 测试：全量单测通过；mock 相关用例迁移到 test_kit/test_probe
- PR-2: 占位符重命名与归档
  - 操作：按“Git 命令建议清单”重命名示例与实现文件，去除 placeholder 命名
  - 测试：所有示例 main.cj 可编译运行
- PR-3: 运行时占位逻辑修正
  - 变更：lockfree_mailbox 占位Envelope逻辑改为真实实现（同 P0-3 PR-3）
- 验证：grep 项目中 placeholder/mock/TODO/FIXME/HACK 应为空（白名单除外）
- 风险/回滚：仅文件层变动，可通过 git revert 逐PR回滚
