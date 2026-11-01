# 📝 CActor 更新日志

本文档记录了 CActor 项目的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### ⚡ 性能（与 protoactor-go 对比，详见 docs/plan/20261031-protoactor-performance.md）
- **多线程吞吐不再随线程数下降**：`invoke` 不再对每条消息做 `message is AutoReceivedMessage`，改为直接匹配 5 种控制消息类。
  仓颉判断"别的包里的类是否实现某接口"在多线程并发时互相阻塞（4 线程每次约 1µs），用户消息总是命中这条慢路径。
  多对 ping-pong（场景 6）从 1.2M 升到 33.6M msg/s，同一时间窗口内约为 protoactor-go（36.2M）的 93%，进程 CPU 基本相同
- **类型化路径**：core 的泛型代码以用户类型实例化后，`is C<T>` 和分配 `C<T>` 都要在运行时查找类型信息，多线程时互相阻塞
  - `TypedBehaviors.isSame / isUnhandled / isStopped / isDeferred`、`start()` 与适配器改为非泛型基类 + 种类标记，
    监督行为与信号 / 转换 / 过滤 / 拦截行为也带标记，普通行为只做一次非泛型类检查
  - `TypedBehaviors.receive / receiveMessage / setup / same / unhandled / stopped / ignore / empty / receivePartial` 标为 `@Frozen`：
    `-O2` 下在调用方的包里实例化，`same()` 4 线程时每次从约 0.4µs 降到约 20ns。这些函数的实现会编译进调用方，修改后依赖方需重新编译
- **运行时不再启动从不使用的 `DispatcherPool`**：它原来随每个运行时启动 4 个旧调度器，线程以 10–100µs 间隔空转轮询，
  空闲时占约 11% CPU 并干扰 Actor 工作线程；去掉后空闲 CPU 约为原来的 1/8
- **邮箱改为双缓冲数组**（与 protoactor-go 的 goring 同类）：稳态零分配，单线程每条 CPU 约 214ns → 161ns，GC 次数降为 0
- **调度**：runNext（被本线程唤醒的 Actor 紧接着在本线程运行，往来频繁的 Actor 留在同一线程）、
  空闲线程举手 + 忙线程捐赠 Cell 的负载均衡；默认工作线程数不超过 cgroup CPU 限额（与 Go 1.25 的 GOMAXPROCS 规则一致）；
  查当前工作线程用 `Thread.currentThread.id`，不用 `ThreadLocal`（仓颉 `ThreadLocal.get` 多线程并发时每次阻塞数微秒）；
  runNext 槽位可被空闲线程窃取（与 Go 同理）：某个线程卡在同一次运行里超过 100µs 时（例如 receive 里同步等待刚唤醒的 Actor），
  空闲线程取走它 runNext 里的 Cell，两者不会互相等待
- **基准**：`real_benchmark` 新增场景 6（多对 ping-pong，protoactor 官方写法）与场景 7（类型化多对互发），
  输出测量窗口内每条消息的 CPU 时间；新增对照程序 `bench/protoactor`


### ✨ 新增（Actor DSL：docs/plan/dsl20261030.md P1–P5 全部落地）
- **`@actor` 属性宏 + 协议清单**：`@actor[cases = [A, B]]` / `@actor[protocol = E]` 在编译期核对
  漏处理、协议漂移、重复处理器；签名支持 `onXxx(m)` / `onXxx(m, ctx)` / 返回 `Unit` 或 `MessageResult`；
  不带方括号的 `@actor` / `@actorReceive` 旧写法由同名非属性宏承接，继续可用
- **`@Message` 增强**：支持 `Option<T>`、`HashMap<String, T>` 字段；字段级错误用 `diagReport` 定位（带行列）
- **`@StateMachine` 宏**：状态枚举 + 转换表生成状态机骨架；漏状态（穷尽 match）、写错状态名、
  非法转换均在编译期报错；宏生成的转换表可直接对接 xstate `StateMachineBuilder`；
  每个转换生成 `guard<From><To>` / `onTransition<From><To>` 骨架，同名覆写即生效；
  支持命名守卫（`transitions = [(A, B, 守卫方法名)]`，方法缺失编译期报错）与
  子状态机（`submachines = [(状态, 子机类型)]`，进入复合状态自动复位子机）；
  `canTransition` / `transitionTo` 展开为 `(from, to)` 枚举 match（无分配、无字符串比较）
- **经典路径 ask**：`askInbox()` 零侵入临时收件箱（`cactor_core.dsl`）——请求消息自带 `replyTo`，
  目标 Actor 无需解包包装消息；超时抛 `TypedAskTimeoutException`（与 typed ask 共用），`tryGet` 返回 `Option`
- **行为层门面**：`cactor_core.dsl.Behaviors`（receive/setup/supervise/withTimers/withStash/intercept/…）
  与 `messageResultToBehavior` 打通宏路径与 lambda 路径语义（`Restart` / `Escalate` 抛
  `SupervisionRequestedException` 交给监督者，与经典运行时一致）
- **流管道 API**：`source |> via(flow) |> to(sink)`（`cactor_streams.dsl`，`to(sink)` 走 `runWith`，与链式写法语义等价）
- **用户文档**：`docs/dsl.md`（三档写法选型、"一句英语"自检表、经典/DSL 对照）

### 🔧 变更
- **ask 收敛**：`cactor_patterns.ask.TypedAskPattern` / `TypedAsk`（早期模拟实现）标记 `@Deprecated`，
  唯一推荐入口为 `cactor_core.typed.TypedAsk.ask`（进程外）与 `TypedActorContext.ask`（Actor 内）
- **公开面**：`cactor` 门面 re-export `cactor_core.dsl.*`；宏包公开宏收敛为 4 个
  （`@actor`、`@actorReceive`（兼容别名）、`@Message`、`@StateMachine`），无占位宏
- **CI 增强**：`tools/ci.sh` 新增宏负例编译测试（`tools/negative_tests.sh`，11 用例）、
  宏公开面检查（`tools/check_macro_surface.sh`）、DSL 行数对照（`tools/dsl_line_count.sh`，代码行降幅 43%）
- **示例**：`hello_world_demo` 改用 `@actor`；新增 `dsl_showcase`（经典/DSL 三组对照）、
  `dsl_agent`（AI Agent 协议一屏 DSL，G5）、
  `dsl_state_machine`（@Message + @StateMachine + @actor + xstate 对接）、`dsl_pipeline`（流管道）、
  `dsl_bench`（宏 vs 手写零开销基准，最好成绩比值 97.5%）

### ⚡ 性能
- **`@Message` 编码**：`MessageFieldWriter` 由 `ArrayList<UInt8>` 逐字节追加改为数组缓冲 + 下标写入，
  编码一条 283 字节的消息快约 1.5 倍；线上字节格式不变（`testWireFormatIsStable` 逐字节锁定）
- **`HashMap` 字段解码**：先按每条最小字节数核对剩余数据，再按条目数预分配容量（快 1.4–3 倍）
- **`@StateMachine`**：`canTransition` / `transitionTo` 改为 `(from, to)` 枚举 match，比查数组 + 字符串比较快 1.8–2.0 倍

### 🐛 修复
- **分片的记忆实体在 receive 里同步等待 Replicator**：`ShardRegionActor` 启动 / 停止实体时调用阻塞的 `addToSet` / `removeFromSet`，
  Replicator 与它排在同一工作线程时要等 10 秒超时，实体启动失败。改用新增的
  `Replicator.updateAsync / addToSetAsync / removeFromSetAsync`（不等待本地生效，`modify` 的异常记录为警告）；
  `update` 的文档注明不要在 receive 里调用
- **`askInbox().get` 在 Actor 处理方法内阻塞会死锁**：默认亲和调度没有工作窃取，被等的 Actor 与调用方
  分在同一工作线程时互相卡住（`dsl_agent` 示例用 `CACTOR_DISPATCHER_THREADS=1` 可稳定复现）。
  现在亲和工作线程上带超时的 `get` / `tryGet` 立即抛 `IllegalStateException`；`blocking` 调度器不受限制。
  运行时新增 `cactor_core.actor.isAffinityWorkerThread()` 标记
- **`dsl_agent` 示例**改为非阻塞写法：工具结果回到 Agent 邮箱（`replyTo = ctx.self`），按 `requestId` 续上对话；
  自检并发 16 轮对话，单工作线程也能通过
- **消息解码更严格**：`Option` 标记只接受 0 / 1；`HashMap` 字段的条目数先校验再分配，重复键视为数据损坏
- **`within`** 改用单调时钟计时（墙钟调整时不再误报或漏报慢处理）
- **`dsl_bench`** 新增不经调度器的直接分派基准作为 A3 判定依据（端到端 ping-pong 噪声过大，仅作参考）

### 计划中
- HTTP集成和Web服务器
- 序列化优化和多格式支持
- 可视化监控工具
- 云原生支持

## [1.2.0] - 2026-10-28

### 🎉 全面模块化大版本

本版本将 CActor 从单包工程全面重构为多模块 workspace，并统一项目版本号、仓颉工具链版本和全部文档。

### 💥 破坏性变更

#### 多模块工程重构
- **单包拆分为 workspace**: 原 `src/` 下的单一 `cactor` 包拆分为 `modules/` 下 12 个独立仓颉模块，根 `cjpm.toml` 改为 `[workspace]`
- **模块划分**（依赖自底向上，无环）:
  - `cactor_foundation` - 基础设施：队列、内存、序列化、网络、配置、DI、监控、断路器
  - `cactor_core` - 核心抽象：Actor/ActorRef/ActorSystem/Message/Supervision/Typed
  - `cactor_patterns` - 通用模式：Ask、路由、背压
  - `cactor_runtime` - 运行时：mailbox、dispatcher、guardian、registry、lifecycle
  - `cactor` - 门面模块：统一对外 API（`cactor` / `cactor.api` / `cactor.config`）
  - `cactor_macros` - Actor DSL 宏
  - `cactor_streams` - 流处理 DSL
  - `cactor_distribution` - 分布式：cluster / remote / persistence / streaming（实验性）
  - `cactor_xstate` - XState 风格状态机
  - `cactor_integration` - 日志、指标、配置与集成测试程序
  - `cactor_tests` - 单元测试与集成测试
  - `cactor_examples` - 示例程序
- **包名变更**: 内部包由 `cactor.<层>.*` 改为 `cactor_<模块>.*`，例如 `cactor.core.actor` → `cactor_core.actor`、`cactor.runtime` → `cactor_runtime`
- **断路器迁移**: `cactor.patterns.circuit_breaker` 迁至 `cactor_foundation.circuit_breaker`，由 `cactor_patterns` 重导出
- **产物类型**: 库模块输出由 `dynamic` 改为 `static`

### 🔢 版本统一
- **项目版本号统一为 1.2.0**: 全部 12 个模块的 `version` 统一为 `1.2.0`；文档及源码注释中的项目版本标识一律统一为 1.2.0，版本线承接 1.1.0

### 🔧 工具链
- **仓颉版本升级**: `cjc-version` 由 `0.60.5` 升级到 `1.2.0`，现有 std API 无需迁移

### 📚 文档
- README、CONTRIBUTING、`docs/`（中英文）、示例 README、`xstate.md` 统一到 1.2.0：项目版本、仓颉版本要求（原 0.53.4 / 0.60.5 / 0.7.0）及安装/部署示例
- 文档中的包名同步为模块化后的新包名（`cactor_core.*`、`cactor_runtime.*` 等）
- 架构文档版本历史新增 1.2.0 记录

### 🧹 项目清理
- 移除空的 `source/CangjieCorpus` 子模块引用
- 移除过时的过程文档：`task0.20.md`、`CURRENT_PROGRESS_REPORT.md`、`PROJECT_STATUS.md`、`docs/DOCUMENTATION_COMPLETION_REPORT.md`
- 修正 `docs/DOCUMENTATION_STRUCTURE.md` 中的失效链接

### 📦 迁移指南
- 只依赖门面 API（`import cactor.*`、`cactor.api.*`、`cactor.config.*`）的代码无需修改
- 直接引用内部包的代码需按上述规则替换包名，并在 `cjpm.toml` 中通过 `path` 依赖对应模块

## [1.1.0] - 2025-07-03

### 🎉 重大版本更新

这是 CActor 的重大版本更新，完成了所有核心企业级功能的实现。

### ✨ 新增功能

#### Ask模式完善
- **生产级实现**: 完整的请求-响应模式支持
- **高性能**: 657,894 req/s，1,520 ns/req
- **异步支持**: 完整的异步回调和批量处理
- **超时管理**: 完善的超时处理机制
- **类型安全**: 编译时类型检查

#### 集群管理增强
- **集群分片系统**: 完整的分片协调和管理
- **多种路由策略**: 轮询、一致性哈希、随机、广播等
- **负载均衡**: 智能负载均衡和健康检查
- **故障转移**: 自动故障检测和恢复
- **支持规模**: 100万Actor，8个工作线程

#### 持久化系统完善
- **事件溯源**: 完整的事件存储和重放机制
- **CQRS架构**: 命令查询责任分离
- **快照管理**: 内存和文件快照存储
- **多存储后端**: 支持内存和文件存储
- **版本控制**: 事件版本管理和冲突检测

#### 测试框架建设
- **TestKit框架**: 完整的测试工具集
- **同步测试**: TestActorRef和同步消息处理
- **消息验证**: TestProbe和期望检查
- **性能测试**: 吞吐量和延迟测试支持
- **集成测试**: 与CActor系统完美集成

#### CActor Streams流处理系统
- **高性能**: 500万元素/秒吞吐量
- **背压控制**: 完整的流量控制机制
- **丰富操作符**: map、filter、fold、scan、distinct等
- **DSL支持**: 声明式流处理管道
- **高内聚低耦合**: 企业级架构设计

#### XState状态机系统
- **强类型**: 基于仓颉枚举的类型安全状态
- **模式匹配**: 高性能的状态转换引擎
- **CActor集成**: 状态机即Actor，天然并发
- **企业级特性**: 事件溯源、状态快照
- **DSL支持**: 声明式状态机定义

### 🏗️ 架构改进

#### 7层架构完善
- **Foundation Layer**: 并发原语、序列化、网络通信
- **Core Layer**: Actor、消息、系统核心抽象
- **Runtime Layer**: 调度器、邮箱、定时器
- **Patterns Layer**: Ask、路由、断路器
- **Distribution Layer**: 远程、集群、持久化
- **Integration Layer**: 监控、日志、配置
- **API Layer**: 统一对外接口

#### 仓颉0.60.5完全适配
- **语法更新**: 完全适配最新仓颉语法
- **API升级**: HashMap.add()、ArrayList.add()等
- **类型系统**: 充分利用仓颉强类型特性
- **并发原语**: 使用仓颉原生并发支持

### 📊 性能提升

#### 核心性能指标
- **Ask模式**: 657,894 req/s，1,520 ns/req
- **集群分片**: 支持100万Actor
- **流处理**: 500万元素/秒
- **状态机**: 纳秒级状态转换
- **整体延迟**: 保持微秒级延迟

### 🧪 测试覆盖

#### 完整测试体系
- **单元测试**: 所有核心组件100%覆盖
- **集成测试**: 端到端功能验证
- **性能测试**: 全面的性能基准测试
- **示例验证**: 所有示例程序正常运行
- **编译验证**: 无错误，仅Unicode字符警告

### 🔧 开发体验

#### 完善的示例系统
- **Hello World**: 入门示例
- **Ask模式**: 请求-响应示例
- **集群分片**: 分布式示例
- **持久化**: 事件溯源示例
- **流处理**: 流处理管道示例
- **状态机**: XState状态机示例

## [1.0.0] - 2025-06-22

### 🎉 首个正式版本

这是 CActor 的首个正式版本，提供了完整的 Actor 系统实现和企业级特性。

### ✨ 新增功能

#### 核心 Actor 系统
- **完整的 Actor 模型**: 实现了标准的 Actor 模型，包括消息传递、状态封装和位置透明性
- **高性能消息传递**: 优化的消息传递机制，支持高吞吐量和低延迟
- **Actor 生命周期管理**: 完整的 Actor 创建、启动、停止和销毁流程
- **监督策略**: 实现了 OneForOne、AllForOne 等监督策略
- **Actor 层次结构**: 支持父子 Actor 关系和监督树

#### 性能优化
- **微秒级延迟**: 所有测试场景下都保持微秒级延迟
- **I/O 密集型优化**: 在 I/O 密集型任务中达到 166,667 msg/s
- **复杂计算优化**: 在中等到重量级计算任务中性能领先
- **内存优化**: 高效的对象池和内存管理
- **批处理支持**: 消息批处理以提高吞吐量

#### 企业级特性
- **Ask 模式**: 完整的请求-响应模式支持
- **路由器**: 多种路由策略（轮询、随机、一致性哈希等）
- **断路器**: 故障隔离和自动恢复机制
- **背压控制**: 流量控制和过载保护
- **配置管理**: 灵活的配置系统

#### 运行时系统
- **多种调度器**: 支持不同的调度策略
- **多种邮箱**: 有界、无界、优先级邮箱
- **定时器支持**: 延迟消息和周期性任务
- **线程池管理**: 高效的线程池实现
- **NUMA 感知**: 针对 NUMA 架构的优化

#### 监控和诊断
- **性能指标**: 详细的性能监控指标
- **健康检查**: Actor 系统健康状态监控
- **调试工具**: 性能分析和诊断工具
- **日志集成**: 结构化日志支持

### 📊 性能基准

基于与主流 Actor 框架的全面对比测试：

| 测试类型 | CActor | ProtoActor-Go | Actix | 排名 |
|----------|--------|---------------|-------|------|
| 轻量级 | 454,545 msg/s | 3,333,333 msg/s | 96,154 msg/s | 🥈 |
| 中等强度 | **54,348 msg/s** | 47,619 msg/s | 47,170 msg/s | 🥇 |
| 重量级 | **35,714 msg/s** | 9,615 msg/s | 9,434 msg/s | 🥇 |
| CPU密集型 | 11,364 msg/s | 17,391 msg/s | 16,393 msg/s | 🥉 |
| I/O密集型 | **166,667 msg/s** | 4,951 msg/s | 4,902 msg/s | 🥇 |
| 内存密集型 | 177 msg/s | 894 msg/s | 971 msg/s | 🥉 |
| 混合负载 | 386 msg/s | 4,975 msg/s | 4,000 msg/s | 🥉 |

#### 延迟性能（所有场景 CActor 最优）
- **轻量级**: 0.0019ms（比其他框架快 1.4-850倍）
- **I/O密集型**: 0.0019ms（比其他框架快 14000-24000倍）
- **内存密集型**: 0.0030ms（比其他框架快 170000+倍）

### 🏗️ 架构设计

实现了完整的 6 层模块化架构：

```
API Layer (API层)           ← 统一对外接口
Integration Layer (集成层)    ← 监控、日志、配置
Distribution Layer (分布式层) ← 远程、集群、持久化
Patterns Layer (模式层)      ← Ask、路由、断路器
Runtime Layer (运行时层)     ← 调度器、邮箱、定时器
Core Layer (核心层)         ← Actor、消息、系统
Foundation Layer (基础设施层) ← 并发、序列化、网络
```

### 📚 文档

- **完整的 API 文档**: 详细的 API 参考文档
- **架构设计文档**: 深入的架构设计说明
- **性能优化指南**: 性能调优最佳实践
- **示例代码**: 丰富的示例和教程
- **基准测试报告**: 详细的性能测试结果

### 🧪 测试覆盖

- **单元测试**: 核心功能的单元测试
- **集成测试**: 系统集成测试
- **性能测试**: 全面的性能基准测试
- **压力测试**: 高负载场景测试
- **示例验证**: 所有示例代码的验证

### 🔧 开发工具

- **构建系统**: 基于 cjpm 的构建配置
- **代码规范**: 统一的代码风格和规范
- **CI/CD**: 持续集成和部署流程
- **性能分析**: 性能分析和优化工具

## [0.9.0] - 2025-06-15

### ✨ 新增功能
- 实现基础 Actor 系统
- 添加消息传递机制
- 实现简单的调度器

### 🐛 修复问题
- 修复内存泄漏问题
- 优化消息处理性能

## [0.8.0] - 2025-06-10

### ✨ 新增功能
- 项目初始化
- 基础架构设计
- 核心接口定义

---

## 版本说明

### 版本号格式
- **主版本号**: 不兼容的 API 修改
- **次版本号**: 向下兼容的功能性新增
- **修订号**: 向下兼容的问题修正

### 变更类型
- **新增功能** (Added): 新功能
- **修改** (Changed): 对现有功能的变更
- **弃用** (Deprecated): 即将移除的功能
- **移除** (Removed): 已移除的功能
- **修复** (Fixed): 任何 bug 修复
- **安全** (Security): 安全相关的修复

---

*更多详细信息请查看 [GitHub Releases](../../releases)*
