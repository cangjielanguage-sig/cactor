# 十个最佳示例：设计与实现计划（2026-10-30）

> **一句话**：把 CActor 最有代表性的 10 个能力各写成一个**可运行、可自检、退出码可判定**的示例，全部放
> 在 `modules/examples/src/best/` 下统一命名 `best01_*` … `best10_*`，每个示例既是教程也是回归测试。
>
> 关联：[20261029.md](20261029.md)（HTTP/API）、[dsl20261030.md](dsl20261030.md)（DSL）、根 README「示例程序」。

---

## 1. 现状问题

当前 `modules/examples/src` 有 46 个目录，是三个时代的叠加物：

- 大量 `*_demo` 只 `println` 成功、不校验结果，失败也返回 0；
- 若干“测试型示例”（`enhanced_actor_test`、`comprehensive_*`）已删除或为占位；
- 同一能力有多处重复（3 套 ask、多套 streams demo）；
- 没有一条“从入门到分布式”的清晰路径。

## 2. 选型原则

1. **每个示例验证一组已验证 [V] 的语义**，不演示占位 API（`CActorFactory.createSystem`、XState 的简化 Actor 等排除）。
2. **自检**：每个示例在结束时对结果断言，失败打印原因并返回 1；CI 可直接把它当冒烟测试。
3. **一行可运行**：`cjpm build && ./target/release/bin/cactor_examples.best0N_xxx`。
4. **难度递进**：01–04 单机核心；05–07 生产模式；08–10 分布式/工程化。
5. **类型化 API 优先**；经典 API 只在演示监督树结构（best03）与远程（best08）时使用。

## 3. 十个示例总表

| # | 目录 | 主题 | 展示的 API | 难度 |
|---|------|------|-----------|------|
| 01 | `best01_hello_typed` | 类型化 Actor 与消息协议 | `TypedBehaviors.receive/same`、enum 协议、`<<`、`typedActorOf` | ⭐ |
| 02 | `best02_ask_roundtrip` | 请求-响应 | `TypedAsk.ask<T,R>`、`ctx.ask`（Actor 内）、超时 | ⭐ |
| 03 | `best03_supervision_tree` | 监督树与容错 | `supervise().onFailure(withLimit)`、`watch`、`TypedTerminated`、经典 Props 策略 | ⭐⭐ |
| 04 | `best04_state_machine` | 行为状态机（热身/暂停） | `withStash` + `withTimers` + 返回新行为 | ⭐⭐ |
| 05 | `best05_backpressure_pipeline` | 有界邮箱与背压 | `TypedProps().withBoundedMailbox`、`blocking` 调度器、`deadLetterCount` | ⭐⭐ |
| 06 | `best06_receptionist_pool` | 服务发现 + 路由池 | `Receptionist`、`RoundRobinRoutingStrategy`、`ask` 聚合 | ⭐⭐⭐ |
| 07 | `best07_event_sourced_account` | 事件溯源 | `EventSourcedActor`、`FileEventStore`、快照、重启恢复 | ⭐⭐⭐ |
| 08 | `best08_remote_nodes` | 远程通信 | `SimpleRemoteManager`、`exposeActor`、远程 ask、`tellReliable`、远程 watch | ⭐⭐⭐⭐ |
| 09 | `best09_cluster_sharding` | 集群分片（多进程） | `Cluster`、`ClusterSharding`、实体路由；复用 `tools/cluster_demo.sh` 思路 | ⭐⭐⭐⭐ |
| 10 | `best10_agent_workflow` | AI Agent 工作流 | 综合：Agent + 工具 Actor + Ask + 监督 + 有界邮箱 + 定时器 | ⭐⭐⭐⭐ |

## 4. 每个示例的验收脚本

```bash
for b in target/release/bin/cactor_examples.best*; do
  CACTOR_LOG_LEVEL=error "$b" || echo "FAIL: $b"
done
tools/cluster_demo.sh     # best09 的多进程部分（可选，需 30s）
```

全部退出码 0 即通过。best09 也可以单独用 `tools/cluster_demo.sh` 验证（已有）。

## 5. 实施步骤

1. 建 `modules/examples/src/best/`，10 个子目录各一个 `main.cj`；`cjpm.toml` 增加对应 `output-type = "executable"` 配置。
2. 逐个实现并编译（每完成 2–3 个构建一次，控制并行链接竞态：`cjpm build -j 4`）。
3. 每个示例本地运行确认退出码 0 且输出自检通过。
4. 更新根 README「示例程序」表：新增 10 行，标注难度与对应文档章节。
5. 更新 `modules/examples/src/README.md`：把「入门路径」指向 best01→best10。
6. `tools/ci.sh` 不改（examples 的可执行目标本就在 build 范围内）；可选加一个 `--best` 开关循环跑 10 个示例。

## 6. 代码骨架约定

- 文件头注释：说明主题、演示的 API、运行方式、自检内容。
- 协议用 enum + `match` 穷尽；状态用“返回新行为”。
- 所有等待用 `awaitCondition`/`get(timeout)`，不裸 sleep（演示处除外，best05 有意测量发送方限速）。
- 结尾统一：`println(if (ok) {"通过"} else {"失败"}); return ok ? 0 : 1`。

## 7. 风险

- 并行链接竞态（`cannot find -lxxx` 偶发）：用 `-j 4` 构建，或重试一次。
- best08/09 涉及端口：绑定端口 0（系统分配），无冲突。
- best10 的“模型调用”用可配置延迟的假 Tool Actor 模拟，不引外部依赖。

## 8. 与文档计划的衔接

- dsl20261030 P2 的「示例改造」完成后，best01/02 可追加 DSL 版本（`@actor` / `@Message`）作对照。
- 20261029 的 `actorSystem {}` 新入口落地后，best01 的创建代码同步替换。
