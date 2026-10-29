# 基准测试

| 程序 | 位置 | 用途 |
|---|---|---|
| CActor 端到端基准 | `modules/examples/src/real_benchmark` | 单 Actor、扇出、ping-pong、Ask、远程 TCP |
| 仓颉原语微基准 | `modules/examples/src/micro_benchmark` | 分配、原子操作、锁、标准库队列、spawn、唤醒的单次开销 |
| Akka 对比基准 | `bench/akka/AkkaBench.java` | 与 `real_benchmark` 场景一一对应的 Akka 2.6.21 实现 |

```bash
source ~/.cangjie/1.2.0/cangjie/envsetup.sh
cjpm build
./target/release/bin/cactor_examples.real_benchmark     # CActor 端到端
./target/release/bin/cactor_examples.micro_benchmark    # 仓颉原语
bench/akka/run.sh 3                                      # Akka（需要 JDK 17+，首次运行会下载依赖到 bench/akka/jars/）
bench/compare.sh                                         # 两边依次运行，结果保存到 bench/results/
```

约定：

- `real_benchmark` 和 `AkkaBench.java` 的场景、消息数、计时方式必须保持一致，修改其中一个时请同步修改另一个。
- 对比时两边要在同一台机器上运行，并且不要同时运行其他负载。
- Akka 使用默认配置，不调参；JIT 需要预热，取最后一轮的结果。
- CActor 的调度参数：`CACTOR_DISPATCHER_THREADS`（调度线程数）、`CACTOR_DISPATCHER_SPIN_US`（空闲自旋时长），以及仓颉运行时的 `cjProcessorNum`、`cjHeapSize` 等。
- `bench/akka/jars/` 和 `bench/results/` 不纳入版本库。

分析和历史数据见 [docs/plan/20261028-performance.md](../docs/plan/20261028-performance.md)。
