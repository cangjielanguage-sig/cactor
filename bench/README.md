# 基准测试

| 程序 | 位置 | 用途 |
|---|---|---|
| CActor 端到端基准 | `modules/examples/src/real_benchmark` | 单 Actor、扇出、ping-pong、Ask、远程 TCP |
| 仓颉原语微基准 | `modules/examples/src/micro_benchmark` | 分配、原子操作、锁、标准库队列、spawn、唤醒的单次开销 |
| Akka 对比基准 | `bench/akka/AkkaBench.java` | 与 `real_benchmark` 场景一一对应的 Akka 2.6.21 实现 |
| protoactor-go 对比基准 | `bench/protoactor/main.go` | 与 `real_benchmark` 场景 1–4、6 对应（需要 Go 1.25.3+） |

```bash
source ~/.cangjie/1.2.0/cangjie/envsetup.sh
cjpm build
./target/release/bin/cactor_examples.real_benchmark     # CActor 端到端
./target/release/bin/cactor_examples.micro_benchmark    # 仓颉原语
bench/akka/run.sh 3                                      # Akka（需要 JDK 17+，首次运行会下载依赖到 bench/akka/jars/）
bench/compare.sh                                         # 两边依次运行，结果保存到 bench/results/
bench/protoactor/run.sh                                  # protoactor-go（首次运行会下载依赖）
```

回归门禁（`tools/bench_gate.sh`，E53）：

```bash
tools/bench_gate.sh --update      # 在本机实测并写入本机基线（需空闲机器，单独提交）
tools/bench_gate.sh               # 与本机基线比较，超阈值退出码 1
tools/bench_gate.sh 10 1          # 覆盖阈值（%）与采样次数（静止 CI 机器用）
```

基线按机器指纹分文件：`bench/results/baseline-<os>-<arch>-<cores>.json`，阈值与采样次数也写在
JSON 里（`threshold_pct` / `repeats`）。**找不到本机基线时脚本以退出码 2 结束**并打印期望的文件名——
不同机器吞吐差数倍，共用一份基线只会"换台机器就红"。临时改用别的基线可设
`CACTOR_BENCH_BASELINE=/path/to/baseline.json`。

约定：

- `real_benchmark` 和 `AkkaBench.java`、`bench/protoactor/main.go` 的场景、消息数、计时方式必须保持一致，修改其中一个时请同步修改另一个。
- 机器上有其他负载（例如容器共用 cgroup 限额）时，墙钟吞吐会大幅波动：两边交替多跑几轮，并以场景 6、7 输出的"窗口 CPU（ns/条）"为准。
- 对比时两边要在同一台机器上运行，并且不要同时运行其他负载。
- Akka 使用默认配置，不调参；JIT 需要预热，取最后一轮的结果。
- CActor 的调度参数：`CACTOR_DISPATCHER_THREADS`（调度线程数）、`CACTOR_DISPATCHER_SPIN_US`（空闲自旋时长），以及仓颉运行时的 `cjProcessorNum`、`cjHeapSize` 等。
- `bench/akka/jars/` 与压力测试原始输出（`bench/results/stress-*`）不纳入版本库；
  **性能基线 `bench/results/baseline-*.json` 必须入库**（门禁依赖它，且要能按机器回溯）。

分析和历史数据见 [docs/plan/20261028-performance.md](../docs/plan/20261028-performance.md)（Akka）
与 [docs/plan/20261031-protoactor-performance.md](../docs/plan/20261031-protoactor-performance.md)（protoactor-go）。
