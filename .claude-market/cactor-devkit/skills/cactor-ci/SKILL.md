---
name: cactor-ci
description: Build, test and validate the CActor repo. Use when checking whether code compiles or tests pass, before committing, or when running benchmarks in the cactor workspace.
---

# CActor 构建与验证

```bash
# cjpm 不在 PATH 时先 source：
source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh"

cjpm build                          # 必须在仓库根目录
python3 tools/check_module_deps.py  # 模块分层检查
cjpm test                           # 全部测试
tools/ci.sh                         # 上述全部 + 示例自检（提交前的最终门禁）
```

## 注意

- **必须在 workspace 根目录执行**；`cjpm build -m <模块>` 不继承 `-O2`，基准结果慢 2–3 倍。
- 单跑某示例：`CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.<名字>`
  （名字见 `modules/examples/src/` 下的目录）。
- 全量测试较慢；如只改了一个模块，先 `cjpm test` 看失败输出中的 CASE 名定位，再读 `tools/ci.sh` 的 grep 过滤方式复现。
- 基准用 `tools/ci.sh --bench`（需空闲机器），长稳测试 `--soak [秒]`。
