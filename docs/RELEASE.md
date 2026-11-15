# CActor 发布指南

本指南约定正式版本的检查与发布步骤；文档补齐不代表 E30 / E51 已全部完成，也不代表某个 tag 或中心仓版本已经发布。执行依据为 [发布脚本](../tools/publish.sh)、[API 快照工具](../tools/api_snapshot.py)、[完整 CI](../tools/ci.sh) 与各模块的 `cjpm.toml`。

## 1. 选择版本并同步 manifest

正式发行采用语义化版本 `X.Y.Z`，Git tag 为 `vX.Y.Z`：不兼容的公开 API / 行为变化升主版本，向后兼容的新能力升次版本，兼容修复升补丁版本。公开包迁移、声明包或二进制类型身份改变、移除符号、默认行为或序列化协议变化，都需要评估消费者迁移和混合版本兼容性，不能只看函数名是否变化。

以下 12 个库必须同步到同一个发行版本，按 `tools/publish.sh` 的依赖顺序发布：

```text
foundation → macros → core → runtime → cactor → patterns
→ testkit → streams → remote → persistence → cluster → xstate
```

顺序是发布顺序，不表示相邻模块都直接依赖前一个。版本字段位于 `modules/<模块>/cjpm.toml`；根 `cjpm.toml` 是 workspace 配置，没有单独的 package version。`tests` / `examples` 是 workspace 成员，不在这 12 个发布库之中；其演示依赖及文档版本也应核对。

2026-11-14 静态核对时，12 库 manifest 均为 `1.2.1`。ROADMAP / CHANGELOG 中的“计划 v2.xx”是实施批次标识，不能直接作为发行版本或已发布 tag 的证据。当前未发布内容包含破坏性变更，须先完成版本决策，不能直接复用 `1.2.1` 发布。

脚本的 `VERSION` 默认为 foundation 的版本，用于发布日志和索引等待；它不会统一改写各库自身的版本。依赖转换也逐个读取被依赖库的 manifest，所以设置 `VERSION=X.Y.Z` 不能代替修改并核对全部 12 个版本。

## 2. 公开 API、弃用与变更记录

有意修改公开签名后，在 workspace 根目录执行：

```bash
python3 tools/api_snapshot.py --update
python3 tools/api_snapshot.py --check
```

审查并纳入 `docs/api-snapshot/` 的差异，同时更新 [CHANGELOG](../CHANGELOG.md)、[ROADMAP](../ROADMAP.md) 和相关 API / 用户文档。破坏性变更必须说明受影响符号或行为、旧写法与新写法、重编译要求，以及持久化格式或远程协议的升级约束。

快照是词法签名门禁，不是自动 semver 判定器，也不保证行为或二进制兼容。`--update` 只是登记差异，不能取代兼容性审查；没有签名变化的默认值、行为或协议变化仍要记录。

弃用应提供 `since`、可操作的替代入口，以及明确的移除版本或保留承诺。2026-11-14 对库源码中行首 `@Deprecated[...]` 的静态盘点如下（排除 tests / examples 与注释文本）：

- 共 46 处：persistence 39、patterns 3、cactor 2、core 1、runtime 1，全部带 `since`。
- 41 处文本已声明在 `2.0` / `2.0.0` 移除；仍有 5 处没有移除版本：`CActorSystemBuilder.withDispatcher`、`CActorSystemBuilder.withMailbox`（[源码](../modules/cactor/src/api/public/cactor_factory.cj)），`BackoffSupervisorStrategy`（[源码](../modules/core/src/supervision/supervision_strategy.cj)），`Ask.askTyped`（[源码](../modules/patterns/src/ask/ask_api.cj)），`CoordinatedShutdown.get`（[源码](../modules/runtime/src/lifecycle/coordinated_shutdown.cj)）。本次文档审计不替这些 API 决定移除时间。
- `Ask` 与 `AskPatternManager` 注解使用 `since: "1.3.0"`，而当前 manifest 为 `1.2.1`。须在发行版本决策时核对首次包含弃用标记的实际版本；仅凭当前工作区不能判定历史发行事实。

## 3. 正式发行门禁

在 workspace 根目录准备环境并运行完整 CI：

```bash
source tools/env.sh
python3 tools/api_snapshot.py --check
tools/ci.sh
```

`tools/env.sh` 准备仓颉工具链与 `CANGJIE_STDX_PATH`，后者用于 remote / cluster 的 stdx 依赖。安装位置与默认值不同时，先设置 `CANGJIE_HOME`（例如安装于 `~/cangjie` 时设置为 `$HOME/cangjie`）。workspace 的构建选项包含 `-O2`，不要用单模块构建代替发行验证。CI 结果须对应最终发行提交；记录源码提交、工具链、操作系统、完整退出状态和日志。检查变更后若再修改代码或公开面，应重新验证受影响门禁。

macOS 上需使用支持 `mapfile` 的 Bash（系统 Bash 3.2 不支持），并让 stdx 加载 OpenSSL 3。已通过 Homebrew 安装 Bash 与 `openssl@3` 时，可在上述命令前设置：

```bash
export PATH="$(brew --prefix bash)/bin:$PATH"
export STDX_OPENSSL_CRYPTO_FILE="$(brew --prefix openssl@3)/lib/libcrypto.3.dylib"
export STDX_OPENSSL_SSL_FILE="$(brew --prefix openssl@3)/lib/libssl.3.dylib"
```

两个 OpenSSL 变量必须一起设置。否则 stdx 可能回退加载 macOS 的无版本号 `libcrypto`，在 TLS 测试中直接 abort；这不是 TLS 断言失败，也不能当作已完成 TLS 验证。

涉及性能声明时，另在空闲机器运行 `tools/ci.sh --bench`，记录基线与候选提交、CPU / cgroup 限额、构建参数、线程数、负载、轮数及原始结果，交替多轮比较。历史报告和微基准不构成当前版本的性能保证。若本次只检查文档，应明确报告未运行编译 / 测试，不能据此宣布完整 CI 通过。

## 4. 固定发行提交与 tag

将上述版本、快照、变更记录和通过门禁的最终结果纳入发行提交后，核对所有库版本、CHANGELOG 标题与 tag 一致，再创建带说明的 tag：

```bash
# X.Y.Z 为已经审查的实际发行版本，替换后执行
git tag -a vX.Y.Z <release-commit> -m "CActor X.Y.Z"
git show --no-patch vX.Y.Z
```

需要向远端发布 tag 时，按项目权限执行 `git push origin vX.Y.Z`。不要覆盖或移动已经公开的发行 tag。2026-11-14 静态核对未发现 `.github/workflows/`；此处是人工发行步骤，不表示 push tag 会自动运行 CI 或发布包。执行 tag / push / publish 前应完成对应授权和检查。

### @Deprecated 生命周期与 2.0.0 移除清单

弃用与移除遵循 Akka 惯例：`@Deprecated` 的 `since` 声明弃用版本，移除版本写在注解消息文本里（仓颉注解没有 `remove` 参数），固定为 **since 的下一个 semver 大版本**——当前全部为 `2.0.0`。2.x 系列内不移除任何公开符号；2.0.0 发布前对照 [`docs/deprecations.md`](deprecations.md) 逐项确认替代入口可用、迁移文档覆盖、api_snapshot 同步删除。

- 全部 `@Deprecated` 注解必须带 `since` 与移除版本（CI 门禁 `tools/check_deprecations.py` 强制，见 §2 与 E51）；
- 2026-11-14 盘点：46 处（persistence 39、patterns 3、cactor 2、core 1、runtime 1），46/46 已声明在 2.0.0 移除，明细见 [`docs/deprecations.md`](deprecations.md)。

## 5. 流水线与分支保护（E50）

CI 分两层：**PR 门禁**（每次合并请求必跑）与 **nightly 长压测**（每天一次）。托管平台为 atomgit（Cangjie-SIG 组织）；平台若支持声明式流水线配置，按其文档在仓库根目录放置对应配置文件（如 `.atomgit/pipeline.yml`），接线方式如下；若平台只支持界面配置，按同一张表在界面录入。本节描述的是应达成的约定，不代表平台侧已经配置完成。

**PR 门禁（必须绿色才允许合并）：**

| 项 | 约定 |
|---|---|
| 触发 | 每个 PR（opened / updated） |
| 命令 | `source tools/env.sh && tools/ci.sh`（不带 `--soak` / `--bench`，全量约 5–10 min） |
| 分支保护 | `main` 禁止直推：合并请求必须评审 + 上述流水线绿色；直推 main 应被平台拒绝 |
| 失败处理 | 红了先修，不允许跳过门禁合并；flake 用例按记录重跑并注明 |

**nightly（定时，建议凌晨低负载时段）：**

| 项 | 约定 |
|---|---|
| 触发 | 定时（cron，每日一次） |
| 命令 | `source tools/env.sh && tools/ci_nightly.sh` |
| 内容 | `ci.sh --soak 600`（完整门禁 + 600 s 稳定性 soak）→ `stress_matrix.sh quick`（压测矩阵）→ `cluster_parallel_soak.sh 10 8`（集群套件 8 进程并行 10 轮，回归 N0）→ `mixed_version_soak.sh`（混合版本多进程验证，DA6 / DA4 / DA8） |
| 失败处理 | 通知维护者；每步都有 timeout 防挂死，失败保留日志与 `bench/results/` 产物 |
| 本地预演 | `tools/ci_nightly.sh --dry-run` 校验参数与步骤（不真正运行）；`--skip-ci` 跳过已单独跑过的第一步 |

注意：nightly **不**接入默认 PR 门禁（时长与负载敏感）；`tools/ci.sh` 只包含轻量回归步骤（`mixed_version_soak.sh` 约 1 min 已作为一行 guard 接入示例自检段）。

## 6. 发布到中心仓

先确认中心仓凭据已在 `$CANGJIE_HOME/tools/config/cangjie-repo.toml` 的 `[repository.home]` 配好，避免把 token 写进文档或发行日志。所有库的模块根目录须有 README；本次静态核对 12 库都有 `README.md`。

在通过验证、与 tag 一致的源码上执行：

```bash
source tools/env.sh
tools/publish.sh
```

脚本将库复制到 workspace 外的暂存目录，把本地 path 依赖改成各库的中心仓版本依赖，逐库执行 `cjpm bundle` 和 `cjpm publish`。暂存依赖从中心仓解析，所以同版本的下层库必须先成功发布。

产物包含 `cjpm.toml`，暂存 manifest 禁止含 `[replace]` 或绝对路径。脚本在 bundle 前用 `^\[replace\]|"/` 守卫拒绝这些内容；这不是完整 TOML 安全分析，仍应审查依赖与暂存 manifest。stdx 路径使用 `${CANGJIE_STDX_PATH}` 环境占位，不能把发布机绝对路径打进消费者产物。

默认 bundle 跳过模块测试和 lint，不能替代发布前完整 CI。可用 `TESTS=1` / `LINT=1` 开启对应阶段。脚本清理本机 cjpm 索引缓存，等待该版本进入中心仓索引后才继续下一个库；索引延迟或网络失败仍可能导致部分发布完成，保留暂存位置与日志以便核查。

中心仓不允许同名同版本覆盖，也没有删除 / 撤回命令。`tools/publish.sh core runtime` 可选择模块，但仅用于核对实际已发布状态后的续发；脚本会恢复依赖顺序，不会自动判断发行是否完整。已经公开的错误产物需要新版本修复；若重发修订版，12 库版本仍须同步，不能依赖覆写旧产物。

发布后核对 12 个库的确切版本已进入索引，并用独立消费者目录从中心仓解析依赖，确认构建不依赖发布机 workspace 路径。记录 tag / 提交、中心仓版本、发布日志及消费者验证结果。未完成这些步骤前，不将 E30 / E51 的 tag、semver 与发布验收记为完成。
