# CLAUDE.md — CActor 项目指南

CActor 是仓颉（Cangjie）语言的 Actor 模型库，定位为 AI Agent 的底层运行时。
仓库为 cjpm 多模块 workspace，根目录 `cjpm.toml` 定义模块与依赖方向（自底向上、无环）。

## 常用命令

```bash
# 必须在 workspace 根目录执行（单模块 cjpm build -m 不继承 -O2）
source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh"   # 若 cjpm 不在 PATH
cjpm build                 # 构建（-O2）
cjpm test                  # 全部测试
tools/ci.sh                # 构建 + 模块分层检查 + 全部测试（提交前必须通过）
tools/ci.sh --bench        # 另跑基准
python3 tools/check_module_deps.py   # 模块依赖方向检查
```

示例可执行文件在 `./target/release/bin/`，如 `cactor_examples.chat_room`。
示例日志用 `CACTOR_LOG_LEVEL=error` 静音。

## 模块分层（依赖只能向下）

```
foundation -> core -> runtime -> patterns；runtime -> cactor(门面)
-> streams；remote / persistence -> cluster -> distribution(聚合) / xstate
-> integration -> tests / examples
macros 为独立宏模块，仅被 examples 使用
```

新增 import 前先想清楚层次，`check_module_deps.py` 会在 CI 拦住反向依赖。

## 核心编程模型（写代码前必读）

- **类型化 Actor 优先**（对标 Akka Typed，`modules/core/src/typed/`）：
  - 行为是函数：`TypedBehavior<T>.onMessage(msg, ctx) -> TypedBehavior<T>`；
    `TypedBehaviors.setup / same / unhandled / stopped / receive / withTimers / withStash / intercept / supervise`。
  - 状态用"返回新行为"保存（不可变，无锁），如 `agent(memory.concat([fact]))`。
  - `TypedActorRef<T>`：`tell` / `<<`（可连写 `ref << a << b`）；ask 用 `TypedAsk.ask<T, R>(ref, factory, timeout)`。
- **枚举即协议**：消息协议就是一个 `enum`，`match` 穷尽检查在编译期发现漏处理的消息。
- **DSL**：`100.millis`、`3.seconds` 时间字面量；`@actorReceive` 宏（`modules/macros/`）
  根据 `onXxx(msg[, ctx])` 方法生成 `receive`；同一类型两个处理方法会编译期报错。
  宏只能用于 class，且不能手写 `receive`。
- **监督**：`TypedBehaviors.supervise(b).onFailure(strategy)`，策略
  `restart / resume / stop / backoff`，`.withLimit(n, within)`；未处理的 `TypedTerminated` 即 DeathPact。
- **经典 Actor**（`modules/core/src/actor/`）仍受支持，但新代码请用类型化 API。
- **测试**（`modules/testkit/`）：`TestInbox<T>` / `TestProbe`、`BehaviorTestKit`（Effect 断言）、
  `ManualTime` 虚拟时钟。测试代码不要 sleep 等真实时间，用 ManualTime。

## 代码风格（docs/CODE_STYLE_GUIDE.md）

- 类/接口 PascalCase，函数/变量 camelCase；消息用不可变 `struct`。
- 中文注释，注释解释"为什么"而不是复述代码。
- 文件头：`/* CActor Core.Typed - ... */` + 包用途说明。

## 文档地图

- `README.md` / `docs/zh/INTRODUCTION.md`：设计理念
- `docs/zh/BEST_PRACTICES.md`：Actor 设计原则（单一职责、不可变消息）
- `docs/zh/architecture.md`、`docs/zh/package-structure.md`
- `docs/zh/api-reference.md`、`docs/zh/getting-started.md`
- `CHANGELOG.md`、`ROADMAP.md`：改公共 API 必须同步更新

## 提交规范

Conventional Commits（`feat:` / `fix:` / `docs:` / `refactor:` / `test:`）。
