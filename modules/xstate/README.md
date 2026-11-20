# cactor_xstate

> **状态：实验性，已冻结（D2）。** 本模块保留发布以兼容既有引用，但**不再演进而待移除**：
> 仍有 36 处占位实现，且与 `@StateMachine` 宏（编译期检查、与 Actor/Behaviors 直接组合）能力重叠。
> **新代码请使用 `@StateMachine`**；本模块按 E51 在 12 个月后（约 2027-11）移除。
> 占位词不计入 `quality_metrics.py` 的总目标（`--allow-placeholders xstate`）。

状态机。XState 风格的状态机：core/lang/features/enterprise 四层，用于显式建模状态转移。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_xstate = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_xstate.core` | 状态机子包 |
| `cactor_xstate.lang` | 状态机子包 |
| `cactor_xstate.features` | 状态机子包 |
| `cactor_xstate.enterprise` | 状态机子包 |

模块根包 `cactor_xstate` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/xstate
cjpm test -m modules/xstate
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
