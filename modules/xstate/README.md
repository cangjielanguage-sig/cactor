# cactor_xstate

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
