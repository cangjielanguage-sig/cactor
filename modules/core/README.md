# cactor_core

核心抽象。Actor/ActorRef/ActorSystem/Message/Supervision 等经典 Actor 抽象，以及类型化 Actor 运行时（`TypedBehavior`、`TypedActorRef`、`TypedBehaviors`、监督与退避、DeathWatch/DeathPact）。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_core = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_core.actor` | 核心抽象子包 |
| `cactor_core.typed` | 核心抽象子包 |
| `cactor_core.system` | 核心抽象子包 |
| `cactor_core.supervision` | 核心抽象子包 |
| `cactor_core.context` | 核心抽象子包 |
| `cactor_core.message` | 核心抽象子包 |
| `cactor_core.guardian` | 核心抽象子包 |
| `cactor_core.dsl` | 核心抽象子包 |
| `cactor_core.error` | 核心抽象子包 |
| `cactor_core.memory` | 核心抽象子包 |
| `cactor_core.config` | 核心抽象子包 |

模块根包 `cactor_core` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/core
cjpm test -m modules/core
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
