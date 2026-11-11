# cactor_persistence

持久化。事件溯源、快照存储、持久状态、持久化查询与插件注册表，支持 CQRS 与 DurableState 两种风格。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_persistence = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_persistence.typed` | 持久化子包 |

模块根包 `cactor_persistence` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/persistence
cjpm test -m modules/persistence
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
