# cactor_patterns

通用模式。Ask 模式、路由（router）、背压（backpressure）、spawn 协议等构建在 core 之上的可复用模式。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_patterns = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_patterns.ask` | 通用模式子包 |
| `cactor_patterns.routing` | 通用模式子包 |
| `cactor_patterns.backpressure` | 通用模式子包 |
| `cactor_patterns.spawnprotocol` | 通用模式子包 |

模块根包 `cactor_patterns` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/patterns
cjpm test -m modules/patterns
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
