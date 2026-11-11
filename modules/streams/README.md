# cactor_streams

流处理。Streams DSL、reactive 与 unified 流抽象，可在 Actor 之上构建背压流处理。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_streams = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_streams.core` | 流处理子包 |
| `cactor_streams.dsl` | 流处理子包 |
| `cactor_streams.reactive` | 流处理子包 |
| `cactor_streams.unified` | 流处理子包 |

模块根包 `cactor_streams` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/streams
cjpm test -m modules/streams
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
