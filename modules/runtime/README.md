# cactor_runtime

运行时。真实的执行引擎：mailbox、dispatcher、scheduler、guardian、registry、lifecycle、监控与事件。承载 `ActorSystem` 的启动、调度、关停与指标采集。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_runtime = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_runtime.actor` | 运行时子包 |
| `cactor_runtime.dispatcher` | 运行时子包 |
| `cactor_runtime.execution` | 运行时子包 |
| `cactor_runtime.guardian` | 运行时子包 |
| `cactor_runtime.lifecycle` | 运行时子包 |
| `cactor_runtime.mailbox` | 运行时子包 |
| `cactor_runtime.message` | 运行时子包 |
| `cactor_runtime.monitoring` | 运行时子包 |
| `cactor_runtime.registry` | 运行时子包 |
| `cactor_runtime.scheduler` | 运行时子包 |
| `cactor_runtime.supervision` | 运行时子包 |
| `cactor_runtime.system` | 运行时子包 |
| `cactor_runtime.typed` | 运行时子包 |
| `cactor_runtime.events` | 运行时子包 |
| `cactor_runtime.context` | 运行时子包 |

模块根包 `cactor_runtime` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/runtime
cjpm test -m modules/runtime
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
