# cactor_foundation

基础设施层。队列、内存、序列化、网络、配置、依赖注入、监控、断路器、哈希、日志等零依赖底座。
所有上层模块只依赖本层，本层不依赖任何其他 CActor 模块，也不依赖 stdx。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_foundation = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_foundation.abstractions` | 基础设施层子包 |
| `cactor_foundation.config` | 基础设施层子包 |
| `cactor_foundation.contracts` | 基础设施层子包 |
| `cactor_foundation.di` | 基础设施层子包 |
| `cactor_foundation.memory` | 基础设施层子包 |
| `cactor_foundation.network` | 基础设施层子包 |
| `cactor_foundation.queue` | 基础设施层子包 |
| `cactor_foundation.serialization` | 基础设施层子包 |
| `cactor_foundation.logging` | 基础设施层子包 |
| `cactor_foundation.monitoring` | 基础设施层子包 |
| `cactor_foundation.circuit_breaker` | 基础设施层子包 |
| `cactor_foundation.hashing` | 基础设施层子包 |
| `cactor_foundation.detector` | 基础设施层子包 |
| `cactor_foundation.plugin` | 基础设施层子包 |
| `cactor_foundation.attributes` | 基础设施层子包 |

模块根包 `cactor_foundation` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/foundation
cjpm test -m modules/foundation
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
