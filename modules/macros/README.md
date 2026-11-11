# cactor_macros

编译期宏。Actor DSL 宏包：`@actorReceive` 等，根据 `onXxx(msg[, ctx])` 方法生成 `receive`，漏处理消息与重复处理在编译期报错。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_macros = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_macros.actormacro` | 编译期宏子包 |
| `cactor_macros.common` | 编译期宏子包 |
| `cactor_macros.message` | 编译期宏子包 |
| `cactor_macros.statemachine` | 编译期宏子包 |

模块根包 `cactor_macros` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/macros
cjpm test -m modules/macros
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
