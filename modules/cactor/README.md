# cactor_cactor

门面模块。统一对外 API：`cactor`、`cactor.api`、`cactor.config`。使用者只需依赖本包即可获得 Actor 系统的常用入口。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor = "1.2.1"
```

## 包

| 包 | 内容 |
| --- | --- |
| `cactor_cactor.api` | 门面模块子包 |
| `cactor_cactor.config` | 门面模块子包 |

模块根包 `cactor_cactor` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/cactor
cjpm test -m modules/cactor
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
