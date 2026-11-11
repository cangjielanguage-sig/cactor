# cactor_testkit

测试工具。`TestProbe`、`TestInbox`、`BehaviorTestKit`（Effect 断言）、`ManualTime` 虚拟时钟、`awaitCondition` 等测试支撑设施。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_testkit = "1.2.1"
```

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/testkit
cjpm test -m modules/testkit
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
