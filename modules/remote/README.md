# cactor_remote

远程通信。传输抽象与远程管理（实验性）。TLS 相关能力通过 `TransportSecurity` 接口注入，底层依赖 stdx。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_remote = "1.2.1"
```

（`stdx` 相关能力需自行声明 stdx 依赖，见根仓库说明。）
## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/remote
cjpm test -m modules/remote
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
