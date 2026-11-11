# cactor_cluster

集群。成员管理、故障检测、ddata、分片与运维操作（实验性），构建在 remote 之上。

## 安装

在 `cjpm.toml` 中声明中心仓依赖：

```toml
[dependencies]
cactor_cluster = "1.2.1"
```

（`stdx` 相关能力需自行声明 stdx 依赖，见根仓库说明。）
## 包

| 包 | 内容 |
| --- | --- |
| `cactor_cluster.membership` | 集群子包 |
| `cactor_cluster.detector` | 集群子包 |
| `cactor_cluster.ddata` | 集群子包 |
| `cactor_cluster.ops` | 集群子包 |

模块根包 `cactor_cluster` 提供对外门面。

## 构建与测试

```bash
# 在 CActor workspace 根目录
cjpm build -m modules/cluster
cjpm test -m modules/cluster
```

## 文档

- 仓库：<https://gitcode.com/Cangjie-SIG/cactor>
- 入门：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/getting-started.md>
- API 参考：<https://gitcode.com/Cangjie-SIG/cactor/blob/main/docs/zh/api-reference.md>

## License

MIT
