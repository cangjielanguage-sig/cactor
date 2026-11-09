# 测试用 TLS 证书（仅供测试与演示）

由 `tools/gen_test_tls_certs.sh` 生成，私钥公开在仓库中，**切勿用于生产**。EC P-256，有效期 100 年。

| 文件 | 内容 |
|---|---|
| `ca.pem` / `ca-key.pem` | 测试 CA |
| `node.pem` / `node-key.pem` | 测试 CA 签发的节点证书（serverAuth + clientAuth，SAN localhost / 127.0.0.1） |
| `rogue-ca.pem` | 另一个 CA |
| `rogue.pem` / `rogue-key.pem` | 另一个 CA 签发、主题与 SAN 都与节点证书相同的"冒充"证书（应被拒绝） |

生产部署请用自己的 CA 为每个节点签发证书，私钥 PKCS#8 PEM，见 `docs/zh/api-reference.md` 的"远程传输 TLS"一节。
