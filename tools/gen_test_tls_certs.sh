#!/usr/bin/env bash
# 生成测试用 TLS 证书（E65）——仅供测试与演示，私钥公开在仓库中，切勿用于生产。
#   tools/gen_test_tls_certs.sh
# 输出：
#   tools/tls-test-certs/*.pem                                   多进程演示（tools/cluster_demo.sh --tls）读取
#   modules/tests/src/unit/remote_semantics/tls_test_certs.cj    单元测试使用的同一组证书（PEM 文本常量）
# 内容：EC P-256、有效期 100 年
#   ca.pem / ca-key.pem          测试 CA（CA:true）
#   node.pem / node-key.pem      由测试 CA 签发的节点证书（serverAuth + clientAuth，SAN localhost / 127.0.0.1）
#   rogue-ca.pem                 另一个 CA
#   rogue.pem / rogue-key.pem    由另一个 CA 签发、主题与 SAN 都与节点证书相同的"冒充"证书
# 私钥为 PKCS#8（stdx GeneralPrivateKey.decodeFromPem 读取）。需要 openssl 3。
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=tools/tls-test-certs
FIXTURE=modules/tests/src/unit/remote_semantics/tls_test_certs.cj
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cat > "$tmp/ext.cnf" <<'CNF'
[ v3_ca ]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[ v3_node ]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:localhost, IP:127.0.0.1
authorityKeyIdentifier = keyid
CNF
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }
ca() {
    key "$tmp/$1-key.pem"
    openssl req -new -x509 -key "$tmp/$1-key.pem" -out "$tmp/$1.pem" -days 36500 -sha256 \
        -subj "/O=CActor Test/CN=$2" -config "$tmp/ext.cnf" -extensions v3_ca
}
node() {
    key "$tmp/$1-key.pem"
    openssl req -new -key "$tmp/$1-key.pem" -out "$tmp/$1.csr" -subj "/O=CActor Test/CN=cactor-node"
    openssl x509 -req -in "$tmp/$1.csr" -CA "$tmp/$2.pem" -CAkey "$tmp/$2-key.pem" -CAcreateserial \
        -out "$tmp/$1.pem" -days 36500 -sha256 -extfile "$tmp/ext.cnf" -extensions v3_node 2>/dev/null
}
ca ca "CActor Test CA"
node node ca
ca rogue-ca "Rogue CA"
node rogue rogue-ca
openssl verify -CAfile "$tmp/ca.pem" "$tmp/node.pem" >/dev/null
! openssl verify -CAfile "$tmp/ca.pem" "$tmp/rogue.pem" >/dev/null 2>&1

mkdir -p "$OUT"
for f in ca ca-key node node-key rogue-ca rogue rogue-key; do
    cp "$tmp/$f.pem" "$OUT/$f.pem"
done
cat > "$OUT/README.md" <<'MD'
# 测试用 TLS 证书（仅供测试与演示）

由 `tools/gen_test_tls_certs.sh` 生成，私钥公开在仓库中，**切勿用于生产**。EC P-256，有效期 100 年。

| 文件 | 内容 |
|---|---|
| `ca.pem` / `ca-key.pem` | 测试 CA |
| `node.pem` / `node-key.pem` | 测试 CA 签发的节点证书（serverAuth + clientAuth，SAN localhost / 127.0.0.1） |
| `rogue-ca.pem` | 另一个 CA |
| `rogue.pem` / `rogue-key.pem` | 另一个 CA 签发、主题与 SAN 都与节点证书相同的"冒充"证书（应被拒绝） |

生产部署请用自己的 CA 为每个节点签发证书，私钥 PKCS#8 PEM，见 `docs/zh/api-reference.md` 的"远程传输 TLS"一节。
MD

pem_const() {
    printf 'public let %s: String = """\n' "$1"
    cat "$2"
    printf '"""\n\n'
}
{
    cat <<'HDR'
/*
 * CActor Tests - 测试用 TLS 证书（E65），由 tools/gen_test_tls_certs.sh 生成，与 tools/tls-test-certs/ 相同。
 * 仅供测试：私钥公开在仓库中，切勿用于生产。EC P-256，有效期 100 年。
 */
package cactor_tests.unit.remote_semantics

HDR
    pem_const TEST_TLS_CA "$OUT/ca.pem"
    pem_const TEST_TLS_NODE_CERT "$OUT/node.pem"
    pem_const TEST_TLS_NODE_KEY "$OUT/node-key.pem"
    pem_const TEST_TLS_ROGUE_CA "$OUT/rogue-ca.pem"
    pem_const TEST_TLS_ROGUE_CERT "$OUT/rogue.pem"
    pem_const TEST_TLS_ROGUE_KEY "$OUT/rogue-key.pem"
} > "$FIXTURE"
echo "已生成 $OUT 与 $FIXTURE"
