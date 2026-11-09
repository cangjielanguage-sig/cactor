/*
 * CActor Remote - 远程传输的 TLS / mTLS（E65，对标 Akka Artery 的 TLS 传输）
 *
 * 基于 stdx.net.tls（OpenSSL 3）。实现基础层的 TransportSecurity：TCP 连接建立后、远程层握手之前完成 TLS 握手，
 * 之后身份交换、共享密钥 HMAC、全部消息帧都在加密流上传输。
 *
 * - 只用 TLS 1.3（节点两端都是 CActor，不需要兼容旧协议）。
 * - 默认双向认证（mTLS）：服务端要求客户端出示证书；两端都只信任配置的 CA——用 CustomVerify 自己校验证书链，
 *   不用 stdx 的 CustomCA（它会同时信任系统根证书，任何持有公网证书的人都能连进集群）。
 * - 不校验主机名（同 Akka 默认 hostname-verification = off）：集群节点按"持有本集群 CA 签发的证书"认证，
 *   节点身份（地址 + 启动标识）由其后的远程层握手确认；需要按证书主题区分节点时，可再叠加共享密钥或用不同的 CA。
 * - 构造时即校验本节点证书由信任的 CA 签发且在有效期内，配置错误立即报出，而不是等到连接时双向失败。
 * - 握手在连接自己的协程里进行（出站在关联写线程、入站在读协程）；stdx 的握手实测约 60 ms，连接长期复用，影响只在建连。
 *
 * ```
 * let tls = RemoteTls.fromPemFiles("node.pem", "node-key.pem", "ca.pem")
 * remote.configure(RemotingSettings.defaults().withTls(tls))   // 在 start 之前
 * remote.start()
 * ```
 * 集群（gossip、心跳、分片、ddata）都经同一个 SimpleRemoteManager，配置一次即全部加密。
 * 私钥须为 PKCS#8 PEM（"BEGIN PRIVATE KEY"）；EC P-256 证书握手最快。
 */
package cactor_remote

import std.fs.{File}
import std.net.{TcpSocket, StreamingSocket}
import std.sync.{AtomicInt64}
import std.time.{DateTime}
import cactor_foundation.network.{NetworkAddress, TransportSecurity}
import stdx.crypto.x509.{X509Certificate, VerifyOption}
import stdx.crypto.keys.{GeneralPrivateKey}
import stdx.crypto.common.{Certificate, PrivateKey}
import stdx.net.tls.{TlsSocket, TlsServerConfig, TlsClientConfig, TlsClientIdentificationMode}
import stdx.net.tls.common.{CertificateVerifyMode, TlsVersion}

/** 只信任配置的 CA 的证书链校验（两端共用）；失败次数供诊断与测试 */
class TrustedCaVerifier {
    private let base: VerifyOption
    let rejected = AtomicInt64(0)

    init(trusted: Array<X509Certificate>) {
        // VerifyOption() 的默认 roots 是系统根证书（加载约 10 ms）：构造一次、每次校验复制后改时间
        var opt = VerifyOption()
        opt.roots = trusted
        this.base = opt
    }

    /** chain：对端出示的证书链，叶子证书在前 */
    func verify(chain: Array<Certificate>): Bool {
        let ok = verifyX509(toX509(chain))
        if (!ok) {
            rejected.fetchAdd(1)
        }
        ok
    }

    func verifyX509(certs: Array<X509Certificate>): Bool {
        if (certs.isEmpty()) {
            return false
        }
        var opt = base
        opt.time = DateTime.now()
        opt.intermediates = certs[1..]
        try {
            certs[0].verify(opt)
        } catch (_: Exception) {
            false
        }
    }

    private static func toX509(chain: Array<Certificate>): Array<X509Certificate> {
        let out = Array<X509Certificate>(chain.size, { i =>
            match (chain[i] as X509Certificate) {
                case Some(x) => x
                case None => throw IllegalArgumentException("对端证书不是 X.509 证书")
            }
        })
        out
    }
}

/**
 * 远程传输的 TLS 配置（实现 TransportSecurity）。不可变，可在多个节点（同一进程内的测试）间共用
 */
public class RemoteTls <: TransportSecurity {
    private let verifier: TrustedCaVerifier
    private let serverConfig: TlsServerConfig
    private let clientConfig: TlsClientConfig
    private let mutual: Bool
    private let subject: String
    // 供 withoutMutualAuthentication 重建
    private let chain: Array<X509Certificate>
    private let key: PrivateKey
    private let trusted: Array<X509Certificate>

    init(chain: Array<X509Certificate>, key: PrivateKey, trusted: Array<X509Certificate>, mutual: Bool) {
        if (chain.isEmpty()) {
            throw IllegalArgumentException("证书链为空")
        }
        if (trusted.isEmpty()) {
            throw IllegalArgumentException("没有信任的 CA 证书")
        }
        let v = TrustedCaVerifier(trusted)
        if (!v.verifyX509(chain)) {
            throw IllegalArgumentException("本节点证书（${chain[0].subject}）不是由信任的 CA 签发，或不在有效期内")
        }
        let asCerts = Array<Certificate>(chain.size, { i => chain[i] })
        let check: (Array<Certificate>) -> Bool = { peer => v.verify(peer) }

        var sc = TlsServerConfig(chain, key)
        sc.supportedVersions = [TlsVersion.V1_3]
        if (mutual) {
            sc.clientIdentityRequired = TlsClientIdentificationMode.Required
            sc.verifyMode = CertificateVerifyMode.CustomVerify(check)
        }
        var cc = TlsClientConfig()
        cc.supportedVersions = [TlsVersion.V1_3]
        cc.verifyMode = CertificateVerifyMode.CustomVerify(check)
        // 始终出示本节点证书：对端是否要求由对端决定
        cc.certificate = (asCerts, key)

        this.verifier = v
        this.serverConfig = sc
        this.clientConfig = cc
        this.mutual = mutual
        this.subject = chain[0].subject.toString()
        this.chain = chain
        this.key = key
        this.trusted = trusted
    }

    /**
     * 从 PEM 文本创建（默认双向认证）。
     * certificateChainPem：本节点证书（可附中间证书，叶子在前）；privateKeyPem：PKCS#8 私钥；trustedCaPem：信任的 CA（可多个）
     */
    public static func fromPem(certificateChainPem: String, privateKeyPem: String, trustedCaPem: String): RemoteTls {
        RemoteTls(X509Certificate.decodeFromPem(certificateChainPem), GeneralPrivateKey.decodeFromPem(privateKeyPem),
            X509Certificate.decodeFromPem(trustedCaPem), true)
    }

    /** 从 PEM 文件创建（默认双向认证） */
    public static func fromPemFiles(certificateChainFile: String, privateKeyFile: String, trustedCaFile: String): RemoteTls {
        fromPem(readText(certificateChainFile), readText(privateKeyFile), readText(trustedCaFile))
    }

    private static func readText(path: String): String {
        String.fromUtf8(File.readFrom(path))
    }

    /**
     * 单向 TLS：服务端不要求客户端证书（只加密并认证服务端）。任何能连上端口的进程都能建立连接，
     * 须配合共享密钥（withSecureCookie）使用；集群部署推荐保持默认的双向认证
     */
    public func withoutMutualAuthentication(): RemoteTls {
        RemoteTls(chain, key, trusted, false)
    }

    public func isMutual(): Bool {
        mutual
    }

    /** 本端证书校验失败的次数（对端证书不是信任的 CA 签发、已过期或不是 X.509） */
    public func rejectedPeerCertificates(): Int64 {
        verifier.rejected.load()
    }

    public func describe(): String {
        "TLS 1.3（${if (mutual) { "双向认证" } else { "单向认证" }}，${subject}）"
    }

    public func secureOutbound(socket: TcpSocket, peer: NetworkAddress, timeout: Duration): StreamingSocket {
        handshake(TlsSocket.client(socket, clientConfig: clientConfig), timeout)
    }

    public func secureInbound(socket: TcpSocket, timeout: Duration): StreamingSocket {
        handshake(TlsSocket.server(socket, serverConfig: serverConfig), timeout)
    }

    private static func handshake(t: TlsSocket, timeout: Duration): StreamingSocket {
        try {
            t.handshake(timeout: timeout) |> ignoreHandshakeResult
        } catch (e: Exception) {
            // 失败时释放 TLS 会话（底层 TCP 连接随后由传输关闭，重复关闭无害）
            try {
                t.close()
            } catch (_: Exception) {}
            throw e
        }
        t
    }
}

func ignoreHandshakeResult<T>(_: T): Unit {}
