# 🔒 CActor 安全指南

## 📋 安全概览

CActor v1.2.0 提供了多层安全防护机制，确保Actor系统在各种环境下的安全运行。本指南涵盖了认证、授权、网络安全、数据保护等关键安全领域。

## 🛡️ 安全架构

### 多层安全模型

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层安全                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   输入验证   │ │   权限控制   │ │   审计日志   │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    Actor层安全                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   消息签名   │ │   Actor隔离  │ │   资源限制   │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    网络层安全                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   TLS加密    │ │   防火墙     │ │   入侵检测   │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    系统层安全                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   进程隔离   │ │   文件权限   │ │   系统监控   │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## 🔐 认证和授权

### 1. Actor认证

```cangjie
// 认证Actor身份
public interface AuthenticatedActor <: Actor {
    public func authenticate(credentials: Credentials): AuthResult
    public func isAuthenticated(): Bool
}

class SecureUserActor <: AuthenticatedActor {
    private var authenticated: Bool = false
    private var userContext: Option<UserContext> = None

    public func authenticate(credentials: Credentials): AuthResult {
        match (authService.verify(credentials)) {
            case success: AuthSuccess =>
                authenticated = true
                userContext = Some(success.userContext)
                AuthResult.Success
            case failure: AuthFailure =>
                authenticated = false
                userContext = None
                AuthResult.Failure(failure.reason)
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (!authenticated) {
            context.sender.tell(UnauthenticatedResponse())
            return MessageResult.Handled
        }

        processAuthenticatedMessage(message, context)
    }
}
```

### 2. 基于角色的访问控制 (RBAC)

```cangjie
// 权限定义
public enum Permission {
    | Read
    | Write
    | Delete
    | Admin
}

public struct Role {
    public let name: String
    public let permissions: Set<Permission>
}

public struct UserContext {
    public let userId: String
    public let roles: Set<Role>

    public func hasPermission(permission: Permission): Bool {
        return roles.any { role => role.permissions.contains(permission) }
    }
}

// 权限检查Actor
class AuthorizedActor <: Actor {
    private var userContext: Option<UserContext> = None

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case auth: AuthMessage =>
                userContext = Some(auth.userContext)
                MessageResult.Handled

            case read: ReadMessage =>
                if (checkPermission(Permission.Read)) {
                    processReadMessage(read)
                } else {
                    context.sender.tell(UnauthorizedResponse("需要读取权限"))
                }
                MessageResult.Handled

            case write: WriteMessage =>
                if (checkPermission(Permission.Write)) {
                    processWriteMessage(write)
                } else {
                    context.sender.tell(UnauthorizedResponse("需要写入权限"))
                }
                MessageResult.Handled
        }
    }

    private func checkPermission(permission: Permission): Bool {
        match (userContext) {
            case Some(context) => context.hasPermission(permission)
            case None => false
        }
    }
}
```

### 3. JWT令牌认证

```cangjie
// JWT令牌处理
public struct JWTToken {
    public let token: String
    public let payload: JWTPayload
    public let expiresAt: DateTime

    public func isValid(): Bool {
        return DateTime.now() < expiresAt && verifySignature()
    }

    private func verifySignature(): Bool {
        // 验证JWT签名
        return jwtService.verify(token)
    }
}

class JWTAuthActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case authReq: AuthRequest =>
                match (validateJWT(authReq.token)) {
                    case Some(payload) =>
                        let userContext = createUserContext(payload)
                        context.sender.tell(AuthSuccess(userContext))
                    case None =>
                        context.sender.tell(AuthFailure("无效的令牌"))
                }
                MessageResult.Handled
        }
    }

    private func validateJWT(token: String): Option<JWTPayload> {
        try {
            let jwt = JWTToken.parse(token)
            if (jwt.isValid()) {
                Some(jwt.payload)
            } else {
                None
            }
        } catch (e: JWTException) {
            None
        }
    }
}
```

## 🔒 消息安全

### 1. 消息签名和验证

```cangjie
// 签名消息
public struct SignedMessage <: Message {
    public let payload: Message
    public let signature: String
    public let timestamp: DateTime
    public let senderId: String

    public func verify(publicKey: PublicKey): Bool {
        let data = serialize(payload, timestamp, senderId)
        return cryptoService.verify(data, signature, publicKey)
    }
}

class SecureMessageActor <: Actor {
    private let publicKeys: HashMap<String, PublicKey> = HashMap<String, PublicKey>()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case signed: SignedMessage =>
                if (verifyMessage(signed)) {
                    processMessage(signed.payload, context)
                } else {
                    println("消息验证失败: ${signed}")
                    context.sender.tell(MessageVerificationFailure())
                }
                MessageResult.Handled

            case regular: Message =>
                // 拒绝未签名的消息
                context.sender.tell(UnsignedMessageRejected())
                MessageResult.Handled
        }
    }

    private func verifyMessage(signed: SignedMessage): Bool {
        match (publicKeys.get(signed.senderId)) {
            case Some(publicKey) =>
                signed.verify(publicKey) && isTimestampValid(signed.timestamp)
            case None =>
                false
        }
    }

    private func isTimestampValid(timestamp: DateTime): Bool {
        let now = DateTime.now()
        let diff = now.minus(timestamp).toSeconds()
        return diff >= 0 && diff <= 300  // 5分钟内有效
    }
}
```

### 2. 消息加密

```cangjie
// 加密消息
public struct EncryptedMessage <: Message {
    public let encryptedPayload: Array<UInt8>
    public let keyId: String
    public let iv: Array<UInt8>

    public func decrypt(key: SecretKey): Option<Message> {
        try {
            let decryptedData = cryptoService.decrypt(encryptedPayload, key, iv)
            Some(deserialize(decryptedData))
        } catch (e: CryptoException) {
            None
        }
    }
}

class EncryptedMessageActor <: Actor {
    private let secretKeys: HashMap<String, SecretKey> = HashMap<String, SecretKey>()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case encrypted: EncryptedMessage =>
                match (decryptMessage(encrypted)) {
                    case Some(decrypted) =>
                        processMessage(decrypted, context)
                    case None =>
                        context.sender.tell(DecryptionFailure())
                }
                MessageResult.Handled
        }
    }

    private func decryptMessage(encrypted: EncryptedMessage): Option<Message> {
        match (secretKeys.get(encrypted.keyId)) {
            case Some(key) => encrypted.decrypt(key)
            case None => None
        }
    }
}
```

## 🌐 网络安全

### 1. TLS配置

```cangjie
// TLS配置
public struct TLSConfig {
    public let enabled: Bool
    public let keyStore: String
    public let keyStorePassword: String
    public let trustStore: String
    public let trustStorePassword: String
    public let protocol: String  // TLSv1.3
    public let cipherSuites: Array<String>

    public static func createSecure(): TLSConfig {
        return TLSConfig(
            enabled: true,
            keyStore: "config/keystore.jks",
            keyStorePassword: System.getenv("KEYSTORE_PASSWORD") ?? "",
            trustStore: "config/truststore.jks",
            trustStorePassword: System.getenv("TRUSTSTORE_PASSWORD") ?? "",
            protocol: "TLSv1.3",
            cipherSuites: [
                "TLS_AES_256_GCM_SHA384",
                "TLS_CHACHA20_POLY1305_SHA256",
                "TLS_AES_128_GCM_SHA256"
            ]
        )
    }
}

// 安全的远程配置
let remoteConfig = RemoteConfig.create()
    .withTLS(TLSConfig.createSecure())
    .withHostnameVerification(true)
    .withCertificateValidation(true)
```

### 2. 网络访问控制

```cangjie
// IP白名单
public struct NetworkACL {
    private let allowedIPs: Set<String>
    private let blockedIPs: Set<String>

    public func isAllowed(ip: String): Bool {
        if (blockedIPs.contains(ip)) {
            return false
        }

        if (allowedIPs.isEmpty()) {
            return true  // 如果没有白名单，允许所有IP
        }

        return allowedIPs.contains(ip) || allowedIPs.any { pattern =>
            matchesPattern(ip, pattern)
        }
    }

    private func matchesPattern(ip: String, pattern: String): Bool {
        // 支持CIDR表示法，如 192.168.1.0/24
        return cidrMatcher.matches(ip, pattern)
    }
}

class NetworkSecurityActor <: Actor {
    private let acl: NetworkACL = loadNetworkACL()

    public func receive(message: Message, context: ActorContext): MessageResult {
        let senderIP = extractSenderIP(context.sender)

        if (!acl.isAllowed(senderIP)) {
            println("拒绝来自 ${senderIP} 的连接")
            context.sender.tell(ConnectionRejected("IP地址不在允许列表中"))
            return MessageResult.Handled
        }

        processMessage(message, context)
    }
}
```

## 🛡️ 输入验证和防护

### 1. 输入验证

```cangjie
// 输入验证器
public interface Validator<T> {
    public func validate(value: T): ValidationResult
}

public struct ValidationResult {
    public let isValid: Bool
    public let errors: Array<String>

    public static func success(): ValidationResult {
        return ValidationResult(true, [])
    }

    public static func failure(errors: Array<String>): ValidationResult {
        return ValidationResult(false, errors)
    }
}

// 用户输入验证器
class UserInputValidator <: Validator<CreateUserMessage> {
    public func validate(message: CreateUserMessage): ValidationResult {
        let errors = ArrayList<String>()

        // 用户名验证
        if (message.userName.isEmpty()) {
            errors.add("用户名不能为空")
        } else if (message.userName.length() > 50) {
            errors.add("用户名长度不能超过50个字符")
        } else if (!message.userName.matches(Regex("^[a-zA-Z0-9_]+$"))) {
            errors.add("用户名只能包含字母、数字和下划线")
        }

        // 邮箱验证
        if (message.email.isEmpty()) {
            errors.add("邮箱不能为空")
        } else if (!isValidEmail(message.email)) {
            errors.add("邮箱格式不正确")
        }

        // SQL注入防护
        if (containsSQLInjection(message.userName) || containsSQLInjection(message.email)) {
            errors.add("输入包含非法字符")
        }

        return if (errors.isEmpty()) {
            ValidationResult.success()
        } else {
            ValidationResult.failure(errors.toArray())
        }
    }

    private func isValidEmail(email: String): Bool {
        return email.matches(Regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    }

    private func containsSQLInjection(input: String): Bool {
        let sqlKeywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "UNION"]
        let upperInput = input.toUpperCase()
        return sqlKeywords.any { keyword => upperInput.contains(keyword) }
    }
}

// 验证Actor
class ValidatedActor <: Actor {
    private let validator = UserInputValidator()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case createUser: CreateUserMessage =>
                let result = validator.validate(createUser)
                if (result.isValid) {
                    processCreateUser(createUser, context)
                } else {
                    context.sender.tell(ValidationErrorResponse(result.errors))
                }
                MessageResult.Handled
        }
    }
}
```

### 2. 速率限制

```cangjie
// 速率限制器
public struct RateLimiter {
    private let maxRequests: Int64
    private let timeWindow: Duration
    private let requests: HashMap<String, ArrayList<DateTime>>

    public func isAllowed(clientId: String): Bool {
        let now = DateTime.now()
        let clientRequests = requests.getOrPut(clientId, { => ArrayList<DateTime>() })

        // 清理过期请求
        clientRequests.removeIf { request =>
            now.minus(request) > timeWindow
        }

        if (clientRequests.size() >= maxRequests) {
            return false
        }

        clientRequests.add(now)
        return true
    }
}

class RateLimitedActor <: Actor {
    private let rateLimiter = RateLimiter(
        maxRequests: 100,
        timeWindow: Duration.fromMinutes(1)
    )

    public func receive(message: Message, context: ActorContext): MessageResult {
        let clientId = extractClientId(context.sender)

        if (!rateLimiter.isAllowed(clientId)) {
            context.sender.tell(RateLimitExceeded("请求过于频繁，请稍后再试"))
            return MessageResult.Handled
        }

        processMessage(message, context)
    }
}
```

## 📊 安全监控和审计

### 1. 安全事件记录

```cangjie
// 安全事件
public enum SecurityEventType {
    | AuthenticationFailure
    | AuthorizationFailure
    | SuspiciousActivity
    | DataAccess
    | ConfigurationChange
}

public struct SecurityEvent {
    public let eventType: SecurityEventType
    public let timestamp: DateTime
    public let userId: Option<String>
    public let sourceIP: String
    public let description: String
    public let severity: SecuritySeverity
}

class SecurityAuditActor <: Actor {
    private let auditLog: AuditLogger = AuditLogger.create()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case event: SecurityEvent =>
                auditLog.log(event)

                // 高严重性事件立即告警
                if (event.severity == SecuritySeverity.Critical) {
                    alertingService.sendAlert(event)
                }

                MessageResult.Handled
        }
    }
}
```

### 2. 异常行为检测

```cangjie
// 异常检测器
class AnomalyDetector {
    private let normalPatterns: HashMap<String, UserBehaviorPattern>

    public func detectAnomaly(userId: String, activity: UserActivity): Option<Anomaly> {
        match (normalPatterns.get(userId)) {
            case Some(pattern) =>
                if (isAnomalous(activity, pattern)) {
                    Some(Anomaly(userId, activity, calculateRiskScore(activity, pattern)))
                } else {
                    None
                }
            case None =>
                // 新用户，建立基线
                establishBaseline(userId, activity)
                None
        }
    }

    private func isAnomalous(activity: UserActivity, pattern: UserBehaviorPattern): Bool {
        return activity.requestRate > pattern.avgRequestRate * 3 ||
               activity.accessTime.hour < 6 || activity.accessTime.hour > 22 ||
               activity.sourceIP != pattern.usualIPs
    }
}

class SecurityMonitorActor <: Actor {
    private let anomalyDetector = AnomalyDetector()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case activity: UserActivity =>
                match (anomalyDetector.detectAnomaly(activity.userId, activity)) {
                    case Some(anomaly) =>
                        // 发送安全告警
                        securityAuditActor.tell(SecurityEvent(
                            SecurityEventType.SuspiciousActivity,
                            DateTime.now(),
                            Some(anomaly.userId),
                            activity.sourceIP,
                            "检测到异常行为: ${anomaly.description}",
                            SecuritySeverity.High
                        ))
                    case None =>
                        // 正常行为，更新用户模式
                        updateUserPattern(activity.userId, activity)
                }
                MessageResult.Handled
        }
    }
}
```

## 🔧 安全配置

### 1. 安全配置文件

```hocon
# config/security.conf
cactor {
  security {
    # 认证配置
    authentication {
      enabled = true
      provider = "jwt"
      jwt {
        secret = ${JWT_SECRET}
        expiration = "1h"
        algorithm = "HS256"
      }
    }

    # 授权配置
    authorization {
      enabled = true
      default-deny = true
      rbac {
        roles-file = "config/roles.json"
        permissions-file = "config/permissions.json"
      }
    }

    # 网络安全
    network {
      tls {
        enabled = true
        protocol = "TLSv1.3"
        keystore = "config/keystore.jks"
        truststore = "config/truststore.jks"
      }

      firewall {
        enabled = true
        allowed-ips = ["192.168.1.0/24", "10.0.0.0/8"]
        blocked-ips = []
      }
    }

    # 速率限制
    rate-limiting {
      enabled = true
      max-requests = 100
      time-window = "1m"
      per-client = true
    }

    # 审计日志
    audit {
      enabled = true
      log-file = "logs/security-audit.log"
      log-level = "INFO"
      include-payload = false
    }
  }
}
```

### 2. 环境变量安全

```bash
# 安全的环境变量设置
export JWT_SECRET=$(openssl rand -base64 32)
export KEYSTORE_PASSWORD=$(openssl rand -base64 16)
export TRUSTSTORE_PASSWORD=$(openssl rand -base64 16)
export DB_PASSWORD=$(vault kv get -field=password secret/database)

# 避免在配置文件中硬编码敏感信息
# ❌ 不要这样做
# jwt.secret = "hardcoded-secret"

# ✅ 使用环境变量
# jwt.secret = ${JWT_SECRET}
```

---

**CActor v1.2.0 - 多层安全防护，企业级安全保障！** 🔒

*最后更新: 2025年7月3日*