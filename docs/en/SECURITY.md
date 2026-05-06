# CActor Security Guide

## Security Overview

CActor v1.1.0 provides multi-layered security mechanisms to ensure the safe operation of the Actor system across various environments. This guide covers key security domains including authentication, authorization, network security, and data protection.

## Security Architecture

### Multi-Layer Security Model

```
+-------------------------------------------------------------+
|                     Application Layer Security                |
|  +---------------+ +---------------+ +---------------+       |
|  | Input         | | Access        | | Audit         |       |
|  | Validation    | | Control       | | Logging       |       |
|  +---------------+ +---------------+ +---------------+       |
+-------------------------------------------------------------+
|                       Actor Layer Security                   |
|  +---------------+ +---------------+ +---------------+       |
|  | Message       | | Actor         | | Resource      |       |
|  | Signing       | | Isolation     | | Limits        |       |
|  +---------------+ +---------------+ +---------------+       |
+-------------------------------------------------------------+
|                       Network Layer Security                  |
|  +---------------+ +---------------+ +---------------+       |
|  | TLS           | | Firewall      | | Intrusion    |       |
|  | Encryption    | |               | | Detection    |       |
|  +---------------+ +---------------+ +---------------+       |
+-------------------------------------------------------------+
|                       System Layer Security                   |
|  +---------------+ +---------------+ +---------------+       |
|  | Process       | | File          | | System        |       |
|  | Isolation     | | Permissions   | | Monitoring   |       |
|  +---------------+ +---------------+ +---------------+       |
+-------------------------------------------------------------+
```

## Authentication and Authorization

### 1. Actor Authentication

```cangjie
// Authenticate Actor identity
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

### 2. Role-Based Access Control (RBAC)

```cangjie
// Permission definitions
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

// Permission-checking Actor
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
                    context.sender.tell(UnauthorizedResponse("Read permission required"))
                }
                MessageResult.Handled

            case write: WriteMessage =>
                if (checkPermission(Permission.Write)) {
                    processWriteMessage(write)
                } else {
                    context.sender.tell(UnauthorizedResponse("Write permission required"))
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

### 3. JWT Token Authentication

```cangjie
// JWT token handling
public struct JWTToken {
    public let token: String
    public let payload: JWTPayload
    public let expiresAt: DateTime

    public func isValid(): Bool {
        return DateTime.now() < expiresAt && verifySignature()
    }

    private func verifySignature(): Bool {
        // Verify JWT signature
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
                        context.sender.tell(AuthFailure("Invalid token"))
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

## Message Security

### 1. Message Signing and Verification

```cangjie
// Signed message
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
                    println("Message verification failed: ${signed}")
                    context.sender.tell(MessageVerificationFailure())
                }
                MessageResult.Handled

            case regular: Message =>
                // Reject unsigned messages
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
        return diff >= 0 && diff <= 300  // Valid within 5 minutes
    }
}
```

### 2. Message Encryption

```cangjie
// Encrypted message
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

## Network Security

### 1. TLS Configuration

```cangjie
// TLS configuration
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

// Secure remote configuration
let remoteConfig = RemoteConfig.create()
    .withTLS(TLSConfig.createSecure())
    .withHostnameVerification(true)
    .withCertificateValidation(true)
```

### 2. Network Access Control

```cangjie
// IP whitelist
public struct NetworkACL {
    private let allowedIPs: Set<String>
    private let blockedIPs: Set<String>

    public func isAllowed(ip: String): Bool {
        if (blockedIPs.contains(ip)) {
            return false
        }

        if (allowedIPs.isEmpty()) {
            return true  // Allow all IPs if no whitelist is configured
        }

        return allowedIPs.contains(ip) || allowedIPs.any { pattern =>
            matchesPattern(ip, pattern)
        }
    }

    private func matchesPattern(ip: String, pattern: String): Bool {
        // Support CIDR notation, e.g., 192.168.1.0/24
        return cidrMatcher.matches(ip, pattern)
    }
}

class NetworkSecurityActor <: Actor {
    private let acl: NetworkACL = loadNetworkACL()

    public func receive(message: Message, context: ActorContext): MessageResult {
        let senderIP = extractSenderIP(context.sender)

        if (!acl.isAllowed(senderIP)) {
            println("Connection rejected from ${senderIP}")
            context.sender.tell(ConnectionRejected("IP address not in allowed list"))
            return MessageResult.Handled
        }

        processMessage(message, context)
    }
}
```

## Input Validation and Protection

### 1. Input Validation

```cangjie
// Input validator
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

// User input validator
class UserInputValidator <: Validator<CreateUserMessage> {
    public func validate(message: CreateUserMessage): ValidationResult {
        let errors = ArrayList<String>()

        // Username validation
        if (message.userName.isEmpty()) {
            errors.add("Username cannot be empty")
        } else if (message.userName.length() > 50) {
            errors.add("Username must not exceed 50 characters")
        } else if (!message.userName.matches(Regex("^[a-zA-Z0-9_]+$"))) {
            errors.add("Username can only contain letters, numbers, and underscores")
        }

        // Email validation
        if (message.email.isEmpty()) {
            errors.add("Email cannot be empty")
        } else if (!isValidEmail(message.email)) {
            errors.add("Invalid email format")
        }

        // SQL injection protection
        if (containsSQLInjection(message.userName) || containsSQLInjection(message.email)) {
            errors.add("Input contains illegal characters")
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

// Validation Actor
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

### 2. Rate Limiting

```cangjie
// Rate limiter
public struct RateLimiter {
    private let maxRequests: Int64
    private let timeWindow: Duration
    private let requests: HashMap<String, ArrayList<DateTime>>

    public func isAllowed(clientId: String): Bool {
        let now = DateTime.now()
        let clientRequests = requests.getOrPut(clientId, { => ArrayList<DateTime>() })

        // Remove expired requests
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
            context.sender.tell(RateLimitExceeded("Too many requests, please try again later"))
            return MessageResult.Handled
        }

        processMessage(message, context)
    }
}
```

## Security Monitoring and Audit

### 1. Security Event Logging

```cangjie
// Security event
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

                // Immediate alert for high-severity events
                if (event.severity == SecuritySeverity.Critical) {
                    alertingService.sendAlert(event)
                }

                MessageResult.Handled
        }
    }
}
```

### 2. Anomalous Behavior Detection

```cangjie
// Anomaly detector
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
                // New user, establish baseline
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
                        // Send security alert
                        securityAuditActor.tell(SecurityEvent(
                            SecurityEventType.SuspiciousActivity,
                            DateTime.now(),
                            Some(anomaly.userId),
                            activity.sourceIP,
                            "Anomalous behavior detected: ${anomaly.description}",
                            SecuritySeverity.High
                        ))
                    case None =>
                        // Normal behavior, update user pattern
                        updateUserPattern(activity.userId, activity)
                }
                MessageResult.Handled
        }
    }
}
```

## Security Configuration

### 1. Security Configuration File

```hocon
# config/security.conf
cactor {
  security {
    # Authentication configuration
    authentication {
      enabled = true
      provider = "jwt"
      jwt {
        secret = ${JWT_SECRET}
        expiration = "1h"
        algorithm = "HS256"
      }
    }

    # Authorization configuration
    authorization {
      enabled = true
      default-deny = true
      rbac {
        roles-file = "config/roles.json"
        permissions-file = "config/permissions.json"
      }
    }

    # Network security
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

    # Rate limiting
    rate-limiting {
      enabled = true
      max-requests = 100
      time-window = "1m"
      per-client = true
    }

    # Audit logging
    audit {
      enabled = true
      log-file = "logs/security-audit.log"
      log-level = "INFO"
      include-payload = false
    }
  }
}
```

### 2. Environment Variable Security

```bash
# Secure environment variable setup
export JWT_SECRET=$(openssl rand -base64 32)
export KEYSTORE_PASSWORD=$(openssl rand -base64 16)
export TRUSTSTORE_PASSWORD=$(openssl rand -base64 16)
export DB_PASSWORD=$(vault kv get -field=password secret/database)

# Avoid hardcoding sensitive information in configuration files
# Do NOT do this
# jwt.secret = "hardcoded-secret"

# DO use environment variables
# jwt.secret = ${JWT_SECRET}
```

---

**CActor v1.1.0 - Multi-layered Security Protection, Enterprise-grade Security Assurance!**

_Last updated: July 3, 2025_
