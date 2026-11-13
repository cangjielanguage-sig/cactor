/*
 * CActor Core.Message - 线协议版本与能力位（DA6：滚动升级的能力协商）
 *
 * 为什么需要：codec 的"读旧帧"兼容只保证新节点能读旧节点写的东西，反过来不成立——
 * TypedPayloadBox codec v3 的版本头和 kind 3 旧节点读不了，此前发送方毫不知情、照常算发送成功，
 * 接收方只记一次笼统的解码失败。能力协商把"对端能读什么"变成握手时交换的事实：
 * - 每个节点在握手里宣告线协议版本 + 能力位（能读哪些格式）；没有宣告的旧节点按保守能力对待；
 * - 发送侧按对端能力编码：能降级就写对端读得懂的旧格式（无损），不能降级就在发送侧明确失败
 *   （WireIncompatibleException：计数 + 限速告警 + 死信 / ask 失败），绝不把发不出去的帧算成功；
 * - 接收侧遇到读不懂的版本或种类抛 UnsupportedWireFormatException，远程层单独计数、限速告警并交给死信监听。
 *
 * 本层只定义能力位与编码约定，握手如何携带能力见 cactor_remote（wire_negotiation.cj）。
 */
package cactor_core.message

/**
 * 线协议版本与能力位。
 *
 * 版本：
 * - 1（LEGACY_VERSION）：握手不带能力扩展的节点（v1.2.x 以及引入协商之前的版本）；
 * - 2（CURRENT_VERSION）：握手携带能力扩展。
 * 能力位描述"本节点能**读**什么"，发送方据此选择写出格式；新增格式时追加新位，旧位含义永不改变。
 */
public class WireProtocol {
    /** 握手里没有能力扩展的旧节点视为此版本 */
    public static let LEGACY_VERSION: Int64 = 1
    /** 本版本实现的线协议版本 */
    public static let CURRENT_VERSION: Int64 = 2
    /** 本版本仍能互通的最低线协议版本（低于它的握手直接拒绝） */
    public static let MIN_SUPPORTED_VERSION: Int64 = 1

    /** 不具备任何可选能力：只能读 TypedPayloadBox codec v2（kind 1/2）及更早的格式 */
    public static let CAP_NONE: UInt64 = 0
    /** 能读 TypedPayloadBox codec v3：版本头为 3 的类型化信封，含 kind 3（带协议标记的嵌套消息） */
    public static let CAP_TYPED_CODEC_V3: UInt64 = 1

    /** 本版本能读的全部能力（握手时默认宣告） */
    public static let LOCAL_CAPABILITIES: UInt64 = 1

    /** caps 是否具备 required 的全部位 */
    public static func supports(caps: UInt64, required: UInt64): Bool {
        (caps & required) == required
    }

    /** 能力位的可读描述（未知位按 bitN 显示，便于排查更高版本的对端） */
    public static func describe(caps: UInt64): String {
        if (caps == 0) {
            return "none"
        }
        var out = ""
        for (i in 0..64) {
            let bit = UInt64(1) << UInt64(i)
            if ((caps & bit) != 0) {
                let name = if (bit == CAP_TYPED_CODEC_V3) { "typed-codec-v3" } else { "bit${i}" }
                out = if (out.isEmpty()) { name } else { "${out},${name}" }
            }
        }
        out
    }
}

/**
 * 发送侧：对端缺少编码这条消息所必需的能力，而且没有它能读的旧格式可退。
 *
 * 这是"明确失败"：帧不会写到网络上。tell 直接抛给调用方，ask 以它失败，
 * 可靠投递在入队前拒绝；远程层同时计数、限速告警并通知死信监听。
 */
public class WireIncompatibleException <: SerializationException {
    /** 消息的线上类型名 */
    public let messageType: String
    /** 编码所需、但对端没有宣告的能力位 */
    public let missingCapabilities: UInt64

    public init(messageType: String, missingCapabilities: UInt64, message: String) {
        super(message)
        this.messageType = messageType
        this.missingCapabilities = missingCapabilities
    }
}

/**
 * 接收侧：帧的版本高于本节点支持的版本，或种类本节点不认识（对端比本节点新、或协商被绕过）。
 *
 * 与一般的解码失败分开计数：它说明集群处于不兼容的混合版本状态，需要运维介入，而不是数据损坏。
 */
public class UnsupportedWireFormatException <: DeserializationException {
    /** 读不懂的帧的线上类型名 */
    public let messageType: String
    /** 帧的目标 Actor（从网络消息外层得知；codec 内部抛出时为 None） */
    public let targetActor: Option<String>
    /** 帧的发送方（同上） */
    public let senderPath: Option<String>

    public init(messageType: String, message: String) {
        this(messageType, message, None<String>, None<String>)
    }

    public init(messageType: String, message: String, targetActor: Option<String>, senderPath: Option<String>) {
        super(message)
        this.messageType = messageType
        this.targetActor = targetActor
        this.senderPath = senderPath
    }
}
