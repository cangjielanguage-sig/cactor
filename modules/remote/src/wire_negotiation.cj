/*
 * CActor Remote - 线协议能力协商（DA6，对标 Akka Artery 握手里的版本号与滚动升级兼容性检查）
 *
 * 握手帧格式（E7′）严格按 7 个 "|" 字段解析，旧节点见到第 8 个字段就会拒绝握手。为了让旧节点照常互通，
 * 能力扩展不新增字段，而是追加在**旧解析器只当不透明字符串、原样参与 MAC** 的字段末尾：
 *   HELLO|1|地址|uid|时间戳|目标地址;wp=<协议版本>;wc=<能力位十六进制>|mac
 *   HELLO-ACK|1|地址|uid|时间戳|回显 mac;wp=<协议版本>;wc=<能力位十六进制>|mac
 * - 旧接受方：第 5 个字段只用于拼 MAC 原文，扩展原样参与校验，握手照常通过；它回旧格式 ACK，
 *   新连接方据此把对端记为旧版本（协议 1、保守能力）。
 * - 旧连接方：只有在配置了共享密钥时才逐字比较 ACK 的回显 mac，所以新接受方**只在 HELLO 带扩展时**
 *   才在 ACK 里带扩展；收到旧格式 HELLO 时回旧格式 ACK，旧连接方逐字比较照常通过。
 * - 扩展是 ";键=值" 列表，未知的键忽略：以后再加字段不需要再动握手格式。
 * - 握手版本号字段（"1"）保持不变：改它会让旧节点直接拒绝，滚动升级期间新旧节点就断开了。
 *
 * 发送方按对端宣告的能力编码（见 cactor_core.message.WireProtocol）；对端协议版本低于
 * minPeerProtocolVersion 时拒绝握手并记录原因（滚动升级完成后调高下限，把漏升级的节点挡在门外）。
 */
package cactor_remote

import std.collection.*
import std.sync.*
import std.convert.*
import cactor_core.message.{Message, WireProtocol, WireIncompatibleException, MessageSerializer}
import cactor_foundation.network.NetworkAddress

/**
 * 线协议协商设置。
 *
 * 默认宣告本版本的协议与全部能力、接受协议 >= 1 的对端（与旧节点互通）。
 * 滚动升级全部完成后建议 `withMinPeerProtocol(WireProtocol.CURRENT_VERSION)`：漏升级的旧节点握手被拒、原因写进日志。
 */
public struct WireProtocolSettings {
    /** 握手中宣告的线协议版本 */
    public let protocolVersion: Int64
    /** 握手中宣告的能力位（本节点能读的格式） */
    public let capabilities: UInt64
    /** 接受的最低对端协议版本：低于它的握手被拒绝 */
    public let minPeerProtocolVersion: Int64
    /** 没有宣告能力的对端（旧节点、或尚未完成握手）按此能力编码；默认 CAP_NONE（最保守） */
    public let legacyPeerCapabilities: UInt64
    /**
     * 演练用：完全模拟引入协商之前的节点——握手不带扩展、忽略对端扩展、按本节点全部能力编码。
     * 只用于混合版本测试与升级演练，生产节点不要打开。
     */
    public let emulateLegacy: Bool

    public init(protocolVersion: Int64, capabilities: UInt64, minPeerProtocolVersion: Int64,
        legacyPeerCapabilities: UInt64, emulateLegacy: Bool) {
        if (protocolVersion < WireProtocol.MIN_SUPPORTED_VERSION) {
            throw IllegalArgumentException("宣告的线协议版本不能低于 ${WireProtocol.MIN_SUPPORTED_VERSION}")
        }
        if (minPeerProtocolVersion < WireProtocol.MIN_SUPPORTED_VERSION ||
            minPeerProtocolVersion > WireProtocol.CURRENT_VERSION) {
            throw IllegalArgumentException(
                "最低对端协议版本须在 ${WireProtocol.MIN_SUPPORTED_VERSION}..${WireProtocol.CURRENT_VERSION} 之间")
        }
        this.protocolVersion = protocolVersion
        this.capabilities = capabilities
        this.minPeerProtocolVersion = minPeerProtocolVersion
        this.legacyPeerCapabilities = legacyPeerCapabilities
        this.emulateLegacy = emulateLegacy
    }

    public static func defaults(): WireProtocolSettings {
        WireProtocolSettings(WireProtocol.CURRENT_VERSION, WireProtocol.LOCAL_CAPABILITIES,
            WireProtocol.MIN_SUPPORTED_VERSION, WireProtocol.CAP_NONE, false)
    }

    /** 演练：模拟引入协商之前的旧节点（见 emulateLegacy） */
    public static func legacyEmulation(): WireProtocolSettings {
        WireProtocolSettings(WireProtocol.LEGACY_VERSION, WireProtocol.LOCAL_CAPABILITIES,
            WireProtocol.MIN_SUPPORTED_VERSION, WireProtocol.LOCAL_CAPABILITIES, true)
    }

    /** 拒绝协议版本低于 v 的对端 */
    public func withMinPeerProtocol(v: Int64): WireProtocolSettings {
        WireProtocolSettings(protocolVersion, capabilities, v, legacyPeerCapabilities, emulateLegacy)
    }

    /** 宣告指定的协议版本与能力（演练"能协商但能力更少"的节点） */
    public func advertising(protocolVersion: Int64, capabilities: UInt64): WireProtocolSettings {
        WireProtocolSettings(protocolVersion, capabilities, minPeerProtocolVersion, legacyPeerCapabilities, emulateLegacy)
    }

    /** 没宣告能力的对端按 caps 编码（确知集群里没有 v1.2.x 节点时可设为 LOCAL_CAPABILITIES） */
    public func withLegacyPeerCapabilities(caps: UInt64): WireProtocolSettings {
        WireProtocolSettings(protocolVersion, capabilities, minPeerProtocolVersion, caps, emulateLegacy)
    }
}

/**
 * 对端实例协商得到的线协议画像（按 "地址#uid" 记录：同址重启的新实例要重新协商）
 */
public struct PeerWireProfile <: ToString {
    public let address: String
    public let uid: UInt64
    /** 对端宣告的协议版本；没有扩展的旧节点为 LEGACY_VERSION */
    public let protocolVersion: Int64
    /** 对端宣告的能力位；旧节点为空（编码时按 legacyPeerCapabilities） */
    public let capabilities: UInt64
    /** 对端是否在握手里宣告了能力（false = 旧节点） */
    public let negotiated: Bool

    public init(address: String, uid: UInt64, protocolVersion: Int64, capabilities: UInt64, negotiated: Bool) {
        this.address = address
        this.uid = uid
        this.protocolVersion = protocolVersion
        this.capabilities = capabilities
        this.negotiated = negotiated
    }

    public func toString(): String {
        "${address}#${uid} protocol=${protocolVersion} caps=${WireProtocol.describe(capabilities)}${if (negotiated) { "" } else { " (legacy)" }}"
    }
}

/** 不兼容事件的方向 */
public enum WireDirection <: ToString {
    | Outbound
    | Inbound

    public func toString(): String {
        match (this) {
            case Outbound => "outbound"
            case Inbound => "inbound"
        }
    }
}

/**
 * 线格式不兼容事件：交给 `SimpleRemoteManager.onWireIncompatible` 注册的监听（通常转发到死信）。
 * 出站事件带原消息（可直接 publishDeadLetter）；入站帧读不懂，只有类型名与目标。
 */
public class WireIncompatibleEvent <: ToString {
    public let direction: WireDirection
    /** 出站为目标节点地址；入站为发送方路径（取不到时为 None） */
    public let peer: Option<String>
    public let targetActor: String
    public let messageType: String
    public let reason: String
    public let message: Option<Message>

    public init(direction: WireDirection, peer: Option<String>, targetActor: String, messageType: String,
        reason: String, message: Option<Message>) {
        this.direction = direction
        this.peer = peer
        this.targetActor = targetActor
        this.messageType = messageType
        this.reason = reason
        this.message = message
    }

    public func toString(): String {
        "WireIncompatible(${direction}, peer=${peer ?? "?"}, target=${targetActor}, type=${messageType}: ${reason})"
    }
}

/** 握手扩展的键 */
const WIRE_EXT_PROTOCOL: String = "wp"
const WIRE_EXT_CAPABILITIES: String = "wc"

/** 握手字段末尾追加的能力扩展 */
func wireExtension(protocolVersion: Int64, capabilities: UInt64): String {
    ";${WIRE_EXT_PROTOCOL}=${protocolVersion};${WIRE_EXT_CAPABILITIES}=${formatHexU64(capabilities)}"
}

/** 握手字段的解析结果：原字段、宣告的 (协议版本, 能力位)；扩展格式非法时 valid = false */
struct WireField {
    let base: String
    let advertised: Option<(Int64, UInt64)>
    let valid: Bool

    init(base: String, advertised: Option<(Int64, UInt64)>, valid: Bool) {
        this.base = base
        this.advertised = advertised
        this.valid = valid
    }
}

/**
 * 拆出字段末尾的扩展。没有 ";" 就是旧节点的字段（advertised = None）。
 * 未知键忽略（向前兼容）；wp 缺失或不是正整数、wc 不是十六进制视为非法扩展（拒绝握手，不猜）。
 */
func decodeWireField(field: String): WireField {
    let parts = field.split(";")
    if (parts.size <= 1) {
        return WireField(field, None<(Int64, UInt64)>, true)
    }
    var proto: Option<Int64> = None<Int64>
    var caps: UInt64 = 0
    var ok = true
    for (i in 1..parts.size) {
        let kv = parts[i].split("=", 2)
        if (kv.size != 2) {
            continue
        }
        if (kv[0] == WIRE_EXT_PROTOCOL) {
            proto = Int64.tryParse(kv[1])
            if (proto.isNone()) {
                ok = false
            }
        } else if (kv[0] == WIRE_EXT_CAPABILITIES) {
            match (parseHexU64(kv[1])) {
                case Some(c) => caps = c
                case None => ok = false
            }
        }
    }
    match (proto) {
        case Some(p) where ok && p >= 0 => WireField(parts[0], Some((p, caps)), true)
        case _ => WireField(parts[0], None<(Int64, UInt64)>, false)
    }
}

func formatHexU64(v: UInt64): String {
    if (v == 0) {
        return "0"
    }
    let digits = "0123456789abcdef".toArray()
    let out = ArrayList<UInt8>()
    var x = v
    while (x != 0) {
        out.add(digits[Int64(x & 0xF)], at: 0)
        x = x >> 4
    }
    String.fromUtf8(out.toArray())
}

func parseHexU64(text: String): Option<UInt64> {
    if (text.isEmpty() || text.size > 16) {
        return None<UInt64>
    }
    var v: UInt64 = 0
    for (b in text.toArray()) {
        let d: UInt64 = if (b >= 48u8 && b <= 57u8) {
            UInt64(b - 48u8)
        } else if (b >= 97u8 && b <= 102u8) {
            UInt64(b - 87u8)
        } else if (b >= 65u8 && b <= 70u8) {
            UInt64(b - 55u8)
        } else {
            return None<UInt64>
        }
        v = (v << 4) | d
    }
    Some(v)
}

/**
 * 发送侧门禁：SimpleRemoteSender 编码前取对端能力，编码失败（对端读不了）时计数、限速告警、通知监听。
 * 由 RemotingCore 在 attach 时装上；单独使用 SimpleRemoteSender 时没有门禁，按本节点全部能力编码（旧行为）。
 */
class WireGate {
    let capabilitiesOf: (NetworkAddress) -> UInt64
    let onIncompatible: (NetworkAddress, String, Message, WireIncompatibleException) -> Unit

    init(capabilitiesOf: (NetworkAddress) -> UInt64,
        onIncompatible: (NetworkAddress, String, Message, WireIncompatibleException) -> Unit) {
        this.capabilitiesOf = capabilitiesOf
        this.onIncompatible = onIncompatible
    }

    /** 入队前的预检（可靠投递）：只查编解码器声明的必需能力，不做整帧编码 */
    func check(address: NetworkAddress, targetActor: String, message: Message): Unit {
        let caps = capabilitiesOf(address)
        let missing = MessageSerializer.requiredCapabilities(message) & !caps
        if (missing != 0) {
            let e = WireIncompatibleException(message.messageType(), missing,
                "${address.toString()} 不具备发送 ${message.messageType()} 所需的线协议能力 ${WireProtocol.describe(missing)}")
            onIncompatible(address, targetActor, message, e)
            throw e
        }
    }
}

// ---------------- RemotingCore 的协商逻辑（字段在 remoting.cj，逻辑集中在此便于审阅） ----------------

extend RemotingCore {
    func configureWire(s: WireProtocolSettings): Unit {
        synchronized(wireLock) {
            wireSettings = s
        }
    }

    func currentWireSettings(): WireProtocolSettings {
        synchronized(wireLock) { wireSettings }
    }

    /** 握手字段末尾的能力扩展；模拟旧节点时为空 */
    func wireHandshakeExtension(): String {
        let s = currentWireSettings()
        if (s.emulateLegacy) {
            ""
        } else {
            wireExtension(s.protocolVersion, s.capabilities)
        }
    }

    /** 解析对端握手字段；模拟旧节点时与旧解析器一样把整段当不透明字符串、不看扩展 */
    func parseWireField(field: String): WireField {
        if (currentWireSettings().emulateLegacy) {
            let base = field.split(";")[0]
            return WireField(base, None<(Int64, UInt64)>, true)
        }
        decodeWireField(field)
    }

    /** 协议准入：扩展非法或版本低于下限时返回拒绝原因（同时计数）；通过返回 None */
    func wireRejection(peer: String, wire: WireField): Option<String> {
        let reason = if (!wire.valid) {
            Some("${peer} 握手中的线协议能力扩展非法")
        } else {
            let peerVersion = match (wire.advertised) {
                case Some((v, _)) => v
                case None => WireProtocol.LEGACY_VERSION
            }
            let min = currentWireSettings().minPeerProtocolVersion
            if (peerVersion < min) {
                Some("${peer} 的线协议版本 ${peerVersion} 低于本节点要求的最低版本 ${min}" +
                    "（本节点 ${WireProtocol.CURRENT_VERSION}）：请先升级对端，或调低 minPeerProtocolVersion")
            } else {
                None<String>
            }
        }
        if (reason.isSome()) {
            wireProtocolRejected.fetchAdd(1)
        }
        reason
    }

    /** 握手通过（admitPeer 成功）后登记对端画像；同址新实例覆盖旧画像 */
    func recordPeerWire(addr: String, uid: UInt64, advertised: Option<(Int64, UInt64)>): Unit {
        let profile = match (advertised) {
            case Some((v, c)) => PeerWireProfile(addr, uid, v, c, true)
            case None => PeerWireProfile(addr, uid, WireProtocol.LEGACY_VERSION, WireProtocol.CAP_NONE, false)
        }
        let changed = synchronized(wireLock) {
            let old = peerWire.get(addr)
            peerWire[addr] = profile
            match (old) {
                case Some(o) => o.uid != uid || o.protocolVersion != profile.protocolVersion ||
                    o.capabilities != profile.capabilities
                case None => true
            }
        }
        if (changed) {
            log_remoting.debug("对端线协议：${profile}")
        }
    }

    /** 当前实例的画像（uid 与最近一次身份不符的旧画像不算） */
    func peerWireProfileOf(addr: String): Option<PeerWireProfile> {
        let uid = currentPeerUid(addr)
        synchronized(wireLock) {
            match ((peerWire.get(addr), uid)) {
                case (Some(p), Some(u)) where p.uid == u => Some(p)
                case _ => None<PeerWireProfile>
            }
        }
    }

    /**
     * 发往该地址时按什么能力编码：宣告过能力的对端用其宣告值；旧节点和尚未握手的对端用 legacyPeerCapabilities
     * （默认最保守：可降级的照常发，不可降级的明确失败，而不是赌对端能读）
     */
    func wireCapabilitiesOf(address: NetworkAddress): UInt64 {
        let s = currentWireSettings()
        if (s.emulateLegacy) {
            return s.legacyPeerCapabilities
        }
        match (peerWireProfileOf(address.toString())) {
            case Some(p) where p.negotiated => p.capabilities
            case _ => s.legacyPeerCapabilities
        }
    }

    func setWireListener(l: (WireIncompatibleEvent) -> Unit): Unit {
        synchronized(wireLock) {
            wireListener = Some(l)
        }
    }

    /** 出站不兼容：计数、限速告警、通知监听（带原消息，可转死信）；异常由调用方继续抛出 */
    func onWireIncompatible(address: NetworkAddress, targetActor: String, message: Message, e: WireIncompatibleException): Unit {
        wireIncompatibleSent.fetchAdd(1)
        let addr = address.toString()
        let profile = match (peerWireProfileOf(addr)) {
            case Some(p) => p.toString()
            case None => "${addr}（尚未完成握手，按保守能力编码）"
        }
        wireIncompatibleLog.tryLog({ text => log_remoting.warn(text) }, { =>
            "发往 ${addr}/${targetActor} 的 ${e.messageType} 未发送：${e.message}；对端 ${profile}"
        })
        if (let Some(l) <- synchronized(wireLock) { wireListener }) {
            try {
                l(WireIncompatibleEvent(WireDirection.Outbound, Some(addr), targetActor, e.messageType, e.message,
                    Some(message)))
            } catch (ex: Exception) {
                log_remoting.error("线格式不兼容监听失败: ${ex.message}")
            }
        }
    }
}

// ---------------- 公开 API ----------------

extend SimpleRemoteManager {
    /** 线协议协商设置（握手时读取，可在运行中调整；已建立的连接沿用其握手结果） */
    public func configureWire(settings: WireProtocolSettings): Unit {
        core.configureWire(settings)
    }

    public func wireSettings(): WireProtocolSettings {
        core.currentWireSettings()
    }

    /** 与该地址当前实例协商得到的线协议画像；尚未握手时为 None */
    public func peerWireProfile(address: NetworkAddress): Option<PeerWireProfile> {
        core.peerWireProfileOf(address.toString())
    }

    /**
     * 线格式不兼容的监听（出站：对端读不了、未发送；入站：本节点读不懂、未投递）。
     * 典型用法：转发到死信 `system.publishDeadLetter(e.message, e.targetActor)`。
     */
    public func onWireIncompatible(listener: (WireIncompatibleEvent) -> Unit): Unit {
        core.setWireListener(listener)
        getReceiver().setWireListener(listener)
    }

    /** 因对端能力不足而拒绝发送的消息数 */
    public func wireIncompatibleSent(): Int64 {
        core.wireIncompatibleSent.load()
    }

    /** 本节点读不懂（版本更高 / 种类未知）而丢弃的入站帧数 */
    public func wireIncompatibleReceived(): Int64 {
        getReceiver().getWireIncompatibleCount()
    }

    /** 因线协议版本过低或扩展非法而拒绝的握手数（也计入 rejectedHandshakes） */
    public func wireProtocolRejected(): Int64 {
        core.wireProtocolRejected.load()
    }
}
