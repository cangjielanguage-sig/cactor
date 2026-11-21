/*
 * CActor Remote - 线协议编解码扩展（B3 压缩/大消息分片，F5 GoAway）
 *
 * 帧外层约定（都在 NetworkMessage 之外，旧对端不会收到这些标记——发送前先看对端能力位）：
 * - 压缩帧：首 4 字节写 -3（合法目标长度不会是负数），随后 4 字节原始长度，再后是 deflate 数据。
 * - 分片：每条片是一个普通 NetworkMessage，目标是内部入口 "$chunk"，载荷是 ChunkedPayload；
 *   每片 ≤ 协商的 maxFrameBytes，接收端按 (发送方, id) 重组后再按普通帧处理。
 * - GoAway：$sys 控制消息 "GOAWAY|<reason>|<detail>"，携带机器可读的原因。
 *
 * 压缩/分片都只在双方宣告对应能力（CAP_COMPRESSION / CAP_CHUNKING）时使用；
 * 旧对端拿到超限消息时发送侧明确抛 FrameTooLargeException，不会静默丢弃。
 */
package cactor_remote

import std.collection.*
import std.io.*
import stdx.compress.zlib.*
import cactor_core.message.{Message, MessageCodec, MessageCodecRegistry, DeserializationException}

/** 压缩帧标记：写在目标长度的位置（合法长度不会为负） */
const COMPRESSED_FRAME: Int32 = -3
/** 压缩阈值（B3）：负载达到 1 KiB 才压缩，小消息套一层头反而变大 */
public let COMPRESSION_THRESHOLD: Int64 = 1024
/** 解压后的单帧上限：防止压缩炸弹 */
public let MAX_DECOMPRESSED_FRAME: Int64 = 16 * 1024 * 1024

/** 分片大消息：发送端已编码的帧被切开，接收端重组 */
public const CHUNKED_PAYLOAD_TYPE: String = "cactor.ChunkedPayload"
/** 分片消息的内部目标入口 */
public const CHUNK_TARGET: String = "$chunk"

/** 发送侧：编码后的帧仍超过协商上限，且对端不支持分片（或本节点不支持）时抛出；消息未发送 */
public class FrameTooLargeException <: Exception {
    public let size: Int64
    public let maxFrameBytes: Int64

    public init(size: Int64, maxFrameBytes: Int64) {
        super("帧 ${size} 字节超过协商上限 ${maxFrameBytes}，且对端未协商大消息分片能力；消息未发送")
        this.size = size
        this.maxFrameBytes = maxFrameBytes
    }
}

/** GoAway 的机器可读原因（F5）：接收端据此区分协议错误、超限与正常关闭 */
public enum GoAwayReason <: ToString {
    | FrameTooLarge
    | ProtocolError
    | Shutdown

    public func toString(): String {
        match (this) {
            case FrameTooLarge => "FrameTooLarge"
            case ProtocolError => "ProtocolError"
            case Shutdown => "Shutdown"
        }
    }

    public static func parse(text: String): Option<GoAwayReason> {
        match (text) {
            case "FrameTooLarge" => Some(FrameTooLarge)
            case "ProtocolError" => Some(ProtocolError)
            case "Shutdown" => Some(Shutdown)
            case _ => None<GoAwayReason>
        }
    }
}

/** 收到的 GoAway 记录（诊断/测试用） */
public struct GoAwayInfo <: ToString {
    public let reason: GoAwayReason
    public let detail: String

    public init(reason: GoAwayReason, detail: String) {
        this.reason = reason
        this.detail = detail
    }

    public func toString(): String {
        "GoAway(${reason}, ${detail})"
    }
}

/** 把 GoAway 编码成 $sys 控制文本 */
func encodeGoAway(reason: GoAwayReason, detail: String): String {
    "GOAWAY|${reason}|${detail}"
}

/** 解析 $sys 控制文本；不是 GoAway 时返回 None */
func parseGoAway(text: String): Option<GoAwayInfo> {
    let parts = text.split("|", 3)
    if (parts.size < 2 || parts[0] != "GOAWAY") {
        return None<GoAwayInfo>
    }
    match (GoAwayReason.parse(parts[1])) {
        case Some(r) => Some(GoAwayInfo(r, if (parts.size >= 3) { parts[2] } else { "" }))
        case None => None<GoAwayInfo>
    }
}

// ---------------- 压缩 ----------------

/** 收集 OutputStream 输出到内存 */
class CollectOutputStream <: OutputStream {
    let data = ArrayList<UInt8>()

    public func write(buffer: Array<UInt8>): Unit {
        data.add(all: buffer)
    }

    public func flush(): Unit {}

    public func close(): Unit {}
}

func deflateBytes(input: Array<UInt8>): Array<UInt8> {
    let sink = CollectOutputStream()
    let stream = CompressOutputStream(sink, wrap: WrapType.DeflateFormat,
        compressLevel: CompressLevel.DefaultCompression)
    try {
        stream.write(input)
        stream.flush()
        stream.close()
    } catch (e: Exception) {
        throw IllegalStateException("压缩失败: ${e.message}")
    }
    sink.data.toArray()
}

func inflateBytes(input: Array<UInt8>, maxBytes: Int64): Array<UInt8> {
    let sink = CollectOutputStream()
    let stream = DecompressOutputStream(sink, wrap: WrapType.DeflateFormat)
    try {
        stream.write(input)
        stream.flush()
        stream.close()
    } catch (e: Exception) {
        throw DeserializationException("解压失败: ${e.message}")
    }
    if (sink.data.size > maxBytes) {
        throw DeserializationException("解压后 ${sink.data.size} 字节超过上限 ${maxBytes}（疑似压缩炸弹）")
    }
    sink.data.toArray()
}

/** 是否带压缩帧标记 */
func isCompressedFrame(data: Array<UInt8>): Bool {
    if (data.size < 8) {
        return false
    }
    let marker = (Int32(data[0]) << 24) | (Int32(data[1]) << 16) | (Int32(data[2]) << 8) | Int32(data[3])
    marker == COMPRESSED_FRAME
}

/** 压缩整帧：[标记 -3][原始长度][deflate 数据] */
func compressFrame(data: Array<UInt8>): Array<UInt8> {
    let body = deflateBytes(data)
    let out = Array<UInt8>(body.size + 8, repeat: 0)
    let n = Int32(data.size)
    out[0] = UInt8((COMPRESSED_FRAME >> 24) & 0xFF)
    out[1] = UInt8((COMPRESSED_FRAME >> 16) & 0xFF)
    out[2] = UInt8((COMPRESSED_FRAME >> 8) & 0xFF)
    out[3] = UInt8(COMPRESSED_FRAME & 0xFF)
    out[4] = UInt8((n >> 24) & 0xFF)
    out[5] = UInt8((n >> 16) & 0xFF)
    out[6] = UInt8((n >> 8) & 0xFF)
    out[7] = UInt8(n & 0xFF)
    body.copyTo(out, 0, 8, body.size)
    out
}

/** 还原压缩帧；不是压缩帧时原样返回 */
func decompressFrame(data: Array<UInt8>): Array<UInt8> {
    if (!isCompressedFrame(data)) {
        return data
    }
    let original = (Int64(Int32(data[4]) << 24) | (Int64(Int32(data[5]) & 0xFF) << 16) |
        (Int64(Int32(data[6]) & 0xFF) << 8) | Int64(Int32(data[7]) & 0xFF))
    if (original < 0 || original > MAX_DECOMPRESSED_FRAME) {
        throw DeserializationException("压缩帧声明的原始长度非法: ${original}")
    }
    inflateBytes(data.slice(8, data.size - 8), MAX_DECOMPRESSED_FRAME)
}

// ---------------- 大消息分片 ----------------

/** 一片大消息：发送端已编码的帧按 ≤ maxFrameBytes 切开 */
public class ChunkedPayload <: Message {
    public let id: String
    public let index: Int64
    public let total: Int64
    public let target: String
    public let payload: Array<UInt8>

    public init(id: String, index: Int64, total: Int64, target: String, payload: Array<UInt8>) {
        this.id = id
        this.index = index
        this.total = total
        this.target = target
        this.payload = payload
    }

    public func messageType(): String {
        CHUNKED_PAYLOAD_TYPE
    }
}

class ChunkedPayloadCodec <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let c = (message as ChunkedPayload).getOrThrow()
        let out = ArrayList<UInt8>()
        writeText(out, c.id)
        writeLong(out, c.index)
        writeLong(out, c.total)
        writeText(out, c.target)
        writeLong(out, c.payload.size)
        out.add(all: c.payload)
        out.toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        let r = ByteReader(payload)
        let id = r.text()
        let index = r.long()
        let total = r.long()
        let target = r.text()
        let n = r.long()
        ChunkedPayload(id, index, total, target, r.bytes(n))
    }

    private func writeText(out: ArrayList<UInt8>, s: String): Unit {
        let b = s.toArray()
        writeLong(out, b.size)
        out.add(all: b)
    }

    private func writeLong(out: ArrayList<UInt8>, v: Int64): Unit {
        var i = 56
        while (i >= 0) {
            out.add(UInt8((v >> i) & 0xFF))
            i -= 8
        }
    }
}

/** 单片分片消息允许携带的最大载荷字节（预留 NetworkMessage 外层与编解码开销） */
let CHUNK_OVERHEAD_RESERVE: Int64 = 512

func registerWireCodecs(): Unit {
    MessageCodecRegistry.register(CHUNKED_PAYLOAD_TYPE, ChunkedPayloadCodec())
}
