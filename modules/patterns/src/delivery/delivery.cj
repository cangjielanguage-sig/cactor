/*
 * CActor Patterns.Delivery - 至少一次投递 + 幂等消费（差距 4.13）
 *
 * 远程层已有可靠通道（`SimpleRemoteManager.tellReliable`：重发、ACK、接收端按通道序号去重），
 * 但那是**通道级**语义，用户看不到消息的投递标识，也无法在业务层做"同一笔消息只生效一次"：
 * - 跨会话/重启的重发会重新计数，业务无法去重；
 * - 发送方重试（换传输、换进程）时没有稳定的业务幂等键。
 *
 * 这里补的是**用户可见**的一层：
 * - `DeliveryEnvelope`：携带稳定 `deliveryId` 的包装消息，经可靠通道发送；
 * - `AtLeastOnceDelivery`：为每条消息生成 `${实例}-${序号}` 的 deliveryId，返回给调用方便于排障；
 * - `IdempotentReceiver`：按 deliveryId 去重，处理成功后才写入去重表（`DeliveryIdStore`）；
 * - `DurableDeliveryIdStore`：去重表落 `DurableStateStore`，进程重启后仍能识别重复投递。
 *
 * 语义边界：至少一次 = **可能重复、不会静默丢失**。处理完成与标记之间崩溃会得到重复投递，
 * 因此业务副作用本身仍需可重入（例如按 business key 的 upsert）；不承诺恰好一次。
 */
package cactor_patterns.delivery

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import cactor_core.message.{Message, MessageCodec, MessageCodecRegistry, MessageSerializer, DeserializationException}
import cactor_core.actor.{ActorRef}
import cactor_remote.{RemoteActorRef, SimpleRemoteManager}
import cactor_persistence.{DurableStateStore}

// ======================= 消息包装 =======================

/** 线上类型名（带包名，避免与其他模块的同名消息冲突） */
public let DELIVERY_ENVELOPE_TYPE: String = "cactor.patterns.DeliveryEnvelope"

/** 用户可见的投递信封：deliveryId 稳定（发送方重试不换），payload 是原始消息 */
public class DeliveryEnvelope <: Message {
    public let deliveryId: String
    public let payload: Message

    public init(deliveryId: String, payload: Message) {
        if (deliveryId.isEmpty()) {
            throw IllegalArgumentException("deliveryId 不能为空")
        }
        this.deliveryId = deliveryId
        this.payload = payload
    }

    public func messageType(): String {
        "${DELIVERY_ENVELOPE_TYPE}"
    }
}

class DeliveryEnvelopeCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let e = (message as DeliveryEnvelope).getOrThrow()
        let out = ArrayList<UInt8>()
        writeText(out, e.deliveryId)
        let body = inner.serialize(e.payload)
        writeLong(out, body.size)
        out.add(all: body)
        out.toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        let r = FrameReader(payload)
        let deliveryId = r.text()
        let body = r.bytes(r.long())
        DeliveryEnvelope(deliveryId, inner.deserialize(body))
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

class FrameReader {
    private let data: Array<UInt8>
    private var pos: Int64 = 0

    init(data: Array<UInt8>) {
        this.data = data
    }

    func long(): Int64 {
        if (pos + 8 > data.size) {
            throw DeserializationException("DeliveryEnvelope 数据不完整")
        }
        var v: Int64 = 0
        for (_ in 0..8) {
            v = (v << 8) | Int64(data[pos])
            pos++
        }
        v
    }

    func bytes(n: Int64): Array<UInt8> {
        if (n < 0 || pos + n > data.size) {
            throw DeserializationException("DeliveryEnvelope 数据不完整")
        }
        let b = data[pos..(pos + n)].clone()
        pos += n
        b
    }

    func text(): String {
        String.fromUtf8(bytes(long()))
    }
}

// ======================= 发送端 =======================

/**
 * 至少一次发送：生成稳定 deliveryId，经远程可靠通道发出。
 *
 * ```
 * let delivery = AtLeastOnceDelivery(remote)
 * let id = delivery.tell(target, StringMessage("charge:42"))
 * // 断线/重启后由调用方用同一 id 重发（或由业务重试逻辑重发同一个 envelope）
 * ```
 */
public class AtLeastOnceDelivery {
    private let remote: SimpleRemoteManager
    private let instanceId: String
    private let nextSeq = AtomicInt64(0)

    /** instanceId 默认取进程启动时刻，避免不同进程生成相同前缀 */
    public init(remote: SimpleRemoteManager, instanceId!: String = "") {
        MessageCodecRegistry.register(DELIVERY_ENVELOPE_TYPE, DeliveryEnvelopeCodec())
        this.remote = remote
        this.instanceId = if (instanceId.isEmpty()) {
            "d${MonoTime.now().hashCode()}"
        } else {
            instanceId
        }
    }

    /** 生成下一个 deliveryId（不发送；需要自定义发送路径时用） */
    public func nextDeliveryId(): String {
        "${instanceId}-${nextSeq.fetchAdd(1) + 1}"
    }

    /** 包装消息（不发送）；同一 envelope 可重发，deliveryId 保持不变 */
    public func envelope(message: Message): DeliveryEnvelope {
        DeliveryEnvelope(nextDeliveryId(), message)
    }

    /** 经可靠通道发送，返回本次 deliveryId */
    public func tell(target: RemoteActorRef, message: Message): String {
        let e = envelope(message)
        remote.tellReliable(target, e)
        e.deliveryId
    }

    /** 指定发送方（远程 reply 能回到该 Actor） */
    public func tell(target: RemoteActorRef, message: Message, sender: ActorRef): String {
        let e = envelope(message)
        remote.tellReliable(target, e, sender)
        e.deliveryId
    }

    /** 重发已有信封（调用方自己保存了 envelope/deliveryId 时） */
    public func resend(target: RemoteActorRef, envelope: DeliveryEnvelope): Unit {
        remote.tellReliable(target, envelope)
    }
}

// ======================= 接收端 =======================

/** deliveryId 去重表；`mark` 只能在业务处理成功后调用 */
public interface DeliveryIdStore {
    func contains(deliveryId: String): Bool

    func mark(deliveryId: String): Unit
}

/** 进程内去重表（测试/单次运行）；进程重启后清空 */
public class MemoryDeliveryIdStore <: DeliveryIdStore {
    private let lock = Mutex()
    private let seen = HashSet<String>()

    public func contains(deliveryId: String): Bool {
        synchronized(lock) {
            seen.contains(deliveryId)
        }
    }

    public func mark(deliveryId: String): Unit {
        synchronized(lock) {
            seen.add(deliveryId)
        }
    }

    /** 已记录条数（观测用） */
    public func size(): Int64 {
        synchronized(lock) {
            Int64(seen.size)
        }
    }
}

/** 去重表落 DurableStateStore：进程重启后仍能识别重复投递 */
public class DurableDeliveryIdStore <: DeliveryIdStore {
    private let store: DurableStateStore
    private let prefix: String

    public init(store: DurableStateStore, prefix!: String = "delivery") {
        this.store = store
        this.prefix = prefix
    }

    private func key(deliveryId: String): String {
        "${prefix}-${deliveryId}"
    }

    public func contains(deliveryId: String): Bool {
        store.get(key(deliveryId)).isSome()
    }

    public func mark(deliveryId: String): Unit {
        let k = key(deliveryId)
        if (store.get(k).isSome()) {
            return
        }
        store.upsert(k, store.revisionOf(k) + 1, "done")
    }
}

/**
 * 幂等消费：同一 deliveryId 只执行一次 `handle`。
 *
 * 锁覆盖"检查 → 执行 → 标记"：同一接收者的并发重复投递不会同时进入业务；
 * 崩溃发生在执行与标记之间时，重发会再次执行（至少一次语义，业务副作用需可重入）。
 */
public class IdempotentReceiver {
    private let store: DeliveryIdStore
    private let lock = Mutex()
    private let processed = AtomicInt64(0)
    private let duplicates = AtomicInt64(0)

    public init(store: DeliveryIdStore) {
        this.store = store
    }

    /**
     * 首次收到该 deliveryId 时执行 handler 并返回 true；重复投递直接返回 false（handler 不执行）。
     * handler 抛异常时不标记，随后重发仍会执行。
     */
    public func handle(envelope: DeliveryEnvelope, handler: (Message) -> Unit): Bool {
        synchronized(lock) {
            if (store.contains(envelope.deliveryId)) {
                duplicates.fetchAdd(1)
                return false
            }
            handler(envelope.payload)
            store.mark(envelope.deliveryId)
            processed.fetchAdd(1)
            true
        }
    }

    /** (执行次数, 重复丢弃次数) */
    public func stats(): (Int64, Int64) {
        (processed.load(), duplicates.load())
    }
}
