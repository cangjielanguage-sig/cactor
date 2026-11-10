/*
 * CActor Core.Typed - 类型化消息的跨进程编解码
 *
 * 类型化 Actor 之间用 `TypedPayloadBox` 传递消息（非泛型载体，见 typed_adapter.cj 的性能说明）。
 * 本机投递无所谓：载荷就是一个 `Any`。但跨进程必须把载荷变成字节，而 `Any` 在编译期已经擦除，
 * 默认序列化器只会报"未知消息类型"。
 *
 * 这里的取舍：**载荷本身必须是 Message**。这不是限制，而是把"能不能跨进程"变成用户看得见的选择
 * ——协议本来就是 `enum`/`struct` 实现的 Message，跨节点可用；把任意 `Any`（闭包、连接、ActorRef）
 * 塞进消息本来就不该被悄悄送去网络。不满足时抛 SerializationException，发送侧立刻失败而不是丢包。
 *
 * 编码方式是嵌套一帧完整消息（类型名 + 版本 + 负载），因此载荷自己的 @Message codec 会被复用，
 * 不需要为每个协议类型再注册一次类型化版本。
 */
package cactor_core.typed

import cactor_core.message.{Message, MessageSerializer, MessageCodec, MessageCodecRegistry, SerializationException}

/** 线上名称：与 TypedPayloadBox.messageType() 保持一致 */
public let TYPED_PAYLOAD_BOX_TYPE: String = "TypedPayloadBox"

/**
 * `TypedPayloadBox` 的编解码器：把载荷按它自己的 codec 编成一帧完整消息。
 *
 * 注意：解码方只还原载荷与信封，`as T` 的转换仍由接收侧用户包里生成的桥接闭包完成
 * （core 泛型代码里做 `as T` 会在运行时查类型信息，是热路径的主要开销）。
 */
public class TypedPayloadBoxCodec <: MessageCodec {
    public init() {}

    public func encode(message: Message): Array<UInt8> {
        let box = match (message as TypedPayloadBox) {
            case Some(b) => b
            case None => throw SerializationException("TypedPayloadBoxCodec 只能编码 TypedPayloadBox")
        }
        let payload = match (box.payload as Message) {
            case Some(m) => m
            case None => throw SerializationException(
                "类型化消息的载荷必须实现 Message 才能跨进程传输（实际类型不可序列化：" +
                "请让协议类型实现 Message 并注册 codec，或把该消息留在本节点）")
        }
        MessageSerializer().serialize(payload)
    }

    public func decode(payload: Array<UInt8>): Message {
        TypedPayloadBox(MessageSerializer().deserialize(payload))
    }
}

/**
 * 注册类型化消息编解码器（幂等）。远程层启动时调用一次即可。
 *
 * 已存在同名 codec 时不覆盖：用户可能显式注册了兼容旧版本的实现，启动流程不该把它顶掉。
 */
public func ensureTypedPayloadCodecRegistered(): Bool {
    if (MessageCodecRegistry.isRegistered(TYPED_PAYLOAD_BOX_TYPE)) {
        return false
    }
    MessageCodecRegistry.register(TYPED_PAYLOAD_BOX_TYPE, TypedPayloadBoxCodec())
    true
}
