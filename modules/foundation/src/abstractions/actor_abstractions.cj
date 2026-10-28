/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - Actor抽象接口
 * Plan 0.20 Phase 3.1: 依赖关系优化
 * 定义核心抽象接口，解决层间直接依赖问题
 */
package cactor_foundation.abstractions


/**
 * 基础消息接口
 * 所有消息都必须实现此接口
 */
public interface IMessage {
    /**
     * 获取消息类型
     */
    func messageType(): String
}

/**
 * 消息处理结果枚举
 */
public enum IMessageResult {
    | Handled      // 消息已处理
    | Unhandled    // 消息未处理
    | Failed       // 消息处理失败
}

/**
 * Actor路径接口
 */
public interface IActorPath {
    /**
     * 获取路径字符串
     */
    func toString(): String
    
    /**
     * 获取Actor名称
     */
    func getName(): String
    
    /**
     * 获取父路径
     */
    func getParent(): Option<IActorPath>
}

/**
 * Actor引用接口
 */
public interface IActorRef {
    /**
     * Actor路径
     */
    prop path: IActorPath
    
    /**
     * 发送消息
     */
    func tell(message: IMessage): Unit
    func tell(message: IMessage, sender: Option<String>): Unit
    
    /**
     * 比较引用
     */
    func equals(other: IActorRef): Bool
    func hashCode(): Int32
}

/**
 * Actor上下文接口
 */
public interface IActorContext {
    /**
     * 获取自身引用
     */
    prop self: IActorRef
    
    /**
     * 获取发送者
     */
    func sender(): Option<String>
    
    /**
     * 停止自己
     */
    func stopSelf(): Unit
    
    /**
     * 创建子Actor
     */
    func actorOf(props: IProps, name: String): IActorRef
    
    /**
     * 停止子Actor
     */
    func stop(child: IActorRef): Unit
}

/**
 * Actor接口
 */
public interface IActor {
    /**
     * Actor名称
     */
    prop name: String
    
    /**
     * Actor描述
     */
    prop description: String
    
    /**
     * 接收消息
     */
    func receive(message: IMessage, context: IActorContext): IMessageResult
    
    /**
     * 生命周期方法
     */
    func preStart(): Unit { /* 默认空实现 */ }
    func postStop(): Unit { /* 默认空实现 */ }
    func preRestart(reason: Exception): Unit { /* 默认空实现 */ }
    func postRestart(reason: Exception): Unit { /* 默认空实现 */ }
}

/**
 * Actor属性接口
 */
public interface IProps {
    /**
     * 创建Actor实例
     */
    func create(): IActor
    
    /**
     * 获取调度器配置
     */
    func getDispatcher(): String
    
    /**
     * 获取邮箱配置
     */
    func getMailbox(): String
    
    /**
     * 获取监督策略
     */
    func getSupervisionStrategy(): String
}

/**
 * Actor系统接口
 */
public interface IActorSystem {
    /**
     * 系统名称
     */
    prop name: String
    
    /**
     * 创建Actor
     */
    func actorOf(props: IProps, name: String): IActorRef
    
    /**
     * 查找Actor
     */
    func actorSelection(path: String): Option<IActorRef>
    
    /**
     * 终止系统
     */
    func terminate(): Unit
    
    /**
     * 等待终止
     */
    func awaitTermination(timeout: Duration): Bool
    
    /**
     * 获取系统状态
     */
    func isTerminated(): Bool
}

/**
 * 调度器接口
 */
public interface IDispatcher {
    /**
     * 调度器名称
     */
    prop name: String
    
    /**
     * 调度消息
     */
    func dispatch(actorRef: IActorRef, message: IMessage): Unit
    
    /**
     * 启动调度器
     */
    func start(): Unit
    
    /**
     * 停止调度器
     */
    func stop(): Unit
    
    /**
     * 获取调度器状态
     */
    func isRunning(): Bool
}

/**
 * 邮箱接口
 */
public interface IMailbox {
    /**
     * 入队消息
     */
    func enqueue(message: IMessage): Bool
    
    /**
     * 出队消息
     */
    func dequeue(): Option<IMessage>
    
    /**
     * 获取队列大小
     */
    func size(): Int32
    
    /**
     * 检查是否为空
     */
    func isEmpty(): Bool
    
    /**
     * 检查是否已满
     */
    func isFull(): Bool
}

/**
 * 监督策略接口
 */
public interface ISupervisionStrategy {
    /**
     * 处理子Actor异常
     */
    func handleChildFailure(child: IActorRef, cause: Exception): ISupervisionDirective
}

/**
 * 监督指令枚举
 */
public enum ISupervisionDirective {
    | Resume     // 恢复Actor
    | Restart    // 重启Actor
    | Stop       // 停止Actor
    | Escalate   // 升级到父Actor
}

/**
 * 配置接口
 */
public interface IConfiguration {
    /**
     * 获取字符串配置
     */
    func getString(key: String): Option<String>
    func getString(key: String, defaultValue: String): String
    
    /**
     * 获取整数配置
     */
    func getInt(key: String): Option<Int32>
    func getInt(key: String, defaultValue: Int32): Int32
    
    /**
     * 获取布尔配置
     */
    func getBool(key: String): Option<Bool>
    func getBool(key: String, defaultValue: Bool): Bool
    
    /**
     * 获取持续时间配置
     */
    func getDuration(key: String): Option<Duration>
    func getDuration(key: String, defaultValue: Duration): Duration
}

/**
 * 事件总线接口
 */
public interface IEventBus {
    /**
     * 发布事件
     */
    func publish(event: IMessage): Unit
    
    /**
     * 订阅事件
     */
    func subscribe(subscriber: IActorRef, eventType: String): Unit
    
    /**
     * 取消订阅
     */
    func unsubscribe(subscriber: IActorRef, eventType: String): Unit
    
    /**
     * 取消所有订阅
     */
    func unsubscribeAll(subscriber: IActorRef): Unit
}

/**
 * 日志接口
 */
public interface ILogger {
    /**
     * 记录调试信息
     */
    func debug(message: String): Unit
    
    /**
     * 记录信息
     */
    func info(message: String): Unit
    
    /**
     * 记录警告
     */
    func warn(message: String): Unit
    
    /**
     * 记录错误
     */
    func error(message: String): Unit
    func error(message: String, exception: Exception): Unit
}

/**
 * 序列化器接口
 */
public interface ISerializer {
    /**
     * 序列化对象
     */
    func serialize(obj: Any): Array<UInt8>
    
    /**
     * 反序列化对象
     */
    func deserialize(data: Array<UInt8>): Any
    
    /**
     * 获取序列化器标识
     */
    func getIdentifier(): String
}

/**
 * 网络传输接口
 */
public interface ITransport {
    /**
     * 发送数据
     */
    func send(address: String, data: Array<UInt8>): Unit
    
    /**
     * 启动传输
     */
    func start(): Unit
    
    /**
     * 停止传输
     */
    func stop(): Unit
    
    /**
     * 获取本地地址
     */
    func getLocalAddress(): String
}
