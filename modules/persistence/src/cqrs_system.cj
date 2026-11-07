/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - CQRS系统实现
 * Plan20阶段1：持久化系统完善 - 命令查询责任分离
 */
package cactor_persistence

import std.collection.*
import std.time.*
import std.sync.*
import cactor_foundation.logging.{Logger, Logging}

let log_cqrs_system: Logger = Logging.logger("cactor_persistence.cqrs_system")

/**
 * 命令接口 - 所有命令必须实现此接口
 */
public interface Command {
    func getCommandId(): String
    func getAggregateId(): String
    func getCommandType(): String
    func getTimestamp(): DateTime
}

/**
 * 查询接口 - 所有查询必须实现此接口
 */
public interface Query {
    func getQueryId(): String
    func getQueryType(): String
    func getParameters(): HashMap<String, String>
}

/**
 * 查询结果接口
 */
public interface QueryResult {
    func getResultType(): String
    func serialize(): String
}

/**
 * 聚合根接口
 */
public interface AggregateRoot {
    func getAggregateId(): String
    func getVersion(): Int64
    func applyEvent(event: EventPayload): AggregateRoot
    func getUncommittedEvents(): Array<EventPayload>
    func markEventsAsCommitted(): Unit
}

/**
 * 命令处理器接口
 */
public interface CommandHandler {
    func handle(command: Command, aggregate: Option<AggregateRoot>): Array<EventPayload>
    func canHandle(command: Command): Bool
}

/**
 * 查询处理器接口
 */
public interface QueryHandler {
    func handle(query: Query): Future<QueryResult>
    func canHandle(query: Query): Bool
}

/**
 * 读模型接口
 */
public interface ReadModel {
    func getModelId(): String
    func update(event: PersistentEvent): Unit
    func query(querySpec: QuerySpec): Future<String>
}

/**
 * 查询规格接口
 */
public interface QuerySpec {
    func getSpecType(): String
    func getParameters(): HashMap<String, String>
}

/**
 * 基础命令实现
 */
public class BaseCommand <: Command {
    private let commandId: String
    private let aggregateId: String
    private let commandType: String
    private let timestamp: DateTime
    
    public init(commandId: String, aggregateId: String, commandType: String) {
        this.commandId = commandId
        this.aggregateId = aggregateId
        this.commandType = commandType
        this.timestamp = DateTime.now()
    }
    
    public func getCommandId(): String { commandId }
    public func getAggregateId(): String { aggregateId }
    public func getCommandType(): String { commandType }
    public func getTimestamp(): DateTime { timestamp }
    
    public func toString(): String {
        "BaseCommand(${commandId}, ${aggregateId}, ${commandType})"
    }
}

/**
 * 基础查询实现
 */
public class BaseQuery <: Query {
    private let queryId: String
    private let queryType: String
    private let parameters: HashMap<String, String>
    
    public init(queryId: String, queryType: String) {
        this.queryId = queryId
        this.queryType = queryType
        this.parameters = HashMap<String, String>()
    }
    
    public func getQueryId(): String { queryId }
    public func getQueryType(): String { queryType }
    public func getParameters(): HashMap<String, String> { parameters }
    
    public func addParameter(key: String, value: String): BaseQuery {
        parameters[key] = value
        this
    }
    
    public func toString(): String {
        "BaseQuery(${queryId}, ${queryType})"
    }
}

/**
 * 基础查询结果实现
 */
public class BaseQueryResult <: QueryResult {
    private let resultType: String
    private let data: String
    
    public init(resultType: String, data: String) {
        this.resultType = resultType
        this.data = data
    }
    
    public func getResultType(): String { resultType }
    public func serialize(): String { data }
    
    public func toString(): String {
        "BaseQueryResult(${resultType}, ${data})"
    }
}

/**
 * 基础聚合根实现
 */
public class BaseAggregateRoot <: AggregateRoot {
    private let aggregateId: String
    private var version: Int64
    private let uncommittedEvents: ArrayList<EventPayload>
    
    public init(aggregateId: String) {
        this.aggregateId = aggregateId
        this.version = 0
        this.uncommittedEvents = ArrayList<EventPayload>()
    }
    
    public func getAggregateId(): String { aggregateId }
    public func getVersion(): Int64 { version }
    
    public func applyEvent(event: EventPayload): AggregateRoot {
        // 子类应该重写此方法来处理具体的事件
        version += 1
        this
    }
    
    public func getUncommittedEvents(): Array<EventPayload> {
        uncommittedEvents.toArray()
    }
    
    public func markEventsAsCommitted(): Unit {
        uncommittedEvents.clear()
    }
    
    protected func addEvent(event: EventPayload): Unit {
        uncommittedEvents.add(event)
        applyEvent(event)
    }
    
    public func toString(): String {
        "BaseAggregateRoot(${aggregateId}, version=${version})"
    }
}

/**
 * CQRS总线 - 协调命令和查询处理
 */
public class CQRSBus {
    private let commandHandlers: HashMap<String, CommandHandler>
    private let queryHandlers: HashMap<String, QueryHandler>
    private let eventStore: EnhancedEventStore
    private let readModels: HashMap<String, ReadModel>
    private let mutex: Mutex
    // 聚合工厂：按聚合 id 创建初始聚合，再重放该聚合的事件得到当前状态（未登记时处理器拿到 None）
    private var aggregateFactory: Option<(String) -> AggregateRoot> = None

    public init(eventStore: EnhancedEventStore) {
        this.commandHandlers = HashMap<String, CommandHandler>()
        this.queryHandlers = HashMap<String, QueryHandler>()
        this.eventStore = eventStore
        this.readModels = HashMap<String, ReadModel>()
        this.mutex = Mutex()
    }
    
    public func registerCommandHandler(commandType: String, handler: CommandHandler): Unit {
        mutex.lock()
        commandHandlers[commandType] = handler
        mutex.unlock()
        log_cqrs_system.debug("注册命令处理器: ${commandType}")
    }

    public func registerQueryHandler(queryType: String, handler: QueryHandler): Unit {
        mutex.lock()
        queryHandlers[queryType] = handler
        mutex.unlock()
        log_cqrs_system.debug("注册查询处理器: ${queryType}")
    }
    
    /** 登记聚合工厂：sendCommand 会用它创建初始聚合并重放已存事件，处理器拿到的是当前状态 */
    public func registerAggregateFactory(factory: (String) -> AggregateRoot): Unit {
        synchronized(mutex) {
            aggregateFactory = Some(factory)
        }
    }

    public func registerReadModel(modelId: String, readModel: ReadModel): Unit {
        mutex.lock()
        readModels[modelId] = readModel
        mutex.unlock()
        log_cqrs_system.debug("注册读模型: ${modelId}")
    }
    
    public func sendCommand(command: Command): Future<Unit> {
        mutex.lock()
        let handler = commandHandlers.get(command.getCommandType())
        mutex.unlock()
        
        match (handler) {
            case Some(h) =>
                try {
                    let aggregateId = command.getAggregateId()
                    // 当前版本 = 已存最高序号。此前恒以 0 为期望版本、序号从 1 开始：同一聚合的第二条命令
                    // 必被存储以版本冲突拒绝，而 Future 被丢弃、读模型照样更新，调用方拿到"成功"（P2）
                    let currentSeq = eventStore.getHighestSequenceNr(aggregateId).get()
                    let aggregate = loadAggregate(aggregateId, currentSeq)

                    let events = h.handle(command, aggregate)

                    if (events.size > 0) {
                        let persistentEvents = Array<PersistentEvent>(events.size, { i =>
                            PersistentEvent(aggregateId, currentSeq + i + 1, events[i])
                        })
                        // 乐观并发：期望版本为读取时的最高序号，并发写同一聚合时后到者被拒绝。
                        // 必须等待写入结果——写入失败时既不能更新读模型，也不能向调用方报告成功
                        eventStore.saveEvents(aggregateId, persistentEvents, currentSeq).get()
                        updateReadModels(persistentEvents)
                    }

                    CompletedFuture<Unit>(())

                } catch (e: Exception) {
                    FailedFuture<Unit>(e)
                }

            case None =>
                FailedFuture<Unit>(Exception("未找到命令处理器: ${command.getCommandType()}"))
        }
    }

    /** 用聚合工厂创建初始聚合并按序号重放已存事件；未登记工厂或聚合尚无事件时返回 None/初始聚合 */
    private func loadAggregate(aggregateId: String, currentSeq: Int64): Option<AggregateRoot> {
        let factory = synchronized(mutex) { aggregateFactory }
        match (factory) {
            case Some(create) =>
                var aggregate = create(aggregateId)
                if (currentSeq > 0) {
                    for (e in eventStore.loadEventStream(aggregateId, 1, currentSeq).get()) {
                        aggregate = aggregate.applyEvent(e.payload)
                    }
                }
                Some(aggregate)
            case None => None
        }
    }
    
    public func sendQuery(query: Query): Future<QueryResult> {
        mutex.lock()
        let handler = queryHandlers.get(query.getQueryType())
        mutex.unlock()
        
        match (handler) {
            case Some(h) =>
                h.handle(query)
            case None =>
                FailedFuture<QueryResult>(Exception("未找到查询处理器: ${query.getQueryType()}"))
        }
    }
    
    private func updateReadModels(events: Array<PersistentEvent>): Unit {
        for (event in events) {
            for ((_, readModel) in readModels) {
                try {
                    readModel.update(event)
                } catch (e: Exception) {
                    log_cqrs_system.error("更新读模型失败: ${e.message}")
                }
            }
        }
    }
}

/**
 * 简单读模型实现
 */
public class SimpleReadModel <: ReadModel {
    private let modelId: String
    private let data: HashMap<String, String>
    private let mutex: Mutex
    
    public init(modelId: String) {
        this.modelId = modelId
        this.data = HashMap<String, String>()
        this.mutex = Mutex()
    }
    
    public func getModelId(): String { modelId }
    
    public func update(event: PersistentEvent): Unit {
        mutex.lock()
        
        // 简化的事件处理逻辑
        let key = "${event.persistenceId}_${event.sequenceNr}"
        let value = event.payload.serialize()
        data[key] = value
        
        mutex.unlock()
        log_cqrs_system.debug("读模型 ${modelId} 更新: ${event.payload.getEventType()}")
    }
    
    public func query(querySpec: QuerySpec): Future<String> {
        mutex.lock()
        
        // 简化的查询实现
        let result = data.get("default")
        
        mutex.unlock()
        
        match (result) {
            case Some(value) =>
                // 简化类型转换
                CompletedFuture<String>(value)
            case None =>
                FailedFuture<String>(Exception("查询结果为空"))
        }
    }
    
    public func getData(): HashMap<String, String> {
        mutex.lock()
        let result = HashMap<String, String>()
        for ((k, v) in data) {
            result[k] = v
        }
        mutex.unlock()
        result
    }
}

/**
 * 简单查询规格实现
 */
public class SimpleQuerySpec <: QuerySpec {
    private let specType: String
    private let parameters: HashMap<String, String>
    
    public init(specType: String) {
        this.specType = specType
        this.parameters = HashMap<String, String>()
    }
    
    public func getSpecType(): String { specType }
    public func getParameters(): HashMap<String, String> { parameters }
    
    public func addParameter(key: String, value: String): SimpleQuerySpec {
        parameters[key] = value
        this
    }
}
