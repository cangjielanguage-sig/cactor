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
                    // 加载聚合根（简化实现）
                    let aggregate: Option<AggregateRoot> = None
                    
                    // 处理命令
                    let events = h.handle(command, aggregate)
                    
                    // 保存事件
                    if (events.size > 0) {
                        let persistentEvents = Array<PersistentEvent>(events.size, { i =>
                            PersistentEvent(command.getAggregateId(), i + 1, events[i])
                        })
                        
                        eventStore.saveEvents(command.getAggregateId(), persistentEvents, 0)
                        
                        // 更新读模型
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
