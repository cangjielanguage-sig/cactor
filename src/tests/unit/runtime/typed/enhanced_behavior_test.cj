package cactor.tests.unit.runtime.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor.core.typed.*

/**
 * 增强的TypedBehavior功能测试套件
 */
@Test
public class EnhancedBehaviorTest {

    /**
     * 测试Setup行为
     */
    @TestCase
    public func testSetupBehavior(): Unit {
        let setupBehavior = TypedBehaviors.setup<String>({ context =>
            TypedBehaviors.receive<String>({ ctx, msg =>
                TypedBehaviors.same<String>()
            })
        })

        // 验证Setup行为的描述
        @Assert(setupBehavior.getDescription(), "SetupBehavior")
    }

    /**
     * 测试消息适配器
     */
    @TestCase
    public func testMessageAdapter(): Unit {
        let adapter = FunctionMessageAdapter<Int32, String>({ value =>
            Some("Number: ${value}")
        })

        let result1 = adapter.adapt(42)
        @Assert(result1.isSome(), true)
        match (result1) {
            case Some(str) => @Assert(str, "Number: 42")
            case None => @Assert(false, true) // 不应该到这里
        }
    }

    /**
     * 测试多消息处理器
     */
    @TestCase
    public func testMultiMessageBehavior(): Unit {
        // 创建一个简单的消息处理器
        let stringHandler = SimpleMessageHandler<String>({ msg =>
            msg.startsWith("str:")
        }, { msg, ctx =>
            TypedBehaviors.same<String>()
        })

        let handlers = Array<MessageHandler<String>>(1, { i => stringHandler })
        let behavior = TypedBehaviors.receiveMessage<String>(handlers)

        // 验证行为描述
        @Assert(behavior.getDescription(), "MultiMessageBehavior")
    }

    /**
     * 测试忽略行为
     */
    @TestCase
    public func testIgnoreBehavior(): Unit {
        let ignoreBehavior = TypedBehaviors.ignore<String>()

        // 验证行为描述
        @Assert(ignoreBehavior.getDescription(), "IgnoreBehavior")
    }

    /**
     * 测试停止行为
     */
    @TestCase
    public func testStoppedBehaviorDirect(): Unit {
        let stoppedBehavior = TypedBehaviors.stopped<String>()

        // 验证行为描述
        @Assert(stoppedBehavior.getDescription(), "StoppedBehavior")
    }

    /**
     * 测试部分行为
     */
    @TestCase
    public func testPartialBehavior(): Unit {
        let partialBehavior = TypedBehaviors.receivePartial<String>({ msg =>
            msg.startsWith("handle:")
        }, { ctx, msg =>
            TypedBehaviors.same<String>()
        })

        // 验证行为描述
        @Assert(partialBehavior.getDescription(), "PartialBehavior")
    }
}

/**
 * 简单的消息处理器实现
 */
class SimpleMessageHandler<T> <: MessageHandler<T> {
    private let predicate: (T) -> Bool
    private let handler: (T, TypedActorContext<T>) -> TypedBehavior<T>
    
    public init(predicate: (T) -> Bool, handler: (T, TypedActorContext<T>) -> TypedBehavior<T>) {
        this.predicate = predicate
        this.handler = handler
    }
    
    public func canHandle(message: T): Bool {
        predicate(message)
    }
    
    public func handle(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        handler(message, context)
    }
}
