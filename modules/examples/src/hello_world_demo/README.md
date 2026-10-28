# CActor Hello World Demo

## 📚 简介

这是CActor框架最简单的入门示例，展示了如何创建和使用Actor系统的基本功能。

## 🎯 学习目标

通过这个示例，你将学会：

- ✅ 如何创建Actor系统
- ✅ 如何定义和实现Actor
- ✅ 如何发送和处理消息
- ✅ 如何管理Actor生命周期
- ✅ 如何优雅地关闭系统

## 🚀 运行示例

```bash
# 在CActor项目根目录下运行
cjpm run --name cactor_examples.hello_world_demo
```

## 📋 示例内容

### 1. HelloWorldActor
最简单的Actor实现，展示基本的消息处理：
- 处理字符串消息
- 处理Ping消息
- 实现生命周期钩子

### 2. GreeterActor
专门负责问候的Actor，演示Actor之间的交互：
- 处理问候请求
- 统计问候次数

### 3. 完整的演示流程
1. 创建Actor系统
2. 创建两个不同的Actor
3. 发送各种类型的消息
4. 观察消息处理过程
5. 优雅地关闭系统

## 🔍 代码亮点

### Actor定义
```cangjie
class HelloWorldActor <: Actor {
    public prop name: String {
        get() { "HelloWorldActor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                println("收到消息: ${msg.getContent()}")
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}
```

### 系统创建和使用
```cangjie
// 创建Actor系统
let system = CActor.system("hello-world-system")

// 创建Actor
let actor = system.actorOf({ => HelloWorldActor() }, "hello-actor")

// 发送消息
actor.tell(StringMessage("Hello World!"))

// 关闭系统
system.terminate()
```

## 📊 运行结果

示例运行时会显示：
- Actor系统启动信息
- 消息处理过程
- Actor生命周期事件
- 系统关闭过程

## 🎓 下一步

完成这个示例后，你可以继续学习：

1. **高级调度器示例** - `cactor_examples.advanced_dispatcher_examples`
2. **高级邮箱示例** - `cactor_examples.advanced_mailbox_examples`
3. **配置使用示例** - `cactor_examples.configuration_usage_test`
4. **类型安全Actor** - `cactor_examples.typed_actor_demo`

## 💡 提示

- 观察控制台输出，了解Actor系统的运行过程
- 尝试修改消息内容，看看Actor如何响应
- 注意Actor的生命周期钩子何时被调用
- 体验CActor的高性能特性（对象池化、批处理等）

## 🔗 相关文档

- [CActor API参考](../../../../docs/zh/api-reference.md)
- [架构设计](../../../../docs/zh/architecture.md)
- [快速开始指南](../../../../docs/zh/QUICK_START.md)
