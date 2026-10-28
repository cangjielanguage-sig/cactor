# 🚀 CActor 快速开始指南

## 📋 环境准备

### 系统要求
- **仓颉语言**: 1.2.0 或更高版本
- **操作系统**: macOS、Linux、Windows
- **内存**: 建议 4GB 以上
- **CPU**: 支持多核处理器

### 安装仓颉语言
请参考[仓颉官方文档](https://cangjie-lang.cn/)安装仓颉语言环境。

## 🏗️ 项目设置

### 1. 克隆项目
```bash
git clone https://github.com/your-repo/cactor.git
cd cactor
```

### 2. 构建项目
```bash
# 构建所有组件
cjpm build

# 验证构建成功
echo "构建完成！"
```

### 3. 运行测试
```bash
# 运行基础测试
cjpm test

# 运行示例程序
cjpm run --name cactor_examples.hello_world_demo
```

## 🎯 第一个Actor程序

### Hello World示例

创建一个简单的Actor程序：

```cangjie
import cactor.api.{CActor}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.context.{ActorContext}

// 定义一个简单的Actor
class HelloWorldActor <: Actor {
    private var messageCount: Int64 = 0

    public prop name: String {
        get() { "HelloWorldActor" }
    }

    public prop description: String {
        get() { "Hello World Actor示例" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("👋 收到消息 #${messageCount}: ${msg.getContent()}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func preStart(): Unit {
        println("🚀 Hello World Actor启动了!")
    }

    public func postStop(): Unit {
        println("🛑 Hello World Actor停止了! 总共处理了 ${messageCount} 条消息")
    }
}

// 主程序
func main(): Unit {
    // 创建Actor系统
    let system = CActor.system("hello-world")

    // 创建Actor
    let helloActor = system.actorOf(() => HelloWorldActor(), "hello")

    // 发送消息
    helloActor.tell(StringMessage("Hello, CActor!"))
    helloActor.tell(StringMessage("这是第二条消息"))
    helloActor.tell(StringMessage("欢迎使用CActor!"))

    // 等待处理完成
    Thread.sleep(Duration.fromMilliseconds(100))

    // 关闭系统
    system.shutdown()
}
```

### 运行程序
```bash
cjpm run --name cactor_examples.hello_world_demo
```

预期输出：
```
🚀 Hello World Actor启动了!
👋 收到消息 #1: Hello, CActor!
👋 收到消息 #2: 这是第二条消息
👋 收到消息 #3: 欢迎使用CActor!
🛑 Hello World Actor停止了! 总共处理了 3 条消息
```

## 🎯 核心功能示例

### 1. Ask模式（请求-响应）
```bash
# 运行Ask模式示例
cjpm run --name cactor_examples.ask_demo
```

展示功能：
- 同步和异步请求
- 超时处理
- 批量请求
- 性能：657,894 req/s

### 2. 集群分片
```bash
# 运行集群分片示例
cjpm run --name cactor_examples.cluster_sharding_demo
```

展示功能：
- 集群分片协调
- 多种路由策略
- 负载均衡
- 支持：100万Actor

### 3. 持久化系统
```bash
# 运行持久化示例
cjpm run --name cactor_examples.persistence_demo
```

展示功能：
- 事件溯源
- 状态快照
- CQRS架构
- 多存储后端

### 4. 流处理
```bash
# 运行流处理示例
cjpm run --name cactor_examples.streams_demo
```

展示功能：
- 高性能流处理
- 背压控制
- 丰富操作符
- 性能：500万元素/秒

### 5. 状态机
```bash
# 运行状态机示例
cjpm run --name cactor_examples.xstate_demo
```

展示功能：
- 强类型状态机
- 模式匹配引擎
- CActor集成
- 纳秒级转换

## 🔧 常用配置

### Actor系统配置
```cangjie
// 创建配置
let config = CActorRuntimeConfig.createProduction()

// 创建配置的Actor系统
let system = CActor.system("MySystem", config)
```

### 性能调优配置
```cangjie
// 高性能配置
let config = CActorRuntimeConfig.createProduction()
    .withDispatcherThreads(8)
    .withMailboxCapacity(10000)
    .withBatchSize(100)

let system = CActor.system("HighPerformanceSystem", config)
```

## 📚 学习路径

### 初学者路径
1. **Hello World** - 基础Actor概念
2. **API Demo** - CActor API使用
3. **Benchmark Demo** - 性能测试

### 进阶路径
1. **Ask Pattern** - 请求-响应模式
2. **Typed Actor** - 类型安全Actor
3. **Advanced Dispatcher** - 高级调度器

### 专家路径
1. **Cluster Sharding** - 集群分片
2. **Persistence** - 持久化系统
3. **Streams** - 流处理系统
4. **XState** - 状态机系统

## 🛠️ 开发工具

### 构建命令
```bash
# 完整构建
cjpm build

# 清理构建
cjpm clean && cjpm build

# 运行特定示例
cjpm run --name <example-name>
```

### 调试技巧
```bash
# 启用详细日志
export CACTOR_LOG_LEVEL=DEBUG

# 性能分析
cjpm run --name cactor_examples.benchmark_demo
```

## 🔗 相关资源

### 文档链接
- [完整API文档](api-reference.md)
- [架构设计](architecture.md)
- [性能优化](performance.md)
- [功能特性](FEATURES.md)

### 示例代码
- [所有示例](../src/examples/README.md)
- [GitHub仓库](https://github.com/your-repo/cactor)

### 社区支持
- [GitHub Issues](https://github.com/your-repo/cactor/issues)
- [GitHub Discussions](https://github.com/your-repo/cactor/discussions)

## ❓ 常见问题

### Q: 如何提高Actor性能？
A: 使用批量消息处理、优化邮箱配置、选择合适的调度器。

### Q: 如何处理Actor故障？
A: 使用监督策略，配置重启策略，实现故障隔离。

### Q: 如何扩展到多节点？
A: 使用集群分片功能，配置集群路由策略。

---

**开始您的CActor之旅！** 🚀

如有问题，请查看[完整文档](index.md)或在[GitHub](https://github.com/your-repo/cactor)上提问。