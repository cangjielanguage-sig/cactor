# CActor Examples 示例集合

## 📚 示例概览

CActor提供了丰富的示例代码，帮助你快速学习和掌握Actor系统的各种功能。

## 🎯 入门示例

### 1. Hello World Demo
**文件**: `hello_world_demo/`  
**运行**: `cjpm run --name cactor.examples.hello_world_demo`  
**难度**: ⭐ 入门级

最简单的CActor入门示例，展示：
- 基本的Actor创建和消息传递
- Actor生命周期管理
- 系统启动和关闭
- 适合初学者的完整注释

### 2. API Demo
**文件**: `api_demo/`  
**运行**: `cjpm run --name cactor.examples.api_demo`  
**难度**: ⭐⭐ 初级

展示CActor API的使用方式：
- 简化的Actor系统创建
- 扩展方法的使用
- 批量操作演示

## 🚀 性能示例

### 3. Benchmark Demo
**文件**: `benchmark_demo/`  
**运行**: `cjpm run --name cactor.examples.benchmark_demo`  
**难度**: ⭐⭐ 初级

性能基准测试示例：
- 高吞吐量消息处理
- 性能指标测量
- 延迟统计分析

## 🔧 高级功能示例

### 4. Advanced Dispatcher Examples
**文件**: `advanced_dispatcher_examples/`  
**运行**: `cjpm run --name cactor.examples.advanced_dispatcher_examples`  
**难度**: ⭐⭐⭐ 中级

高级调度器功能演示：
- 线程池调度器（CPU密集型任务）
- 工作窃取调度器（负载均衡）
- 固定线程调度器（实时性能）

### 5. Advanced Mailbox Examples
**文件**: `advanced_mailbox_examples/`  
**运行**: `cjpm run --name cactor.examples.advanced_mailbox_examples`  
**难度**: ⭐⭐⭐ 中级

高级邮箱功能演示：
- SPSC邮箱（高频交易场景）
- MPSC邮箱（多生产者场景）
- 有界邮箱（背压控制）

### 6. Configuration Usage Test
**文件**: `configuration_usage_test/`  
**运行**: `cjpm run --name cactor.examples.configuration_usage_test`  
**难度**: ⭐⭐ 初级

配置系统使用示例：
- 邮箱配置
- 调度器配置
- 监督策略配置

## 🎭 类型安全示例

### 7. Typed Actor Demo
**文件**: `typed_actor_demo/`  
**运行**: `cjpm run --name cactor.examples.typed_actor_demo`  
**难度**: ⭐⭐⭐ 中级

类型安全Actor系统：
- TypedActor使用
- 编译时类型检查
- 类型安全消息处理

### 8. Enhanced Typed Actor Demo
**文件**: `enhanced_typed_actor_demo/`  
**运行**: `cjpm run --name cactor.examples.enhanced_typed_actor_demo`  
**难度**: ⭐⭐⭐⭐ 高级

增强的类型安全Actor：
- 高级类型安全特性
- 复杂消息类型处理

## 🔮 宏系统示例

### 9. Enhanced Actor Test
**文件**: `enhanced_actor_test/`  
**运行**: `cjpm run --name cactor.examples.enhanced_actor_test`  
**难度**: ⭐⭐⭐ 中级

宏增强的Actor：
- @actor宏使用
- @handler宏标记
- 自动消息路由

### 10. True Actor Macro Test
**文件**: `true_actor_macro_test/`  
**运行**: `cjpm run --name cactor.examples.true_actor_macro_test`  
**难度**: ⭐⭐⭐⭐ 高级

真实宏功能测试：
- 编译时代码生成
- 性能监控注入
- 类型安全检查

### 11. Comprehensive Macro Test
**文件**: `comprehensive_macro_test/`  
**运行**: `cjpm run --name cactor.examples.comprehensive_macro_test`  
**难度**: ⭐⭐⭐⭐ 高级

综合宏功能演示：
- 多种宏的组合使用
- 复杂代码生成
- 宏系统集成测试

## 🧪 综合测试示例

### 12. Comprehensive Improvement Test
**文件**: `comprehensive_improvement_test/`  
**运行**: `cjpm run --name cactor.examples.comprehensive_improvement_test`  
**难度**: ⭐⭐⭐⭐⭐ 专家级

综合功能改进测试：
- 类型安全Actor系统
- Ask模式实现
- 监督策略完善
- 宏系统集成

## 📋 学习路径推荐

### 初学者路径
1. **Hello World Demo** - 了解基本概念
2. **API Demo** - 学习API使用
3. **Benchmark Demo** - 体验性能特性
4. **Configuration Usage Test** - 掌握配置系统

### 进阶路径
1. **Advanced Dispatcher Examples** - 深入调度器
2. **Advanced Mailbox Examples** - 掌握邮箱系统
3. **Typed Actor Demo** - 学习类型安全
4. **Enhanced Actor Test** - 使用宏系统

### 专家路径
1. **True Actor Macro Test** - 深入宏功能
2. **Comprehensive Macro Test** - 宏系统集成
3. **Comprehensive Improvement Test** - 综合应用

## 🚀 快速开始

```bash
# 1. 构建所有示例
cjpm build

# 2. 从Hello World开始
cjpm run --name cactor.examples.hello_world_demo

# 3. 尝试性能测试
cjpm run --name cactor.examples.benchmark_demo

# 4. 探索高级功能
cjpm run --name cactor.examples.advanced_dispatcher_examples
```

## 💡 提示

- 每个示例都有详细的注释和说明
- 建议按照难度等级逐步学习
- 观察控制台输出了解运行过程
- 可以修改示例代码进行实验

## 🔗 相关文档

- [CActor API参考](../docs/zh/api-reference.md)
- [快速开始指南](../docs/zh/quick-start.md)
- [架构设计文档](../docs/zh/architecture.md)
