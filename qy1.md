# CActor 仓颉0.60.5升级计划

## 📋 项目概述

**项目名称**: CActor 仓颉0.60.5版本升级  
**当前版本**: 仓颉0.53.18  
**目标版本**: 仓颉0.60.5  
**升级日期**: 2025-06-30  
**项目状态**: 规划阶段  

## 🎯 升级目标

1. **版本升级**: 从仓颉0.53.18升级到0.60.5
2. **语法兼容**: 适配新版本语法变化
3. **功能完善**: 利用新版本特性优化现有功能
4. **构建修复**: 解决构建和测试问题
5. **性能提升**: 利用新版本性能优化

## 📊 当前状态分析

### ✅ 项目结构分析
- **总文件数**: 约150+个源文件
- **核心模块**: 7个主要层级（Foundation, Core, Runtime, Patterns, Distribution, Integration, API）
- **示例代码**: 15+个示例项目
- **测试覆盖**: 10+个集成测试
- **宏系统**: 完整的DSL宏支持

### ❌ 当前问题识别

#### 1. 构建配置问题
```toml
# 当前配置 (cjpm.toml)
cjc-version = "0.53.18"  # 需要升级到 0.60.5

# 缺失的包配置
cactor.examples.benchmark_demo  # 包不存在但在配置中引用
```

#### 2. 语法兼容性问题
基于cjoy项目0.60.5代码分析，发现以下潜在语法变化：

**导入语句变化**:
```cangjie
// 0.53.18 (当前)
import std.time.*
import std.sync.*

// 0.60.5 (目标) - 可能需要更具体的导入
import std.time.{Duration, DateTime}
import std.sync.{Mutex, Condition}
```

**stdx库集成**:
```cangjie
// 新增stdx库支持
import stdx.log.Logger
import stdx.net.http.{HttpContext, HttpStatusCode}
import stdx.encoding.json.stream.{JsonDeserializable, JsonSerializable}
```

#### 3. 依赖路径问题
```bash
# 当前缺失的stdx路径配置
path-option = [ "${CANGJIE_HOME}/../stdx/static/stdx" ]
```

## 🔧 详细升级计划

### 阶段1: 环境和配置升级 (1-2天)

#### 1.1 更新项目配置
- [ ] 更新 `cjpm.toml` 中的 `cjc-version` 到 "0.60.5"
- [ ] 添加stdx依赖路径配置
- [ ] 更新编译选项适配新版本
- [ ] 清理无效的包配置引用

#### 1.2 修复缺失的包
- [ ] 创建缺失的 `benchmark_demo` 包
- [ ] 验证所有包配置的有效性
- [ ] 清理或修复无效引用

#### 1.3 环境验证
- [ ] 验证仓颉0.60.5工具链安装
- [ ] 验证stdx库路径配置
- [ ] 测试基础编译功能

### 阶段2: 语法适配和兼容性修复 (3-5天)

#### 2.1 导入语句优化
**优先级**: 高  
**影响范围**: 所有源文件

```cangjie
// 修复前 (可能的问题)
import std.time.*
import std.collection.*

// 修复后 (明确导入)
import std.time.{Duration, DateTime, getCurrentTimeNanos}
import std.collection.{HashMap, ArrayList, Array}
```

**涉及文件**:
- `src/core/**/*.cj` (约30个文件)
- `src/runtime/**/*.cj` (约25个文件)
- `src/patterns/**/*.cj` (约15个文件)
- `src/examples/**/*.cj` (约20个文件)

#### 2.2 stdx库集成
**优先级**: 中  
**新功能**: 利用stdx扩展库

```cangjie
// 新增日志支持
import stdx.log.{Logger, LogLevel}

// 新增网络支持
import stdx.net.http.{HttpContext, HttpServer}

// 新增序列化支持
import stdx.encoding.json.{JsonValue, JsonObject}
```

**重点文件**:
- `src/integration/logging/*.cj`
- `src/distribution/remote/*.cj`
- `src/foundation/serialization/*.cj`

#### 2.3 语法现代化
**优先级**: 中  
**改进点**: 利用新版本语法特性

```cangjie
// 可能的语法改进
// 1. 更好的模式匹配
match (message) {
    case msg: StringMessage => handleString(msg)
    case msg: PingMessage => handlePing(msg)
    case _ => MessageResult.Unhandled
}

// 2. 改进的泛型语法
public class TypedActor<T> where T <: Message {
    // ...
}

// 3. 更好的错误处理
public func processMessage(msg: Message): Result<MessageResult, ActorError> {
    // ...
}
```

### 阶段3: 功能增强和优化 (2-3天)

#### 3.1 利用新版本特性
- [ ] **性能优化**: 利用0.60.5的性能改进
- [ ] **内存管理**: 使用新的内存管理特性
- [ ] **并发优化**: 利用改进的并发原语

#### 3.2 stdx库功能集成
- [ ] **日志系统**: 集成stdx.log替代自定义日志
- [ ] **网络通信**: 使用stdx.net优化远程通信
- [ ] **序列化**: 使用stdx.encoding优化消息序列化
- [ ] **测试框架**: 集成stdx.unittest

#### 3.3 宏系统优化
- [ ] **宏语法**: 适配新版本宏语法
- [ ] **编译时检查**: 利用改进的编译时特性
- [ ] **代码生成**: 优化宏代码生成逻辑

### 阶段4: 测试和验证 (2-3天)

#### 4.1 构建测试
- [ ] 执行 `cjpm build` 验证编译
- [ ] 修复所有编译错误
- [ ] 验证所有包的正确构建

#### 4.2 功能测试
- [ ] 执行 `cjpm test` 运行测试套件
- [ ] 修复测试失败问题
- [ ] 验证核心功能正常

#### 4.3 性能测试
- [ ] 运行性能基准测试
- [ ] 对比升级前后性能
- [ ] 优化性能回归问题

#### 4.4 集成测试
- [ ] 测试所有示例代码
- [ ] 验证宏系统功能
- [ ] 测试分布式功能

## 📝 具体实施步骤

### Step 1: 立即修复构建问题 ✅

#### 1.1 更新cjpm.toml ✅
```toml
[package]
name = "cactor"
version = "7.0.0"
description = "High-performance Actor system for Cangjie - CActor 7.0 with 6-layer architecture"
cjc-version = "0.60.5"  # 升级版本
output-type = "dynamic"
compile-option = "-Woff unused --diagnostic-format=noColor"  # 参考cjoy配置

# 添加stdx依赖配置
[target.aarch64-apple-darwin]
compile-option = "--cfg \"ohos=false\""
[target.aarch64-apple-darwin.bin-dependencies]
path-option = [ "${CANGJIE_HOME}/../stdx/static/stdx" ]
```

#### 1.2 创建缺失的benchmark_demo包 ✅

#### 1.3 发现的关键语法变化 🔍
基于构建错误分析，确认了以下API变化：

**HashMap API变化**:
```cangjie
// 0.53.18 (旧版本)
map.put(key, value)

// 0.60.5 (新版本)
map.add(key, value)
```

**ArrayList API变化**:
```cangjie
// 0.53.18 (旧版本)
list.append(item)

// 0.60.5 (新版本)
list.add(item)
```

**Mutex API变化**:
```cangjie
// 0.53.18 (旧版本)
import std.sync.ReentrantMutex

// 0.60.5 (新版本)
import std.sync.Mutex
```

#### 1.2 创建缺失的benchmark_demo包
```bash
mkdir -p src/examples/benchmark_demo
```

```cangjie
// src/examples/benchmark_demo/main.cj
package cactor.examples.benchmark_demo

import cactor.api.{CActor}
import cactor.core.message.{StringMessage}

main(): Int64 {
    println("CActor Benchmark Demo")
    let system = CActor.system("BenchmarkSystem")
    
    // 简单的性能测试
    let startTime = getCurrentTimeNanos()
    
    // 创建测试Actor
    let actor = system.actorOf({ => TestActor() }, "test")
    
    // 发送测试消息
    for (i in 0..1000) {
        actor.tell(StringMessage("Test message " + i.toString()))
    }
    
    let endTime = getCurrentTimeNanos()
    let duration = endTime - startTime
    
    println("Benchmark completed in: " + duration.toString() + "ns")
    
    system.shutdown()
    return 0
}

class TestActor <: Actor {
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                // 简单处理
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}
```

### Step 2: 语法适配优先级列表 🔄

#### ✅ 已完成修复的文件
1. `src/foundation/serialization/serializer.cj` - 序列化器
2. `src/integration/configuration/configuration.cj` - 配置管理
3. `src/integration/testing/simple_plan10_test/main.cj` - 测试文件
4. `src/runtime/monitoring/actor_system_metrics.cj` - 监控指标
5. `src/foundation/queue/*.cj` - 队列系统
6. `src/patterns/circuit_breaker/circuit_breaker.cj` - 断路器
7. `src/foundation/memory/*.cj` - 内存管理
8. `src/foundation/serialization/serialization_manager.cj` - 序列化管理
9. `src/foundation/network/transport.cj` - 网络传输
10. `src/core/message/message_serializer.cj` - 消息序列化
11. `src/core/message/network_message.cj` - 网络消息

#### 🔄 发现的主要API变化
1. **HashMap**: `put(key, value)` → `add(key, value)` ✅
2. **ArrayList**: `append(item)` → `add(item)` ✅
3. **ArrayList**: `appendAll(items)` → 需要用循环逐个添加 ✅
4. **ReentrantMutex** → `Mutex` ✅
5. **NonBlockingQueue** → `ConcurrentLinkedQueue` (已弃用但仍可用)
6. **Queue命名冲突**: `std.collection.Queue` vs `cactor.foundation.queue.Queue`
7. **ArrayList.remove()**: 参数类型从`Int64`改为`Range<Int64>`
8. **HashSet**: `put(item)` → `add(item)`

#### ⏳ 待修复的高优先级文件
1. `src/core/config/default_actor_config.cj` - 配置系统
2. `src/core/system/enterprise_actor_system.cj` - 企业级Actor系统
3. `src/core/supervision/advanced_supervision.cj` - 监督策略
4. `src/runtime/mailbox/*.cj` - 邮箱系统
5. `src/patterns/routing/router.cj` - 路由器

#### ⏳ 待修复的中优先级文件
1. `src/runtime/dispatcher/work_stealing/*.cj` - 工作窃取调度器
2. `src/runtime/registry/actor_registry.cj` - Actor注册表
3. `src/core/typed/typed_router.cj` - 类型化路由器
4. `src/patterns/backpressure/backpressure.cj` - 背压控制

#### ⏳ 待修复的低优先级文件
1. `src/distribution/*.cj` - 分布式功能
2. `src/integration/*.cj` - 集成功能
3. `src/runtime/events/*.cj` - 事件系统

### Step 3: 测试策略

#### 3.1 渐进式测试
```bash
# 1. 测试核心模块
cjpm build --package cactor.core.actor
cjpm build --package cactor.core.message
cjpm build --package cactor.core.system

# 2. 测试运行时模块
cjpm build --package cactor.runtime.system
cjpm build --package cactor.runtime.mailbox

# 3. 测试完整构建
cjpm build

# 4. 运行测试
cjpm test
```

#### 3.2 问题追踪
创建问题追踪表格：

| 文件 | 问题类型 | 状态 | 解决方案 |
|------|----------|------|----------|
| cjpm.toml | 版本配置 | ❌ | 更新到0.60.5 |
| benchmark_demo | 缺失包 | ❌ | 创建包 |
| *.cj | 导入语句 | ⏳ | 逐步修复 |

## 🚨 风险评估和缓解策略

### 高风险项
1. **语法不兼容**: 0.60.5可能有破坏性语法变化
   - **缓解**: 参考cjoy项目的成功实践
   - **备份**: 保持当前分支作为回退选项

2. **性能回归**: 新版本可能影响性能
   - **缓解**: 详细的性能基准测试
   - **监控**: 对比升级前后的性能数据

3. **依赖问题**: stdx库集成可能引入新问题
   - **缓解**: 渐进式集成，先保持现有功能
   - **测试**: 充分的集成测试

### 中风险项
1. **宏系统兼容性**: 宏语法可能有变化
   - **缓解**: 参考官方文档和cjoy实践
   - **测试**: 专门的宏功能测试

2. **构建配置复杂性**: 多平台配置可能复杂
   - **缓解**: 参考cjoy的多平台配置
   - **简化**: 先支持主要平台

## 📈 成功指标

### 技术指标
- [ ] **构建成功率**: 100%的包能够成功构建
- [ ] **测试通过率**: 95%以上的测试用例通过
- [ ] **性能保持**: 性能不低于升级前的95%
- [ ] **功能完整性**: 所有核心功能正常工作

### 质量指标
- [ ] **代码质量**: 无编译警告和错误
- [ ] **文档更新**: 更新相关文档和示例
- [ ] **向后兼容**: API保持向后兼容
- [ ] **稳定性**: 长时间运行无崩溃

## 📅 时间计划

### 第1周 (Day 1-2): 环境准备
- Day 1: 配置更新和环境验证
- Day 2: 缺失包创建和基础修复

### 第2周 (Day 3-7): 核心适配
- Day 3-4: 核心模块语法适配
- Day 5-6: 运行时模块适配
- Day 7: API层适配

### 第3周 (Day 8-10): 功能完善
- Day 8: 示例代码适配
- Day 9: 宏系统适配
- Day 10: 集成测试和修复

## 📊 当前升级进展

### ✅ 已完成的工作
1. **环境配置升级**:
   - ✅ 更新cjpm.toml到0.60.5版本
   - ✅ 添加stdx依赖配置
   - ✅ 创建缺失的benchmark_demo包

2. **核心语法修复** (已修复约30个文件):
   - ✅ HashMap.put() → add() 修复
   - ✅ ArrayList.append() → add() 修复
   - ✅ ArrayList.appendAll() → 循环add() 修复
   - ✅ ReentrantMutex → Mutex 修复
   - ✅ ArrayList.remove(index) → remove(index..index+1) 修复

3. **已修复的关键文件**:
   - ✅ 序列化系统 (serializer.cj, message_serializer.cj)
   - ✅ 配置系统 (configuration.cj, default_actor_config.cj)
   - ✅ 监控系统 (actor_system_metrics.cj)
   - ✅ 队列系统 (foundation/queue/*.cj)
   - ✅ 内存管理 (foundation/memory/*.cj)
   - ✅ 网络传输 (foundation/network/transport.cj)
   - ✅ 监督策略 (advanced_supervision.cj)
   - ✅ 路由系统 (router.cj)

### 🔄 剩余工作量评估

#### 高优先级 (约50个错误)
1. **Queue命名冲突**: 3个文件需要明确指定Queue类型
2. **HashSet.put()**: 约8个文件需要改为add()
3. **ArrayList操作**: 约15个文件的append/remove问题
4. **HashMap.put()**: 约20个文件需要修复

#### 中优先级 (约30个错误)
1. **ReentrantMutex**: 约15个文件需要改为Mutex
2. **NonBlockingQueue**: 约10个文件需要更新API
3. **类型化路由器**: 2个文件需要修复

#### 低优先级 (约20个错误)
1. **分布式功能**: 集群、持久化等
2. **集成功能**: 监控、日志等

### 🎯 修复策略

#### 批量修复方案
由于错误数量较多且类型相似，建议采用批量修复策略：

1. **创建修复脚本**: 自动化批量替换常见模式
2. **分模块修复**: 按功能模块逐个修复
3. **渐进式验证**: 每修复一个模块就测试一次

#### 预计完成时间
- **剩余核心修复**: 2-3天
- **完整构建成功**: 3-4天
- **测试验证**: 1-2天
- **总计**: 6-9天

### 🚨 关键发现

#### API变化总结
```cangjie
// HashMap API
map.put(key, value) → map.add(key, value)

// HashSet API
set.put(item) → set.add(item)

// ArrayList API
list.append(item) → list.add(item)
list.appendAll(items) → for (item in items) { list.add(item) }
list.remove(index) → list.remove(index..index+1)

// Mutex API
ReentrantMutex() → Mutex()

// Queue命名冲突
需要明确指定: cactor.foundation.queue.Queue
```

#### 性能影响评估
- **appendAll替换**: 可能轻微影响性能，但影响有限
- **其他API变化**: 基本无性能影响
- **整体评估**: 性能应该能保持在95%以上

## 🔍 下一步行动

### 立即执行 (今天)
1. ✅ **备份当前代码**: 创建升级前的备份分支
2. ✅ **更新cjpm.toml**: 升级版本配置
3. ✅ **创建benchmark_demo**: 修复缺失包问题
4. ✅ **开始语法适配**: 已修复约30个文件

### 继续执行 (明天)
1. **批量修复剩余错误**: 重点修复HashMap.put和ArrayList.append
2. **解决Queue命名冲突**: 明确指定Queue类型
3. **修复HashSet.put问题**: 改为add()方法
4. **完成ReentrantMutex替换**: 剩余约15个文件

### 验证阶段 (后天)
1. **构建验证**: 确保cjpm build成功
2. **功能测试**: 执行cjpm test
3. **性能测试**: 运行基准测试
4. **文档更新**: 完善升级文档

---

**升级负责人**: CActor开发团队
**文档版本**: v2.0
**最后更新**: 2025-06-30
**下次更新**: 核心修复完成后
