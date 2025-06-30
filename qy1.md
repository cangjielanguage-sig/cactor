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

### Step 2: 语法适配优先级列表

#### 高优先级文件 (立即修复)
1. `src/core/actor/actor.cj` - 核心Actor接口
2. `src/core/message/message.cj` - 消息系统
3. `src/core/system/actor_system.cj` - Actor系统
4. `src/runtime/system/cactor_runtime_system.cj` - 运行时系统
5. `src/api/cactor.cj` - 公共API

#### 中优先级文件 (第二批修复)
1. `src/patterns/ask/*.cj` - Ask模式
2. `src/runtime/mailbox/*.cj` - 邮箱系统
3. `src/runtime/dispatcher/*.cj` - 调度器
4. `src/examples/*.cj` - 示例代码

#### 低优先级文件 (最后修复)
1. `src/distribution/*.cj` - 分布式功能
2. `src/integration/*.cj` - 集成功能
3. `src/macros/*.cj` - 宏系统

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

## 🔍 下一步行动

### 立即执行 (今天)
1. **备份当前代码**: 创建升级前的备份分支
2. **更新cjpm.toml**: 升级版本配置
3. **创建benchmark_demo**: 修复缺失包问题
4. **测试基础构建**: 验证基本配置正确

### 明天执行
1. **开始语法适配**: 从核心模块开始
2. **建立测试流程**: 设置持续测试
3. **问题追踪**: 建立问题跟踪系统

---

**升级负责人**: CActor开发团队  
**文档版本**: v1.0  
**最后更新**: 2025-06-30  
**下次更新**: 升级完成后
