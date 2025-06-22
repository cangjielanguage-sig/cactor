# ProtoActor-Go Benchmark

这个项目提供了 ProtoActor-Go 的性能基准测试，用于与 CActor (仓颉语言)、Actix (Rust) 和 Tokio (Rust) 进行性能对比。

## 关于 ProtoActor

ProtoActor 是一个跨平台的 Actor 框架，支持多种语言：
- **Go**: 高性能的 Go 实现
- **.NET**: C# 实现，企业级应用
- **Java**: JVM 生态集成
- **Kotlin**: 现代 JVM 语言支持

## 测试场景

### 工作负载类型
1. **轻量级**: 简单数学计算 (10个数据点)
2. **中等强度**: 数据排序和处理 (50个数据点)
3. **重量级**: 复杂算法 - 排序、二分查找、哈希计算 (200个数据点)

### 测试配置
- **轻量级测试**: 10,000 消息, 5 Actors
- **中等强度测试**: 5,000 消息, 8 Actors  
- **重量级测试**: 1,000 消息, 10 Actors

## 快速开始

### 安装依赖

```bash
cd go_protoactor_benchmark
go mod tidy
```

### 运行基准测试

```bash
# 运行 ProtoActor-Go 基准测试
go run main.go

# 运行综合对比测试 (包含其他框架)
go run main.go comparison.go
```

### 编译优化版本

```bash
# 编译优化版本
go build -ldflags="-s -w" -o protoactor_benchmark main.go

# 运行优化版本
./protoactor_benchmark
```

## 预期性能结果

### ProtoActor-Go
- **轻量级**: ~150,000-300,000 msg/s
- **中等强度**: ~60,000-120,000 msg/s
- **重量级**: ~20,000-40,000 msg/s
- **延迟**: 0.1-1.0 ms

### 与其他框架对比
- **CActor**: 语言级优化，性能最高
- **ProtoActor-Go**: 跨平台优势，性能优秀
- **Actix**: Rust 生态，零成本抽象
- **Tokio**: 异步基线，灵活控制

## 技术特点

### ProtoActor-Go 优势
1. **跨平台**: 同一套 API 支持多种语言
2. **高性能**: 优化的消息传递和调度
3. **成熟稳定**: 生产环境验证
4. **丰富功能**: 支持集群、持久化、监督等

### 与其他框架的差异
1. **vs CActor**: 
   - ProtoActor 更成熟，CActor 性能更高
   - ProtoActor 跨平台，CActor 专注仓颉生态

2. **vs Actix**:
   - ProtoActor 更简单易用，Actix 更底层控制
   - ProtoActor 跨语言，Actix 专注 Rust

3. **vs Tokio**:
   - ProtoActor 提供 Actor 抽象，Tokio 是异步基础设施
   - ProtoActor 更高级，Tokio 更灵活

## 测试环境

- **Go**: 1.21+
- **操作系统**: macOS/Linux
- **硬件**: 多核 CPU，8GB+ 内存

## 性能优化建议

### Go 编译优化
```bash
# 启用所有优化
go build -ldflags="-s -w" -gcflags="-B -C" -o protoactor_benchmark

# 使用 PGO (Profile-Guided Optimization)
go build -pgo=auto -o protoactor_benchmark
```

### 运行时优化
```bash
# 设置 GOMAXPROCS
export GOMAXPROCS=8

# 调整 GC 参数
export GOGC=100
export GOMEMLIMIT=4GiB
```

## 扩展测试

### 添加新的工作负载

```go
// 在 WorkloadLevel 中添加新类型
const (
    Light    WorkloadLevel = 1
    Medium   WorkloadLevel = 2
    Heavy    WorkloadLevel = 3
    Extreme  WorkloadLevel = 4  // 新增
)
```

### 自定义测试配置

```go
// 创建自定义基准测试配置
configs := []struct {
    name          string
    messageCount  uint64
    actorCount    int
    workloadLevel WorkloadLevel
}{
    {"自定义测试", 50000, 20, Heavy},
}
```

## 与其他语言的 ProtoActor 对比

### ProtoActor .NET
- **性能**: 通常比 Go 版本快 10-30%
- **生态**: .NET 企业级生态更丰富
- **部署**: 需要 .NET 运行时

### ProtoActor Java
- **性能**: JVM 优化后性能接近 .NET 版本
- **生态**: Java 生态系统庞大
- **启动**: JVM 启动时间较长

### 选择建议
- **Go**: 部署简单，性能优秀，推荐用于微服务
- **.NET**: 企业应用，Windows 环境首选
- **Java**: 大型系统，需要 JVM 生态集成

## 贡献

欢迎提交 PR 来改进基准测试：

1. 添加新的测试场景
2. 优化性能测试方法
3. 改进对比分析
4. 修复 bug 和改进文档

## 许可证

MIT License
