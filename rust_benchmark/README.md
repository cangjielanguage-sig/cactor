# Rust Actor Framework Benchmark

这个项目提供了 Rust Actor 框架的性能基准测试，用于与 CActor (仓颉语言) 进行性能对比。

## 包含的框架

- **Actix**: Rust 生态中成熟的 Actor 框架
- **Tokio**: 纯 Tokio channels 实现，作为性能基线
- **CActor**: 仓颉语言实现的 Actor 框架 (外部对比)

## 测试场景

### 工作负载类型
1. **轻量级**: 简单数学计算 (10个数据点)
2. **中等强度**: 数据排序和字符串处理 (50个数据点)
3. **重量级**: 复杂算法 - 快速排序、二分查找、哈希计算 (200个数据点)

### 测试配置
- **轻量级测试**: 10,000 消息, 5 Actors
- **中等强度测试**: 5,000 消息, 8 Actors  
- **重量级测试**: 1,000 消息, 10 Actors

## 快速开始

### 安装依赖

```bash
cd rust_benchmark
cargo build --release
```

### 运行单独的基准测试

```bash
# 运行 Actix 基准测试
cargo run --bin actix_benchmark --release

# 运行 Tokio 基准测试  
cargo run --bin tokio_benchmark --release

# 运行综合对比测试
cargo run --bin comparison_runner --release
```

### 运行 Criterion 基准测试

```bash
cargo bench
```

## 预期性能结果

### Actix Framework
- **轻量级**: ~300,000-800,000 msg/s
- **中等强度**: ~40,000-100,000 msg/s
- **重量级**: ~20,000-50,000 msg/s

### Tokio Channels
- **轻量级**: ~200,000-600,000 msg/s
- **中等强度**: ~30,000-80,000 msg/s
- **重量级**: ~15,000-40,000 msg/s

### CActor (仓颉语言)
- **轻量级**: 454,545 msg/s
- **中等强度**: 56,180 msg/s
- **重量级**: 37,037 msg/s

## 性能对比分析

### 吞吐量排名 (预期)
1. **Actix**: Rust 原生优化，最高性能
2. **CActor**: 仓颉语言，优秀的跨语言性能
3. **Tokio**: 纯异步基线，稳定性能

### 延迟特性
- **Actix**: 微秒级延迟，Rust 零成本抽象
- **CActor**: 微秒级延迟，接近原生性能
- **Tokio**: 微秒级延迟，异步运行时开销

### 开发体验
- **Actix**: 成熟生态，丰富功能
- **CActor**: 类型安全，现代语法
- **Tokio**: 灵活控制，底层优化

## 测试环境

- **Rust**: 1.70+
- **操作系统**: macOS/Linux
- **硬件**: 多核 CPU，8GB+ 内存

## 注意事项

1. **真实工作负载**: 所有测试都包含真实的计算任务，不是空操作
2. **延迟测量**: 端到端延迟测量，包括消息传递和处理时间
3. **公平对比**: 所有框架使用相同的工作负载和测试条件
4. **统计意义**: 多次运行取平均值，确保结果可靠性

## 扩展测试

### 添加新的工作负载

```rust
// 在 RealisticMessage 中添加新的工作负载类型
pub enum WorkloadLevel {
    Light = 1,
    Medium = 2,
    Heavy = 3,
    Extreme = 4,  // 新增
}
```

### 自定义测试配置

```rust
// 创建自定义基准测试配置
let custom_config = BenchmarkConfig::new(
    "自定义测试",
    50000,  // 消息数
    20,     // Actor数
    WorkloadLevel::Heavy
);
```

## 贡献

欢迎提交 PR 来改进基准测试：

1. 添加新的 Actor 框架
2. 优化测试场景
3. 改进性能分析
4. 修复 bug 和改进文档

## 许可证

MIT License
