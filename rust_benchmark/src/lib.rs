/*
 * Rust Actor Framework Benchmark Library
 * 提供 Actix 和 Tokio 基准测试的公共接口
 */

pub mod actix_benchmark;
pub mod tokio_benchmark;

pub use actix_benchmark::{ActixBenchmarkRunner, RealisticActor as ActixActor, RealisticMessage as ActixMessage};
pub use tokio_benchmark::{TokioBenchmarkRunner, TokioActor, RealisticMessage as TokioMessage};

/// 通用的工作负载级别
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WorkloadLevel {
    Light = 1,
    Medium = 2,
    Heavy = 3,
}

impl From<WorkloadLevel> for actix_benchmark::WorkloadLevel {
    fn from(level: WorkloadLevel) -> Self {
        match level {
            WorkloadLevel::Light => actix_benchmark::WorkloadLevel::Light,
            WorkloadLevel::Medium => actix_benchmark::WorkloadLevel::Medium,
            WorkloadLevel::Heavy => actix_benchmark::WorkloadLevel::Heavy,
        }
    }
}

impl From<WorkloadLevel> for tokio_benchmark::WorkloadLevel {
    fn from(level: WorkloadLevel) -> Self {
        match level {
            WorkloadLevel::Light => tokio_benchmark::WorkloadLevel::Light,
            WorkloadLevel::Medium => tokio_benchmark::WorkloadLevel::Medium,
            WorkloadLevel::Heavy => tokio_benchmark::WorkloadLevel::Heavy,
        }
    }
}

/// 基准测试配置
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub name: String,
    pub message_count: u64,
    pub actor_count: usize,
    pub workload_level: WorkloadLevel,
}

impl BenchmarkConfig {
    pub fn new(name: &str, message_count: u64, actor_count: usize, workload_level: WorkloadLevel) -> Self {
        Self {
            name: name.to_string(),
            message_count,
            actor_count,
            workload_level,
        }
    }
    
    /// 创建轻量级测试配置
    pub fn light() -> Self {
        Self::new("轻量级测试", 10000, 5, WorkloadLevel::Light)
    }
    
    /// 创建中等强度测试配置
    pub fn medium() -> Self {
        Self::new("中等强度测试", 5000, 8, WorkloadLevel::Medium)
    }
    
    /// 创建重量级测试配置
    pub fn heavy() -> Self {
        Self::new("重量级测试", 1000, 10, WorkloadLevel::Heavy)
    }
    
    /// 创建高并发测试配置
    pub fn high_concurrency() -> Self {
        Self::new("高并发测试", 20000, 20, WorkloadLevel::Light)
    }
}

/// 统一的基准测试结果
#[derive(Debug, Clone)]
pub struct UnifiedBenchmarkResult {
    pub framework: String,
    pub test_name: String,
    pub total_messages: u64,
    pub processed_messages: u64,
    pub duration_ms: u64,
    pub throughput_msg_per_sec: f64,
    pub average_latency_ms: f64,
    pub completed: bool,
}

impl From<actix_benchmark::BenchmarkResult> for UnifiedBenchmarkResult {
    fn from(result: actix_benchmark::BenchmarkResult) -> Self {
        Self {
            framework: "Actix".to_string(),
            test_name: result.test_name,
            total_messages: result.total_messages,
            processed_messages: result.processed_messages,
            duration_ms: result.duration_ms,
            throughput_msg_per_sec: result.throughput_msg_per_sec,
            average_latency_ms: result.average_latency_ms,
            completed: result.completed,
        }
    }
}

impl From<tokio_benchmark::BenchmarkResult> for UnifiedBenchmarkResult {
    fn from(result: tokio_benchmark::BenchmarkResult) -> Self {
        Self {
            framework: "Tokio".to_string(),
            test_name: result.test_name,
            total_messages: result.total_messages,
            processed_messages: result.processed_messages,
            duration_ms: result.duration_ms,
            throughput_msg_per_sec: result.throughput_msg_per_sec,
            average_latency_ms: result.average_latency_ms,
            completed: result.completed,
        }
    }
}

impl UnifiedBenchmarkResult {
    pub fn print_result(&self) {
        println!();
        println!("📊 {} 基准测试结果:", self.framework);
        println!("  测试名称: {}", self.test_name);
        println!("  总处理消息数: {}", self.processed_messages);
        println!("  测试时长: {} ms", self.duration_ms);
        println!("  吞吐量: {:.2} msg/s", self.throughput_msg_per_sec);
        println!("  平均延迟: {:.4} ms", self.average_latency_ms);
        println!("  完成状态: {}", if self.completed { "✅ 完成" } else { "⚠️ 超时" });
    }
}

/// 综合基准测试运行器
pub struct ComprehensiveBenchmarkRunner;

impl ComprehensiveBenchmarkRunner {
    /// 运行所有框架的基准测试
    pub async fn run_all_benchmarks(configs: &[BenchmarkConfig]) -> Vec<UnifiedBenchmarkResult> {
        let mut results = Vec::new();
        
        for config in configs {
            println!("\n🔄 运行配置: {}", config.name);
            println!("{}", "=".repeat(50));
            
            // 运行 Actix 基准测试
            let actix_result = ActixBenchmarkRunner::run_benchmark(
                &config.name,
                config.message_count,
                config.actor_count,
                config.workload_level.into(),
            ).await;
            results.push(actix_result.into());
            
            // 运行 Tokio 基准测试
            let tokio_result = TokioBenchmarkRunner::run_benchmark(
                &config.name,
                config.message_count,
                config.actor_count,
                config.workload_level.into(),
            ).await;
            results.push(tokio_result.into());
        }
        
        results
    }
    
    /// 打印对比分析
    pub fn print_comparison_analysis(results: &[UnifiedBenchmarkResult]) {
        println!("\n🔍 框架性能对比分析");
        println!("{}", "=".repeat(60));
        
        // 按测试名称分组
        let mut grouped_results: std::collections::HashMap<String, Vec<&UnifiedBenchmarkResult>> = 
            std::collections::HashMap::new();
        
        for result in results {
            grouped_results
                .entry(result.test_name.clone())
                .or_insert_with(Vec::new)
                .push(result);
        }
        
        println!("\n📊 吞吐量对比:");
        println!("{:<20} {:<15} {:<15} {:<15}", "测试类型", "Actix (msg/s)", "Tokio (msg/s)", "性能比率");
        println!("{}", "-".repeat(65));
        
        for (test_name, test_results) in &grouped_results {
            let actix_result = test_results.iter().find(|r| r.framework == "Actix");
            let tokio_result = test_results.iter().find(|r| r.framework == "Tokio");
            
            if let (Some(actix), Some(tokio)) = (actix_result, tokio_result) {
                let ratio = if tokio.throughput_msg_per_sec > 0.0 {
                    actix.throughput_msg_per_sec / tokio.throughput_msg_per_sec
                } else {
                    0.0
                };
                
                println!("{:<20} {:<15.0} {:<15.0} {:<15.2}x", 
                         test_name,
                         actix.throughput_msg_per_sec,
                         tokio.throughput_msg_per_sec,
                         ratio);
            }
        }
        
        println!("\n📈 延迟对比:");
        println!("{:<20} {:<15} {:<15} {:<15}", "测试类型", "Actix (ms)", "Tokio (ms)", "延迟比率");
        println!("{}", "-".repeat(65));
        
        for (test_name, test_results) in &grouped_results {
            let actix_result = test_results.iter().find(|r| r.framework == "Actix");
            let tokio_result = test_results.iter().find(|r| r.framework == "Tokio");
            
            if let (Some(actix), Some(tokio)) = (actix_result, tokio_result) {
                let ratio = if actix.average_latency_ms > 0.0 {
                    tokio.average_latency_ms / actix.average_latency_ms
                } else {
                    0.0
                };
                
                println!("{:<20} {:<15.4} {:<15.4} {:<15.2}x", 
                         test_name,
                         actix.average_latency_ms,
                         tokio.average_latency_ms,
                         ratio);
            }
        }
    }
}
