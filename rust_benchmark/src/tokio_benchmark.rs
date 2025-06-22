/*
 * Tokio Channel Benchmark
 * 使用纯 Tokio channels 实现的基准测试，作为性能基线
 */

use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// 工作负载级别
#[derive(Debug, Clone, Copy)]
pub enum WorkloadLevel {
    Light = 1,
    Medium = 2,
    Heavy = 3,
}

/// 真实工作负载消息
#[derive(Debug, Clone)]
pub struct RealisticMessage {
    pub id: u64,
    pub workload_level: WorkloadLevel,
    pub data: Vec<i64>,
    pub send_time: u128,
}

impl RealisticMessage {
    pub fn new(id: u64, workload_level: WorkloadLevel) -> Self {
        let data_size = match workload_level {
            WorkloadLevel::Light => 10,
            WorkloadLevel::Medium => 50,
            WorkloadLevel::Heavy => 200,
        };
        
        let mut rng = rand::thread_rng();
        let data: Vec<i64> = (0..data_size)
            .map(|_| rng.gen_range(0..10000))
            .collect();
        
        Self {
            id,
            workload_level,
            data,
            send_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }
}

/// Tokio Actor 模拟器
pub struct TokioActor {
    receiver: mpsc::UnboundedReceiver<RealisticMessage>,
    processed_count: Arc<AtomicU64>,
    total_latency: Arc<AtomicU64>,
    compute_results: Arc<AtomicU64>,
}

impl TokioActor {
    pub fn new() -> (Self, mpsc::UnboundedSender<RealisticMessage>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        let actor = Self {
            receiver,
            processed_count: Arc::new(AtomicU64::new(0)),
            total_latency: Arc::new(AtomicU64::new(0)),
            compute_results: Arc::new(AtomicU64::new(0)),
        };
        
        (actor, sender)
    }
    
    pub fn get_processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }
    
    pub fn get_average_latency_ms(&self) -> f64 {
        let count = self.processed_count.load(Ordering::Relaxed);
        if count > 0 {
            let total_latency = self.total_latency.load(Ordering::Relaxed);
            (total_latency as f64) / (count as f64) / 1_000_000.0 // 转换为毫秒
        } else {
            0.0
        }
    }
    
    /// 运行 Actor 消息循环
    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
    
    /// 处理消息
    async fn handle_message(&self, msg: RealisticMessage) {
        // 计算延迟
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let latency = current_time - msg.send_time;
        
        self.total_latency.fetch_add(latency as u64, Ordering::Relaxed);
        
        // 执行相应的工作负载
        let result = match msg.workload_level {
            WorkloadLevel::Light => self.perform_light_workload(&msg.data),
            WorkloadLevel::Medium => self.perform_medium_workload(&msg.data),
            WorkloadLevel::Heavy => self.perform_heavy_workload(&msg.data),
        };
        
        self.compute_results.fetch_add(result as u64, Ordering::Relaxed);
        self.processed_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 轻量级工作负载：简单数学计算
    fn perform_light_workload(&self, data: &[i64]) -> i64 {
        let mut sum = 0i64;
        for &value in data {
            sum += value * 2 + 1;
        }
        sum % 1_000_000
    }
    
    /// 中等工作负载：数据处理
    fn perform_medium_workload(&self, data: &[i64]) -> i64 {
        let mut sorted_data = data.to_vec();
        sorted_data.sort_unstable();
        
        let mut sum = 0i64;
        let mut product = 1i64;
        for &value in &sorted_data {
            sum += value;
            product = (product * (value % 100 + 1)) % 1_000_000;
        }
        
        sum + product
    }
    
    /// 重量级工作负载：复杂算法
    fn perform_heavy_workload(&self, data: &[i64]) -> i64 {
        let mut sorted_data = data.to_vec();
        sorted_data.sort_unstable();
        
        // 多次二分查找
        let mut search_results = 0i64;
        for i in 0..10 {
            let target = data[i % data.len()];
            if let Ok(found) = sorted_data.binary_search(&target) {
                search_results += found as i64;
            }
        }
        
        // 复杂哈希计算
        let mut hash = 0i64;
        for &value in data {
            hash = ((hash * 31) + value) % 1_000_000_007;
            hash = hash ^ (value << 3) ^ (value >> 2);
        }
        
        hash + search_results
    }
}

/// Tokio 基准测试运行器
pub struct TokioBenchmarkRunner;

impl TokioBenchmarkRunner {
    /// 运行基准测试
    pub async fn run_benchmark(
        test_name: &str,
        message_count: u64,
        actor_count: usize,
        workload_level: WorkloadLevel,
    ) -> BenchmarkResult {
        println!("⚡ 开始 Tokio 基准测试: {}", test_name);
        println!("  消息数: {}", message_count);
        println!("  Actor数: {}", actor_count);
        println!("  工作负载: {:?}", workload_level);
        
        // 创建 Actor 实例和发送器
        let mut actors = Vec::new();
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        
        for _i in 0..actor_count {
            let (actor, sender) = TokioActor::new();
            let processed_count = actor.processed_count.clone();
            let total_latency = actor.total_latency.clone();
            
            actors.push((processed_count, total_latency));
            senders.push(sender);
            
            // 启动 Actor 任务
            let handle = tokio::spawn(actor.run());
            handles.push(handle);
        }
        
        println!("✅ Tokio Actor系统创建完成");
        
        // 记录开始时间
        let start_time = Instant::now();
        
        // 发送消息
        println!("📤 开始发送消息...");
        for i in 0..message_count {
            let actor_index = (i as usize) % actor_count;
            let message = RealisticMessage::new(i, workload_level);
            
            if let Err(_) = senders[actor_index].send(message) {
                eprintln!("Failed to send message {}", i);
            }
        }
        
        println!("📤 消息发送完成，等待处理...");
        
        // 等待处理完成
        let completed = Self::wait_for_completion(&actors, message_count, Duration::from_secs(60)).await;
        
        // 记录结束时间
        let duration = start_time.elapsed();
        
        // 关闭所有发送器以结束 Actor 任务
        drop(senders);
        
        // 等待所有任务完成
        for handle in handles {
            let _ = handle.await;
        }
        
        // 计算统计信息
        let mut total_processed = 0u64;
        let mut total_latency = 0.0;
        
        for (processed_count, latency_sum) in &actors {
            let count = processed_count.load(Ordering::Relaxed);
            total_processed += count;
            
            if count > 0 {
                let latency = latency_sum.load(Ordering::Relaxed);
                total_latency += (latency as f64) / (count as f64) / 1_000_000.0;
            }
        }
        
        let avg_latency = if actor_count > 0 { 
            total_latency / (actor_count as f64) 
        } else { 
            0.0 
        };
        
        let throughput = if duration.as_millis() > 0 {
            (total_processed as f64) * 1000.0 / (duration.as_millis() as f64)
        } else {
            0.0
        };
        
        BenchmarkResult {
            test_name: test_name.to_string(),
            total_messages: message_count,
            processed_messages: total_processed,
            duration_ms: duration.as_millis() as u64,
            throughput_msg_per_sec: throughput,
            average_latency_ms: avg_latency,
            completed,
        }
    }
    
    /// 等待所有Actor处理完成
    async fn wait_for_completion(
        actors: &[(Arc<AtomicU64>, Arc<AtomicU64>)],
        expected_count: u64,
        timeout: Duration,
    ) -> bool {
        let start_time = Instant::now();
        
        while start_time.elapsed() < timeout {
            let mut total_processed = 0u64;
            
            for (processed_count, _) in actors {
                total_processed += processed_count.load(Ordering::Relaxed);
            }
            
            if total_processed >= expected_count {
                return true;
            }
            
            // 等待100ms后再次检查
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        false
    }
}

/// 基准测试结果
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub test_name: String,
    pub total_messages: u64,
    pub processed_messages: u64,
    pub duration_ms: u64,
    pub throughput_msg_per_sec: f64,
    pub average_latency_ms: f64,
    pub completed: bool,
}

impl BenchmarkResult {
    pub fn print_result(&self) {
        println!();
        println!("📊 Tokio 基准测试结果:");
        println!("  总处理消息数: {}", self.processed_messages);
        println!("  测试时长: {} ms", self.duration_ms);
        println!("  吞吐量: {:.2} msg/s", self.throughput_msg_per_sec);
        println!("  平均延迟: {:.4} ms", self.average_latency_ms);
        println!("  完成状态: {}", if self.completed { "✅ 完成" } else { "⚠️ 超时" });
    }
}

#[tokio::main]
async fn main() {
    println!("⚡ Tokio Channel 基准测试");
    println!("纯 Tokio 实现作为性能基线");
    println!("{}", "=".repeat(50));
    
    // 运行不同配置的基准测试
    let configs = [
        ("轻量级测试", 10000, 5, WorkloadLevel::Light),
        ("中等强度测试", 5000, 8, WorkloadLevel::Medium),
        ("重量级测试", 1000, 10, WorkloadLevel::Heavy),
    ];
    
    let mut results = Vec::new();
    
    for (name, msg_count, actor_count, workload) in configs {
        println!();
        let result = TokioBenchmarkRunner::run_benchmark(name, msg_count, actor_count, workload).await;
        result.print_result();
        results.push(result);
        println!("{}", "=".repeat(50));
    }
    
    // 打印汇总结果
    println!("\n📈 Tokio 基准测试汇总:");
    for result in &results {
        println!("  {}: {:.2} msg/s, {:.4} ms 延迟", 
                 result.test_name, 
                 result.throughput_msg_per_sec, 
                 result.average_latency_ms);
    }
    
    println!("\n🎉 Tokio 基准测试完成！");
}
