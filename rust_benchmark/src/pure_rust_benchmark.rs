/*
 * Pure Rust Benchmark
 * 纯 Rust 实现，不使用 Actor 框架，测试 Rust 的原生性能
 */

use std::sync::mpsc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use rand::Rng;

#[derive(Debug, Clone, Copy)]
pub enum WorkloadLevel {
    Light = 1,
    Medium = 2,
    Heavy = 3,
}

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

pub struct PureRustWorker {
    processed_count: Arc<AtomicU64>,
    total_latency: Arc<AtomicU64>,
    compute_results: Arc<AtomicU64>,
}

impl PureRustWorker {
    pub fn new() -> Self {
        Self {
            processed_count: Arc::new(AtomicU64::new(0)),
            total_latency: Arc::new(AtomicU64::new(0)),
            compute_results: Arc::new(AtomicU64::new(0)),
        }
    }
    
    pub fn get_processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }
    
    pub fn get_average_latency_ms(&self) -> f64 {
        let count = self.processed_count.load(Ordering::Relaxed);
        if count > 0 {
            let total_latency = self.total_latency.load(Ordering::Relaxed);
            (total_latency as f64) / (count as f64) / 1_000_000.0
        } else {
            0.0
        }
    }
    
    pub fn run(self, receiver: mpsc::Receiver<RealisticMessage>) {
        while let Ok(msg) = receiver.recv() {
            self.handle_message(msg);
        }
    }
    
    fn handle_message(&self, msg: RealisticMessage) {
        // 计算延迟
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let latency = current_time - msg.send_time;
        
        self.total_latency.fetch_add(latency as u64, Ordering::Relaxed);
        
        // 执行工作负载
        let result = match msg.workload_level {
            WorkloadLevel::Light => self.perform_light_workload(&msg.data),
            WorkloadLevel::Medium => self.perform_medium_workload(&msg.data),
            WorkloadLevel::Heavy => self.perform_heavy_workload(&msg.data),
        };
        
        self.compute_results.fetch_add(result as u64, Ordering::Relaxed);
        self.processed_count.fetch_add(1, Ordering::Relaxed);
    }
    
    // 与其他框架完全相同的轻量级工作负载
    fn perform_light_workload(&self, data: &[i64]) -> i64 {
        let mut sum = 0i64;
        for &value in data {
            sum += value * 2 + 1;
        }
        sum % 1_000_000
    }
    
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
    
    fn perform_heavy_workload(&self, data: &[i64]) -> i64 {
        let mut sorted_data = data.to_vec();
        sorted_data.sort_unstable();
        
        let mut search_results = 0i64;
        for i in 0..10 {
            let target = data[i % data.len()];
            if let Ok(found) = sorted_data.binary_search(&target) {
                search_results += found as i64;
            }
        }
        
        let mut hash = 0i64;
        for &value in data {
            hash = ((hash * 31) + value) % 1_000_000_007;
            hash = hash ^ (value << 3) ^ (value >> 2);
        }
        
        hash + search_results
    }
}

pub struct PureRustBenchmarkRunner;

impl PureRustBenchmarkRunner {
    pub fn run_benchmark(
        test_name: &str,
        message_count: u64,
        worker_count: usize,
        workload_level: WorkloadLevel,
    ) -> BenchmarkResult {
        println!("⚡ 开始纯 Rust 基准测试: {}", test_name);
        println!("  消息数: {}", message_count);
        println!("  Worker数: {}", worker_count);
        println!("  工作负载: {:?}", workload_level);
        
        // 创建 workers 和 channels
        let mut workers = Vec::new();
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        
        for _i in 0..worker_count {
            let (sender, receiver) = mpsc::channel();
            let worker = PureRustWorker::new();
            
            let processed_count = worker.processed_count.clone();
            let total_latency = worker.total_latency.clone();
            
            workers.push((processed_count, total_latency));
            senders.push(sender);
            
            // 启动 worker 线程
            let handle = thread::spawn(move || {
                worker.run(receiver);
            });
            handles.push(handle);
        }
        
        println!("✅ 纯 Rust Worker系统创建完成");
        
        // 记录开始时间
        let start_time = Instant::now();
        
        // 发送消息
        println!("📤 开始发送消息...");
        for i in 0..message_count {
            let worker_index = (i as usize) % worker_count;
            let message = RealisticMessage::new(i, workload_level);
            
            if let Err(_) = senders[worker_index].send(message) {
                eprintln!("Failed to send message {}", i);
            }
        }
        
        println!("📤 消息发送完成，等待处理...");
        
        // 等待处理完成
        let completed = Self::wait_for_completion(&workers, message_count as i64, Duration::from_secs(60));
        
        // 记录结束时间
        let duration = start_time.elapsed();
        
        // 关闭所有发送器
        drop(senders);
        
        // 等待所有线程完成
        for handle in handles {
            let _ = handle.join();
        }
        
        // 计算统计信息
        let mut total_processed = 0u64;
        let mut total_latency = 0.0;
        
        for (processed_count, latency_sum) in &workers {
            let count = processed_count.load(Ordering::Relaxed);
            total_processed += count;
            
            if count > 0 {
                let latency = latency_sum.load(Ordering::Relaxed);
                total_latency += (latency as f64) / (count as f64) / 1_000_000.0;
            }
        }
        
        let avg_latency = if worker_count > 0 { 
            total_latency / (worker_count as f64) 
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
            processed_messages: total_processed as i64,
            duration_ms: duration.as_millis() as i64,
            throughput_msg_per_sec: throughput,
            average_latency_ms: avg_latency,
            completed,
        }
    }
    
    fn wait_for_completion(
        workers: &[(Arc<AtomicU64>, Arc<AtomicU64>)],
        expected_count: i64,
        timeout: Duration,
    ) -> bool {
        let start_time = Instant::now();
        
        while start_time.elapsed() < timeout {
            let mut total_processed = 0u64;
            
            for (processed_count, _) in workers {
                total_processed += processed_count.load(Ordering::Relaxed);
            }
            
            if total_processed >= expected_count as u64 {
                return true;
            }
            
            thread::sleep(Duration::from_millis(100));
        }
        
        false
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub test_name: String,
    pub total_messages: u64,
    pub processed_messages: i64,
    pub duration_ms: i64,
    pub throughput_msg_per_sec: f64,
    pub average_latency_ms: f64,
    pub completed: bool,
}

impl BenchmarkResult {
    pub fn print_result(&self) {
        println!();
        println!("📊 纯 Rust 基准测试结果:");
        println!("  总处理消息数: {}", self.processed_messages);
        println!("  测试时长: {} ms", self.duration_ms);
        println!("  吞吐量: {:.2} msg/s", self.throughput_msg_per_sec);
        println!("  平均延迟: {:.4} ms", self.average_latency_ms);
        println!("  完成状态: {}", if self.completed { "✅ 完成" } else { "⚠️ 超时" });
    }
}

fn main() {
    println!("⚡ 纯 Rust 基准测试");
    println!("测试 Rust 原生性能，不使用 Actor 框架");
    println!("{}", "=".repeat(50));
    
    let configs = [
        ("轻量级测试", 10000, 5, WorkloadLevel::Light),
        ("中等强度测试", 5000, 8, WorkloadLevel::Medium),
        ("重量级测试", 1000, 10, WorkloadLevel::Heavy),
    ];
    
    let mut results = Vec::new();
    
    for (name, msg_count, worker_count, workload) in configs {
        println!();
        let result = PureRustBenchmarkRunner::run_benchmark(name, msg_count, worker_count, workload);
        result.print_result();
        results.push(result);
        println!("{}", "=".repeat(50));
    }
    
    println!("\n📈 纯 Rust 基准测试汇总:");
    for result in &results {
        println!("  {}: {:.2} msg/s, {:.4} ms 延迟", 
                 result.test_name, 
                 result.throughput_msg_per_sec, 
                 result.average_latency_ms);
    }
    
    println!("\n🎉 纯 Rust 基准测试完成！");
}
