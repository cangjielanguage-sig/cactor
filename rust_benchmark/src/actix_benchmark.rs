/*
 * Actix Actor Framework Benchmark
 * 与 CActor 对比的 Rust Actix 基准测试
 */

use actix::prelude::*;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 工作负载级别
#[derive(Debug, Clone, Copy)]
pub enum WorkloadLevel {
    Light = 1,
    Medium = 2,
    Heavy = 3,
    CpuIntensive = 4,
    IoIntensive = 5,
    MemoryIntensive = 6,
    Mixed = 7,
}

/// 真实工作负载消息
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
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
            WorkloadLevel::CpuIntensive => 500,
            WorkloadLevel::IoIntensive => 20,
            WorkloadLevel::MemoryIntensive => 1000,
            WorkloadLevel::Mixed => 100,
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

/// 真实工作负载 Actor
#[derive(Clone)]
pub struct RealisticActor {
    processed_count: Arc<AtomicU64>,
    total_latency: Arc<AtomicU64>,
    compute_results: Arc<AtomicU64>,
}

impl RealisticActor {
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
            (total_latency as f64) / (count as f64) / 1_000_000.0 // 转换为毫秒
        } else {
            0.0
        }
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

    /// CPU密集型工作负载：数学计算
    fn perform_cpu_intensive_workload(&self, data: &[i64]) -> i64 {
        let mut result = 0i64;

        // 素数计算
        for &value in data {
            let n = (value % 1000 + 100) as u64;
            if self.is_prime(n) {
                result += value;
            }
        }

        // 矩阵乘法模拟
        let matrix_size = (data.len().min(20)) as usize;
        let mut matrix_a = vec![vec![0i64; matrix_size]; matrix_size];
        let mut matrix_b = vec![vec![0i64; matrix_size]; matrix_size];

        // 初始化矩阵
        for i in 0..matrix_size {
            for j in 0..matrix_size {
                matrix_a[i][j] = data[i % data.len()] % 100;
                matrix_b[i][j] = data[j % data.len()] % 100;
            }
        }

        // 矩阵乘法
        for i in 0..matrix_size {
            for j in 0..matrix_size {
                for k in 0..matrix_size {
                    result += matrix_a[i][k] * matrix_b[k][j];
                }
            }
        }

        result % 1_000_000_007
    }

    /// I/O密集型工作负载：文件操作模拟
    fn perform_io_intensive_workload(&self, data: &[i64]) -> i64 {
        use std::fs;
        use std::io::Write;

        let mut result = 0i64;

        // 模拟文件写入
        let temp_file = format!("/tmp/actor_test_{}.txt", std::process::id());

        if let Ok(mut file) = fs::File::create(&temp_file) {
            for &value in data {
                if let Ok(_) = writeln!(file, "{}", value) {
                    result += 1;
                }
            }
        }

        // 模拟文件读取
        if let Ok(content) = fs::read_to_string(&temp_file) {
            result += content.lines().count() as i64;
        }

        // 清理临时文件
        let _ = fs::remove_file(&temp_file);

        // 模拟网络延迟
        std::thread::sleep(std::time::Duration::from_micros(10));

        result
    }

    /// 内存密集型工作负载：大数据结构操作
    fn perform_memory_intensive_workload(&self, data: &[i64]) -> i64 {
        use std::collections::{HashMap, BTreeMap, HashSet};

        let mut result = 0i64;

        // 大HashMap操作
        let mut hash_map = HashMap::new();
        for (i, &value) in data.iter().enumerate() {
            hash_map.insert(i, value);
            hash_map.insert(value as usize % 1000, i as i64);
        }

        // 大BTreeMap操作
        let mut btree_map = BTreeMap::new();
        for &value in data {
            btree_map.insert(value, value * 2);
        }

        // 大HashSet操作
        let mut hash_set = HashSet::new();
        for &value in data {
            hash_set.insert(value);
            hash_set.insert(value * 3);
            hash_set.insert(value / 2);
        }

        // 大Vector操作
        let mut big_vector: Vec<i64> = Vec::with_capacity(data.len() * 100);
        for &value in data {
            for i in 0..100 {
                big_vector.push(value + i);
            }
        }

        // 排序大数据
        big_vector.sort_unstable();

        // 计算结果
        result += hash_map.len() as i64;
        result += btree_map.len() as i64;
        result += hash_set.len() as i64;
        result += big_vector.len() as i64;

        result % 1_000_000_007
    }

    /// 混合工作负载：CPU + I/O + 内存
    fn perform_mixed_workload(&self, data: &[i64]) -> i64 {
        let cpu_result = self.perform_cpu_intensive_workload(&data[..data.len()/3]);
        let io_result = self.perform_io_intensive_workload(&data[data.len()/3..2*data.len()/3]);
        let memory_result = self.perform_memory_intensive_workload(&data[2*data.len()/3..]);

        (cpu_result + io_result + memory_result) % 1_000_000_007
    }

    /// 素数判断辅助函数
    fn is_prime(&self, n: u64) -> bool {
        if n < 2 {
            return false;
        }
        if n == 2 {
            return true;
        }
        if n % 2 == 0 {
            return false;
        }

        let sqrt_n = (n as f64).sqrt() as u64;
        for i in (3..=sqrt_n).step_by(2) {
            if n % i == 0 {
                return false;
            }
        }
        true
    }
}

impl Actor for RealisticActor {
    type Context = Context<Self>;
}

impl Handler<RealisticMessage> for RealisticActor {
    type Result = ();
    
    fn handle(&mut self, msg: RealisticMessage, _ctx: &mut Self::Context) -> Self::Result {
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
            WorkloadLevel::CpuIntensive => self.perform_cpu_intensive_workload(&msg.data),
            WorkloadLevel::IoIntensive => self.perform_io_intensive_workload(&msg.data),
            WorkloadLevel::MemoryIntensive => self.perform_memory_intensive_workload(&msg.data),
            WorkloadLevel::Mixed => self.perform_mixed_workload(&msg.data),
        };
        
        self.compute_results.fetch_add(result as u64, Ordering::Relaxed);
        self.processed_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Actix 基准测试运行器
pub struct ActixBenchmarkRunner;

impl ActixBenchmarkRunner {
    /// 运行基准测试
    pub async fn run_benchmark(
        test_name: &str,
        message_count: u64,
        actor_count: usize,
        workload_level: WorkloadLevel,
    ) -> BenchmarkResult {
        println!("🦀 开始 Actix 基准测试: {}", test_name);
        println!("  消息数: {}", message_count);
        println!("  Actor数: {}", actor_count);
        println!("  工作负载: {:?}", workload_level);
        
        // 创建 Actor 实例
        let mut actors = Vec::new();
        let mut actor_addrs = Vec::new();

        for _i in 0..actor_count {
            let actor = RealisticActor::new();
            actors.push(actor.clone());
            let addr = actor.start();
            actor_addrs.push(addr);
        }

        println!("✅ Actix Actor系统创建完成");

        // 记录开始时间
        let start_time = Instant::now();

        // 发送消息
        println!("📤 开始发送消息...");
        for i in 0..message_count {
            let actor_index = (i as usize) % actor_count;
            let message = RealisticMessage::new(i, workload_level);
            actor_addrs[actor_index].do_send(message);
        }

        println!("📤 消息发送完成，等待处理...");

        // 等待处理完成
        let completed = Self::wait_for_completion(&actors, message_count, Duration::from_secs(60)).await;

        // 记录结束时间
        let duration = start_time.elapsed();

        // 计算统计信息
        let mut total_processed = 0u64;
        let mut total_latency = 0.0;

        for actor in &actors {
            total_processed += actor.get_processed_count();
            total_latency += actor.get_average_latency_ms();
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
        actors: &[RealisticActor],
        expected_count: u64,
        timeout: Duration,
    ) -> bool {
        let start_time = Instant::now();
        
        while start_time.elapsed() < timeout {
            let mut total_processed = 0u64;
            
            for actor in actors {
                total_processed += actor.get_processed_count();
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
        println!("📊 Actix 基准测试结果:");
        println!("  总处理消息数: {}", self.processed_messages);
        println!("  测试时长: {} ms", self.duration_ms);
        println!("  吞吐量: {:.2} msg/s", self.throughput_msg_per_sec);
        println!("  平均延迟: {:.4} ms", self.average_latency_ms);
        println!("  完成状态: {}", if self.completed { "✅ 完成" } else { "⚠️ 超时" });
    }
}

#[actix_rt::main]
async fn main() {
    println!("🦀 Actix Actor Framework 基准测试");
    println!("与 CActor 对比的 Rust 实现");
    println!("{}", "=".repeat(50));
    
    // 运行不同配置的基准测试
    let configs = [
        ("轻量级测试", 10000, 5, WorkloadLevel::Light),
        ("中等强度测试", 5000, 8, WorkloadLevel::Medium),
        ("重量级测试", 1000, 10, WorkloadLevel::Heavy),
        ("CPU密集型测试", 2000, 6, WorkloadLevel::CpuIntensive),
        ("I/O密集型测试", 500, 4, WorkloadLevel::IoIntensive),
        ("内存密集型测试", 1000, 4, WorkloadLevel::MemoryIntensive),
        ("混合负载测试", 1000, 6, WorkloadLevel::Mixed),
    ];
    
    let mut results = Vec::new();
    
    for (name, msg_count, actor_count, workload) in configs {
        println!();
        let result = ActixBenchmarkRunner::run_benchmark(name, msg_count, actor_count, workload).await;
        result.print_result();
        results.push(result);
        println!("{}", "=".repeat(50));
    }
    
    // 打印汇总结果
    println!("\n📈 Actix 基准测试汇总:");
    for result in &results {
        println!("  {}: {:.2} msg/s, {:.4} ms 延迟", 
                 result.test_name, 
                 result.throughput_msg_per_sec, 
                 result.average_latency_ms);
    }
    
    println!("\n🎉 Actix 基准测试完成！");
}
