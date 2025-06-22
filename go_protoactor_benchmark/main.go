/*
 * ProtoActor-Go Benchmark
 * 与 CActor 对比的 Go ProtoActor 基准测试
 */

package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

// 工作负载级别
type WorkloadLevel int

const (
	Light           WorkloadLevel = 1
	Medium          WorkloadLevel = 2
	Heavy           WorkloadLevel = 3
	CpuIntensive    WorkloadLevel = 4
	IoIntensive     WorkloadLevel = 5
	MemoryIntensive WorkloadLevel = 6
	Mixed           WorkloadLevel = 7
)

// 真实工作负载消息
type RealisticMessage struct {
	ID            uint64
	WorkloadLevel WorkloadLevel
	Data          []int64
	SendTime      int64 // 纳秒时间戳
}

// 创建真实工作负载消息
func NewRealisticMessage(id uint64, workloadLevel WorkloadLevel) *RealisticMessage {
	var dataSize int
	switch workloadLevel {
	case Light:
		dataSize = 10
	case Medium:
		dataSize = 50
	case Heavy:
		dataSize = 200
	case CpuIntensive:
		dataSize = 500
	case IoIntensive:
		dataSize = 20
	case MemoryIntensive:
		dataSize = 1000
	case Mixed:
		dataSize = 100
	}

	data := make([]int64, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = rand.Int63n(10000)
	}

	return &RealisticMessage{
		ID:            id,
		WorkloadLevel: workloadLevel,
		Data:          data,
		SendTime:      time.Now().UnixNano(),
	}
}

// 真实工作负载 Actor
type RealisticActor struct {
	processedCount *int64
	totalLatency   *int64
	computeResults *int64
}

// 创建新的 RealisticActor
func NewRealisticActor() *RealisticActor {
	return &RealisticActor{
		processedCount: new(int64),
		totalLatency:   new(int64),
		computeResults: new(int64),
	}
}

// 获取处理消息数量
func (a *RealisticActor) GetProcessedCount() int64 {
	return atomic.LoadInt64(a.processedCount)
}

// 获取平均延迟（毫秒）
func (a *RealisticActor) GetAverageLatencyMs() float64 {
	count := atomic.LoadInt64(a.processedCount)
	if count > 0 {
		totalLatency := atomic.LoadInt64(a.totalLatency)
		return float64(totalLatency) / float64(count) / 1_000_000.0 // 转换为毫秒
	}
	return 0.0
}

// Actor 消息处理
func (a *RealisticActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *RealisticMessage:
		a.handleRealisticMessage(msg)
	}
}

// 处理真实工作负载消息
func (a *RealisticActor) handleRealisticMessage(msg *RealisticMessage) {
	// 计算延迟
	currentTime := time.Now().UnixNano()
	latency := currentTime - msg.SendTime
	atomic.AddInt64(a.totalLatency, latency)

	// 执行相应的工作负载
	var result int64
	switch msg.WorkloadLevel {
	case Light:
		result = a.performLightWorkload(msg.Data)
	case Medium:
		result = a.performMediumWorkload(msg.Data)
	case Heavy:
		result = a.performHeavyWorkload(msg.Data)
	case CpuIntensive:
		result = a.performCpuIntensiveWorkload(msg.Data)
	case IoIntensive:
		result = a.performIoIntensiveWorkload(msg.Data)
	case MemoryIntensive:
		result = a.performMemoryIntensiveWorkload(msg.Data)
	case Mixed:
		result = a.performMixedWorkload(msg.Data)
	}

	atomic.AddInt64(a.computeResults, result)
	atomic.AddInt64(a.processedCount, 1)
}

// 轻量级工作负载：简单数学计算
func (a *RealisticActor) performLightWorkload(data []int64) int64 {
	var sum int64
	for _, value := range data {
		sum += value*2 + 1
	}
	return sum % 1_000_000
}

// 中等工作负载：数据处理
func (a *RealisticActor) performMediumWorkload(data []int64) int64 {
	sortedData := make([]int64, len(data))
	copy(sortedData, data)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i] < sortedData[j]
	})

	var sum int64
	var product int64 = 1
	for _, value := range sortedData {
		sum += value
		product = (product * (value%100 + 1)) % 1_000_000
	}

	return sum + product
}

// 重量级工作负载：复杂算法
func (a *RealisticActor) performHeavyWorkload(data []int64) int64 {
	sortedData := make([]int64, len(data))
	copy(sortedData, data)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i] < sortedData[j]
	})

	// 多次二分查找
	var searchResults int64
	for i := 0; i < 10; i++ {
		target := data[i%len(data)]
		idx := sort.Search(len(sortedData), func(j int) bool {
			return sortedData[j] >= target
		})
		if idx < len(sortedData) && sortedData[idx] == target {
			searchResults += int64(idx)
		}
	}

	// 复杂哈希计算
	var hash int64
	for _, value := range data {
		hash = ((hash * 31) + value) % 1_000_000_007
		hash = hash ^ (value << 3) ^ (value >> 2)
	}

	return hash + searchResults
}

// CPU密集型工作负载：数学计算
func (a *RealisticActor) performCpuIntensiveWorkload(data []int64) int64 {
	var result int64

	// 素数计算
	for _, value := range data {
		n := uint64(value%1000 + 100)
		if a.isPrime(n) {
			result += value
		}
	}

	// 矩阵乘法模拟
	matrixSize := len(data)
	if matrixSize > 20 {
		matrixSize = 20
	}

	matrixA := make([][]int64, matrixSize)
	matrixB := make([][]int64, matrixSize)

	// 初始化矩阵
	for i := 0; i < matrixSize; i++ {
		matrixA[i] = make([]int64, matrixSize)
		matrixB[i] = make([]int64, matrixSize)
		for j := 0; j < matrixSize; j++ {
			matrixA[i][j] = data[i%len(data)] % 100
			matrixB[i][j] = data[j%len(data)] % 100
		}
	}

	// 矩阵乘法
	for i := 0; i < matrixSize; i++ {
		for j := 0; j < matrixSize; j++ {
			for k := 0; k < matrixSize; k++ {
				result += matrixA[i][k] * matrixB[k][j]
			}
		}
	}

	return result % 1_000_000_007
}

// I/O密集型工作负载：文件操作模拟
func (a *RealisticActor) performIoIntensiveWorkload(data []int64) int64 {
	var result int64

	// 模拟文件写入
	tempFile := fmt.Sprintf("/tmp/actor_test_%d.txt", time.Now().UnixNano())

	file, err := os.Create(tempFile)
	if err == nil {
		for _, value := range data {
			fmt.Fprintf(file, "%d\n", value)
			result++
		}
		file.Close()
	}

	// 模拟文件读取
	if content, err := os.ReadFile(tempFile); err == nil {
		lines := strings.Split(string(content), "\n")
		result += int64(len(lines))
	}

	// 清理临时文件
	os.Remove(tempFile)

	// 模拟网络延迟
	time.Sleep(10 * time.Microsecond)

	return result
}

// 内存密集型工作负载：大数据结构操作
func (a *RealisticActor) performMemoryIntensiveWorkload(data []int64) int64 {
	var result int64

	// 大map操作
	hashMap := make(map[int]int64)
	for i, value := range data {
		hashMap[i] = value
		hashMap[int(value%1000)] = int64(i)
	}

	// 大slice操作
	bigSlice := make([]int64, 0, len(data)*100)
	for _, value := range data {
		for i := 0; i < 100; i++ {
			bigSlice = append(bigSlice, value+int64(i))
		}
	}

	// 排序大数据
	sort.Slice(bigSlice, func(i, j int) bool {
		return bigSlice[i] < bigSlice[j]
	})

	// 大set操作（用map模拟）
	hashSet := make(map[int64]bool)
	for _, value := range data {
		hashSet[value] = true
		hashSet[value*3] = true
		hashSet[value/2] = true
	}

	// 计算结果
	result += int64(len(hashMap))
	result += int64(len(bigSlice))
	result += int64(len(hashSet))

	return result % 1_000_000_007
}

// 混合工作负载：CPU + I/O + 内存
func (a *RealisticActor) performMixedWorkload(data []int64) int64 {
	third := len(data) / 3
	if third == 0 {
		third = 1
	}

	cpuResult := a.performCpuIntensiveWorkload(data[:third])
	ioResult := a.performIoIntensiveWorkload(data[third : 2*third])
	memoryResult := a.performMemoryIntensiveWorkload(data[2*third:])

	return (cpuResult + ioResult + memoryResult) % 1_000_000_007
}

// 素数判断辅助函数
func (a *RealisticActor) isPrime(n uint64) bool {
	if n < 2 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}

	sqrtN := uint64(math.Sqrt(float64(n)))
	for i := uint64(3); i <= sqrtN; i += 2 {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// ProtoActor 基准测试运行器
type ProtoActorBenchmarkRunner struct{}

// 基准测试结果
type BenchmarkResult struct {
	TestName            string
	TotalMessages       uint64
	ProcessedMessages   int64
	DurationMs          int64
	ThroughputMsgPerSec float64
	AverageLatencyMs    float64
	Completed           bool
}

// 打印结果
func (r *BenchmarkResult) PrintResult() {
	fmt.Println()
	fmt.Println("📊 ProtoActor-Go 基准测试结果:")
	fmt.Printf("  总处理消息数: %d\n", r.ProcessedMessages)
	fmt.Printf("  测试时长: %d ms\n", r.DurationMs)
	fmt.Printf("  吞吐量: %.2f msg/s\n", r.ThroughputMsgPerSec)
	fmt.Printf("  平均延迟: %.4f ms\n", r.AverageLatencyMs)
	if r.Completed {
		fmt.Println("  完成状态: ✅ 完成")
	} else {
		fmt.Println("  完成状态: ⚠️ 超时")
	}
}

// 运行基准测试
func (runner *ProtoActorBenchmarkRunner) RunBenchmark(
	testName string,
	messageCount uint64,
	actorCount int,
	workloadLevel WorkloadLevel,
) *BenchmarkResult {
	fmt.Printf("🐹 开始 ProtoActor-Go 基准测试: %s\n", testName)
	fmt.Printf("  消息数: %d\n", messageCount)
	fmt.Printf("  Actor数: %d\n", actorCount)
	fmt.Printf("  工作负载: %v\n", workloadLevel)

	// 创建 Actor 系统
	system := actor.NewActorSystem()

	// 创建 Actor 实例
	actors := make([]*RealisticActor, actorCount)
	actorPIDs := make([]*actor.PID, actorCount)

	for i := 0; i < actorCount; i++ {
		actors[i] = NewRealisticActor()
		props := actor.PropsFromProducer(func() actor.Actor {
			return actors[i]
		})
		actorPIDs[i] = system.Root.Spawn(props)
	}

	fmt.Println("✅ ProtoActor-Go Actor系统创建完成")

	// 记录开始时间
	startTime := time.Now()

	// 发送消息
	fmt.Println("📤 开始发送消息...")
	for i := uint64(0); i < messageCount; i++ {
		actorIndex := int(i) % actorCount
		message := NewRealisticMessage(i, workloadLevel)
		system.Root.Send(actorPIDs[actorIndex], message)
	}

	fmt.Println("📤 消息发送完成，等待处理...")

	// 等待处理完成
	completed := runner.waitForCompletion(actors, int64(messageCount), 60*time.Second)

	// 记录结束时间
	duration := time.Since(startTime)

	// 停止所有 Actor
	for _, pid := range actorPIDs {
		system.Root.Stop(pid)
	}

	// 计算统计信息
	var totalProcessed int64
	var totalLatency float64

	for _, actor := range actors {
		totalProcessed += actor.GetProcessedCount()
		totalLatency += actor.GetAverageLatencyMs()
	}

	avgLatency := totalLatency / float64(actorCount)
	throughput := float64(totalProcessed) * 1000.0 / float64(duration.Milliseconds())

	return &BenchmarkResult{
		TestName:            testName,
		TotalMessages:       messageCount,
		ProcessedMessages:   totalProcessed,
		DurationMs:          duration.Milliseconds(),
		ThroughputMsgPerSec: throughput,
		AverageLatencyMs:    avgLatency,
		Completed:           completed,
	}
}

// 等待所有Actor处理完成
func (runner *ProtoActorBenchmarkRunner) waitForCompletion(
	actors []*RealisticActor,
	expectedCount int64,
	timeout time.Duration,
) bool {
	startTime := time.Now()
	endTime := startTime.Add(timeout)

	for time.Now().Before(endTime) {
		var totalProcessed int64

		for _, actor := range actors {
			totalProcessed += actor.GetProcessedCount()
		}

		if totalProcessed >= expectedCount {
			return true
		}

		// 等待100ms后再次检查
		time.Sleep(100 * time.Millisecond)
	}

	return false
}

func main() {
	fmt.Println("🐹 ProtoActor-Go 基准测试")
	fmt.Println("与 CActor 对比的 Go 实现")
	fmt.Println(fmt.Sprintf("%s", "=================================================="))

	// 运行不同配置的基准测试
	configs := []struct {
		name          string
		messageCount  uint64
		actorCount    int
		workloadLevel WorkloadLevel
	}{
		{"轻量级测试", 10000, 5, Light},
		{"中等强度测试", 5000, 8, Medium},
		{"重量级测试", 1000, 10, Heavy},
		{"CPU密集型测试", 2000, 6, CpuIntensive},
		{"I/O密集型测试", 500, 4, IoIntensive},
		{"内存密集型测试", 1000, 4, MemoryIntensive},
		{"混合负载测试", 1000, 6, Mixed},
	}

	var results []*BenchmarkResult
	runner := &ProtoActorBenchmarkRunner{}

	for _, config := range configs {
		fmt.Println()
		result := runner.RunBenchmark(config.name, config.messageCount, config.actorCount, config.workloadLevel)
		result.PrintResult()
		results = append(results, result)
		fmt.Println(fmt.Sprintf("%s", "=================================================="))
	}

	// 打印汇总结果
	fmt.Println("\n📈 ProtoActor-Go 基准测试汇总:")
	for _, result := range results {
		fmt.Printf("  %s: %.2f msg/s, %.4f ms 延迟\n",
			result.TestName,
			result.ThroughputMsgPerSec,
			result.AverageLatencyMs)
	}

	fmt.Println("\n🎉 ProtoActor-Go 基准测试完成！")
}
