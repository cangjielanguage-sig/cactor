// protoactor-go 对照基准：场景、消息数、计时方式与 CActor real_benchmark（modules/examples/src/real_benchmark）一致。
// 修改任一边的场景时请同步修改另一边。运行：bench/protoactor/run.sh
//
//	[1] 单 Actor 吞吐：1 个发送方 tell 100 万条
//	[2] 扇出吞吐：8 发送方 × 100 Actor，共 200 万条
//	[3] ping-pong 往返延迟：2 万次，丢弃前 10%
//	[4] Ask 往返延迟：2 万次
//	[6] 多对 ping-pong：protoactor 官方 inprocessbenchmark 写法（64 对 × 每对 10 万条，批量 100，
//	    消息对象复用，往返两个方向都计数）。官方程序用回信的 Sender 发下一批，而回信的 Sender 是 client 自己，
//	    第一批之后实际变成自己发给自己；这里记住 echo，保证每条消息都真的在两个 Actor 之间往返
package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

type strMsg struct{ s string }
type pingMsg struct{ id int64 }
type pongMsg struct{ id int64 }

func perSecond(n int64, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return n * int64(time.Second) / int64(d)
}

func report(title string, samples []int64) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum int64
	for _, v := range samples {
		sum += v
	}
	pct := func(p float64) float64 { return float64(samples[int(float64(len(samples)-1)*p)]) / 1000.0 }
	fmt.Printf("  %s: n=%d 平均=%.3fµs P50=%.3fµs P99=%.3fµs P99.9=%.3fµs 最大=%.3fµs\n", title, len(samples),
		float64(sum)/float64(len(samples))/1000.0, pct(0.5), pct(0.99), pct(0.999), float64(samples[len(samples)-1])/1000.0)
}

func waitFor(cond func() bool) {
	for !cond() {
		time.Sleep(50 * time.Microsecond)
	}
}

func benchSingleActor(sys *actor.ActorSystem, n int64) {
	var counter int64
	pid := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if _, ok := ctx.Message().(*strMsg); ok {
			atomic.AddInt64(&counter, 1)
		}
	}))
	msg := &strMsg{"x"}
	start := time.Now()
	for i := int64(0); i < n; i++ {
		sys.Root.Send(pid, msg)
	}
	sent := time.Since(start)
	waitFor(func() bool { return atomic.LoadInt64(&counter) >= n })
	total := time.Since(start)
	fmt.Printf("[1] 单 Actor 吞吐：%d 条，发送耗时 %dms，处理完成 %dms → %d msg/s\n", n, sent.Milliseconds(), total.Milliseconds(), perSecond(n, total))
}

func benchFanOut(sys *actor.ActorSystem, senders, actors, perSender int64) {
	counters := make([]int64, actors*8) // 每个计数器隔开 64 字节，避免伪共享
	pids := make([]*actor.PID, actors)
	for i := int64(0); i < actors; i++ {
		idx := i * 8
		pids[i] = sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
			if _, ok := ctx.Message().(*strMsg); ok {
				atomic.AddInt64(&counters[idx], 1)
			}
		}))
	}
	total := senders * perSender
	msg := &strMsg{"x"}
	start := time.Now()
	var wg sync.WaitGroup
	for s := int64(0); s < senders; s++ {
		wg.Add(1)
		go func(s int64) {
			defer wg.Done()
			for i := int64(0); i < perSender; i++ {
				sys.Root.Send(pids[(s+i)%actors], msg)
			}
		}(s)
	}
	wg.Wait()
	waitFor(func() bool {
		var sum int64
		for i := int64(0); i < actors; i++ {
			sum += atomic.LoadInt64(&counters[i*8])
		}
		return sum >= total
	})
	elapsed := time.Since(start)
	fmt.Printf("[2] 扇出吞吐：%d 发送方 × %d Actor，共 %d 条，%dms → %d msg/s\n", senders, actors, total, elapsed.Milliseconds(), perSecond(total, elapsed))
}

func benchPingPong(sys *actor.ActorSystem, rounds int64) {
	var done int64 = -1
	pingPID := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if p, ok := ctx.Message().(*pongMsg); ok {
			atomic.StoreInt64(&done, p.id)
		}
	}))
	pongPID := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if p, ok := ctx.Message().(*pingMsg); ok {
			ctx.Send(pingPID, &pongMsg{p.id})
		}
	}))
	samples := make([]int64, 0, rounds)
	for i := int64(0); i < rounds; i++ {
		t0 := time.Now()
		sys.Root.Send(pongPID, &pingMsg{i})
		for atomic.LoadInt64(&done) != i {
		}
		if i >= rounds/10 {
			samples = append(samples, time.Since(t0).Nanoseconds())
		}
	}
	report("[3] ping-pong 往返延迟", samples)
}

func benchAsk(sys *actor.ActorSystem, rounds int64) {
	echo := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if m, ok := ctx.Message().(*strMsg); ok {
			ctx.Respond(m)
		}
	}))
	msg := &strMsg{"q"}
	samples := make([]int64, 0, rounds)
	for i := int64(0); i < rounds; i++ {
		t0 := time.Now()
		if _, err := sys.Root.RequestFuture(echo, msg, 5*time.Second).Result(); err != nil {
			panic(err)
		}
		if i >= rounds/10 {
			samples = append(samples, time.Since(t0).Nanoseconds())
		}
	}
	report("[4] Ask 往返延迟", samples)
}

// ---- [6] protoactor 官方 inprocessbenchmark 写法 ----

type batchMsg struct{ sender *actor.PID }
type startMsg struct{ sender *actor.PID }

type pingClient struct {
	echo      *actor.PID
	remaining int64
	batchSize int64
	inFlight  int64
	wg        *sync.WaitGroup
}

func (p *pingClient) sendBatch(ctx actor.Context, echo *actor.PID) bool {
	if p.remaining == 0 {
		return false
	}
	m := &batchMsg{ctx.Self()}
	for i := int64(0); i < p.batchSize; i++ {
		ctx.Send(echo, m)
	}
	p.remaining -= p.batchSize
	p.inFlight = p.batchSize
	return true
}

func (p *pingClient) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *startMsg:
		// 记住 echo：官方 inprocessbenchmark 用回信里的 msg.Sender 发下一批，
		// 而回信的 Sender 就是自己，第一批之后实际变成了自己发给自己
		p.echo = msg.sender
		p.sendBatch(ctx, p.echo)
	case *batchMsg:
		p.inFlight--
		if p.inFlight > 0 {
			return
		}
		if !p.sendBatch(ctx, p.echo) {
			p.wg.Done()
		}
	}
}

func benchInProc(sys *actor.ActorSystem, clients, perClient, batch int64) {
	var wg sync.WaitGroup
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if m, ok := ctx.Message().(*batchMsg); ok {
			ctx.Send(m.sender, m)
		}
	})
	pairs := make([][2]*actor.PID, clients)
	for i := int64(0); i < clients; i++ {
		client := sys.Root.Spawn(actor.PropsFromProducer(func() actor.Actor {
			return &pingClient{remaining: perClient, batchSize: batch, wg: &wg}
		}))
		pairs[i] = [2]*actor.PID{client, sys.Root.Spawn(echoProps)}
	}
	wg.Add(int(clients))
	start := time.Now()
	for _, p := range pairs {
		sys.Root.Send(p[0], &startMsg{p[1]})
	}
	wg.Wait()
	elapsed := time.Since(start)
	total := clients * perClient * 2
	fmt.Printf("[6] 多对 ping-pong：%d 对 × %d 条（批量 %d），共 %d 条，%dms → %d msg/s\n", clients, perClient, batch, total, elapsed.Milliseconds(), perSecond(total, elapsed))
}

// ---- [7] 规模：创建 N 个 Actor，常驻时 ping-pong，广播一条，全部停止 ----
// 与 CActor scale_test / scale_latency、AkkaBench spawn 模式同口径。

type scaleTick struct{}

func boundedWait(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

func benchScale(sys *actor.ActorSystem, n int64) {
	var counter, stopped int64
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch ctx.Message().(type) {
		case *scaleTick:
			atomic.AddInt64(&counter, 1)
		case *actor.Stopped:
			atomic.AddInt64(&stopped, 1)
		}
	})
	pids := make([]*actor.PID, 0, n)
	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)
	t0 := time.Now()
	for i := int64(0); i < n; i++ {
		pids = append(pids, sys.Root.Spawn(props))
	}
	created := time.Since(t0)
	fmt.Printf("[7] 规模 %d Actor：创建 %dms（%d 个/秒）\n", n, created.Milliseconds(), n*int64(time.Second)/int64(created))

	// 大量 Actor 常驻时一对 ping-pong（与空系统 [3] 对照）
	benchPingPong(sys, 5000)

	t1 := time.Now()
	msg := &scaleTick{}
	for _, p := range pids {
		sys.Root.Send(p, msg)
	}
	ok1 := boundedWait(func() bool { return atomic.LoadInt64(&counter) >= n }, 120*time.Second)
	drain := time.Since(t1)
	runtime.GC()
	runtime.ReadMemStats(&ms1)
	t2 := time.Now()
	for _, p := range pids {
		sys.Root.Poison(p)
	}
	ok2 := boundedWait(func() bool { return atomic.LoadInt64(&stopped) >= n }, 120*time.Second)
	stop := time.Since(t2)
	perActor := float64(ms1.HeapAlloc-ms0.HeapAlloc) / float64(n)
	fmt.Printf("[7] 广播 1 条 drain：%dms（完成 %d/%d），停止：%dms（完成 %d/%d）；存活堆增量 %d 字节/Actor（含测试侧 PID 切片）\n",
		drain.Milliseconds(), atomic.LoadInt64(&counter), n, stop.Milliseconds(), atomic.LoadInt64(&stopped), n, int64(perActor))
	if !ok1 || !ok2 {
		fmt.Println("[7] 规模测试：失败（超时）")
	}
}

func argInt(i int, def int64) int64 {
	if len(os.Args) > i {
		if v, err := strconv.ParseInt(os.Args[i], 10, 64); err == nil {
			return v
		}
	}
	return def
}

func main() {
	quiet := func(*actor.ActorSystem) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
	sys := actor.NewActorSystem(actor.WithLoggerFactory(quiet))
	fmt.Printf("=== protoactor-go 基准（GOMAXPROCS=%d）===\n", runtime.GOMAXPROCS(0))
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "all" || mode == "basic" {
		benchSingleActor(sys, 1_000_000)
		benchFanOut(sys, 8, 100, 250_000)
		benchPingPong(sys, 20_000)
		benchAsk(sys, 20_000)
	}
	if mode == "all" || mode == "inproc" {
		benchInProc(sys, argInt(2, 64), argInt(3, 100_000), 100)
	}
	if mode == "spawn" {
		benchScale(sys, argInt(2, 1_000_000))
	}
}
