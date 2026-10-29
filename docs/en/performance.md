# CActor Performance Tuning Guide

This guide covers two things: how to make programs built on CActor run faster, and how to optimize CActor itself.

All numbers come from the benchmark programs in this repository (see §6). They were measured on a 32-core Intel Xeon with Cangjie 1.2.0, so expect different results on other hardware.

The full same-machine comparison with Akka, and how each bottleneck was found, are in the [performance analysis](../plan/20261028-performance.md) (in Chinese).

## Contents

1. [Current performance](#1-current-performance)
2. [Step one: make sure compiler optimization is on](#2-step-one-make-sure-compiler-optimization-is-on)
3. [Runtime parameters](#3-runtime-parameters)
4. [Performance rules for writing actors](#4-performance-rules-for-writing-actors)
5. [Settings that currently have no effect](#5-settings-that-currently-have-no-effect)
6. [How to measure](#6-how-to-measure)
7. [Optimizing CActor itself](#7-optimizing-cactor-itself)

---

## 1. Current performance

| Scenario | CActor | Akka 2.6.21 (same machine, warmed up) |
|---|---|---|
| Single-actor throughput | 5.6–7.2 M msg/s | 2.7–3.4 M msg/s |
| Fan-out (8 senders × 100 actors) | 2.5–4.5 M msg/s | 9.1–11.9 M msg/s |
| Ping-pong round trip P50 / P99 | 1.9–4.0 / 2.7–11 µs | 3.0 / 5.9–6.7 µs |
| Ask round trip P50 / P99 | 5.0–6.0 / 11–13 µs | 8.8 / 10–16 µs |
| Remote TCP (loopback) | 146–194 k msg/s | about 308 k msg/s |

What the table shows:

- **Point-to-point** work matches or beats Akka. That covers one sender to one actor, and request/reply.
- **Fan-out**, from many senders to many actors, is still clearly slower. So is **remoting**. The causes are in §3.4 and §3.5 of the analysis.

If your workload is mostly fan-out or remoting, expect this gap.

## 2. Step one: make sure compiler optimization is on

This is the biggest win, and the easiest one to miss. **cjpm passes no `-O` flag by default, and cjc defaults to `-O0`.** That holds even though the output goes to `target/release`.

The CActor workspace already sets this in `cjpm.toml`:

```toml
compile-option = "-O2 -Woff unused -Woff parser --diagnostic-format=noColor"
```

**Your own project needs it too.** An application that depends on CActor still compiles its own code at `-O0` unless told otherwise:

```toml
[package]          # or [workspace]
compile-option = "-O2"
```

To check that it took effect:

```bash
cjpm build -V 2>&1 | grep -o "cjc .*" | head -3   # every cjc command should contain -O2
```

Measured effect (same code, only the compiler flag changed):

| | -O0 | -O2 |
|---|---|---|
| Remote TCP throughput | 47 k/s | 175 k/s (3.6×) |
| Fan-out throughput | 0.55 M/s | 0.72 M/s |
| Interface call | 11 ns | 0.2 ns (devirtualized and inlined) |
| Non-escaping object allocation | 9.8 ns | eliminated |

## 3. Runtime parameters

All parameters are environment variables, read once when the process starts.

| Variable | Default | Effect |
|---|---|---|
| `CACTOR_DISPATCHER_THREADS` | 1/4 of the CPU cores (4–64) | Number of worker threads that run actors |
| `CACTOR_DISPATCHER_SPIN_US` | `50` | Microseconds an idle worker spins before sleeping |
| `cjProcessorNum` | number of CPU cores | Maximum number of Cangjie threads running at once (at most 2× the cores) |
| `cjHeapSize` | 256MB (64MB if physical memory is under 1GB) | Upper limit of the Cangjie heap |
| `cjStackSize` | 128KB | Stack size of each Cangjie thread |

Example:

```bash
CACTOR_DISPATCHER_THREADS=8 CACTOR_DISPATCHER_SPIN_US=50 ./your_app
```

### 3.1 Worker threads: more is not better

Each actor is pinned to one worker when it is created, in creation order (affinity scheduling). With more workers, each worker holds fewer actors. Workers then go idle, sleep and wake more often, and fan-out gets slower.

Measured on 32 cores with 50µs idle spin. Runs per setting: 4 and 8 ran 4 times each, 16 twice, 32 once.

| `CACTOR_DISPATCHER_THREADS` | Single actor | Fan-out | Ping-pong P99 |
|---|---|---|---|
| 4 | 4.6–7.2 M/s | 2.3–7.7 M/s (very noisy) | 2.2–2.7 µs |
| 8 (default) | 5.3–6.1 M/s | 2.7–4.8 M/s | 3.2–11 µs |
| 16 | 4.7–4.8 M/s | 1.3–1.6 M/s | 2.7–10 µs |
| 32 | 8.1 M/s | 1.4 M/s | 2.6 µs |

What the numbers show:

- With 16 or 32 threads, fan-out consistently drops to 1.3–1.6 M/s.
- Between 4 and 8 there is no consistent winner; the run-to-run variation is larger than the difference.
- Single-actor throughput and ping-pong latency barely depend on the thread count.

Recommendations:

- Start with the default.
- Don't raise the thread count just because the machine has more cores. Change it only when your own load test shows a gain.
- If the same process runs other CPU-heavy Cangjie threads, such as senders or I/O, be even more conservative.

### 3.2 Idle spinning: CPU in exchange for latency

When a worker runs out of work, it spins for `CACTOR_DISPATCHER_SPIN_US` microseconds before it sleeps. Waking from sleep costs about 1.4–2.6 µs, and it makes tail latency noticeably worse.

Measured on 32 cores with 8 workers. The 50µs setting ran twice; the others ran once.

| `CACTOR_DISPATCHER_SPIN_US` | Ping-pong P50 / P99 / P99.9 | Ask P50 | Cost |
|---|---|---|---|
| 0 | 20.9 / 140 / 8418 µs | 27.8 µs | almost no CPU when idle |
| 20 | 2.1 / 2.7 / 12.9 µs | 5.0 µs | up to 20 µs of spinning between messages |
| 50 (default) | 2.0–4.0 / 3.2–11 / 17–27 µs | 5.2–5.3 µs | up to 50 µs |
| 200 | 4.0 / 10.4 / 18.4 µs | 5.4 µs | up to 200 µs |

What the numbers show:

- With spinning off (0), latency gets about 10× worse and P99.9 reaches milliseconds.
- Any spin of 20µs or more gives about the same latency; the differences between 20, 50 and 200 are smaller than the run-to-run noise.
- Throughput barely depends on this setting.

Recommendations:

- **Latency-sensitive services** (trading, games, RPC): keep the default. Raising it to 200 gave no measurable gain.
- **Tight CPU or shared machines** (background jobs, batch work, hosts shared with other services): you can go down to 20. `0` saves more idle CPU, but request/reply latency gets about 10× worse.
- Whatever the setting, a sleeping worker still wakes every 10 ms to check for work. An idle process never uses exactly zero CPU.

### 3.3 Cangjie runtime parameters

- **`cjProcessorNum`:** usually leave it at the default. Setting it below the worker count limits the parallelism of the whole process.
- **`cjHeapSize`:** going from 256MB to 2GB made no measurable difference to the benchmarks above. If large messages or a lot of actor state make GC frequent, raise it. First use `MRT_REPORT=<path>` to write a GC log and confirm that GC is the problem.
- **Other parameters:** see [Cangjie runtime environment variables](https://docs.cangjie-lang.cn/en/docs/1.0.0/user_manual/source_en/Appendix/runtime_env.html).

## 4. Performance rules for writing actors

### 4.1 Never block inside `receive`

`receive` runs on a worker thread that is shared by many actors. Blocking holds that thread, and **every actor on the same worker stops**.

All of the following block:

- `sleep`.
- Synchronous file or network I/O.
- Waiting on `Future.get()`.
- Waiting for a lock or a condition.
- `AskFuture.get()` and `await()`.
- `RemoteActorRef.tell`, which writes to the socket synchronously and blocks when the network is slow.

**A blocking ask to another actor from inside `receive` can stall until the timeout.** If the target happens to be on the same worker, it can't run until the current `receive` returns, and the current `receive` is waiting for it. Measured:

| Case | Result |
|---|---|
| The two actors are on different workers | reply after 68 µs |
| The two actors are on the same worker | waits the full 2 seconds, then fails with a timeout |

Which worker an actor lands on depends on creation order, so you can't count on them being apart. There are two correct patterns.

The first is to register a callback instead of waiting:

```cangjie
askManager.ask(target, request).onComplete({ r =>
    // Don't modify actor state here; the callback may run on another thread.
    // Send the result back to yourself as a message instead.
    selfRef.tell(ResultMessage(r))
})
```

The second is tell plus a reply message. Put your own reference in the request, and have the other actor `tell` a reply back when it's done. This is the natural actor style, and also the cheapest.

If you really need blocking I/O, run it on a separate Cangjie thread with `spawn`, and `tell` the result back when it finishes.

### 4.2 Ask costs much more than tell

| Operation | Cost |
|---|---|
| `tell` | about 0.15 µs per message on the sender side (single-actor case: 1M messages in about 150 ms) |
| Ask round trip | P50 about 5 µs; each call also creates a request ID, a temporary reply reference and a future |

Use ask only at system boundaries, for example when an HTTP request comes in and must wait for a result. Between actors, use tell.

### 4.3 No logging on hot paths

One string interpolation such as `"actor-${id} ..."` costs about 60–90 ns, more than delivering a message, and `println` adds I/O on top. Don't call `println` or build debug strings in high-frequency message handling.

### 4.4 Message design

- **Prefer immutable messages.** They can be reused: the benchmark sends the same `StringMessage` instance one million times.
- **Allocation is cheap, but data copies aren't.** An allocation costs about 7.5 ns, so object pools aren't worth it for messages. Still, don't put large copied data in every message, such as a copy of a whole array.
- **Cross-node messages must be serializable.** Register a codec for custom messages with `MessageCodecRegistry`. Keep the encoding compact, because remoting is currently limited by one `write` per message.

### 4.5 Split hot actors

A single actor processes messages serially. With an empty handler it tops out at about 6–7 M msg/s, and the heavier the handler, the lower the limit. If every request goes to one actor, that actor is the bottleneck:

- Split the state across several actors by key, for example by user ID modulo N.
- For read-mostly data, copy it to several read-only actors.

Splitting turns the workload into fan-out, which is currently slower than in Akka (§1), so load-test to confirm the gain.

### 4.6 Don't create an actor per request

Creating an actor is much heavier than sending a message. It takes several locks, checks for a duplicate name, registers the actor and runs `preStart`. Use long-lived actors for long-lived entities, and plain messages or `spawn` for short tasks.

## 5. Settings that currently have no effect

The following APIs and settings exist in the code, but **they currently don't change how messages are processed**. Don't use them for tuning:

| API / setting | What actually happens |
|---|---|
| `actorOfWithMailbox`, `Props.withMailbox(...)` | Only the name is recorded; every actor uses the same mailbox (a lock-free MPSC queue) |
| `actorOfWithDispatcher`, `Props.withDispatcher(...)` | Only the name is recorded; every actor runs on the same affinity dispatcher |
| The dispatcher and mailbox implementations under `runtime.dispatcher.*` and `runtime.mailbox.*` (WorkStealing, Batching, RingBuffer, etc.) | Not on the main message path |
| Configuration presets such as `HighPerformanceActorSystemConfig` | No effect on the main message path |
| Maximum messages an actor processes per scheduling turn (64) | Hard-coded as `SimpleUserActorRef.THROUGHPUT`; not configurable |

These duplicate implementations will be merged or removed in the deduplication phase (M3 in the [development plan](../plan/20261028.md), in Chinese).

## 6. How to measure

```bash
source ~/.cangjie/1.2.0/cangjie/envsetup.sh
cjpm build
./target/release/bin/cactor_examples.real_benchmark     # CActor end to end
./target/release/bin/cactor_examples.micro_benchmark    # cost of individual Cangjie primitives
bench/akka/run.sh 3                                      # Akka comparison (needs JDK 17+)
bench/compare.sh                                         # runs both and saves results to bench/results/
```

Things to watch:

- **The machine must be idle.** Other load, such as a compile, cuts both sides' numbers sharply. One run at a load average of about 32 dropped remote throughput from about 150k to 56k.
- **Run at least 3 times.** M:N scheduling causes noticeable run-to-run variation; fan-out, for example, ranges from 2.5 to 4.5 M/s. Look at the range, not a single run.
- **Warm up Akka.** Use the last round; the first round is several times slower.
- **Measure your own workload.** The benchmark actors do almost nothing. In real applications the handler is usually heavier than message delivery, so the bottleneck is often in your own code.
- When tuning, change one parameter at a time and measure again after each change.

See [bench/README.md](../../bench/README.md) (in Chinese) for more.

## 7. Optimizing CActor itself

### 7.1 Workflow

1. **Measure before you change anything.** Use `real_benchmark` to find the slow scenario.
2. **Find the cause with a micro-benchmark.** Add a small case to `micro_benchmark` that tests only the suspect primitive, such as one queue or one wakeup. Every problem found in the last round was found this way:
   - `ConcurrentLinkedQueue` costs 190 ns per add+remove pair, against 36 ns for a hand-written MPSC queue.
   - The global ready queue collapses under contention when there are many threads.
   - Spinning blocks a freshly woken thread.
3. **Make the change, then run the full test suite.** `cjpm test` must pass completely, especially the concurrency-semantics tests under `unit/runtime/concurrency`.
4. **Run the benchmark again, at least 3 times,** and compare against Akka. Record the conclusions and numbers in the [performance analysis](../plan/20261028-performance.md).

### 7.2 Hot-path rules

The main message path is in `modules/runtime/src/guardian/runtime_user_guardian.cj`: `tell` → `CellMailbox` → `ActorCellDispatcher` → `run` → `invoke`.

- **Allocations per message are the main cost.** Each message currently allocates:
  - the mailbox node and the atomic reference inside it;
  - a `RunNode` and its atomic reference when the actor is scheduled;
  - a `SimpleUserActorContext`.

  In Cangjie, atomics are separate heap objects. They can't be embedded in a node as fields the way they can on the JVM, so every extra atomic field means one more allocation.
- **Don't use the standard concurrent collections on hot paths.** `ConcurrentLinkedQueue` costs about 190 ns per add+remove pair, and `LinkedBlockingQueue` about 300 ns.
- **Read before you CAS.** When the flag is already set, the read costs 2 ns. A CAS costs 9 ns and bounces the cache line between cores.
- **Bound spinning by time, not by iteration count.** With a count, the spin gets shorter as the code gets faster, and tail latency quietly gets worse.
- **Don't spin in place waiting for another thread inside a lock-free algorithm.** Under M:N scheduling, the thread you are waiting for may be queued on the processor you are spinning on. Return and retry later, or yield.
- **Before a thread sleeps, think about who will wake it.** A Cangjie thread woken by `notify` may have to wait until the waker's processor is free. Having the waiting side spin briefly first, as `Completion.await` does, avoids this delay.
- **No `println` or string building on hot paths.**

### 7.3 Optimizations not done yet

| Direction | Expected benefit |
|---|---|
| Outbound write coalescing for remoting; batched multi-frame parsing on receive | Remote throughput on par with Akka |
| Fewer allocations per message: reuse ready-queue nodes, or use the cell itself as an intrusive node | Fan-out throughput |
| Work stealing on top of affinity scheduling | Throughput under uneven load |
| A separate dispatcher for blocking work (blocking I/O, FFI) | Blocking no longer slows ordinary actors (§4.1) |
| Evaluate `--lto=thin` and PGO (`--pgo-instr-gen` / `--pgo-instr-use`) | Cross-package inlining, devirtualization of hot calls |
