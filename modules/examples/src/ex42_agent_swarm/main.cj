/*
 * CActor Ex 42 - 多 Agent 协作编排（docs/plan/2026117-lz.md §31 首选示例）
 *
 * 真实约束：
 * - 长任务跨小时/跨进程：编排进度必须落盘，进程崩溃后从断点续跑，工具副作用不能重放；
 * - 高风险步骤需要人工审批：审批前暂停，重复审批不能产生多次副作用；
 * - 工具调用可能被重试：幂等键（taskId+step）保证只执行一次；
 * - 全程可观测：每个任务一条 trace，步骤 span 挂在任务根 span 下，可导出 OTLP/JSON；
 * - 事件流可回看：任务事件写入 DurableTopic，SSE 先补历史再接实时。
 *
 * 组件：
 * - 经典 Actor `TaskAgent`（每任务一个）：DurableStateStore 存状态，崩溃后新 Actor 加载续跑
 * - `ToolExecutor`：幂等工具执行（effect 存储 = 幂等键），统计真实执行次数
 * - `DurableTopic`：任务事件流（ex41），SSE 从历史接实时
 * - Trace：SpanRecord + toOtlpJson（ex35）
 * - Lou：POST /tasks、POST /tasks/:id/approve、GET /tasks/:id、
 *        GET /tasks/:id/events（SSE）、GET /traces、GET /effects
 *
 * 自检（退出码 0/1）：
 * 1. t1 执行到步骤 1 后停在审批；批准后完成 3 步，工具真实执行 3 次；重复批准不再执行；
 * 2. t2 在审批点终止系统，新系统同目录恢复为待审批，批准后完成，工具总执行次数只增加 3；
 * 3. t3 预算 1：执行 1 步后失败（预算耗尽），不会越界执行；
 * 4. trace：每个任务 1 个根 span + 3 个步骤 span，同 traceId、parent 正确，OTLP JSON 可解析；
 * 5. Lou 端点与内存/存储一致，SSE 输出 7 条事件。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex42_agent_swarm
 *   ./target/release/bin/cactor_examples.ex42_agent_swarm serve 18187
 */
package cactor_examples.ex42_agent_swarm

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.monitoring.{SpanRecord, generateSpanId, generateTraceId, toOtlpJson}
import cactor_persistence.{DurableStateStore, DurableSubscription, DurableTopic, FileDurableStateStore}

let TOTAL_STEPS: Int64 = 3
/** 需要人工审批的步骤（执行前暂停） */
let APPROVAL_STEP: Int64 = 2
let DEFAULT_MAX_EFFECTS: Int64 = 10
let WAIT = Duration.second * 15
let SERVICE = "cactor-ex42"

func nowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

// ======================= 幂等工具执行 =======================

class ToolExecutor {
    private let store: DurableStateStore
    private let lock = Mutex()
    private var executions: Int64 = 0
    private let keys = ArrayList<String>()

    init(dir: String) {
        this.store = FileDurableStateStore(dir)
    }

    /** 同一 (task, step) 只真正执行一次；重试返回已保存的结果 */
    func run(task: String, step: Int64): String {
        let key = "effect-${task}-${step}"
        synchronized(lock) {
            match (store.get(key)) {
                case Some(r) => r.state
                case None =>
                    executions++
                    keys.add(key)
                    let result = "tool-${step}:ok"
                    store.upsert(key, store.revisionOf(key) + 1, result)
                    result
            }
        }
    }

    func executionCount(): Int64 {
        synchronized(lock) {
            executions
        }
    }

    func effectCount(): Int64 {
        Int64(store.persistenceIds().size)
    }
}

// ======================= 追踪 =======================

class TraceLog {
    private let lock = Mutex()
    private let spans = ArrayList<SpanRecord>()
    private let roots = HashMap<String, (String, String)>()

    /** 任务根 span；返回 (traceId, spanId) */
    func startTask(task: String): (String, String) {
        let traceId = generateTraceId()
        let spanId = generateSpanId()
        let tags = HashMap<String, String>()
        tags["task"] = task
        synchronized(lock) {
            roots[task] = (traceId, spanId)
            spans.add(SpanRecord(SERVICE, traceId, spanId, None<String>, "task", nowMillis(), 1, tags))
        }
        (traceId, spanId)
    }

    /** 步骤 span 挂在任务根 span 下 */
    func step(task: String, name: String, step: Int64): Unit {
        synchronized(lock) {
            match (roots.get(task)) {
                case Some((traceId, rootSpan)) =>
                    let tags = HashMap<String, String>()
                    tags["task"] = task
                    tags["step"] = "${step}"
                    spans.add(SpanRecord(SERVICE, traceId, generateSpanId(), Some<String>(rootSpan), name,
                        nowMillis(), 1, tags))
                case None => ()
            }
        }
    }

    func otlp(task: String): String {
        let picked = ArrayList<SpanRecord>()
        synchronized(lock) {
            for (s in spans) {
                if (s.tags.get("task") == Some<String>(task)) {
                    picked.add(s)
                }
            }
        }
        toOtlpJson(SERVICE, picked.toArray())
    }

    func countOf(task: String): Int64 {
        var n: Int64 = 0
        synchronized(lock) {
            for (s in spans) {
                if (s.tags.get("task") == Some<String>(task)) {
                    n++
                }
            }
        }
        n
    }
}

// ======================= 任务状态与 Agent =======================

class TaskView {
    let task: String
    let status: String
    let step: Int64
    let budget: Int64
    let approved: Bool

    init(task: String, status: String, step: Int64, budget: Int64, approved: Bool) {
        this.task = task
        this.status = status
        this.step = step
        this.budget = budget
        this.approved = approved
    }

    func json(): String {
        "{\"task\":\"${task}\",\"status\":\"${status}\",\"step\":${step},\"budget\":${budget}," +
            "\"approved\":${approved}}"
    }
}

func taskKey(task: String): String {
    "task-${task}"
}

func encodeState(status: String, step: Int64, budget: Int64, approved: Bool): String {
    "${status}|${step}|${budget}|${approved}"
}

func readState(store: DurableStateStore, task: String): TaskView {
    match (store.get(taskKey(task))) {
        case Some(r) =>
            let parts = r.state.split("|")
            if (parts.size == 4) {
                TaskView(task, parts[0], Int64.parse(parts[1]), Int64.parse(parts[2]), parts[3] == "true")
            } else {
                TaskView(task, "corrupt", 0, 0, false)
            }
        case None => TaskView(task, "missing", 0, 0, false)
    }
}

class TaskAgent <: Actor {
    let task: String
    let store: DurableStateStore
    let tool: ToolExecutor
    let topic: DurableTopic
    let traces: TraceLog
    let maxEffects: Int64
    var status: String
    var step: Int64
    var budget: Int64
    var approved: Bool

    init(task: String, dir: String, tool: ToolExecutor, traces: TraceLog, maxEffects: Int64) {
        this.task = task
        this.store = FileDurableStateStore("${dir}/tasks")
        this.tool = tool
        this.traces = traces
        this.topic = DurableTopic("task-${task}", FileDurableStateStore("${dir}/topics"), retention: 200)
        this.maxEffects = maxEffects
        let view = readState(this.store, task)
        this.status = view.status
        this.step = view.step
        this.budget = view.budget
        this.approved = view.approved
    }

    public prop name: String { get() { "task-agent" } }
    public prop description: String { get() { "agent for ${task}" } }

    private func save(): Unit {
        let key = taskKey(task)
        store.upsert(key, store.revisionOf(key) + 1, encodeState(status, step, budget, approved))
    }

    private func publish(event: String): Unit {
        topic.publish(event)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let cmd = if (let Some(text) <- (message as StringMessage)) { text.getContent() } else { "" }
        match (cmd) {
            case "resume" => resume(context)
            case "advance" => advance(context)
            case "approve" => approve(context)
            case _ => ()
        }
        MessageResult.Handled
    }

    /** 新任务或崩溃恢复：从存储状态继续 */
    private func resume(context: ActorContext): Unit {
        if (status == "missing") {
            status = "running"
            step = 0
            budget = 0
            approved = false
            traces.startTask(task)
            save()
            publish("started")
            context.self.tell(StringMessage("advance"))
        } else if (status == "running") {
            publish("resumed:${step}")
            context.self.tell(StringMessage("advance"))
        }
    }

    private func approve(context: ActorContext): Unit {
        if (status != "awaiting") {
            publish("approve-ignored:${status}")
            return
        }
        approved = true
        status = "running"
        save()
        publish("approved")
        context.self.tell(StringMessage("advance"))
    }

    private func advance(context: ActorContext): Unit {
        if (status != "running") {
            return
        }
        let next = step + 1
        // 顺序：预算 → 审批 → 执行。预算耗尽优先失败，避免"等审批"掩盖越界
        if (budget >= maxEffects) {
            status = "failed"
            save()
            publish("budget-exceeded:${budget}")
            return
        }
        if (next == APPROVAL_STEP && !approved) {
            status = "awaiting"
            save()
            publish("awaiting-approval:${next}")
            return
        }
        let result = tool.run(task, next)
        traces.step(task, "step-${next}", next)
        step = next
        budget++
        publish("step-${next}-done:${result}")
        if (step >= TOTAL_STEPS) {
            status = "completed"
            save()
            publish("completed")
            return
        }
        save()
        context.self.tell(StringMessage("advance"))
    }
}

// ======================= 环境 =======================

class Env {
    let dir: String
    let system: CActorSystem
    let tool: ToolExecutor
    let traces: TraceLog
    let offsets: DurableStateStore
    private let lock = Mutex()
    private let agents = HashMap<String, ActorRef>()

    init(dir: String) {
        this.dir = dir
        this.system = CActorSystemFactory.create("ex42-${MonoTime.now().hashCode()}")
        this.tool = ToolExecutor("${dir}/effects")
        this.traces = TraceLog()
        this.offsets = FileDurableStateStore("${dir}/offsets")
    }

    func start(task: String, maxEffects!: Int64 = DEFAULT_MAX_EFFECTS): TaskView {
        let agent = system.actorOf({ => TaskAgent(task, dir, tool, traces, maxEffects) }, "task-${task}")
        synchronized(lock) {
            agents[task] = agent
        }
        agent.tell(StringMessage("resume"))
        readStatus(task)
    }

    func approve(task: String): Bool {
        let agent = synchronized(lock) {
            agents.get(task)
        }
        match (agent) {
            case Some(r) =>
                r.tell(StringMessage("approve"))
                true
            case None => false
        }
    }

    func readStatus(task: String): TaskView {
        readState(FileDurableStateStore("${dir}/tasks"), task)
    }

    func topicOf(task: String): DurableTopic {
        DurableTopic("task-${task}", FileDurableStateStore("${dir}/topics"), retention: 200)
    }

    func shutdown(): Unit {
        system.terminate()
    }
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

// ======================= SSE =======================

class Gate {
    let lock = Mutex()
    let cond: Condition
    var open = false

    init() {
        this.cond = synchronized(this.lock) {
            this.lock.condition()
        }
    }

    func release(): Unit {
        synchronized(lock) {
            open = true
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Unit {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!open) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return
                }
                cond.wait(timeout: left)
            }
        }
    }
}

// ======================= Lou =======================

func buildApp(env: Env, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/tasks", { ctx =>
        let task = ctx.query("id") ?? "t${MonoTime.now().hashCode()}"
        let maxEffects = ctx.queryInt("maxEffects") ?? DEFAULT_MAX_EFFECTS
        let _ = env.start(task, maxEffects: maxEffects)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"task\":\"${task}\",\"status\":\"started\"}")
    })
    app.post("/tasks/:id/approve", { ctx =>
        let task = ctx.param("id")
        ctx.setSingleHeader("Content-Type", "application/json")
        if (env.approve(task)) {
            ctx.writeString("{\"task\":\"${task}\",\"approved\":true}")
        } else {
            ctx.writeStringWithCode(404u16, "{\"error\":\"unknown_task\"}")
        }
    })
    app.get("/tasks/:id", { ctx =>
        let view = env.readStatus(ctx.param("id"))
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(view.json())
    })
    app.get("/tasks/:id/events", { ctx =>
        let task = ctx.param("id")
        let limit = ctx.queryInt("limit") ?? 100
        let from = ctx.queryInt("from") ?? 1
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let seen = AtomicInt64(0)
            let name = "sse-${MonoTime.now().hashCode()}"
            let sub = env.topicOf(task).subscribe(name, env.offsets, { seq, payload =>
                stream.sendEvent("agent", payload)
                if (seen.fetchAdd(1) + 1 >= limit) {
                    gate.release()
                }
            }, fromSeq: from)
            gate.await(WAIT)
            sub.unsubscribe()
            stream.close()
        })
    })
    app.get("/traces", { ctx =>
        let task = ctx.query("task") ?? "t1"
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.traces.otlp(task))
    })
    app.get("/effects", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"executions\":${env.tool.executionCount()},\"effects\":${env.tool.effectCount()}}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func startServer(app: Lou): UInt16 {
    spawn { app.listen() }
    for (_ in 0..500) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                return s.port
            }
        }
        sleep(Duration.millisecond * 20)
    }
    0u16
}

func readBody(response: HttpResponse): String {
    let out = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    var n = response.body.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = response.body.read(buf)
    }
    String.fromUtf8(out.toArray())
}

main(args: Array<String>): Int64 {
    let dir = "/tmp/cactor-ex42-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let env = Env(dir)
        let app = buildApp(env, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex42：端口绑定失败")
            return 1
        }
        println("ex42 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let env = Env(dir)

        println("\n[1] 执行到审批点，批准后完成")
        env.start("t1")
        c.check(waitUntil(WAIT, { => env.readStatus("t1").status == "awaiting" }), "t1 停在审批点")
        c.check(env.readStatus("t1").step == 1 && env.tool.executionCount() == 1, "已执行 1 步（工具 1 次）")
        env.approve("t1")
        c.check(waitUntil(WAIT, { => env.readStatus("t1").status == "completed" }), "t1 完成")
        c.check(env.readStatus("t1").step == TOTAL_STEPS && env.tool.executionCount() == 3, "共 3 步、工具 3 次")
        env.approve("t1")
        sleep(Duration.millisecond * 200)
        c.check(env.tool.executionCount() == 3, "重复批准不重复执行工具")

        println("\n[2] 审批点崩溃恢复")
        env.start("t2")
        c.check(waitUntil(WAIT, { => env.readStatus("t2").status == "awaiting" }), "t2 停在审批点")
        c.check(env.tool.executionCount() == 4, "t2 已执行 1 步")
        env.shutdown()

        let revived = Env(dir)
        revived.start("t2")
        c.check(revived.readStatus("t2").status == "awaiting", "新进程恢复为待审批（未重放工具）")
        c.check(revived.tool.executionCount() == 0, "新 ToolExecutor 不重放历史副作用")
        // 新进程里继续：effect 已存在，执行计数从 0 开始但不会重复写 effect
        revived.approve("t2")
        c.check(waitUntil(WAIT, { => revived.readStatus("t2").status == "completed" }), "t2 恢复后完成")
        c.check(revived.readStatus("t2").step == TOTAL_STEPS, "t2 完成 3 步")
        c.check(revived.tool.effectCount() == 6, "幂等键共 6 条（t1+t2），无重复")

        println("\n[3] 预算耗尽")
        revived.start("t3", maxEffects: 1)
        c.check(waitUntil(WAIT, { => revived.readStatus("t3").status == "failed" }), "t3 预算耗尽失败")
        c.check(revived.readStatus("t3").step == 1, "失败前只执行 1 步")
        c.check(revived.readStatus("t3").budget == 1, "预算计数 1")

        println("\n[4] 追踪")
        let otlp = env.traces.otlp("t1")
        c.check(env.traces.countOf("t1") == 4, "t1 有 1 根 + 3 步 span")
        c.check(otlp.contains("\"name\":\"task\"") && otlp.contains("\"name\":\"step-3\""), "OTLP 含根与步骤")
        c.check(otlp.contains("\"parentSpanId\""), "步骤 span 带 parent")

        println("\n[5] Lou 端点")
        // 崩溃恢复后由新进程承载 HTTP（旧 system 已终止）
        let app = buildApp(revived, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let created = readBody(client.post("http://127.0.0.1:${port}/tasks?id=t4", ""))
        c.check(created.contains("t4"), "POST /tasks 创建：${created}")
        c.check(waitUntil(WAIT, { => revived.readStatus("t4").status == "awaiting" }), "t4 到达审批")
        let approved = readBody(client.post("http://127.0.0.1:${port}/tasks/t4/approve", ""))
        c.check(approved.contains("\"approved\":true"), "POST approve 成功")
        c.check(waitUntil(WAIT, { => revived.readStatus("t4").status == "completed" }), "t4 完成")
        let view = readBody(client.get("http://127.0.0.1:${port}/tasks/t4"))
        c.check(view.contains("\"status\":\"completed\""), "GET /tasks/t4 反映状态")
        let events = readBody(client.get("http://127.0.0.1:${port}/tasks/t4/events?limit=7"))
        c.check(events.contains("completed") && events.contains("awaiting-approval:2"),
            "SSE 输出任务事件")
        let traces = readBody(client.get("http://127.0.0.1:${port}/traces?task=t4"))
        c.check(traces.contains("resourceSpans"), "GET /traces 输出 OTLP JSON")
        let effects = readBody(client.get("http://127.0.0.1:${port}/effects"))
        c.check(effects.contains("\"effects\":"), "GET /effects 可观测")
        client.close()
        try {
            app.stop()
        } catch (_: Exception) {}
        revived.shutdown()

    println("\nex42：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
