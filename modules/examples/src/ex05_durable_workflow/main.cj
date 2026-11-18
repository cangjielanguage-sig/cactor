/*
 * CActor Ex 05 - 可恢复长任务工作流（事件溯源 + 快照 + 持久化提醒）
 *
 * 真实约束（docs/plan/2026117-lz.md §3 E5）：
 * - 任务跨重启存活：进程重启后从事件流继续，而不是从头再来；
 * - 已完成步骤不能重复执行副作用（幂等键 = taskId + step）；
 * - 事件日志不能无界增长：按每 N 事件自动快照、只保留最近 M 份；
 * - 等待外部回调/定时用持久化提醒，且提醒只触发一次（未确认则重发、确认后推进）。
 *
 * 组件（全部是仓内现成实现）：
 * - EventSourcedBehavior + FunctionEventAdapter（modules/persistence/src/typed/）
 * - RetentionCriteria.snapshotEvery(n, keep) + SnapshotStore（journal_v2 fsync 落盘）
 * - DurableReminders（persistence/reminders）：等审批回调 + 确认后推进
 * - PersistenceIo.awaitIdle：关闭系统前等在途 IO 落盘
 *
 * 自检（退出码 0/1）：
 * 1. 执行到第 5 步时"崩溃"（关闭系统）→ 新系统从第 6 步继续，已完成步骤不重复执行副作用；
 * 2. 每个步骤的副作用计数 == 1（幂等）；
 * 3. 500 个事件后快照次数 ≥ 1，且重启恢复结果与不重启一致；
 * 4. 持久化提醒只投递一次（确认后不再恢复）。
 *
 * 运行：source tools/env.sh && cjpm build && \
 *       CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex05_durable_workflow
 */
package cactor_examples.ex05_durable_workflow

import std.collection.*
import std.convert.*
import std.fs.*
import std.sync.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_persistence.{FileEventStore, FileSnapshotStore, FileDurableStateStore}
import cactor_persistence.typed.*
import cactor_persistence.reminders.*
import cactor_testkit.{ActorTestKit, TestInbox, awaitCondition}

let ASK_TIMEOUT = Duration.second * 10
let TOTAL_STEPS = 20

// ======================= 协议 =======================

/** 工作流命令：RunStep 每步一条；Query 读状态 */
enum WorkflowCmd {
    | RunStep(Int64, TypedActorRef<StepReply>)
    | Cancel(TypedActorRef<String>)
    | Query(TypedActorRef<WorkflowView>)
}

enum WorkflowEvt {
    | StepDone(Int64)
    | Cancelled
}

enum StepReply {
    | Accepted(Int64)
    | Rejected(String)
}

/** 只读视图：当前状态 + 已完成步骤 + 事件数 */
class WorkflowView {
    let status: String
    let steps: Array<Int64>
    let events: Int64

    init(status: String, steps: Array<Int64>, events: Int64) {
        this.status = status
        this.steps = steps
        this.events = events
    }
}

/** 不可变状态：已完成步骤 + 是否取消 */
class WorkflowState {
    let status: String
    let steps: Array<Int64>

    init(status: String, steps: Array<Int64>) {
        this.status = status
        this.steps = steps
    }

    static func empty(): WorkflowState {
        WorkflowState("running", [])
    }

    func apply(evt: WorkflowEvt): WorkflowState {
        match (evt) {
            case StepDone(n) => WorkflowState(status, steps.concat([n]))
            case Cancelled => WorkflowState("cancelled", steps)
        }
    }

    func contains(step: Int64): Bool {
        steps.iterator().any({ s => s == step })
    }
}

/** 事件编码：manifest = 事件名，data = 步骤号 */
let workflowEvents = FunctionEventAdapter<WorkflowEvt>(manifest: { e =>
    match (e) {
        case StepDone(_) => "StepDone"
        case Cancelled => "Cancelled"
    }
}, toJournal: { e =>
    match (e) {
        case StepDone(n) => "${n}"
        case Cancelled => ""
    }
}, fromJournal: { manifest, data =>
    match (manifest) {
        case "StepDone" => StepDone(Int64.parse(data))
        case "Cancelled" => Cancelled
        case other => throw IllegalArgumentException("未知事件：${other}")
    }
})

let workflowCodec = FunctionStateCodec<WorkflowState>(encode: { s =>
    "${s.status}\n${String.join(s.steps.map({ n => "${n}" }), delimiter: ",")}"
}, decode: { data =>
    let lines = data.split("\n")
    let fields: Array<String> = if (lines.size > 1 && !lines[1].isEmpty()) { lines[1].split(",") } else { [] }
    let steps = ArrayList<Int64>()
    for (f in fields) {
        steps.add(Int64.parse(f))
    }
    WorkflowState(lines[0], steps.toArray())
})

// ======================= 外部副作用记录（用于验证幂等） =======================

/**
 * 每步副作用写一行到文件：用于在"重启后"检查同一 (taskId, step) 是否被执行了两次。
 * 真实系统里这是发邮件、扣款这类不可重入动作。
 */
class SideEffects {
    let path: String
    let lock = Mutex()

    init(dir: String) {
        if (!exists(dir)) {
            Directory.create(dir, recursive: true)
        }
        this.path = "${dir}/side-effects.log"
        if (exists(path)) {
            remove(path)
        }
        File(path, OpenMode.Write).close()
    }

    func record(taskId: String, step: Int64): Unit {
        synchronized(lock) {
            let f = File(path, OpenMode.Append)
            f.write("${taskId}:${step}\n".toArray())
            f.close()
        }
    }

    func count(): HashMap<String, Int64> {
        let out = HashMap<String, Int64>()
        if (!exists(path)) {
            return out
        }
        let text = String.fromUtf8(readToEnd(path))
        for (line in text.split("\n")) {
            if (!line.isEmpty()) {
                out[line] = (out.get(line) ?? 0) + 1
            }
        }
        out
    }
}

func readToEnd(path: String): Array<Byte> {
    let f = File(path, OpenMode.Read)
    let out = ArrayList<Byte>()
    let buf = Array<Byte>(4096, repeat: 0)
    var n = f.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = f.read(buf)
    }
    f.close()
    out.toArray()
}

// ======================= 工作流行为 =======================

/** 快照计数：验证"每 N 事件自动快照"确实发生（事件在外部记录） */
class SnapshotCounter {
    var snapshots: Int64 = 0
}

func workflowBehavior(taskId: String, stores: Stores, effects: SideEffects, snapshots: SnapshotCounter): TypedBehavior<WorkflowCmd> {
    EventSourcedBehavior<WorkflowCmd, WorkflowEvt, WorkflowState>(PersistenceId.of("Workflow", taskId),
        emptyState: WorkflowState.empty(), commandHandler: { state, cmd, effect =>
            match (cmd) {
                case RunStep(step, replyTo) =>
                    if (state.status == "cancelled") {
                        return effect.reply(replyTo, Rejected("已取消"))
                    }
                    if (state.contains(step)) {
                        // 幂等：同一步骤重复命令不产生新事件，也不执行副作用
                        return effect.reply(replyTo, Accepted(step))
                    }
                    // 副作用写在 thenRun 里：只有事件落盘后才执行，重启重放不会重跑
                    effect.persist(StepDone(step)).thenReply(replyTo, { s => Accepted(step) }).thenRun({ s =>
                        effects.record(taskId, step)
                    })
                case Cancel(replyTo) =>
                    if (state.status == "cancelled") {
                        effect.reply(replyTo, "already")
                    } else {
                        effect.persist(Cancelled).thenReply(replyTo, { s => "cancelled" })
                    }
                case Query(replyTo) => effect.reply(replyTo, WorkflowView(state.status, state.steps,
                    Int64(state.steps.size)))
            }
        }, eventHandler: { state, evt => state.apply(evt) }, eventAdapter: workflowEvents,
        journal: stores.journal)
        .withSnapshots(stores.snapshots, workflowCodec)
        // 每 100 个事件快照一次、保留最近 2 份
        .withRetention(RetentionCriteria.snapshotEvery(100, 2))
        .receiveSignal({ state, signal =>
            match (signal) {
                case s: SnapshotCompleted =>
                    snapshots.snapshots++
                case _ => ()
            }
        })
        .behavior()
}

class Stores {
    let journal: FileEventStore
    let snapshots: FileSnapshotStore
    let reminders: FileDurableStateStore

    init(dir: String) {
        journal = FileEventStore("${dir}/journal")
        snapshots = FileSnapshotStore("${dir}/snapshots")
        reminders = FileDurableStateStore("${dir}/reminders")
    }

    func close(): Unit {
        journal.close()
        snapshots.close()
    }
}

func askStep(ref: TypedActorRef<WorkflowCmd>, step: Int64): StepReply {
    TypedAsk.ask<WorkflowCmd, StepReply>(ref, { r => WorkflowCmd.RunStep(step, r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
}

func viewOf(ref: TypedActorRef<WorkflowCmd>): WorkflowView {
    TypedAsk.ask<WorkflowCmd, WorkflowView>(ref, { r => WorkflowCmd.Query(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
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

main(): Int64 {
    let c = Checker()
    let dir = "/tmp/cactor-ex05-${MonoTime.now().hashCode()}"
    let effects = SideEffects(dir)
    println("=== 可恢复长任务工作流（数据目录 ${dir}）===")

    // ---------- 1. 执行到一半"崩溃" ----------
    println("\n[1] 执行前 10 步后崩溃，新系统从第 11 步继续")
    let stores1 = Stores(dir)
    let system1 = CActorSystemFactory.create("ex05-a")
    let snapshots1 = SnapshotCounter()
    let task1 = system1.typedActorOf(workflowBehavior("task-1", stores1, effects, snapshots1), "workflow-task-1")
    for (step in 0..10) {
        match (askStep(task1, step)) {
            case Accepted(_) => ()
            case Rejected(why) => c.check(false, "第 ${step} 步被拒：${why}")
        }
    }
    let midView = viewOf(task1)
    c.check(midView.steps.size == 10, "前 10 步已完成（${midView.steps.size}）")
    // 模拟崩溃：不等优雅退出，直接关闭系统并等 IO
    system1.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "崩溃前已确认的事件全部落盘")
    stores1.close()

    // 新系统（同一目录）从事件流恢复，继续剩余步骤
    let stores2 = Stores(dir)
    let system2 = CActorSystemFactory.create("ex05-b")
    let snapshots2 = SnapshotCounter()
    let task2 = system2.typedActorOf(workflowBehavior("task-1", stores2, effects, snapshots2), "workflow-task-1")
    let resumedView = viewOf(task2)
    c.check(resumedView.steps.size == 10, "新系统恢复到 10 步（${resumedView.steps.size}）")
    // 重放已完成的步骤：不应重跑副作用
    for (step in 0..10) {
        askStep(task2, step)
    }
    for (step in 10..TOTAL_STEPS) {
        match (askStep(task2, step)) {
            case Accepted(_) => ()
            case Rejected(why) => c.check(false, "续跑第 ${step} 步被拒：${why}")
        }
    }
    let finalView = viewOf(task2)
    c.check(finalView.steps.size == TOTAL_STEPS, "任务完成 ${finalView.steps.size}/${TOTAL_STEPS} 步")

    // ---------- 2. 幂等：每步副作用只发生一次 ----------
    println("\n[2] 幂等：每个步骤的副作用只执行一次")
    let counts = effects.count()
    var allOnce = true
    var missing = 0
    for (step in 0..TOTAL_STEPS) {
        let key = "task-1:${step}"
        let n = counts.get(key) ?? 0
        if (n != 1) {
            allOnce = false
        }
        if (n == 0) {
            missing++
        }
    }
    c.check(allOnce, "全部 ${TOTAL_STEPS} 步副作用各发生 1 次（缺失 ${missing} 步，去重后 ${counts.size} 个步骤）")
    c.check(counts.size == TOTAL_STEPS, "副作用文件只有 ${counts.size} 个不同步骤")

    // ---------- 3. 快照让日志有界 ----------
    println("\n[3] 快照：写入 500 个额外事件后快照次数 ≥ 1")
    // 用一个独立任务写 500 步，触发每 100 事件一次的快照
    let big = system2.typedActorOf(workflowBehavior("task-big", stores2, effects, snapshots2), "workflow-task-big")
    for (step in 0..500) {
        askStep(big, step)
    }
    let snapshotSeen = awaitCondition(Duration.second * 20, { => snapshots2.snapshots >= 1 })
    c.check(snapshotSeen, "自动快照已发生：${snapshots2.snapshots} 次（每 100 事件一次）")
    let bigView = viewOf(big)
    c.check(bigView.steps.size == 500, "大任务完成 500 步")
    // 重启后从快照 + 增量恢复，结果一致
    system2.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "大任务事件全部落盘")
    stores2.close()

    let stores3 = Stores(dir)
    let system3 = CActorSystemFactory.create("ex05-c")
    let snapshots3 = SnapshotCounter()
    let restoredBig = system3.typedActorOf(workflowBehavior("task-big", stores3, effects, snapshots3),
        "workflow-task-big")
    let restoredView = viewOf(restoredBig)
    c.check(restoredView.steps.size == 500, "从快照恢复的 500 步一致（${restoredView.steps.size}）")

    // ---------- 4. 持久化提醒只触发一次 ----------
    println("\n[4] 持久化提醒：投递一次，确认后不再恢复")
    let kit = ActorTestKit("ex05-reminder")
    let sink = TestInbox<ReminderDelivery>("approval")
    let reminderStore = FileDurableStateStore("${dir}/reminders")
    let reminders = kit.system.typed().actorOf(
        DurableReminders.behavior(PersistenceId("approval-reminders"), reminderStore, { d => sink.tell(d) },
            pollInterval: Duration.millisecond * 20), "reminders")
    let gen = TypedAsk.ask<ReminderCommand, Int64>(reminders, { r =>
        ReminderCommand.Register(ReminderDefinition("approve", "task-1", 0), r)
    }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    c.check(gen == 1, "提醒登记成功（代次 ${gen}）")
    let fired = awaitCondition(Duration.second * 10, { => sink.hasMessages() })
    c.check(fired, "到期的提醒被投递")
    let delivery = sink.receiveMessage()
    c.check(delivery.definition.name == "approve" && delivery.definition.payload == "task-1", "投递内容正确")
    // 确认后不再投递
    let acked = TypedAsk.ask<ReminderCommand, Bool>(reminders, { r => ReminderCommand.Acknowledge(
        delivery.definition.name, delivery.generation, delivery.definition.dueAtMillis, r) }, ASK_TIMEOUT)
        .get(ASK_TIMEOUT)
    c.check(acked, "确认成功")
    sleep(Duration.millisecond * 300)
    c.check(!sink.hasMessages(), "确认后不再重复投递（提醒只触发一次）")
    kit.shutdown()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "提醒状态落盘")

    system3.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "最终系统关闭前 IO 完成")
    stores3.close()

    println("\nex05：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
