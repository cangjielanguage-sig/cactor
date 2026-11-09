/*
 * CActor Persistence.Typed - 持久化 IO 线程池（对标 Akka 的 akka.persistence.dispatchers.default-plugin-dispatcher）
 *
 * 为什么不在 Actor 线程里直接写日志：fsync 每次约 1 ms（本机实测），默认调度器的工作线程与 CPU 核数相当，
 * 一个线程被占住，同线程上的其他 Actor 都要等；而且持久化期间 Actor 什么都做不了（连停止请求都要排队）。
 * 这里用一组独立的仓颉线程执行存储读写，结果以信号送回 Actor（ctx.pipeSignalToSelf），Actor 线程只做内存操作。
 *
 * 线程数：CACTOR_PERSISTENCE_IO_THREADS（默认 8，最少 1）。多个实体同时写同一个文件日志目录时，
 * 这些线程并发 append，FileEventStore 的组提交把它们的 fsync 合并（同一日志文件内）。
 * 同一个实体同一时刻最多有一个写入在途（写入期间新命令暂存），因此同一持久化标识的写入天然有序。
 */
package cactor_persistence.typed

import std.env.*
import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor_core.typed.*
import cactor_persistence.{PersistenceException}

/** 持久化 IO 线程池（全进程共享，第一次提交任务时启动） */
public class PersistenceIo {
    private static let pool: IoPool = IoPool(ioThreadCount())

    /** 在 IO 线程上执行 task，结果（或异常）完成返回的 Promise */
    public static func submit<V>(task: () -> V): TypedPromise<V> {
        let p = TypedPromise<V>()
        pool.execute({ =>
            let outcome = try {
                AskOutcome<V>.Replied(task())
            } catch (e: Exception) {
                AskOutcome<V>.Failed(e)
            } catch (e: Error) {
                // Error 也必须完成 Promise：否则等待它的 Actor 永远停在"写入中"、命令无限暂存
                AskOutcome<V>.Failed(PersistenceException("持久化 IO 出错：${e}"))
            }
            let _ = p.tryComplete(outcome)
        })
        p
    }

    /** 线程数 */
    public static func threads(): Int64 {
        pool.size
    }

    /** 已执行完的任务数（诊断用） */
    public static func completedTasks(): Int64 {
        pool.completed.load()
    }

    /**
     * 等待已提交的 IO 全部执行完（队列为空且没有正在执行的任务），超时返回 false。
     * ActorSystem 关闭时不会等在途的持久化 IO（与 Akka 相同：日志写入照常完成，结果进死信）；
     * 需要"进程退出前全部落盘"或关闭后立即删除数据目录时，在 terminate 之后调用它
     */
    public static func awaitIdle(timeout: Duration): Bool {
        pool.awaitIdle(timeout)
    }
}

func ioThreadCount(): Int64 {
    match (getVariable("CACTOR_PERSISTENCE_IO_THREADS")) {
        case Some(v) => match (Int64.tryParse(v)) {
            case Some(n) where n >= 1 => n
            case _ => 8
        }
        case None => 8
    }
}

class IoPool {
    let size: Int64
    let completed = AtomicInt64(0)
    private let lock = Mutex()
    private let cond: Condition
    private let idle: Condition
    private let queue = ArrayDeque<() -> Unit>()
    private let started = AtomicBool(false)
    private var running = 0

    init(size: Int64) {
        this.size = size
        this.cond = synchronized(lock) { lock.condition() }
        this.idle = synchronized(lock) { lock.condition() }
    }

    func awaitIdle(timeout: Duration): Bool {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!queue.isEmpty() || running > 0) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return false
                }
                idle.wait(timeout: left)
            }
            true
        }
    }

    func execute(task: () -> Unit): Unit {
        if (!started.load() && started.compareAndSwap(false, true)) {
            for (_ in 0..size) {
                spawn {
                    workerLoop()
                }
            }
        }
        synchronized(lock) {
            queue.addLast(task)
            cond.notify()
        }
    }

    private func workerLoop(): Unit {
        while (true) {
            let task = synchronized(lock) {
                while (queue.isEmpty()) {
                    cond.wait()
                }
                running++
                queue.removeFirst().getOrThrow()
            }
            try {
                task()
            } catch (_: Exception) {
                // submit 已把任务异常转成 Promise 的失败；这里兜住其他异常，线程继续工作
            } catch (_: Error) {}
            completed.fetchAdd(1)
            synchronized(lock) {
                running--
                if (running == 0 && queue.isEmpty()) {
                    idle.notifyAll()
                }
            }
        }
    }
}
