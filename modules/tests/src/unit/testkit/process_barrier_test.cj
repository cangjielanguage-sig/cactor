/*
 * CActor Tests - 跨进程 barrier 的用例
 *
 * 这里要证明两件事：
 *   1. 同一进程内的多个线程能靠文件会合（协议本身正确）；
 *   2. 别的**操作系统进程**写的到位文件也会被看到（这才叫跨进程）。
 * 第 2 条用一个 `/bin/sh` 子进程去 `touch` 到位文件：父进程不知道 sh 何时结束，
 * 只能靠目录里的文件判断，这正是 barrier 在生产多进程测试里要做的事。
 */
package cactor_tests.unit.testkit

import cactor_testkit.ProcessBarrier
import std.fs.*
import std.process.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*

@Test
public class ProcessBarrierTest {
    @TestCase
    func threadsRendezvousThroughFiles(): Unit {
        let dir = ProcessBarrier.tempDirectory("threads")
        let barrier = ProcessBarrier(dir, timeout: Duration.second * 10)
        let sibling = spawn {
            let other = ProcessBarrier(dir, timeout: Duration.second * 10)
            other.arrive("sibling", 2, Duration.second * 10)
        }
        let mine = barrier.arrive("main", 2, Duration.second * 10)
        let theirs = sibling.get()
        @Expect(mine, true)
        @Expect(theirs, true)
        @Expect(barrier.count(), 2)
        @Expect(barrier.isOpen(), true)
        @Expect(String.join(barrier.parties(), delimiter: ","), "main,sibling")
        barrier.reset()
        @Expect(barrier.count(), 0)
        @Expect(barrier.isOpen(), false)
        remove(dir)
    }

    @TestCase
    func observesMarkersFromOtherProcesses(): Unit {
        let dir = ProcessBarrier.tempDirectory("processes")
        let barrier = ProcessBarrier(dir, timeout: Duration.second * 20)
        // 两个独立的操作系统进程各写一个到位文件：barrier 只能靠轮询目录发现它们
        let cmd = "touch ${dir}/p1.ready ${dir}/p2.ready"
        let child = launch("/bin/sh", ["-c", cmd])
        let exitCode = child.wait()
        @Expect(exitCode, 0)
        let mine = barrier.arrive("parent", 3, Duration.second * 5)
        @Expect(mine, true)
        @Expect(String.join(barrier.parties(), delimiter: ","), "p1,p2,parent")
        @Expect(barrier.isOpen(), true)
        remove(dir, recursive: true)
    }

    @TestCase
    func waitTimesOutWithoutEnoughParties(): Unit {
        let dir = ProcessBarrier.tempDirectory("timeout")
        let barrier = ProcessBarrier(dir, timeout: Duration.millisecond * 300)
        let started = DateTime.now()
        let ok = barrier.arrive("lonely", 2)
        let elapsed = DateTime.now() - started
        @Expect(ok, false)
        @Expect(elapsed >= Duration.millisecond * 250, true)
        @Expect(barrier.count(), 1)
        @Expect(barrier.isOpen(), false)
        remove(dir, recursive: true)
    }

    @TestCase
    func rejectsInvalidParticipantNames(): Unit {
        let dir = ProcessBarrier.tempDirectory("names")
        let barrier = ProcessBarrier(dir, timeout: Duration.second * 1)
        var rejected = 0
        try {
            barrier.markReady("a/b")
        } catch (e: IllegalArgumentException) {
            rejected++
        }
        try {
            barrier.markReady("../escape")
        } catch (e: IllegalArgumentException) {
            rejected++
        }
        @Expect(rejected, 2)
        remove(dir, recursive: true)
    }
}
