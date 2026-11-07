/*
 * CActor - TypedPromise 组合子（E36：对标 Future.flatMap / recover / recoverWith / sequence）
 *
 * 全部是非阻塞组合：回调在完成方线程上执行，只有测试线程（不是亲和工作线程）最后 get 取结果
 */
package cactor_tests.unit.runtime.typed_alignment

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.{TypedPromise, TypedPromises}

class PcBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 失败原因的消息；成功返回 "<ok>"，未完成返回 "<pending>" */
func failureText<T>(p: TypedPromise<T>): String {
    match (p.outcome()) {
        case Some(o) =>
            match (o.failure()) {
                case Some(e) => e.message
                case None => "<ok>"
            }
        case None => "<pending>"
    }
}

@Test
public class PromiseCombinatorsTest {
    @TestCase
    public func flatMapChainsAsyncSteps(): Unit {
        let first = TypedPromise<Int64>()
        let second = TypedPromise<String>()
        let chained = first.flatMap<String>({ v => second.map<String>({ s => "${s}${v}" }) })
        @Expect(chained.isCompleted(), false)
        first.trySuccess(7) |> ignoreDone
        // 第二步还没完成：整条链也没完成
        @Expect(chained.isCompleted(), false)
        spawn {
            second.trySuccess("n=") |> ignoreDone
        }
        @Expect(chained.get(Duration.second * 5), "n=7")
    }

    @TestCase
    public func flatMapPropagatesFailureOfEitherStep(): Unit {
        let failedFirst = TypedPromises.failed<Int64>(PcBoom("first")).flatMap<Int64>({ v => TypedPromises.successful<Int64>(v) })
        @Expect(failureText(failedFirst), "first")
        let throwing = TypedPromises.successful<Int64>(1).flatMap<Int64>({ _ => throw PcBoom("in-f") })
        @Expect(failureText(throwing), "in-f")
        let failedSecond = TypedPromises.successful<Int64>(1).flatMap<Int64>({ _ => TypedPromises.failed<Int64>(PcBoom("second")) })
        @Expect(failureText(failedSecond), "second")
    }

    @TestCase
    public func recoverAndRecoverWith(): Unit {
        @Expect(TypedPromises.failed<Int64>(PcBoom("x")).recover({ _ => -1 }).get(Duration.second), -1)
        @Expect(TypedPromises.successful<Int64>(5).recover({ _ => -1 }).get(Duration.second), 5)
        let rethrown = TypedPromises.failed<Int64>(PcBoom("x")).recover({ e => throw PcBoom("again:${e.message}") })
        @Expect(failureText(rethrown), "again:x")
        // 超时后改问备用服务
        let backup = TypedPromise<Int64>()
        let viaBackup = TypedPromises.failed<Int64>(PcBoom("timeout")).recoverWith({ _ => backup })
        @Expect(viaBackup.isCompleted(), false)
        backup.trySuccess(42) |> ignoreDone
        @Expect(viaBackup.get(Duration.second), 42)
    }

    @TestCase
    public func sequenceKeepsInputOrderRegardlessOfCompletionOrder(): Unit {
        let ps = Array<TypedPromise<Int64>>(4, { _ => TypedPromise<Int64>() })
        let all = TypedPromises.sequence<Int64>(ps)
        // 倒序、跨线程完成
        let fs = ArrayList<Future<Unit>>()
        for (i in 0..4) {
            let k = 3 - i
            fs.add(spawn {
                ps[k].trySuccess(k * 10) |> ignoreDone
            })
        }
        for (f in fs) {
            f.get()
        }
        @Expect(all.get(Duration.second * 5), [0, 10, 20, 30])
    }

    @TestCase
    public func sequenceFailsFastOnFirstFailure(): Unit {
        let ps = Array<TypedPromise<Int64>>(3, { _ => TypedPromise<Int64>() })
        let all = TypedPromises.sequence<Int64>(ps)
        ps[1].tryFailure(PcBoom("b")) |> ignoreDone
        // 其余两个还没完成，整体已经失败
        @Expect(failureText(all), "b")
        ps[0].trySuccess(1) |> ignoreDone
        ps[2].trySuccess(3) |> ignoreDone
        @Expect(failureText(all), "b")
        @Expect(TypedPromises.sequence<Int64>(Array<TypedPromise<Int64>>()).get(Duration.second).size, 0)
    }
}

func ignoreDone(_: Bool): Unit {}
