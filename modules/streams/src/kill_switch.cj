/*
 * CActor Streams - Kill switch（对标 Akka Streams KillSwitches）
 *
 * kill switch 是一个透传阶段，物化值可以从流外（任意线程）结束流：
 * - shutdown：下游正常完成、上游被取消
 * - abort(e)：下游以 e 失败、上游被取消
 * UniqueKillSwitch 控制一次物化；SharedKillSwitch 可以放进任意多条流，一次控制全部。
 * 流已经结束后再调用是空操作。
 */
package cactor_streams

import std.collection.*
import std.sync.*

public interface KillSwitch {
    func shutdown(): Unit

    func abort(cause: Exception): Unit
}

class KillSwitchLogic<T> <: FlowLogic<T, T> {
    let shared: Option<SharedKillSwitch>
    /** 在共享 kill switch 中的登记号 */
    var sharedKey: Int64 = 0

    init(island: Island, inlet: Conn<T>, shared: Option<SharedKillSwitch>) {
        super(island, inlet)
        this.shared = shared
    }

    public override func preStart(): Unit {
        if (let Some(s) <- shared) {
            s.register(this)
        }
    }

    public func onPush(): Unit {
        outlet.push(inlet.grab())
    }

    func shutdownAsync(): Unit {
        let logic = this
        asyncCallback({ => logic.completeStage() })
    }

    func abortAsync(cause: Exception): Unit {
        let logic = this
        asyncCallback({ => logic.failStage(cause) })
    }

    public override func postStop(): Unit {
        if (let Some(s) <- shared) {
            s.unregister(this)
        }
    }
}

/** 只控制一次物化的 kill switch（KillSwitches.single 的物化值） */
public class UniqueKillSwitch <: KillSwitch {
    private let shutdownFn: () -> Unit
    private let abortFn: (Exception) -> Unit

    init(shutdownFn: () -> Unit, abortFn: (Exception) -> Unit) {
        this.shutdownFn = shutdownFn
        this.abortFn = abortFn
    }

    public func shutdown(): Unit {
        shutdownFn()
    }

    public func abort(cause: Exception): Unit {
        abortFn(cause)
    }
}

/** 本岛之外登记的目标：不依赖元素类型 */
interface KillSwitchTarget {
    func shutdownAsync(): Unit

    func abortAsync(cause: Exception): Unit
}

/** 可共享的 kill switch：flow<T>() 每物化一次就登记一个阶段 */
public class SharedKillSwitch <: KillSwitch {
    private let lock = Mutex()
    private let targets = HashMap<Int64, KillSwitchTarget>()
    /** None = 运行中；Some(None) = 已 shutdown；Some(Some(e)) = 已 abort */
    private var state: Option<Option<Exception>> = None<Option<Exception>>
    private var nextKey: Int64 = 0
    public let name: String

    init(name: String) {
        this.name = name
    }

    /** 透传 Flow：物化后受本 kill switch 控制；kill switch 已触发时物化出的流立即结束 */
    public func flow<T>(): Flow<T, T, SharedKillSwitch> {
        let sw = this
        Flow<T, T, SharedKillSwitch>({ b, c =>
            let logic = KillSwitchLogic<T>(b.current, c, Some(sw))
            b.add(logic)
            (logic.outlet, sw)
        })
    }

    func register<T>(logic: KillSwitchLogic<T>): Unit {
        let st = synchronized(lock) {
            if (state.isNone()) {
                nextKey++
                logic.sharedKey = nextKey
                targets[nextKey] = KillSwitchAdapter<T>(logic)
            }
            state
        }
        match (st) {
            case Some(None) => logic.completeStage()
            case Some(Some(e)) => logic.failStage(e)
            case None => ()
        }
    }

    func unregister<T>(logic: KillSwitchLogic<T>): Unit {
        synchronized(lock) {
            targets.remove(logic.sharedKey)
        }
    }

    public func shutdown(): Unit {
        trigger(None<Exception>)
    }

    public func abort(cause: Exception): Unit {
        trigger(Some(cause))
    }

    private func trigger(t: Option<Exception>): Unit {
        let all = synchronized(lock) {
            if (state.isSome()) {
                return
            }
            state = Some(t)
            let xs = targets.values().toArray()
            targets.clear()
            xs
        }
        for (x in all) {
            match (t) {
                case Some(e) => x.abortAsync(e)
                case None => x.shutdownAsync()
            }
        }
    }
}

class KillSwitchAdapter<T> <: KillSwitchTarget {
    let logic: KillSwitchLogic<T>

    init(logic: KillSwitchLogic<T>) {
        this.logic = logic
    }

    public func shutdownAsync(): Unit {
        logic.shutdownAsync()
    }

    public func abortAsync(cause: Exception): Unit {
        logic.abortAsync(cause)
    }
}

/** kill switch 工厂 */
public struct KillSwitches {
    /** 每次物化得到一个独立的 UniqueKillSwitch */
    public static func single<T>(): Flow<T, T, UniqueKillSwitch> {
        Flow<T, T, UniqueKillSwitch>({ b, c =>
            let logic = KillSwitchLogic<T>(b.current, c, None<SharedKillSwitch>)
            b.add(logic)
            (logic.outlet, UniqueKillSwitch({ => logic.shutdownAsync() }, { e => logic.abortAsync(e) }))
        })
    }

    /** 一个可放进多条流的 kill switch */
    public static func shared(name: String): SharedKillSwitch {
        SharedKillSwitch(name)
    }
}
