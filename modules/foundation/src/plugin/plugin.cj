/*
 * CActor Foundation - 通用插件内核
 * 与宿主无关：ActorSystem、HTTP Application、HttpClient 共用。
 *  - PluginRegistry：按名字登记，重复安装报错
 *  - InterceptorChain：环绕式拦截器链，先登记的在外层，build 后为一个普通函数（请求路径上不查表）
 *  - HookList：普通钩子，freeze 后为数组遍历
 */
package cactor_foundation.plugin

import std.sync.*
import std.collection.*

public class DuplicatePluginException <: Exception {
    public init(name: String) {
        super("插件已安装: ${name}")
    }
}

public class PluginFrozenException <: Exception {
    public init(name: String) {
        super("宿主已启动，不能再安装插件: ${name}")
    }
}

public class PluginRegistry {
    private let lock = Mutex()
    private let names = ArrayList<String>()
    private var frozen = false

    public init() {}

    public func register(name: String): Unit {
        synchronized(lock) {
            if (frozen) {
                throw PluginFrozenException(name)
            }
            for (n in names) {
                if (n == name) {
                    throw DuplicatePluginException(name)
                }
            }
            names.add(name)
        }
    }

    public func contains(name: String): Bool {
        synchronized(lock) {
            for (n in names) {
                if (n == name) {
                    return true
                }
            }
            false
        }
    }

    public func installed(): Array<String> {
        synchronized(lock) { names.toArray() }
    }

    public func freeze(): Unit {
        synchronized(lock) {
            frozen = true
        }
    }

    public func isFrozen(): Bool {
        synchronized(lock) { frozen }
    }
}

public class InterceptorChain<C> {
    private let interceptors = ArrayList<(C, () -> Unit) -> Unit>()

    public init() {}

    public func add(interceptor: (C, () -> Unit) -> Unit): Unit {
        interceptors.add(interceptor)
    }

    public func size(): Int64 {
        interceptors.size
    }

    /** 组合为单个函数：interceptors[0] 在最外层，terminal 在最内层 */
    public func build(terminal: (C) -> Unit): (C) -> Unit {
        let arr = interceptors.toArray()
        var f: (C) -> Unit = terminal
        var i = arr.size - 1
        while (i >= 0) {
            f = wrap(arr[i], f)
            i--
        }
        f
    }

    private func wrap(interceptor: (C, () -> Unit) -> Unit, next: (C) -> Unit): (C) -> Unit {
        { c: C => interceptor(c, { => next(c) }) }
    }
}

public class HookList<C> {
    private let hooks = ArrayList<(C) -> Unit>()
    private var frozenHooks: Array<(C) -> Unit> = Array<(C) -> Unit>()

    public init() {}

    public func add(hook: (C) -> Unit): Unit {
        hooks.add(hook)
    }

    public func freeze(): Unit {
        frozenHooks = hooks.toArray()
    }

    public func isEmpty(): Bool {
        hooks.isEmpty()
    }

    public func run(c: C): Unit {
        for (h in frozenHooks) {
            h(c)
        }
    }
}
