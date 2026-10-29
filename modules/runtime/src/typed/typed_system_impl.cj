/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - 类型安全 Actor 系统
 *
 * 基于 CActorRuntime 的经典 Actor Cell 运行类型化 Actor（桥接层见 cactor_core.typed.typed_adapter）。
 */
package cactor_runtime.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.typed.*
import cactor_core.actor.*
import cactor_runtime.*

/**
 * 类型安全 Actor 系统
 */
public class TypedActorSystemImpl <: TypedActorSystem {
    private let systemName: String
    private let runtime: CActorRuntime
    private let startTimeStamp: Int64
    private let terminated: AtomicBool
    private let ownsRuntime: Bool
    private let receptionistLock = Mutex()
    private var receptionistValue: Option<Receptionist> = None<Receptionist>

    /** 在已有运行时上创建类型化视图（terminate 不会关闭运行时） */
    public init(name: String, runtime: CActorRuntime) {
        this(name, runtime, false)
    }

    /** ownsRuntime 为 true 时 terminate 会关闭底层运行时 */
    public init(name: String, runtime: CActorRuntime, ownsRuntime: Bool) {
        this.systemName = name
        this.runtime = runtime
        this.startTimeStamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.terminated = AtomicBool(false)
        this.ownsRuntime = ownsRuntime
    }

    public prop name: String {
        get() { systemName }
    }

    /** 创建顶层类型化 Actor */
    public func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        actorOf<U>(behavior, name, TypedProps())
    }

    /** 指定调度器 / 邮箱创建顶层类型化 Actor */
    public func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> {
        if (terminated.load()) {
            throw IllegalStateException("Actor 系统 ${systemName} 已终止")
        }
        let classic = runtime.actorOf(TypedSpawn.props<U>(behavior, Some<TypedActorSystem>(this), props), name)
        ClassicTypedRef<U>(classic)
    }

    /** 兼容旧接口 */
    public func createActor<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        actorOf<U>(behavior, name)
    }

    public func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> {
        actorOf<Any>(behavior, name)
    }

    /** 停止一个类型化 Actor */
    public func stop<U>(ref: TypedActorRef<U>): Unit {
        match (ref.toClassic()) {
            case Some(c) => runtime.stop(c)
            case None => throw IllegalArgumentException("${ref.pathString()} 不是由 Actor 支撑的引用")
        }
    }

    /** 请求-响应（在 Actor 之外使用） */
    public func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req,
        timeout: Duration): TypedPromise<Res> {
        TypedAsk.ask<Req, Res>(target, createRequest, timeout)
    }

    /** 服务注册表（首次使用时创建） */
    public func receptionist(): Receptionist {
        synchronized(receptionistLock) {
            match (receptionistValue) {
                case Some(r) => r
                case None =>
                    let rt = runtime
                    let r = Receptionist(Some<(() -> Actor) -> ActorRef>({ creator =>
                        rt.actorOf(PropsFactory.create<Actor>(creator), "receptionist")
                    }))
                    receptionistValue = Some(r)
                    r
            }
        }
    }

    public func terminate(): Bool {
        if (terminated.compareAndSwap(false, true)) {
            if (ownsRuntime) {
                runtime.shutdown()
            }
            return true
        }
        false
    }

    public func isTerminated(): Bool {
        terminated.load()
    }

    public func startTime(): Int64 {
        startTimeStamp
    }

    /** 按路径查找，例如 "/user/worker" 或 "worker" */
    public func actorSelection(path: String): TypedActorSelection<Any> {
        actorSelectionAs<Any>(path)
    }

    /** 按路径查找，以协议 T 返回（类型由调用方保证） */
    public func actorSelectionAs<T>(path: String): TypedActorSelection<T> {
        LookupTypedSelection<T>(normalize(path), runtime)
    }

    /** 按名称或路径查找存活的 Actor，并以协议 U 返回引用（类型由调用方保证） */
    public func findActor<U>(nameOrPath: String): Option<TypedActorRef<U>> {
        match (runtime.lookup(normalize(nameOrPath))) {
            case Some(c) => Some<TypedActorRef<U>>(ClassicTypedRef<U>(c))
            case None => None<TypedActorRef<U>>
        }
    }

    private func normalize(path: String): String {
        if (path.startsWith("/")) {
            path
        } else {
            "/user/${path}"
        }
    }
}

/** 基于运行时路径查找的选择器 */
class LookupTypedSelection<T> <: TypedActorSelection<T> {
    private let path: String
    private let runtime: CActorRuntime

    init(path: String, runtime: CActorRuntime) {
        this.path = path
        this.runtime = runtime
    }

    public func resolveOne(timeout: Duration): Option<TypedActorRef<T>> {
        let deadline = MonoTime.now() + timeout
        while (true) {
            if (let Some(c) <- runtime.lookup(path)) {
                return Some<TypedActorRef<T>>(ClassicTypedRef<T>(c))
            }
            if (MonoTime.now() >= deadline) {
                return None<TypedActorRef<T>>
            }
            sleep(Duration.millisecond)
        }
        None<TypedActorRef<T>>
    }

    public func tell(message: T): Unit {
        if (let Some(c) <- runtime.lookup(path)) {
            ClassicTypedRef<T>(c).tell(message)
        }
    }

    public func pathString(): String {
        path
    }
}
