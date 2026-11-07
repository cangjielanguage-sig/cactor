---
name: cactor-actor-dev
description: Write CActor (Cangjie Actor framework) code correctly. Use when creating or modifying typed actors, behaviors, message protocols, supervision, timers, ask patterns, or any .cj file under modules/ in the cactor repo.
---

# CActor Actor 开发

写任何 Actor 相关代码前，先按此清单走。

## 决策顺序

1. **用类型化 API**（`cactor_core.typed`），除非维护经典 Actor 旧代码。
2. **协议 = enum**：一条消息一个 case；需要回复的带上 `TypedActorRef<R>`。
3. **状态放在行为参数里**，处理完返回新行为，不引入可变字段、不引入锁。
4. **失败交给监督**：不在 onMessage 里 try-catch 吞异常。

## 行为骨架

```cangjie
import cactor_core.typed.*
import cactor_core.dsl.*

enum WorkerCmd {
    | Do(String, TypedActorRef<String>)
    | Reset
}

func worker(name: String): TypedBehavior<WorkerCmd> {
    TypedBehaviors.receive<WorkerCmd>({ ctx, cmd =>
        match (cmd) {
            case WorkerCmd.Do(job, replyTo) =>
                replyTo << "done:${job}"
                TypedBehaviors.same<WorkerCmd>()
            case WorkerCmd.Reset => worker(name)   // 重置状态 = 返回初始行为
        }
    })
}
```

## 常用组合

- 定时器：`TypedBehaviors.withTimers<T>({ timers => timers.startTimerAtFixedRate(key, msg, 30.seconds); inner })`
- 监督：`TypedBehaviors.supervise(behavior).onFailure(TypedSupervisorStrategy.restart().withLimit(3, 1.minutes))`；
  退避重启用 `TypedSupervisorStrategy.restartWithBackoff(100.millis, 10.seconds, 0.2)`（setup 失败时不限次数的 restart 会直接停止）
  嵌套 supervise 由内向外匹配；计数跨重启累计，放在 setup 里也有效。
- Ask 只有三个入口：Actor 外 `TypedAsk.ask<Cmd, R>(ref, { r => Cmd.X(r) }, 3.seconds).get(3.seconds)`；
  Actor 内 `ctx.ask(target, factory, timeout) { outcome => ... }`；经典 `AskInbox.ask(ref, msg).get(timeout)`
  （目标照常 `context.reply`）。`cactor_patterns.ask.Ask` / `AskPatternManager` 已 @Deprecated，不要在新代码里用。
  `TypedPromise` 组合用 `map` / `flatMap` / `recover` / `TypedPromises.sequence`，不要在 Actor 内 `get`。
- 子 Actor：`ctx.actorOf(TypedBehaviors.setup<T> { ctx => ... }, "name")`——初始行为以 setup 开头才走快路径
  （ctx.actorOf 是虚调用，@Frozen 对它无效，hooks 由 setup 在调用方包里构造）。
- 未处理 / 协议之外的消息以 `UnhandledMessage` 进死信订阅者并限速记日志；`childAs<U>` 是未检查转换，用错会在对方告警。
- 信号处理：`behavior.receiveSignal({ ctx, sig => ... })`；不处理 `TypedTerminated` 即 DeathPact。
- 类型适配：`TypedAdapters.messageAdapter(target, f)` 让外部类型发进 `TypedActorRef<T>`。
- 路由：`TypedRouters.pool<T>(4, worker(), TypedRouting<T>().consistentHashing({ m => key }))` /
  `TypedRouters.group<T>(serviceKey)`；经典用 `Routers.pool(n, props)` / `Routers.group(refs)`（转发保留发送方）。
  不要再手工维护 `BasicRouter` 的目标列表——它不 watch 目标，死目标会继续被轮到。
- 停止与 Akka 相同：`system.stop(ref)` / `ctx.stop(child)` 处理完当前消息即停、剩余消息进死信、名字保留到终止；
  要"处理完排队消息再停"用 `PoisonPill` 或 `system.gracefulStop(ref, timeout)`（Actor 外，返回 true 时名字已释放）。
  stop 之后立即同名 actorOf 会报"名称已存在"。语义对照见 `docs/zh/akka-semantics.md`。
- 经典钩子与 Akka 相同：`preStart` 在 Actor 线程执行（失败 = 启动失败，默认停止，不会从 `actorOf` 抛出）；
  `preRestart` 默认调 `postStop`，`postRestart` 默认调 `preStart`；重启等旧子 Actor 全部终止后才建新实例。

## `@actorReceive` 宏（modules/macros/）

- 只能用于 **class**；类中不能手写 `receive`。
- 写 `func onXxx(msg: T)` / `func onXxx(msg: T, ctx: ActorContext)`，宏生成按类型分派。
- 同一消息类型两个 onXxx 会编译期报错——这是设计特性，不是 bug。
- 首参为 `ActorContext` 的 `onStarted(context)` 是生命周期钩子，宏会跳过；兜底的 `onAny(m: Message)` 总被排在最后。
- `@Message` 默认以简单类名为线上名称；不同包有同名消息类时用 `@Message[name = "包名.类名"]`，否则 `registerCodec` 抛异常。

## 禁止事项

- 在 workspace 子目录单独 `cjpm build -m xxx` 后跑基准（不继承 -O2，数字失真 2–3 倍）。
- import 反向依赖（见 CLAUDE.md 分层图），先跑 `python3 tools/check_module_deps.py`。
- 用真实 sleep 等待异步断言，改用 `modules/testkit/` 的 `TestInbox` / `BehaviorTestKit` / `ManualTime`。

## 参考文件

- 完整 API 语义：`modules/core/src/typed/typed_actor.cj` 头注释
- 最佳实践：`docs/zh/BEST_PRACTICES.md`
- 可运行示例：`modules/examples/src/`（按需挑最接近的例子改）
