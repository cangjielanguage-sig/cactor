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
- 监督：`TypedBehaviors.supervise(behavior).onFailure(TypedSupervisorStrategy.restart().withLimit(3, 1.minutes))`
- Ask：`TypedAsk.ask<Cmd, R>(ref, { r => Cmd.X(r) }, 3.seconds).get(3.seconds)`
- 信号处理：`behavior.receiveSignal({ ctx, sig => ... })`；不处理 `TypedTerminated` 即 DeathPact。
- 类型适配：`TypedAdapters.messageAdapter(target, f)` 让外部类型发进 `TypedActorRef<T>`。

## `@actorReceive` 宏（modules/macros/）

- 只能用于 **class**；类中不能手写 `receive`。
- 写 `func onXxx(msg: T)` / `func onXxx(msg: T, ctx: ActorContext)`，宏生成按类型分派。
- 同一消息类型两个 onXxx 会编译期报错——这是设计特性，不是 bug。

## 禁止事项

- 在 workspace 子目录单独 `cjpm build -m xxx` 后跑基准（不继承 -O2，数字失真 2–3 倍）。
- import 反向依赖（见 CLAUDE.md 分层图），先跑 `python3 tools/check_module_deps.py`。
- 用真实 sleep 等待异步断言，改用 `modules/testkit/` 的 `TestInbox` / `BehaviorTestKit` / `ManualTime`。

## 参考文件

- 完整 API 语义：`modules/core/src/typed/typed_actor.cj` 头注释
- 最佳实践：`docs/zh/BEST_PRACTICES.md`
- 可运行示例：`modules/examples/src/`（按需挑最接近的例子改）
