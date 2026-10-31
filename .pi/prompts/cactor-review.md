# /cactor-review

Review the current changes for CActor conventions. If no arguments, review uncommitted changes (`git diff HEAD`); otherwise review $ARGUMENTS.

## 检查清单（逐项核对 diff）

### 架构分层
- [ ] 新 import 是否遵守依赖方向：foundation -> core -> runtime -> patterns -> cactor -> streams -> remote/persistence -> cluster -> xstate -> integration -> tests/examples。macros 只被 examples 使用。

### 类型化 Actor 规范
- [ ] 新 Actor 用 `TypedBehavior<T>` / `TypedBehaviors.*`，而非经典 Actor（除非明确维护旧代码）
- [ ] 消息协议是 enum，`match` 穷尽，无通配 `case _` 吞掉新消息（穷尽检查是编译期安全网，加 `_` 会毁掉它）
- [ ] 状态通过"返回新行为"传递，无跨消息可变字段
- [ ] onMessage 内不 try-catch 吞异常，失败交给 `TypedBehaviors.supervise(...).onFailure(...)`
- [ ] ask 用 `TypedAsk.ask` 并设超时；发送用 `<<`；时长用时间字面量（`3.seconds`）

### 测试
- [ ] 异步行为用 `TestInbox` / `BehaviorTestKit` / `ManualTime`，无真实 sleep
- [ ] 改了公共 API 是否同步更新 `CHANGELOG.md` 与 `docs/zh/api-reference.md`

### 收尾
- [ ] 建议运行 `tools/ci.sh` 验证（不要自己跑基准）
