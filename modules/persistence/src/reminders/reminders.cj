/* CActor Persistence.Reminders - 单写者、确认后推进的持久化提醒。
 * 恢复依赖稳定 PersistenceId 和共享的持久存储；不承担集群选主或复制。
 */
package cactor_persistence.reminders

import std.time.*
import std.convert.*
import std.collection.*
import cactor_core.typed.*
import cactor_persistence.{DurableStateStore, PersistenceCorruptionException}
import cactor_persistence.typed.*
import cactor_foundation.hashing.{toHex, fromHex}

public struct ReminderDefinition {
    public let name: String
    public let payload: String
    public let dueAtMillis: Int64
    public let periodMillis: Int64

    public init(name: String, payload: String, dueAtMillis: Int64, periodMillis!: Int64 = 0) {
        if (name.isEmpty() || dueAtMillis < 0 || periodMillis < 0) {
            throw IllegalArgumentException("提醒名称不能为空，UTC 到期时间与周期不能为负")
        }
        this.name = name
        this.payload = payload
        this.dueAtMillis = dueAtMillis
        this.periodMillis = periodMillis
    }
}

/** (name, generation, dueAtMillis) 是一次投递的稳定去重标识，重发保持不变。 */
public struct ReminderDelivery {
    public let definition: ReminderDefinition
    public let generation: Int64
    init(definition: ReminderDefinition, generation: Int64) {
        this.definition = definition
        this.generation = generation
    }
}

public enum ReminderCommand {
    | Register(ReminderDefinition, TypedActorRef<Int64>)
    | Remove(String, TypedActorRef<Bool>)
    | Acknowledge(String, Int64, Int64, TypedActorRef<Bool>)
    | Get(TypedActorRef<Array<ReminderDelivery>>)
    | Tick
}

struct ReminderState {
    let generation: Int64
    let entries: Array<ReminderDelivery>
    init(generation: Int64, entries: Array<ReminderDelivery>) {
        this.generation = generation
        this.entries = entries
    }
}

class ReminderCodec <: StateCodec<ReminderState> {
    public func encode(state: ReminderState): String {
        var text = "reminders-v1|${state.generation}"
        for (item in state.entries) {
            let d = item.definition
            text += "\n${toHex(d.name.toArray())}|${item.generation}|${d.dueAtMillis}|${d.periodMillis}|${toHex(d.payload.toArray())}"
        }
        text
    }

    public func decode(data: String): ReminderState {
        try {
            let rows = data.split("\n")
            let header = rows[0].split("|")
            if (header.size != 2 || header[0] != "reminders-v1") {
                throw IllegalArgumentException("未知提醒状态版本")
            }
            let generation = Int64.parse(header[1])
            if (generation < 0) { throw IllegalArgumentException("无效提醒代次") }
            let entries = ArrayList<ReminderDelivery>()
            let names = HashSet<String>()
            for (i in 1..rows.size) {
                let fields = rows[i].split("|")
                if (fields.size != 5) { throw IllegalArgumentException("提醒字段数错误") }
                let name = String.fromUtf8(fromHex(fields[0]).getOrThrow())
                let gen = Int64.parse(fields[1])
                if (gen <= 0 || gen > generation || names.contains(name)) {
                    throw IllegalArgumentException("提醒代次或名称重复")
                }
                names.add(name)
                let definition = ReminderDefinition(name, String.fromUtf8(fromHex(fields[4]).getOrThrow()),
                    Int64.parse(fields[2]), periodMillis: Int64.parse(fields[3]))
                entries.add(ReminderDelivery(definition, gen))
            }
            ReminderState(generation, entries.toArray())
        } catch (error: Exception) {
            throw PersistenceCorruptionException("持久化提醒状态损坏：${error.message}")
        }
    }
}

public struct DurableReminders {
    /**
     * 持久确认后推进，未确认时每轮重发；周期错过的时刻合并，不补发风暴。
     * deliver 应迅速 tell 到业务 Actor，业务提交副作用后再 Acknowledge。
     * 时间使用 UTC 毫秒，可注入以便测试；持久化 IO 由现有引擎异步执行。
     */
    public static func behavior(persistenceId: PersistenceId, store: DurableStateStore,
        deliver: (ReminderDelivery) -> Unit, pollInterval!: Duration = Duration.second,
        nowMillis!: () -> Int64 = { => DateTime.now().toUnixTimeStamp().toMilliseconds() }): TypedBehavior<ReminderCommand> {
        if (pollInterval <= Duration.Zero) { throw IllegalArgumentException("轮询间隔必须大于零") }
        TypedBehaviors.withTimers<ReminderCommand>({ timers =>
            timers.startTimerWithFixedDelay("durable-reminders", ReminderCommand.Tick, pollInterval)
            DurableStateBehavior<ReminderCommand, ReminderState>(persistenceId,
                emptyState: ReminderState(0, Array<ReminderDelivery>()),
                commandHandler: { state, cmd, effect => handle(state, cmd, effect, deliver, nowMillis) },
                codec: ReminderCodec(), store: store).behavior()
        })
    }

    private static func handle(state: ReminderState, cmd: ReminderCommand, effect: DurableEffectFactory<ReminderState>,
        deliver: (ReminderDelivery) -> Unit, nowMillis: () -> Int64): DurableEffect<ReminderState> {
        match (cmd) {
            case Register(definition, replyTo) =>
                if (state.generation == Int64.Max) { throw IllegalStateException("提醒代次已耗尽") }
                let generation = state.generation + 1
                let entries = ArrayList<ReminderDelivery>()
                for (item in state.entries) {
                    if (item.definition.name != definition.name) { entries.add(item) }
                }
                entries.add(ReminderDelivery(definition, generation))
                effect.persist(ReminderState(generation, entries.toArray())).thenReply(replyTo, { _ => generation })
            case Remove(name, replyTo) =>
                let entries = ArrayList<ReminderDelivery>()
                for (item in state.entries) {
                    if (item.definition.name != name) { entries.add(item) }
                }
                if (entries.size == state.entries.size) { effect.reply(replyTo, false) }
                else { effect.persist(ReminderState(state.generation, entries.toArray())).thenReply(replyTo, { _ => true }) }
            case Acknowledge(name, generation, due, replyTo) =>
                let now = nowMillis()
                let entries = ArrayList<ReminderDelivery>()
                var accepted = false
                for (item in state.entries) {
                    let d = item.definition
                    if (d.name == name && item.generation == generation && d.dueAtMillis == due && now >= due) {
                        accepted = true
                        if (d.periodMillis > 0) {
                            // 先算余数，避免补偿大量错过周期时乘法溢出。
                            let delta = d.periodMillis - (now - due) % d.periodMillis
                            if (now > Int64.Max - delta) { throw IllegalStateException("提醒下次到期时间溢出") }
                            entries.add(ReminderDelivery(ReminderDefinition(d.name, d.payload, now + delta,
                                periodMillis: d.periodMillis), item.generation))
                        }
                    } else { entries.add(item) }
                }
                if (!accepted) { effect.reply(replyTo, false) }
                else { effect.persist(ReminderState(state.generation, entries.toArray())).thenReply(replyTo, { _ => true }) }
            case Get(replyTo) => effect.reply(replyTo,
                Array<ReminderDelivery>(state.entries.size, { i => state.entries[i] }))
            case Tick =>
                let now = nowMillis()
                effect.none().thenRun({ current =>
                    for (item in current.entries) {
                        if (item.definition.dueAtMillis <= now) { deliver(item) }
                    }
                })
        }
    }
}
