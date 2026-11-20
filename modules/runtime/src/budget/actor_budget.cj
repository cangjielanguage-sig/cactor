/*
 * CActor Runtime - 全局 Actor 预算（C1）
 *
 * 单个邮箱有界（Props.bounded）只能限制一个 Actor 的积压，拦不住"创建了太多 Actor"这类 OOM：
 * 100 万个 Actor 的存活堆约 1.1 GB，堆上限只有 64 MB 时只能靠创建方自觉。
 * 这里提供系统级预算：maxActors（按存活 Cell 计，含子 Actor）与 actorMemoryBudgetBytes
 * （按每 Actor 估计字节数换算成 Actor 上限，二者取小）。超限抛 ActorBudgetExceededException，
 * 而不是等到堆耗尽。
 */
package cactor_runtime.budget

/** 每 Actor 的估计存活堆（字节）：与 scale_test 实测同量级，用于把内存预算换算成 Actor 上限 */
public let ESTIMATED_BYTES_PER_ACTOR: Int64 = 1200

/** 创建 Actor 超过系统预算（C1）。已有 Actor 不受影响，调用方可以退避后重试。 */
public class ActorBudgetExceededException <: Exception {
    public let limit: Int64
    public let liveActors: Int64

    public init(limit: Int64, liveActors: Int64) {
        super("Actor 预算已满：当前存活 ${liveActors}，上限 ${limit}。停止不用的 Actor 或调大 CActorRuntimeConfig.maxActors")
        this.limit = limit
        this.liveActors = liveActors
    }
}

/** 预算使用快照（对标 ActorSystem 的可靠投递预算快照风格） */
public struct ActorBudgetUsage {
    /** 当前存活 Actor 数（含子 Actor） */
    public let liveActors: Int64
    /** Actor 数上限；0 表示不限 */
    public let maxActors: Int64
    /** 内存预算（字节）；0 表示不限 */
    public let memoryBudgetBytes: Int64
    /** 换算内存预算用的每 Actor 估计字节数 */
    public let estimatedBytesPerActor: Int64
    /** 因预算被拒绝的创建次数（进程内累计） */
    public let rejections: Int64

    public init(liveActors: Int64, maxActors: Int64, memoryBudgetBytes: Int64,
        estimatedBytesPerActor: Int64, rejections: Int64) {
        this.liveActors = liveActors
        this.maxActors = maxActors
        this.memoryBudgetBytes = memoryBudgetBytes
        this.estimatedBytesPerActor = estimatedBytesPerActor
        this.rejections = rejections
    }

    /** 是否已配置预算 */
    public func isLimited(): Bool {
        maxActors > 0
    }

    /** 距离上限还剩多少（未配置预算时为 -1） */
    public func remaining(): Int64 {
        if (maxActors <= 0) {
            return -1
        }
        let left = maxActors - liveActors
        if (left < 0) { 0 } else { left }
    }
}
