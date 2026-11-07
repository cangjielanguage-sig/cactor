/*
 * CActor Foundation - 稳定哈希与 Rendezvous（HRW）选择
 *
 * 为什么不用 String.hashCode：它的结果不保证跨进程、跨版本稳定，且对"共享前缀、仅末位不同"的
 * 字符串（如顺序分配的端口、`worker-1..N`）低位聚集。路由 / 分片需要的是：
 *   1. 同一个键在任何进程里都落到同一个目标（稳定）；
 *   2. 目标增减时只有落在该目标上的键迁移（一致性）——取模做不到，目标数一变几乎全部键重新分布。
 *
 * Rendezvous 哈希（Highest Random Weight）：对每个候选计算 mix(hash(key) ⊕ hash(candidate))，取最大者。
 * 无需维护哈希环和虚拟节点，O(n) 选择，增减一个候选只影响 1/n 的键。
 *
 * 乘法按 2^64 取模：仓颉默认检查整数溢出，这里用 @OverflowWrapping 显式声明回绕语义。
 */
package cactor_foundation.hashing

/** 64 位 FNV-1a：跨进程、跨版本稳定 */
@OverflowWrapping
public func stableHash64(s: String): UInt64 {
    var h: UInt64 = 0xcbf29ce484222325
    for (b in s) {
        h = h ^ UInt64(b)
        h = h * 0x100000001b3
    }
    h
}

/** 64 位最终混合（MurmurHash3 fmix64）：把输入的每一位雪崩到输出，消除 FNV 对相近输入的平移关系 */
@OverflowWrapping
public func mix64(h: UInt64): UInt64 {
    var x = h
    x = x ^ (x >> 33)
    x = x * 0xff51afd7ed558ccd
    x = x ^ (x >> 33)
    x = x * 0xc4ceb9fe1a85ec53
    x = x ^ (x >> 33)
    x
}

/** 映射到非负 Int64 的 [0, n)（n > 0），不会因符号位得到负下标 */
public func boundedIndex(h: UInt64, n: Int64): Int64 {
    Int64(h % UInt64(n))
}

/**
 * Rendezvous 选择：返回 candidates 中与 key 权重最高的下标；candidates 为空时返回 -1。
 * candidates 是各候选的稳定哈希（例如 stableHash64(actor 路径)），调用方可缓存。
 */
public func rendezvousIndex(keyHash: UInt64, candidates: Array<UInt64>): Int64 {
    var best: Int64 = -1
    var bestWeight: UInt64 = 0
    for (i in 0..candidates.size) {
        let w = mix64(keyHash ^ candidates[i])
        if (best < 0 || w > bestWeight) {
            best = i
            bestWeight = w
        }
    }
    best
}
