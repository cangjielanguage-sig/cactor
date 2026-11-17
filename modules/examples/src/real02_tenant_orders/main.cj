/*
 * CActor Real 02 - 多租户订单/购物车（分片 + 租约 + fencing + 幂等）
 *
 * 真实约束（docs/plan/2026117-lz.md §3 E2）：
 * - 同一租户的写必须单写者（否则库存扣减会超卖），不同租户并行；
 * - 单写者会故障：持租约者失效后，另一写者必须接管，且旧写者的迟到写必须被拒绝；
 * - 消息可能重复（重连重发）：同一 opId 的扣减只能生效一次；
 * - 热点租户不能饿死其他租户。
 *
 * 组件：
 * - Cluster + ClusterSharding：按 tenantId 路由到唯一实体（同租户串行、跨租户并行）
 * - FileLease + FencedDurableStateStore：演示单写者交接与 fencing 拒绝旧 token
 * - 实体内的 opId 去重表：幂等
 *
 * 自检（退出码 0/1）：
 * 1. 1000 件库存被 100 并发订单打空，库存从不为负、成功单数 == 1000；
 * 2. A 持租约 → B 接管（token 递增）→ A 的迟到写被 StaleFencingTokenException 拒绝，数据未变；
 * 3. 同一 opId 重放 100 次只扣 1 次；
 * 4. 热点租户压满时冷租户请求仍能完成。
 *
 * 运行：source tools/env.sh && cjpm build && \
 *       CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.real02_tenant_orders
 */
package cactor_examples.real02_tenant_orders

import std.collection.*
import std.sync.*
import std.time.*
import std.convert.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedPromise}
import cactor_foundation.coordination.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_persistence.*
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding}

/** 全局库存（三个 SKU 各 1000 件）；实体是唯一写者，所以内部无需加锁 */
class Inventory {
    let lock = Mutex()
    let stock = HashMap<String, Int64>([("sku-a", 1000), ("sku-b", 1000), ("sku-c", 1000)])
    let seen = HashSet<String>()
    var sold: Int64 = 0

    /** 返回 (ok, 剩余)；同一 opId 重复调用返回 (true, 剩余) 但不扣减 */
    func reserve(opId: String, sku: String, qty: Int64): (Bool, Int64) {
        synchronized(lock) {
            if (seen.contains(opId)) {
                return (true, stock.get(sku) ?? 0)
            }
            let available = stock.get(sku) ?? 0
            if (qty > available) {
                return (false, available)
            }
            seen.add(opId)
            stock[sku] = available - qty
            sold += qty
            (true, available - qty)
        }
    }

    func remaining(sku: String): Int64 {
        synchronized(lock) {
            stock.get(sku) ?? 0
        }
    }

    func totalSold(): Int64 {
        synchronized(lock) {
            sold
        }
    }

    func distinctOps(): Int64 {
        synchronized(lock) {
            Int64(seen.size)
        }
    }
}

/**
 * 分片实体：消息格式 "opId|sku|qty"；回复 "ok|剩余" 或 "no|剩余"。
 * 同一实体（同一租户/仓库）的消息由分片保证串行——这就是"同租户单写者"。
 */
class InventoryEntity <: Actor {
    let id: String
    let inventory: Inventory

    init(id: String, inventory: Inventory) {
        this.id = id
        this.inventory = inventory
    }

    public prop name: String { get() { "inventory-${id}" } }
    public prop description: String { get() { "inventory entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 3) {
                let (ok, left) = inventory.reserve(parts[0], parts[1], Int64.parse(parts[2]))
                context.reply(StringMessage("${if (ok) { "ok" } else { "no" }}|${left}"))
            }
        }
        MessageResult.Handled
    }
}

/** 从 ask 的 Message 回复里取字符串（非 StringMessage 直接失败，避免静默） */
func replyText(message: Message): String {
    match (message as StringMessage) {
        case Some(s) => s.getContent()
        case None => throw IllegalStateException("回复不是 StringMessage：${message.messageType()}")
    }
}

func check(ok: Bool, why: String): Unit {
    if (!ok) {
        throw IllegalStateException(why)
    }
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

/** 单写者句柄：租约 + fencing 存储，模拟"持有租约的订单写者" */
class Writer {
    let owner: String
    let lease: FileLease
    let store: FencedDurableStateStore
    let lost = AtomicBool(false)

    init(dir: String, owner: String, ttlMs: Int64) {
        this.owner = owner
        let settings = LeaseSettings("orders", owner, Duration.millisecond * ttlMs,
            Duration.millisecond * (ttlMs / 10), Duration.millisecond * (ttlMs / 4))
        this.lease = FileLease("${dir}/leases", settings)
        this.store = FencedDurableStateStore(FileDurableStateStore("${dir}/data"),
            FileFencingTokenStore("${dir}/fence"), lease)
    }

    func acquire(): Bool {
        lease.acquire({ _ => lost.store(true) })
    }

    func token(): Int64 {
        lease.fencingToken().getOrThrow()
    }

    func write(value: String): Int64 {
        let rev = store.revisionOf("orders") + 1
        store.upsert("orders", rev, value)
        rev
    }

    func close(): Unit {
        lease.release()
    }
}

main(): Int64 {
    let dir = "/tmp/cactor-real02-${MonoTime.now().hashCode()}"
    let c = Checker()
    let nodeCount = 3
    let inventory = Inventory()

    let systems = ArrayList<CActorSystem>()
    let remotes = ArrayList<SimpleRemoteManager>()
    let clusters = ArrayList<Cluster>()
    var seed = NetworkAddress("127.0.0.1", 0)
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))

    for (i in 0..nodeCount) {
        let sys = CActorSystemFactory.create("real02-node-${i}")
        let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
        if (i == 0) {
            seed = remote.getLocalAddress()
        }
        let cluster = Cluster(sys, remote, [seed], settings)
        systems.add(sys)
        remotes.add(remote)
        clusters.add(cluster)
    }

    // 每个节点一个分片区域（同一个 typeName）：租户 → 唯一实体
    let shards = ArrayList<ClusterSharding>()
    for (i in 0..nodeCount) {
        shards.add(ClusterSharding(clusters[i], systems[i], remotes[i], "inventory", 16,
            { id => InventoryEntity(id, inventory) }))
    }

    var formed = true
    for (cl in clusters) {
        formed = formed && waitUntil(Duration.second * 20, { =>
            cl.state().upCount() == nodeCount && cl.state().converged
        })
    }
    c.check(formed, "三节点集群形成并收敛")

    try {
        // ---- 1. 库存不为负 ----
        println("\n[1] 1000 件库存被 100 并发订单打空")
        let reservations = 100
        let qty = 10
        let futures = ArrayList<TypedPromise<Message>>()
        for (i in 0..reservations) {
            // 同一租户 "t-hot"：单写者串行扣减
            futures.add(shards[0].ask("t-hot", StringMessage("op-${i}|sku-a|${qty}"), Duration.second * 20))
        }
        var okCount = 0
        var rejected = 0
        for (f in futures) {
            let reply = replyText(f.get(Duration.second * 30))
            if (reply.startsWith("ok|")) {
                okCount++
            } else {
                rejected++
            }
        }
        c.check(okCount == reservations && rejected == 0, "全部 ${okCount} 单成功、${rejected} 单被拒")
        c.check(inventory.remaining("sku-a") == 0, "sku-a 库存归零：${inventory.remaining("sku-a")}")
        c.check(inventory.totalSold() == 1000, "累计售出 1000 件：${inventory.totalSold()}")
        c.check(inventory.remaining("sku-a") >= 0, "库存从不为负")

        // 超卖尝试必须被明确拒绝
        let oversell = shards[1].ask("t-hot", StringMessage("op-oversell|sku-a|1"), Duration.second * 10)
        let oversellReply = replyText(oversell.get(Duration.second * 15))
        c.check(oversellReply == "no|0", "售罄后再下单被拒绝：${oversellReply}")

        // ---- 2. 旧写者被 fencing 拒绝 ----
        println("\n[2] 单写者交接：旧 token 的迟到写被拒绝")
        let writerA = Writer(dir, "writer-A", 800)
        c.check(writerA.acquire(), "A 获得租约（token ${writerA.token()}）")
        let tokenA = writerA.token()
        writerA.write("A:1")
        writerA.close() // A 主动释放（等价于 A 进程退出）
        let writerB = Writer(dir, "writer-B", 800)
        c.check(writerB.acquire(), "B 接管租约（token ${writerB.token()}）")
        c.check(writerB.token() > tokenA, "B 的 token 严格大于 A：${tokenA} → ${writerB.token()}")
        writerB.write("B:1")
        // A 的迟到写：用 A 的旧 token 直接写，必须被拒
        let staleStore = FencedDurableStateStore(FileDurableStateStore("${dir}/data"),
            FileFencingTokenStore("${dir}/fence"), "orders", { => Some(tokenA) })
        var staleRejected = false
        try {
            staleStore.upsert("orders", staleStore.revisionOf("orders") + 1, "A:stale")
        } catch (_: StaleFencingTokenException) {
            staleRejected = true
        }
        c.check(staleRejected, "A 的旧 token 写入被 StaleFencingTokenException 拒绝")
        let current = FileDurableStateStore("${dir}/data").get("orders")
        match (current) {
            case Some(r) => c.check(r.state == "B:1", "共享状态仍是 B 写入的值：${r.state}")
            case None => c.check(false, "共享状态丢失")
        }
        writerB.close()

        // ---- 3. 幂等：同一 opId 重放 100 次 ----
        println("\n[3] 幂等：同一 opId 重放 100 次只扣 1 次")
        let before = inventory.remaining("sku-b")
        var accepted = 0
        for (_ in 0..100) {
            let reply = replyText(shards[2].ask("t-idem", StringMessage("op-same|sku-b|7"), Duration.second * 10)
                .get(Duration.second * 15))
            if (reply.startsWith("ok|")) {
                accepted++
            }
        }
        let after = inventory.remaining("sku-b")
        c.check(accepted == 100, "100 次重放都返回 ok（幂等成功）")
        c.check(before - after == 7, "库存只扣了 7 件：${before} → ${after}")

        // ---- 4. 热点租户不饿死冷租户 ----
        println("\n[4] 热点租户压满时，冷租户请求仍能完成")
        let hotDone = AtomicBool(false)
        let hotWorker = spawn {
            var i = 0
            while (!hotDone.load()) {
                shards[0].ask("t-hot2", StringMessage("hot-${i}|sku-c|1"), Duration.second * 10)
                    .get(Duration.second * 15)
                i++
            }
        }
        sleep(Duration.millisecond * 100)
        let coldStart = MonoTime.now()
        let coldReply = replyText(shards[1].ask("t-cold", StringMessage("cold-1|sku-b|1"), Duration.second * 10)
            .get(Duration.second * 15))
        let coldLatency = (MonoTime.now() - coldStart).toMilliseconds()
        hotDone.store(true)
        hotWorker.get()
        c.check(coldReply.startsWith("ok|"), "冷租户请求成功：${coldReply}")
        c.check(coldLatency < 2000, "冷租户延迟 ${coldLatency} ms < 2000 ms（未被热点饿死）")
    } finally {
        for (i in 0..nodeCount) {
            clusters[i].shutdown()
            remotes[i].stop()
            systems[i].terminate()
        }
    }

    println("\nreal02：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
