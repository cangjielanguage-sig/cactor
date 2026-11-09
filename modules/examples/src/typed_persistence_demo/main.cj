/*
 * CActor - 类型化持久化示例：购物车（EventSourcedBehavior）+ 用户资料（DurableStateBehavior）
 *
 *   cactor_examples.typed_persistence_demo [数据目录]
 *
 * 同一进程里先后启动三个 ActorSystem，共用一个磁盘目录，演示并自检：
 *   1. 第一个系统：64 个购物车并发加购（每个 25 件，全部 ask 并发发出），规则校验（重复加购、数量非法被拒绝）；
 *   2. 关掉系统，第二个系统从磁盘恢复：每个购物车的商品一件不少；对 cart-0 结账（snapshotWhen 结账时快照）；
 *   3. 第三个系统再恢复：cart-0 从快照恢复为已结账，结账后的加购被拒绝；用户资料删除后修订号不回退。
 * 退出码：0 全部校验通过，1 有不一致。
 */
package cactor_examples.typed_persistence_demo

import std.fs.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystemFactory, CActorSystem}
import cactor_core.typed.*
import cactor_persistence.{FileEventStore, FileSnapshotStore, FileDurableStateStore}
import cactor_persistence.typed.*

let ASK_TIMEOUT = Duration.second * 10
let CARTS = 64
let ITEMS_PER_CART = 25

// ======================= 购物车协议 =======================

/** 命令：每条都带回复地址（Akka 的 "ask 模式下的请求-响应" 写法） */
enum CartCmd {
    | AddItem(String, Int64, TypedActorRef<CartReply>)
    | RemoveItem(String, TypedActorRef<CartReply>)
    | Checkout(TypedActorRef<CartReply>)
    | GetCart(TypedActorRef<CartReply>)
}

enum CartEvt {
    | ItemAdded(String, Int64)
    | ItemRemoved(String)
    | CheckedOut(Int64)
}

enum CartReply {
    | Accepted(Cart)
    | Rejected(String)
}

/** 不可变状态：商品按加入顺序保存；事件处理器返回新对象 */
class Cart {
    let items: Array<(String, Int64)>
    let checkedOutAt: Option<Int64>

    init(items: Array<(String, Int64)>, checkedOutAt: Option<Int64>) {
        this.items = items
        this.checkedOutAt = checkedOutAt
    }

    static func empty(): Cart {
        Cart([], None)
    }

    func has(item: String): Bool {
        items.iterator().any({ p => p[0] == item })
    }

    func totalQuantity(): Int64 {
        var n = 0
        for ((_, q) in items) {
            n += q
        }
        n
    }

    func isCheckedOut(): Bool {
        checkedOutAt.isSome()
    }

    func apply(evt: CartEvt): Cart {
        match (evt) {
            case ItemAdded(item, qty) => Cart(items.concat([(item, qty)]), checkedOutAt)
            case ItemRemoved(item) => Cart(items.filter({ p => p[0] != item }).toArray(), checkedOutAt)
            case CheckedOut(at) => Cart(items, Some(at))
        }
    }
}

func replyToOf(cmd: CartCmd): TypedActorRef<CartReply> {
    match (cmd) {
        case AddItem(_, _, r) => r
        case RemoveItem(_, r) => r
        case Checkout(r) => r
        case GetCart(r) => r
    }
}

/** 命令处理：只读状态、返回效果；回复写在 thenReply 里——在事件落盘之后执行，拿到的是新状态 */
func handleCart(cart: Cart, cmd: CartCmd, effect: EffectFactory<CartEvt, Cart>): Effect<CartEvt, Cart> {
    match (cmd) {
        case GetCart(replyTo) => effect.reply(replyTo, Accepted(cart))
        case _ where cart.isCheckedOut() => effect.reply(replyToOf(cmd), Rejected("购物车已结账"))
        case AddItem(item, qty, replyTo) where qty <= 0 => effect.reply(replyTo, Rejected("数量必须为正：${qty}"))
        case AddItem(item, _, replyTo) where cart.has(item) => effect.reply(replyTo, Rejected("已在购物车中：${item}"))
        case AddItem(item, qty, replyTo) =>
            effect.persist(ItemAdded(item, qty)).thenReply(replyTo, { c => Accepted(c) })
        case RemoveItem(item, replyTo) where !cart.has(item) => effect.reply(replyTo, Rejected("不在购物车中：${item}"))
        case RemoveItem(item, replyTo) => effect.persist(ItemRemoved(item)).thenReply(replyTo, { c => Accepted(c) })
        case Checkout(replyTo) where cart.items.isEmpty() => effect.reply(replyTo, Rejected("空购物车不能结账"))
        case Checkout(replyTo) =>
            effect.persist(CheckedOut(DateTime.now().toUnixTimeStamp().toMilliseconds())).thenReply(replyTo, { c =>
                Accepted(c)
            })
    }
}

// 日志格式：manifest 是事件名，数据是以 \t 分隔的字段（演示用；生产中通常用 JSON / protobuf）
let cartEvents = FunctionEventAdapter<CartEvt>(manifest: { e =>
    match (e) {
        case ItemAdded(_, _) => "ItemAdded"
        case ItemRemoved(_) => "ItemRemoved"
        case CheckedOut(_) => "CheckedOut"
    }
}, toJournal: { e =>
    match (e) {
        case ItemAdded(item, qty) => "${item}\t${qty}"
        case ItemRemoved(item) => item
        case CheckedOut(at) => "${at}"
    }
}, fromJournal: { manifest, data =>
    match (manifest) {
        case "ItemAdded" =>
            let f = data.split("\t")
            ItemAdded(f[0], Int64.parse(f[1]))
        case "ItemRemoved" => ItemRemoved(data)
        case "CheckedOut" => CheckedOut(Int64.parse(data))
        case other => throw IllegalArgumentException("未知事件：${other}")
    }
})

let cartCodec = FunctionStateCodec<Cart>(encode: { c =>
    let sb = StringBuilder(match (c.checkedOutAt) {
        case Some(at) => "${at}"
        case None => "-"
    })
    for ((item, qty) in c.items) {
        sb.append("\n${item}\t${qty}")
    }
    sb.toString()
}, decode: { data =>
    let lines = data.split("\n")
    let items = ArrayList<(String, Int64)>()
    for (i in 1..lines.size) {
        let f = lines[i].split("\t")
        items.add((f[0], Int64.parse(f[1])))
    }
    Cart(items.toArray(), if (lines[0] == "-") { None } else { Int64.parse(lines[0]) })
})

class Stores {
    let journal: FileEventStore
    let snapshots: FileSnapshotStore
    let profiles: FileDurableStateStore

    init(dir: String) {
        journal = FileEventStore("${dir}/journal")
        snapshots = FileSnapshotStore("${dir}/snapshots")
        profiles = FileDurableStateStore("${dir}/profiles")
    }

    func close(): Unit {
        journal.close()
        snapshots.close()
    }
}

func cartBehavior(cartId: String, stores: Stores, log: (String) -> Unit): TypedBehavior<CartCmd> {
    EventSourcedBehavior<CartCmd, CartEvt, Cart>(PersistenceId.of("ShoppingCart", cartId), emptyState: Cart.empty(),
        commandHandler: handleCart, eventHandler: { cart, evt => cart.apply(evt) }, eventAdapter: cartEvents,
        journal: stores.journal)
        .withSnapshots(stores.snapshots, cartCodec)
        // 每 10 个事件快照一次、保留 2 份；结账时也快照（之后恢复只读快照）
        .withRetention(RetentionCriteria.snapshotEvery(10, 2))
        .snapshotWhen({ cart, evt, _ =>
            match (evt) {
                case CheckedOut(_) => true
                case _ => false
            }
        })
        .receiveSignal({ cart, signal =>
            match (signal) {
                case r: RecoveryCompleted =>
                    log("  ${cartId} 恢复完成：序号 ${r.highestSequenceNr}，${cart.items.size} 种商品${if (cart.isCheckedOut()) { "（已结账）" } else { "" }}")
                case s: SnapshotCompleted => log("  ${cartId} 快照已保存：序号 ${s.sequenceNr}")
                case _ => ()
            }
        })
        .behavior()
}

// ======================= 用户资料（持久状态） =======================

enum ProfileCmd {
    | SetName(String, TypedActorRef<String>)
    | Forget(TypedActorRef<String>)
    | Show(TypedActorRef<String>)
}

func profileBehavior(userId: String, stores: Stores): TypedBehavior<ProfileCmd> {
    DurableStateBehavior<ProfileCmd, String>(PersistenceId.of("Profile", userId), emptyState: "(匿名)",
        commandHandler: { name, cmd, effect =>
            match (cmd) {
                case SetName(n, replyTo) => effect.persist(n).thenReply(replyTo, { s => s })
                case Forget(replyTo) => effect.delete().thenReply(replyTo, { s => s })
                case Show(replyTo) => effect.reply(replyTo, name)
            }
        }, codec: FunctionStateCodec<String>(encode: { s => s }, decode: { s => s }), store: stores.profiles).behavior()
}

// ======================= 自检 =======================

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

func ask(ref: TypedActorRef<CartCmd>, f: (TypedActorRef<CartReply>) -> CartCmd): CartReply {
    TypedAsk.ask<CartCmd, CartReply>(ref, f, ASK_TIMEOUT).get(ASK_TIMEOUT)
}

func cartOf(reply: CartReply): Cart {
    match (reply) {
        case Accepted(c) => c
        case Rejected(why) => throw IllegalStateException("意外被拒绝：${why}")
    }
}

func isRejected(reply: CartReply): Bool {
    match (reply) {
        case Rejected(_) => true
        case _ => false
    }
}

func spawnCarts(system: CActorSystem, stores: Stores, verbose: Bool): Array<TypedActorRef<CartCmd>> {
    Array<TypedActorRef<CartCmd>>(CARTS, { i =>
        let log: (String) -> Unit = if (verbose && i == 0) { { m: String => println(m) } } else { { _: String => () } }
        system.typedActorOf(cartBehavior("cart-${i}", stores, log), "cart-${i}")
    })
}

main(args: Array<String>): Int64 {
    let dir = if (args.size > 0) { args[0] } else { "/tmp/cactor-typed-persistence-demo-${MonoTime.now().hashCode()}" }
    if (exists(dir)) {
        remove(dir, recursive: true)
    }
    let c = Checker()
    println("=== 类型化持久化示例（数据目录 ${dir}，持久化 IO 线程 ${PersistenceIo.threads()}）===")

    // ---------- 第一个系统：并发加购 ----------
    println("\n[1] 第一个系统：${CARTS} 个购物车并发加购，每个 ${ITEMS_PER_CART} 件")
    let stores1 = Stores(dir)
    let sys1 = CActorSystemFactory.create("typed-persistence-1")
    let carts1 = spawnCarts(sys1, stores1, true)
    let started = MonoTime.now()
    // 全部 ask 先发出去再等：同一购物车的命令在写入期间暂存、按顺序落盘，不同购物车的写入在 IO 线程上并行
    let pending = ArrayList<TypedFuture<CartReply>>()
    for (k in 0..ITEMS_PER_CART) {
        for (i in 0..CARTS) {
            pending.add(TypedAsk.ask<CartCmd, CartReply>(carts1[i], { r => AddItem("sku-${k}", k + 1, r) }, ASK_TIMEOUT))
        }
    }
    var accepted = 0
    for (f in pending) {
        if (!isRejected(f.get(ASK_TIMEOUT))) {
            accepted++
        }
    }
    let elapsed = (MonoTime.now() - started).toMilliseconds()
    println("  ${accepted} 条加购在 ${elapsed} ms 内全部落盘（每条事件 fsync 后才回复）")
    c.check(accepted == CARTS * ITEMS_PER_CART, "全部加购被接受")
    c.check(isRejected(ask(carts1[0], { r => AddItem("sku-0", 1, r) })), "重复加购被拒绝")
    c.check(isRejected(ask(carts1[0], { r => AddItem("sku-x", 0, r) })), "数量非法被拒绝")
    let afterRemove = cartOf(ask(carts1[0], { r => RemoveItem("sku-3", r) }))
    c.check(afterRemove.items.size == ITEMS_PER_CART - 1 && !afterRemove.has("sku-3"), "移除商品后回复里已是新状态")

    let profiles1 = sys1.typedActorOf(profileBehavior("u-1", stores1), "profile-u-1")
    TypedAsk.ask<ProfileCmd, String>(profiles1, { r => SetName("Alice", r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    TypedAsk.ask<ProfileCmd, String>(profiles1, { r => SetName("Alice Liddell", r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    sys1.terminate()
    // 关闭系统不等在途的持久化 IO（例如异步保存的快照）；换下一个系统之前等它们落盘
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "系统 1 关闭后在途的持久化 IO 全部完成")
    stores1.close()

    // ---------- 第二个系统：从磁盘恢复，结账 ----------
    println("\n[2] 第二个系统：从磁盘恢复，cart-0 结账")
    let stores2 = Stores(dir)
    let sys2 = CActorSystemFactory.create("typed-persistence-2")
    let carts2 = spawnCarts(sys2, stores2, true)
    var intact = 0
    for (i in 0..CARTS) {
        let cart = cartOf(ask(carts2[i], { r => GetCart(r) }))
        let expectedItems = if (i == 0) { ITEMS_PER_CART - 1 } else { ITEMS_PER_CART }
        if (cart.items.size == expectedItems && !cart.isCheckedOut()) {
            intact++
        }
    }
    c.check(intact == CARTS, "${CARTS} 个购物车全部恢复、商品一件不少")
    let checkedOut = cartOf(ask(carts2[0], { r => Checkout(r) }))
    c.check(checkedOut.isCheckedOut(), "cart-0 结账成功")
    c.check(isRejected(ask(carts2[0], { r => AddItem("late", 1, r) })), "结账后加购被拒绝")
    let profiles2 = sys2.typedActorOf(profileBehavior("u-1", stores2), "profile-u-1")
    let name = TypedAsk.ask<ProfileCmd, String>(profiles2, { r => Show(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    c.check(name == "Alice Liddell", "用户资料恢复为最后一次写入：${name}")
    TypedAsk.ask<ProfileCmd, String>(profiles2, { r => Forget(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    sys2.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "系统 2 关闭后在途的持久化 IO 全部完成")
    stores2.close()

    // ---------- 第三个系统：快照恢复，删除不回退 ----------
    println("\n[3] 第三个系统：cart-0 从结账快照恢复；用户资料删除后修订号不回退")
    let stores3 = Stores(dir)
    let sys3 = CActorSystemFactory.create("typed-persistence-3")
    let carts3 = spawnCarts(sys3, stores3, true)
    let cart0 = cartOf(ask(carts3[0], { r => GetCart(r) }))
    c.check(cart0.isCheckedOut() && cart0.items.size == ITEMS_PER_CART - 1, "cart-0 恢复为已结账，${cart0.items.size} 种商品")
    let snap = stores3.snapshots.getLatestSnapshot(PersistenceId.of("ShoppingCart", "cart-0").id)
    c.check(snap.isSome() && snap.getOrThrow().getSequenceNumber() == ITEMS_PER_CART + 2,
        "结账时保存了快照（序号 ${ITEMS_PER_CART + 2}）")
    let profiles3 = sys3.typedActorOf(profileBehavior("u-1", stores3), "profile-u-1")
    let forgotten = TypedAsk.ask<ProfileCmd, String>(profiles3, { r => Show(r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    let pid = PersistenceId.of("Profile", "u-1").id
    c.check(forgotten == "(匿名)" && stores3.profiles.revisionOf(pid) == 3, "删除后为空状态，修订号 3（墓碑）")
    TypedAsk.ask<ProfileCmd, String>(profiles3, { r => SetName("Bob", r) }, ASK_TIMEOUT).get(ASK_TIMEOUT)
    c.check(stores3.profiles.revisionOf(pid) == 4, "之后的写入接在墓碑之后：修订号 4")
    sys3.terminate()
    c.check(PersistenceIo.awaitIdle(Duration.second * 30), "系统 3 关闭后在途的持久化 IO 全部完成")
    stores3.close()
    remove(dir, recursive: true)

    if (c.failures == 0) {
        println("\n全部校验通过")
        0
    } else {
        println("\n${c.failures} 项校验失败")
        1
    }
}
