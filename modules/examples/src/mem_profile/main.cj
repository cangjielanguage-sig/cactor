/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 每 Actor 内存归因微基准
 *
 * 逐项测量构成一个 Actor Cell 的各个对象的存活堆占用，用于定位"每 Actor 3.4KB"的来源。
 *
 * 测量方法与可靠性：堆用量在 GC 后按 region（256KB 粒度）结算，小批量测量会因 GC 粒度与
 * 碎片产生噪声（甚至负值）。因此**每项单独跑一个进程**、一次采样、数量取足（默认 20 万），
 * 使增量远大于 GC 粒度。用 all 参数可以一次测完所有项，但结果只作参考，精确值请用单项模式。
 *
 * 运行：
 *   ./target/release/bin/cactor_examples.mem_profile all [每项个数]
 *   ./target/release/bin/cactor_examples.mem_profile <项名> [每项个数]
 */
package cactor_examples.mem_profile

import std.sync.*
import std.convert.*
import std.runtime.{gc, getUsedHeapSize}
import std.collection.*
import cactor_core.actor.{Actor, ActorPath, Props, PropsFactory}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}
import cactor_foundation.queue.{ConcurrentHashMap}

class Dummy <: Actor {
    public prop name: String { get() { "dummy" } }
    public prop description: String { get() { "dummy" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

// 全局保活列表：测量期间对象必须可达，否则会被 GC 回收、堆增量测得 0
let gKeep = ArrayList<Any>()

/** 测量一段分配代码产生的存活堆增量（字节/个） */
func measure(n: Int64, label: String, body: (Int64) -> Unit): Unit {
    gc(heavy: true)
    let before = getUsedHeapSize()
    body(n)
    gc(heavy: true)
    let after = getUsedHeapSize()
    println("${label}\t${(after - before) / n} 字节/个")
}

func itemAtomicInt64(n: Int64): Unit { for (_ in 0..n) { gKeep.add(AtomicInt64(0)) } }
func itemAtomicBool(n: Int64): Unit { for (_ in 0..n) { gKeep.add(AtomicBool(false)) } }
func itemMutex(n: Int64): Unit { for (_ in 0..n) { gKeep.add(Mutex()) } }
func itemArrayList0(n: Int64): Unit { for (_ in 0..n) { gKeep.add(ArrayList<Int64>(0)) } }
func itemHashMap0(n: Int64): Unit { for (_ in 0..n) { gKeep.add(HashMap<String, Int64>(0)) } }
func itemArrayEmpty(n: Int64): Unit { for (_ in 0..n) { gKeep.add(Array<Option<Message>>()) } }
func itemArray8(n: Int64): Unit { for (_ in 0..n) { gKeep.add(Array<Option<Message>>(8, repeat: None<Message>)) } }
func itemString(n: Int64): Unit { for (_ in 0..n) { gKeep.add("c${n}") } }
func itemActorPath(n: Int64): Unit { for (_ in 0..n) { gKeep.add(ActorPath("sys", "/user/p/c${n}")) } }
func itemProps(n: Int64): Unit { for (_ in 0..n) { gKeep.add(PropsFactory.create<Actor>({ => Dummy() })) } }
func itemClosure(n: Int64): Unit { for (_ in 0..n) { gKeep.add({ => Dummy() }) } }
func itemActor(n: Int64): Unit { for (_ in 0..n) { gKeep.add(Dummy()) } }
func itemRefSlot(n: Int64): Unit { for (_ in 0..n) { gKeep.add(Array<Int64>(1, repeat: 0)) } }
// 定长字符串：测"每字符成本 + 固定开销"，为路径字符串存废决策提供依据
func itemStringLen(n: Int64, len: Int64): Unit {
    for (_ in 0..n) {
        let sb = StringBuilder()
        for (_ in 0..len) { sb.append("c") }
        gKeep.add(sb.toString())
    }
}
// 并发容器每条目成本：与普通 HashMap 对比，判断顶层注册表是否值得换实现
func itemConcurrentMap(n: Int64): Unit {
    let m = ConcurrentHashMap<String, Int64>()
    for (i in 0..n) { m.put("c${i}", i) }
    gKeep.add(m)
}
func itemPlainMap(n: Int64): Unit {
    let m = HashMap<String, Int64>(n)
    for (i in 0..n) { m.add("c${i}", i) }
    gKeep.add(m)
}

func itemAtomicOptionRef(n: Int64): Unit {
    for (_ in 0..n) { gKeep.add(AtomicOptionReference<Dummy>()) }
}
// 插值构造的定长字符串（与运行时实际构造方式一致）
func itemStringInterp(n: Int64, len: Int64): Unit {
    for (i in 0..n) {
        let pad = "xxxxxxxxxxxxxxxx"[0..len]
        gKeep.add("c${i}${pad}")
    }
}
// HashMap 的 String 键是否复制：n 个字符串 + 一个装下它们的 HashMap。
// 与"只有 n 个字符串"对比，差值即每个键的额外成本（若接近一个字符串的长度说明 HashMap 复制了键）
func itemMapKeys(n: Int64): Unit {
    let map = HashMap<String, Int64>(n)
    for (i in 0..n) {
        let k = "c${i}padpadpad"
        map.add(k, i)
        gKeep.add(k)
    }
    gKeep.add(map)
}

main(args: Array<String>): Int64 {
    let which = if (args.size > 0) { args[0] } else { "all" }
    let n = if (args.size > 1) { Int64.parse(args[1]) } else { 200_000 }
    println("=== CActor 每 Actor 内存归因（每项 ${n} 份）===")
    match (which) {
        case "atomicint64" => measure(n, "AtomicInt64", itemAtomicInt64)
        case "atomicbool" => measure(n, "AtomicBool", itemAtomicBool)
        case "mutex" => measure(n, "Mutex", itemMutex)
        case "arraylist0" => measure(n, "ArrayList<Int64>(0)", itemArrayList0)
        case "hashmap0" => measure(n, "HashMap<String,Int64>(0)", itemHashMap0)
        case "arrayempty" => measure(n, "Array<Option<Message>>() 空", itemArrayEmpty)
        case "array8" => measure(n, "Array<Option<Message>>(8)", itemArray8)
        case "string" => measure(n, "String(\"c123\")", itemString)
        case "actorpath" => measure(n, "ActorPath", itemActorPath)
        case "props" => measure(n, "Props<Actor>", itemProps)
        case "closure" => measure(n, "闭包 () -> Actor", itemClosure)
        case "actor" => measure(n, "Actor 实例", itemActor)
        case "refslot" => measure(n, "引用数组槽位（1 槽）", itemRefSlot)
        case "atomicoptionref" => measure(n, "AtomicOptionReference", itemAtomicOptionRef)
        case "interp7" => measure(n, "插值 String(7 字符)", { x => itemStringInterp(x, 3) })
        case "interp21" => measure(n, "插值 String(21 字符)", { x => itemStringInterp(x, 17) })
        case "concurrentmap" => measure(n, "ConcurrentHashMap 每条目", itemConcurrentMap)
        case "plainmap" => measure(n, "HashMap 每条目", itemPlainMap)
        case "mapkeys" => measure(n, "HashMap 键（n 个字符串 + 1 个 map）", itemMapKeys)
        case "string8" => measure(n, "String(8 字符)", { x => itemStringLen(x, 8) })
        case "string16" => measure(n, "String(16 字符)", { x => itemStringLen(x, 16) })
        case "string24" => measure(n, "String(24 字符)", { x => itemStringLen(x, 24) })
        case "string32" => measure(n, "String(32 字符)", { x => itemStringLen(x, 32) })
        case _ =>
            measure(n, "AtomicInt64", itemAtomicInt64)
            measure(n, "AtomicBool", itemAtomicBool)
            measure(n, "Mutex", itemMutex)
            measure(n, "ArrayList<Int64>(0)", itemArrayList0)
            measure(n, "HashMap<String,Int64>(0)", itemHashMap0)
            measure(n, "Array<Option<Message>>() 空", itemArrayEmpty)
            measure(n, "Array<Option<Message>>(8)", itemArray8)
            measure(n, "String(\"c123\")", itemString)
            measure(n, "ActorPath", itemActorPath)
            measure(n, "Props<Actor>", itemProps)
            measure(n, "闭包 () -> Actor", itemClosure)
            measure(n, "Actor 实例", itemActor)
            measure(n, "引用数组槽位（1 槽）", itemRefSlot)
    }
    return 0
}
