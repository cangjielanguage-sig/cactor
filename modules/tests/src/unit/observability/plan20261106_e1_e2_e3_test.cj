/*
 * CActor - E1/E2/E3 验收测试（docs/plan/20261106.md 阶段一）
 * - E1（C1）：JsonSerializer 真实解析——round-trip 含转义字符、Unicode、嵌套 JSON
 * - E2（C2/C3）：TimerMetric 平均值按真实次数、时间戳为真实时钟、注册表数量真实
 * - E3（B8）：持久化快照真实保存/恢复、persist 失败上抛且回退序号
 */
package cactor_tests.unit.observability

import std.unittest.*
import std.unittest.testmacro.*
import std.collection.*
import std.time.*
import cactor_foundation.serialization.{JsonSerializer, DeserializationException}
import cactor_foundation.monitoring.{TimerMetric, MetricRegistry, CounterMetric}
import cactor_persistence.{
    AbstractPersistentActor, PersistenceException, MemorySnapshotStore,
    MemoryEnhancedEventStore, SimpleEventPayload, PersistentEvent, Snapshot
}

// ---------- E1: JsonSerializer round-trip ----------

@Test
func testJsonRoundTripSimple() {
    let s = JsonSerializer()
    let data = s.serialize("hello world")
    @Assert(s.deserialize(data) == "hello world")
}

@Test
func testJsonRoundTripEscapes() {
    let s = JsonSerializer()
    // 覆盖全部转义类别：引号、反斜杠、斜杠、控制字符、多字节 UTF-8
    let cases = [
        "say \"hi\"", "back\\slash", "slash/ok", "line1\nline2",
        "tab\there", "cr\rend", "back\u{8}space", "form\u{c}feed",
        "中文内容", "emoji😀", ""
    ]
    for (input in cases) {
        let data = s.serialize(input)
        @Assert(s.deserialize(data) == input)
    }
}

@Test
func testJsonDeserializeUnicodeEscape() {
    let s = JsonSerializer()
    // 手工构造含 \uXXXX 转义的 JSON 再解析
    let raw = "{\"data\":\"\\u4e2d\\u6587\"}"
    @Assert(s.deserialize(raw.toArray()) == "中文")
}

@Test
func testJsonDeserializeRejectsMissingData() {
    let s = JsonSerializer()
    @ExpectThrows[DeserializationException](s.deserialize("{\"other\":1}".toArray()))
}

@Test
func testJsonParseIgnoresNestedDataKey() {
    let s = JsonSerializer()
    // "data" 字样出现在前面的字符串值里时，仍应解析顶层 data 字段
    let raw = "{\"meta\":\"has \\\"data\\\" inside\",\"data\":\"top-level\"}"
    @Assert(s.deserialize(raw.toArray()) == "top-level")
}

// ---------- E2: TimerMetric / MetricRegistry ----------

@Test
func testTimerAverageDividesByRealCount() {
    let t = TimerMetric("test.timer")
    t.recordTime(100.0)
    t.recordTime(200.0)
    t.recordTime(300.0)
    // 修复前：totalTime/10，这里会得到 60；修复后应为 (100+200+300)/3 = 200
    @Assert(t.getAverageTime() == 200.0)
    @Assert(t.getValue().value == 200.0)
}

@Test
func testTimerSingleRecord() {
    let t = TimerMetric("test.timer2")
    t.recordTime(100.0)
    // 修复前：1 次 100ms 报 10ms；修复后应报 100
    @Assert(t.getAverageTime() == 100.0)
}

@Test
func testTimerEmptyIsZero() {
    let t = TimerMetric("test.timer3")
    @Assert(t.getAverageTime() == 0.0)
}

@Test
func testMetricCountIsReal() {
    let r = MetricRegistry()
    @Assert(r.getMetricCount() == 0)
    r.register(CounterMetric("c1"))
    r.register(CounterMetric("c2"))
    r.register(TimerMetric("t1"))
    // 修复前恒返回 10
    @Assert(r.getMetricCount() == 3)
    @Assert(r.getMetricCount() == Int32(r.getAllMetricNames().size))
}

@Test
func testTimestampIsRealClock() {
    let t = TimerMetric("test.ts")
    t.recordTime(1.0)
    let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
    let stamped = t.getValue().timestamp
    // 时间戳应为真实时钟：与当前时间相差不超过 1 分钟（修复前恒为 2022 年的固定值）
    let diff = if (now > stamped) { now - stamped } else { stamped - now }
    @Assert(diff < 60_000)
}

// ---------- E3: 持久化快照真实保存 / persist 失败上抛 ----------

/** 探针持久化 Actor：记录恢复的事件与快照 */
class ProbePersistentActor <: AbstractPersistentActor {
    let pid: String
    var recoveredEvents: ArrayList<String>
    var recoveredSnapshot: Option<String>

    init(pid: String, es: MemoryEnhancedEventStore, ss: MemorySnapshotStore) {
        super(es, ss)
        this.pid = pid
        this.recoveredEvents = ArrayList<String>()
        this.recoveredSnapshot = None<String>
    }

    public func persistenceId(): String { pid }

    protected override func onRecoveryEvent(event: PersistentEvent): Unit {
        recoveredEvents.add(event.payload.serialize())
    }

    protected override func onRecoverySnapshot(snapshot: Snapshot): Unit {
        recoveredSnapshot = Some(snapshot.getStateData())
    }
}

@Test
func testSnapshotSavesRealDataAndRoundTrips() {
    let es = MemoryEnhancedEventStore()
    let ss = MemorySnapshotStore()
    let a = ProbePersistentActor("snap-1", es, ss)

    a.startRecovery()
    // 持久化 2 个事件（seq 1、2），再保存快照
    a.persist(SimpleEventPayload("e1", "d1"), { _ => () })
    a.persist(SimpleEventPayload("e2", "d2"), { _ => () })
    a.saveSnapshot("real-state-data")
    @Assert(a.getLastSequenceNr() == 2)

    // 直接从存储读回：状态数据必须是写入的真实值，而不是 "snapshot-data" 字面量
    let latest = ss.getLatestSnapshot("snap-1")
    @Assert(latest.isSome())
    let sn = latest.getOrThrow()
    @Assert(sn.getStateData() == "real-state-data")
    @Assert(sn.getSequenceNumber() == 2)
}

@Test
func testRecoveryFromSnapshotCarriesRealData() {
    let es = MemoryEnhancedEventStore()
    let ss = MemorySnapshotStore()
    let a1 = ProbePersistentActor("snap-2", es, ss)
    a1.startRecovery()
    a1.persist(SimpleEventPayload("e1", "d1"), { _ => () })
    a1.saveSnapshot("state-v1")

    // 新实例从同一存储恢复：应能拿到快照携带的真实状态数据
    let a2 = ProbePersistentActor("snap-2", es, ss)
    a2.startRecovery()
    @Assert(a2.recoveredSnapshot.isSome())
    @Assert(a2.recoveredSnapshot.getOrThrow() == "state-v1")
    @Assert(a2.getLastSequenceNr() == 1)
}

@Test
func testPersistFailureThrowsAndRollsBackSeq() {
    let ss = MemorySnapshotStore()
    let es = MemoryEnhancedEventStore()
    // 预置 "fail-1" 到版本 1，让 Actor 恢复到 seq=1
    es.saveEvents("fail-1",
        [PersistentEvent("fail-1", 1, SimpleEventPayload("e1", "d1"))], 0).get()

    let a = ProbePersistentActor("fail-1", es, ss)
    a.startRecovery()
    @Assert(a.getLastSequenceNr() == 1)

    // 恢复后外部写入把事件流推进到版本 2，本地视图落后 → persist 的
    // expectedVersion=1 与存储版本 2 冲突。
    // 修复前：persist 丢弃 saveEvents 的 Future，冲突被静默吞掉；修复后必须上抛。
    es.saveEvents("fail-1",
        [PersistentEvent("fail-1", 2, SimpleEventPayload("e1ext", "d1"))], 1).get()
    @ExpectThrows[PersistenceException](a.persist(SimpleEventPayload("e2", "d2"), { _ => () }))
    // 序号必须回退，保持状态一致（失败的事件不占号）
    @Assert(a.getLastSequenceNr() == 1)
}
