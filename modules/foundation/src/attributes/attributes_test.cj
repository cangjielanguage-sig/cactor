package cactor_foundation.attributes

import std.unittest.*
import std.unittest.testmacro.*

class Box {
    var n: Int64 = 0
}

@Test
public class AttributesTest {
    @TestCase
    func putGetTyped(): Unit {
        let a = Attributes()
        let k1 = AttributeKey<Int64>("n")
        let k2 = AttributeKey<String>("s")
        a.put(k1, 42)
        a.put(k2, "x")
        @Expect(a.get(k1) ?? 0, 42)
        @Expect(a.get(k2) ?? "", "x")
        @Expect(a.contains(k1), true)
        a.remove(k1)
        @Expect(a.get(k1).isNone(), true)
    }

    @TestCase
    func keysWithSameNameAreDistinct(): Unit {
        let a = Attributes()
        let k1 = AttributeKey<Int64>("same")
        let k2 = AttributeKey<Int64>("same")
        a.put(k1, 1)
        @Expect(a.get(k2).isNone(), true)
    }

    @TestCase
    func computeIfAbsentCreatesOnce(): Unit {
        let a = Attributes()
        let k = AttributeKey<Box>("box")
        let b1 = a.computeIfAbsent(k, { => Box() })
        b1.n = 7
        let b2 = a.computeIfAbsent(k, { => Box() })
        @Expect(b2.n, 7)
    }
}
