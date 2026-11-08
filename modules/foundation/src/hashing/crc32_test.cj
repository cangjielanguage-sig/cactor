/*
 * CRC-32 标准校验值（与 zlib crc32 一致）与分段计算
 */
package cactor_foundation.hashing

import std.unittest.*
import std.unittest.testmacro.*

@Test
public class Crc32Test {
    @TestCase
    func standardCheckValues(): Unit {
        @Expect(crc32(Array<UInt8>()), 0u32)
        @Expect(crc32("123456789".toArray()), 0xCBF43926u32)
        @Expect(crc32("The quick brown fox jumps over the lazy dog".toArray()), 0x414FA339u32)
        // 全零输入的 CRC 不为 0：日志尾部被零填充时，长度非零的零记录过不了校验
        @Expect(crc32(Array<UInt8>(32, repeat: 0)), 0x190A55ADu32)
    }

    @TestCase
    func incrementalEqualsWhole(): Unit {
        let data = "hello, journal v2 with crc32".toArray()
        let whole = crc32(data)
        let part = crc32Update(crc32(data, 0, 10), data, 10, data.size)
        @Expect(part, whole)
        @Expect(crc32(data, 3, 9), crc32(data[3..9]))
    }
}
