/*
 * CActor Foundation - CRC-32（IEEE 802.3 / zlib 多项式 0xEDB88320，反射输入输出）
 *
 * 持久化日志（Journal v2）每条记录带 CRC32：恢复时据此区分"写到一半的尾部"与完整记录，
 * 并发现中间损坏（P6）。查表法每字节一次查表与移位；表在首次使用时生成（全局 let 初始化一次）。
 * 与 zlib crc32() 结果一致，标准校验值 crc32("123456789") = 0xCBF43926。
 */
package cactor_foundation.hashing

let CRC32_TABLE: Array<UInt32> = buildCrc32Table()

func buildCrc32Table(): Array<UInt32> {
    let table = Array<UInt32>(256, repeat: 0)
    for (n in 0..256) {
        var c = UInt32(n)
        for (_ in 0..8) {
            c = if ((c & 1) != 0) { 0xEDB88320u32 ^ (c >> 1) } else { c >> 1 }
        }
        table[n] = c
    }
    table
}

/** 在已有校验值 crc 上继续累加 data[start..end]（分段计算时把上一段的结果传入；首段传 0） */
public func crc32Update(crc: UInt32, data: Array<UInt8>, start: Int64, end: Int64): UInt32 {
    var c = crc ^ 0xFFFFFFFFu32
    for (i in start..end) {
        c = CRC32_TABLE[Int64((c ^ UInt32(data[i])) & 0xFF)] ^ (c >> 8)
    }
    c ^ 0xFFFFFFFFu32
}

/** 整个数组的 CRC-32 */
public func crc32(data: Array<UInt8>): UInt32 {
    crc32Update(0, data, 0, data.size)
}

/** data[start..end] 的 CRC-32 */
public func crc32(data: Array<UInt8>, start: Int64, end: Int64): UInt32 {
    crc32Update(0, data, start, end)
}
