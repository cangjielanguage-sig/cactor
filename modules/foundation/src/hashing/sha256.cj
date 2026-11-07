/*
 * CActor Foundation - SHA-256 与 HMAC-SHA256（远程握手鉴权用，E7′）
 *
 * 为什么自己实现：标准库 std 没有摘要算法，stdx.crypto 依赖 OpenSSL 动态库，会给每个使用远程的
 * 应用增加部署依赖；握手每条连接只算两三次 HMAC，纯仓颉实现的性能足够。
 * 按 FIPS 180-4 / RFC 2104 实现，用例对照 FIPS 与 RFC 4231 的标准向量。
 * 32 位加法按 2^32 取模：用 @OverflowWrapping 显式声明回绕语义（仓颉默认检查整数溢出）。
 */
package cactor_foundation.hashing

let SHA256_K: Array<UInt32> = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
]

/** SHA-256 的块长（字节），HMAC 的密钥按它填充 */
public let SHA256_BLOCK_SIZE: Int64 = 64

func rotr(x: UInt32, n: UInt32): UInt32 {
    (x >> n) | (x << (32 - n))
}

/** 处理一个 64 字节块，更新状态 h（8 个字） */
@OverflowWrapping
func sha256Block(h: Array<UInt32>, block: Array<UInt8>, offset: Int64, w: Array<UInt32>): Unit {
    for (t in 0..16) {
        let i = offset + t * 4
        w[t] = (UInt32(block[i]) << 24) | (UInt32(block[i + 1]) << 16) | (UInt32(block[i + 2]) << 8) | UInt32(block[i + 3])
    }
    for (t in 16..64) {
        let s0 = rotr(w[t - 15], 7) ^ rotr(w[t - 15], 18) ^ (w[t - 15] >> 3)
        let s1 = rotr(w[t - 2], 17) ^ rotr(w[t - 2], 19) ^ (w[t - 2] >> 10)
        w[t] = w[t - 16] + s0 + w[t - 7] + s1
    }
    var a = h[0]
    var b = h[1]
    var c = h[2]
    var d = h[3]
    var e = h[4]
    var f = h[5]
    var g = h[6]
    var hh = h[7]
    for (t in 0..64) {
        let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
        let ch = (e & f) ^ ((!e) & g)
        let t1 = hh + s1 + ch + SHA256_K[t] + w[t]
        let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
        let maj = (a & b) ^ (a & c) ^ (b & c)
        let t2 = s0 + maj
        hh = g
        g = f
        f = e
        e = d + t1
        d = c
        c = b
        b = a
        a = t1 + t2
    }
    h[0] = h[0] + a
    h[1] = h[1] + b
    h[2] = h[2] + c
    h[3] = h[3] + d
    h[4] = h[4] + e
    h[5] = h[5] + f
    h[6] = h[6] + g
    h[7] = h[7] + hh
}

/** SHA-256 摘要（32 字节） */
public func sha256(data: Array<UInt8>): Array<UInt8> {
    let h: Array<UInt32> = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19]
    let w = Array<UInt32>(64, repeat: 0)
    let full = data.size / 64
    for (i in 0..full) {
        sha256Block(h, data, i * 64, w)
    }
    // 尾块：剩余字节 + 0x80 + 补零 + 64 位大端比特长度（一块或两块）
    let rest = data.size - full * 64
    let tailBlocks = if (rest + 9 > 64) { 2 } else { 1 }
    let tail = Array<UInt8>(tailBlocks * 64, repeat: 0)
    data.copyTo(tail, full * 64, 0, rest)
    tail[rest] = 0x80
    let bits = UInt64(data.size) * 8
    for (k in 0..8) {
        tail[tail.size - 1 - k] = UInt8((bits >> UInt64(k * 8)) & 0xFF)
    }
    for (i in 0..tailBlocks) {
        sha256Block(h, tail, i * 64, w)
    }
    let out = Array<UInt8>(32, repeat: 0)
    for (i in 0..8) {
        out[i * 4] = UInt8((h[i] >> 24) & 0xFF)
        out[i * 4 + 1] = UInt8((h[i] >> 16) & 0xFF)
        out[i * 4 + 2] = UInt8((h[i] >> 8) & 0xFF)
        out[i * 4 + 3] = UInt8(h[i] & 0xFF)
    }
    out
}

/** HMAC-SHA256（RFC 2104）：密钥长于块长时先做摘要，再补零到块长 */
public func hmacSha256(key: Array<UInt8>, message: Array<UInt8>): Array<UInt8> {
    let k0 = Array<UInt8>(SHA256_BLOCK_SIZE, repeat: 0)
    let k = if (key.size > SHA256_BLOCK_SIZE) { sha256(key) } else { key }
    k.copyTo(k0, 0, 0, k.size)
    let inner = Array<UInt8>(SHA256_BLOCK_SIZE + message.size, repeat: 0)
    let outer = Array<UInt8>(SHA256_BLOCK_SIZE + 32, repeat: 0)
    for (i in 0..SHA256_BLOCK_SIZE) {
        inner[i] = k0[i] ^ 0x36
        outer[i] = k0[i] ^ 0x5c
    }
    message.copyTo(inner, 0, SHA256_BLOCK_SIZE, message.size)
    sha256(inner).copyTo(outer, 0, SHA256_BLOCK_SIZE, 32)
    sha256(outer)
}

/** 常量时间比较：耗时只与长度有关，不因第一个不同字节的位置泄露信息（校验 MAC 用） */
public func constantTimeEquals(a: Array<UInt8>, b: Array<UInt8>): Bool {
    if (a.size != b.size) {
        return false
    }
    var diff: UInt8 = 0
    for (i in 0..a.size) {
        diff = diff | (a[i] ^ b[i])
    }
    diff == 0
}

let HEX_DIGITS: Array<UInt8> = "0123456789abcdef".toArray()

/** 小写十六进制 */
public func toHex(bytes: Array<UInt8>): String {
    let out = Array<UInt8>(bytes.size * 2, repeat: 0)
    for (i in 0..bytes.size) {
        out[i * 2] = HEX_DIGITS[Int64(bytes[i] >> 4)]
        out[i * 2 + 1] = HEX_DIGITS[Int64(bytes[i] & 0x0F)]
    }
    String.fromUtf8(out)
}

/** 解析十六进制（大小写均可）；格式不对返回 None */
public func fromHex(text: String): Option<Array<UInt8>> {
    let s = text.toArray()
    if (s.size % 2 != 0) {
        return None<Array<UInt8>>
    }
    let out = Array<UInt8>(s.size / 2, repeat: 0)
    for (i in 0..out.size) {
        match ((hexValue(s[i * 2]), hexValue(s[i * 2 + 1]))) {
            case (Some(hi), Some(lo)) => out[i] = (hi << 4) | lo
            case _ => return None<Array<UInt8>>
        }
    }
    Some(out)
}

func hexValue(c: UInt8): Option<UInt8> {
    if (c >= 0x30 && c <= 0x39) {
        Some(c - 0x30)
    } else if (c >= 0x61 && c <= 0x66) {
        Some(c - 0x61 + 10)
    } else if (c >= 0x41 && c <= 0x46) {
        Some(c - 0x41 + 10)
    } else {
        None<UInt8>
    }
}
