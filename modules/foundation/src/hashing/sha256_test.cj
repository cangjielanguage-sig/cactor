/*
 * SHA-256 / HMAC-SHA256 标准向量（FIPS 180-4 示例、RFC 4231 测试用例 1、2、6、7）
 */
package cactor_foundation.hashing

import std.unittest.*
import std.unittest.testmacro.*

func bytesRepeat(b: UInt8, n: Int64): Array<UInt8> {
    Array<UInt8>(n, repeat: b)
}

@Test
public class Sha256Test {
    @TestCase
    func sha256StandardVectors(): Unit {
        @Expect(toHex(sha256(Array<UInt8>())), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        @Expect(toHex(sha256("abc".toArray())), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        // 56 字节：尾块需要两块
        @Expect(toHex(sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".toArray())),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
        // 100 万个 'a'
        @Expect(toHex(sha256(bytesRepeat(0x61, 1_000_000))),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0")
    }

    @TestCase
    func hmacRfc4231Vectors(): Unit {
        // 用例 1
        @Expect(toHex(hmacSha256(bytesRepeat(0x0b, 20), "Hi There".toArray())),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7")
        // 用例 2：密钥短于块长
        @Expect(toHex(hmacSha256("Jefe".toArray(), "what do ya want for nothing?".toArray())),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
        // 用例 6：密钥长于块长，先做摘要
        @Expect(toHex(hmacSha256(bytesRepeat(0xaa, 131), "Test Using Larger Than Block-Size Key - Hash Key First".toArray())),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54")
        // 用例 7：密钥与消息都长于块长
        @Expect(toHex(hmacSha256(bytesRepeat(0xaa, 131),
            "This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.".toArray())),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2")
    }

    @TestCase
    func hexAndConstantTimeEquals(): Unit {
        let d = sha256("x".toArray())
        @Expect(constantTimeEquals(fromHex(toHex(d)).getOrThrow(), d), true)
        @Expect(toHex(fromHex("ABcd").getOrThrow()), "abcd")
        @Expect(fromHex("abc").isNone(), true)
        @Expect(fromHex("zz").isNone(), true)
        let other = d.clone()
        other[31] = other[31] ^ 1
        @Expect(constantTimeEquals(d, other), false)
        @Expect(constantTimeEquals(d, d[0..31]), false)
    }
}
