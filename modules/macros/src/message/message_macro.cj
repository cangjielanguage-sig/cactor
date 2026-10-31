/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - @Message 宏（自 macros.cj 迁移，对照 docs/plan/dsl20261030.md §8.3）
 *
 * 生成：messageType()、二进制编解码 writeMessageFields / init(reader)、registerCodec()。
 *
 * 支持的字段类型（P2 增强后）：
 *   String、Int64、Float64、Bool
 *   Array<String>、Array<Int64>、Array<Float64>、Array<Bool>
 *   Option<String>、Option<Int64>、Option<Float64>、Option<Bool>
 *   HashMap<String, String>、HashMap<String, Int64>、HashMap<String, Float64>、HashMap<String, Bool>
 *
 * 报错：字段级错误用 diagReport 定位到具体字段（带行列），类级错误用 ASTException。
 */
macro package cactor_macros.message

import std.ast.*
import std.collection.*

public macro Message(input: Tokens): Tokens {
    let decl = parseDecl(input)
    let cls = match (decl) {
        case c: ClassDecl => c
        case _ => throw ASTException("@Message 只能用于 class")
    }
    let className = cls.identifier.value
    // 没有泛型参数时访问 genericParam 会抛异常
    let isGeneric = try {
        cls.genericParam.parameters.size > 0
    } catch (e: ASTException) {
        false
    }
    if (isGeneric) {
        throw ASTException("@Message: ${className} 是泛型类，不支持")
    }
    var isMessage = false
    for (t in cls.superTypes) {
        if (t.toTokens().toString().replace(" ", "") == "Message") {
            isMessage = true
        }
    }
    if (!isMessage) {
        throw ASTException("@Message: ${className} 需要声明 <: Message")
    }
    var writes = quote()
    var reads = quote()
    var hasError = false
    for (d in cls.body.decls) {
        match (d) {
            case f: FuncDecl where f.identifier.value == "messageType" =>
                throw ASTException("@Message: ${className} 不能手写 messageType()，宏会生成为 \"${className}\"")
            case v: VarDecl =>
                var isStatic = false
                for (m in v.modifiers) {
                    if (m.keyword.kind == TokenKind.STATIC) {
                        isStatic = true
                    }
                }
                if (isStatic) {
                    continue
                }
                let fieldName = v.identifier.value
                let typeText = try {
                    v.declType.toTokens().toString().replace(" ", "")
                } catch (e: Exception) {
                    diagReport(
                        DiagReportLevel.ERROR,
                        Tokens(v.identifier),
                        "@Message: ${className}.${fieldName} 需要显式写出类型",
                        "例如：let ${fieldName}: String")
                    hasError = true
                    continue
                }
                var hasInit = false
                try {
                    hasInit = v.expr.toTokens().size > 0
                } catch (e: Exception) {
                    hasInit = false
                }
                if (hasInit && v.keyword.kind == TokenKind.LET) {
                    diagReport(
                        DiagReportLevel.ERROR,
                        Tokens(v.identifier),
                        "@Message: ${className}.${fieldName} 是带初始值的 let 字段，反序列化时无法赋值",
                        "改为 var，或在 init 中赋值")
                    hasError = true
                    continue
                }
                let suffix = codecSuffixOf(typeText)
                if (suffix.isEmpty()) {
                    diagReport(
                        DiagReportLevel.ERROR,
                        Tokens(v.identifier),
                        "@Message: ${className}.${fieldName} 的类型 ${typeText} 不支持",
                        "支持：String、Int64、Float64、Bool 及其 Array，Option<T>，HashMap<String, T>")
                    hasError = true
                    continue
                }
                let fid = v.identifier
                let writer = Token(TokenKind.IDENTIFIER, "write${suffix}")
                let reader = Token(TokenKind.IDENTIFIER, "read${suffix}")
                writes = quote($writes
                    __w.$writer(this.$fid)
                )
                reads = quote($reads
                    this.$fid = __r.$reader()
                )
            case _ => ()
        }
    }
    if (hasError) {
        throw ASTException("@Message: ${className} 存在不受支持的字段（详见上方诊断）")
    }
    let codecId = Token(TokenKind.IDENTIFIER, "${className}MessageCodec")
    let classId = cls.identifier
    let ds = cls.body.decls
    ds.add(parseDecl(quote(
        public func messageType(): String {
            $(className)
        }
    )))
    ds.add(parseDecl(quote(
        func writeMessageFields(__w: MessageFieldWriter): Unit {
            $writes
        }
    )))
    ds.add(parseDecl(quote(
        init(__r: MessageFieldReader) {
            $reads
        }
    )))
    ds.add(parseDecl(quote(
        public static func registerCodec(): Unit {
            MessageCodecRegistry.register($(className), $codecId())
        }
    )))
    cls.body.decls = ds
    let codec = quote(
        class $codecId <: MessageCodec {
            public func encode(message: Message): Array<UInt8> {
                let w = MessageFieldWriter()
                match (message as $classId) {
                    case Some(m) => m.writeMessageFields(w)
                    case None => throw SerializationException("${message.messageType()} 不是 " + $(className))
                }
                w.toArray()
            }

            public func decode(payload: Array<UInt8>): Message {
                let r = MessageFieldReader(payload)
                let m = $classId(r)
                if (!r.isAtEnd()) {
                    throw DeserializationException($(className) + " 的数据有多余字节")
                }
                m
            }
        }
    )
    quote(
        $(cls.toTokens())
        $codec
    )
}

/**
 * 字段类型 → 编解码方法后缀。
 * 例：Option<String> → OptionString（writeOptionString / readOptionString）；
 *     HashMap<String, Int64> → Int64Map（writeInt64Map / readInt64Map）。
 * 不支持的类型返回空串（由调用方报错）。
 */
func codecSuffixOf(typeText: String): String {
    match (typeText) {
        case "String" => "String"
        case "Int64" => "Int64"
        case "Float64" => "Float64"
        case "Bool" => "Bool"
        case "Array<String>" => "Strings"
        case "Array<Int64>" => "Int64s"
        case "Array<Float64>" => "Float64s"
        case "Array<Bool>" => "Bools"
        case "Option<String>" => "OptionString"
        case "Option<Int64>" => "OptionInt64"
        case "Option<Float64>" => "OptionFloat64"
        case "Option<Bool>" => "OptionBool"
        case "HashMap<String,String>" => "StringMap"
        case "HashMap<String,Int64>" => "Int64Map"
        case "HashMap<String,Float64>" => "Float64Map"
        case "HashMap<String,Bool>" => "BoolMap"
        case _ => ""
    }
}
