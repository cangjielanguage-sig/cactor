/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - @Message 宏（自 macros.cj 迁移，对照 docs/plan/dsl20261030.md §8.3）
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
                    throw ASTException("@Message: ${className}.${fieldName} 需要显式写出类型")
                }
                var hasInit = false
                try {
                    hasInit = v.expr.toTokens().size > 0
                } catch (e: Exception) {
                    hasInit = false
                }
                if (hasInit && v.keyword.kind == TokenKind.LET) {
                    throw ASTException("@Message: ${className}.${fieldName} 是带初始值的 let 字段，反序列化时无法赋值（改为 var 或在 init 中赋值）")
                }
                let suffix = match (typeText) {
                    case "String" => "String"
                    case "Int64" => "Int64"
                    case "Float64" => "Float64"
                    case "Bool" => "Bool"
                    case "Array<String>" => "Strings"
                    case "Array<Int64>" => "Int64s"
                    case "Array<Float64>" => "Float64s"
                    case "Array<Bool>" => "Bools"
                    case _ => throw ASTException(
                        "@Message: ${className}.${fieldName} 的类型 ${typeText} 不支持（只支持 String、Int64、Float64、Bool 及其 Array）")
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
