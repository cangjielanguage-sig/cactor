/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - @actor 宏（对照 docs/plan/dsl20261030.md §8.1）
 *
 * 调用形态（属性宏，参数必须用方括号）：
 *
 *   @actor[]                                          // 无协议声明
 *   @actor[cases = [PlaceOrder, GetOrder, Cancel]]    // 显式协议清单 → 编译期穷尽性检查
 *   @actor[protocol = OrderMsg]                       // 单一协议类型（枚举协议写法）
 *
 * 规则：
 * - 只能用于 class；不能手写 receive；同一消息类型只能有一个 onXxx。
 * - 处理方法命名 onXxx，签名允许：
 *     func onXxx(msg: T)                       （Unit 返回，视为 Handled）
 *     func onXxx(msg: T, ctx: ActorContext)    （Unit 返回，视为 Handled）
 *     func onXxx(msg: T, ctx: ActorContext): MessageResult
 * - 自动生成：按类型分派的 receive、缺省的 name / description。
 * - MessageResult / Unhandled 语义与手写 receive 完全一致（等价性测试保证）。
 */
macro package cactor_macros.actormacro

import std.ast.*
import std.collection.*
import cactor_macros.common.*

public macro actor(attr: Tokens, input: Tokens): Tokens {
    transformActor(attr, input, "@actor")
}

// 兼容别名：@actorReceive 等价于 @actor（保留一个版本周期后删除）
public macro actorReceive(attr: Tokens, input: Tokens): Tokens {
    transformActor(attr, input, "@actorReceive")
}

/** 一个处理方法的信息 */
class HandlerInfo {
    let methodName: String
    let typeName: String
    let rawType: String
    let arity: Int64

    init(methodName: String, typeName: String, rawType: String, arity: Int64) {
        this.methodName = methodName
        this.typeName = typeName
        this.rawType = rawType
        this.arity = arity
    }
}

func transformActor(attr: Tokens, input: Tokens, macroName: String): Tokens {
    let decl = parseDecl(input)
    let cls = match (decl) {
        case c: ClassDecl => c
        case _ => throw ASTException("${macroName} 只能用于 class")
    }
    let className = cls.identifier.value
    let actorAttr = parseActorAttr(attr, className, macroName)
    var arms = quote()
    var handlerCount = 0
    let handlers = ArrayList<HandlerInfo>()
    let seenTypes = ArrayList<String>()
    var hasName = false
    var hasDescription = false
    for (d in cls.body.decls) {
        match (d) {
            case f: FuncDecl =>
                let fname = f.identifier.value
                if (fname == "receive") {
                    throw ASTException("${macroName}: ${className} 已经定义了 receive（宏会生成，请删除或改名）")
                }
                if (fname.startsWith("on") && fname.size > 2 && f.funcParams.size >= 1) {
                    checkHandlerSignature(f, className, macroName)
                    let t = f.funcParams[0].paramType
                    let rawType = t.toTokens().toString().replace(" ", "")
                    let typeName = normalizeTypeName(rawType)
                    for (seen in seenTypes) {
                        if (seen == typeName) {
                            throw ASTException("${macroName}: ${className} 中有多个处理 ${typeName} 的方法（${fname}），后一个永远不会被调用")
                        }
                    }
                    seenTypes.add(typeName)
                    handlers.add(HandlerInfo(fname, typeName, rawType, f.funcParams.size))
                    let id = f.identifier
                    // 按返回类型区分：MessageResult 返回 → 原样；Unit 返回 → 补 Handled。
                    // 返回类型在 FuncDecl 上：无显式返回类型（Unit）时 returnType 抛异常或为空。
                    var isResult = false
                    try {
                        let rt = f.declType.toTokens().toString().replace(" ", "")
                        if (rt == "MessageResult") {
                            isResult = true
                        }
                    } catch (e: Exception) { }
                    if (f.funcParams.size == 1) {
                        if (isResult) {
                            arms = quote($arms
                                case m: $t => this.$id(m)
                            )
                        } else {
                            arms = quote($arms
                                case m: $t => this.$id(m); MessageResult.Handled
                            )
                        }
                    } else {
                        if (isResult) {
                            arms = quote($arms
                                case m: $t => this.$id(m, context)
                            )
                        } else {
                            arms = quote($arms
                                case m: $t => this.$id(m, context); MessageResult.Handled
                            )
                        }
                    }
                    handlerCount++
                }
            case p: PropDecl =>
                if (p.identifier.value == "name") {
                    hasName = true
                }
                if (p.identifier.value == "description") {
                    hasDescription = true
                }
            case _ => ()
        }
    }
    if (handlerCount == 0) {
        throw ASTException("${macroName}: ${className} 中没有 onXxx(msg[, ctx]) 处理方法")
    }
    // 协议清单核对：漏处理 / 多处理都在编译期暴露
    checkProtocolCoverage(handlers, actorAttr, className, macroName)
    let ds = cls.body.decls

    ds.add(parseDecl(quote(
        public func receive(message: Message, context: ActorContext): MessageResult {
            match (message) {
                $arms
                case _ => MessageResult.Unhandled
            }
        }
    )))
    if (!hasName) {
        ds.add(parseDecl(quote(
            public prop name: String {
                get() { $(className) }
            }
        )))
    }
    if (!hasDescription) {
        ds.add(parseDecl(quote(
            public prop description: String {
                get() { $(className) }
            }
        )))
    }
    cls.body.decls = ds
    cls.toTokens()
}

/**
 * 协议清单核对（dsl20261030.md §7.1 / §11 降级路线）：
 * - cases 清单里的每个类型都必须有且只有一个处理方法 → 漏处理编译期报错；
 * - 处理方法的类型必须都在清单里 → 多处理编译期报错（防止协议漂移）；
 * - protocol = T 要求存在一个处理 T 的方法（枚举协议的写法）。
 */
func checkProtocolCoverage(
    handlers: ArrayList<HandlerInfo>,
    actorAttr: ActorAttr,
    className: String,
    macroName: String
): Unit {
    if (!actorAttr.hasCases && !actorAttr.hasProtocol) {
        return
    }
    let declared = ArrayList<String>()
    for (c in actorAttr.cases) {
        declared.add(c)
    }
    if (actorAttr.hasProtocol) {
        declared.add(actorAttr.protocol)
    }
    // 漏处理
    for (c in declared) {
        var found = false
        for (h in handlers) {
            if (h.typeName == c) {
                found = true
            }
        }
        if (!found) {
            throw ASTException(
                "${macroName}: ${className} 未处理 ${c}，缺少 on${c}(m: ${c}, ctx: ActorContext)")
        }
    }
    // 多处理（协议漂移）
    for (h in handlers) {
        var listed = false
        for (c in declared) {
            if (c == h.typeName) {
                listed = true
            }
        }
        if (!listed) {
            throw ASTException(
                "${macroName}: ${className} 处理了未在协议清单中声明的类型 ${h.typeName}（${h.methodName}）")
        }
    }
}

// 签名检查：首参必须有类型；参数数 1 或 2；二参必须是 ActorContext；返回类型 Unit 或 MessageResult
func checkHandlerSignature(f: FuncDecl, className: String, macroName: String): Unit {
    let fname = f.identifier.value
    let t = f.funcParams[0]
    try {
        let _ = t.paramType.toTokens().size
    } catch (e: Exception) {
        fail("${macroName}: ${className}.${fname} 首个参数必须显式写出消息类型", fname)
    }
    if (f.funcParams.size > 2) {
        fail("${macroName}: ${className}.${fname} 最多两个参数 (msg, ctx)", fname)
    }
    if (f.funcParams.size == 2) {
        let ctxType = normalizeTypeName(f.funcParams[1].paramType.toTokens().toString())
        if (ctxType != "ActorContext") {
            fail("${macroName}: ${className}.${fname} 第二个参数必须是 ActorContext，实际是 ${ctxType}", fname)
        }
    }
}
