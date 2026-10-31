/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 宏共享工具（对照 docs/plan/dsl20261030.md §6.1 / §8.1）
 */
package cactor_macros.common

import std.ast.*
import std.collection.*

// 修饰符判断：FuncDecl 是否带某修饰符关键字
public func hasModifier(f: FuncDecl, kind: TokenKind): Bool {
    for (m in f.modifiers) {
        if (m.keyword.kind == kind) {
            return true
        }
    }
    return false
}

// VarDecl 是否带某修饰符关键字（如 static）
public func hasModifier(v: VarDecl, kind: TokenKind): Bool {
    for (m in v.modifiers) {
        if (m.keyword.kind == kind) {
            return true
        }
    }
    return false
}

// 规范化报错：<原因>（<方法名>）
public func fail(what: String, method: String): Nothing {
    throw ASTException("${what}（${method}）")
}

/**
 * 去掉 token 里的换行（NL）。
 * 属性宏的参数常常跨行书写，解析前先压平，避免换行 token 干扰键名解析。
 */
public func stripNewlines(tokens: Tokens): Tokens {
    let out = ArrayList<Token>()
    for (t in tokens) {
        if (t.kind != TokenKind.NL) {
            out.add(t)
        }
    }
    Tokens(out)
}
