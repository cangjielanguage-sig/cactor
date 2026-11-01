/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 宏属性解析（对照 docs/plan/dsl20261030.md §7.2 / §11 降级路线）
 *
 * @actor 从"非属性宏"升级为"属性宏"后，调用形态为：
 *
 *   @actor[]                                          // 无协议声明
 *   @actor[cases = [PlaceOrder, GetOrder, Cancel]]    // 显式协议清单（编译期穷尽性检查）
 *   @actor[protocol = OrderMsg]                       // 单一协议类型（枚举协议写法）
 *
 * 为什么用显式清单而不是 `protocol = <枚举>` 自动解析：
 *   宏只能看到自己被标注的那段 token，无法跨包查到调用方包里的 enum 声明
 *   （dsl20261030.md §12 假设 A 已证伪）。因此协议清单由调用方显式给出，
 *   宏在编译期逐项核对"清单 ↔ 处理方法"，把漏处理/多处理挡在编译期。
 */
package cactor_macros.common

import std.ast.*
import std.collection.*

/** 解析后的 @actor 属性 */
public class ActorAttr {
    /** 协议清单中的消息类型名（已去空白、去包名前缀） */
    public var cases: ArrayList<String> = ArrayList<String>()
    /** 是否显式给出了 cases 清单 */
    public var hasCases: Bool = false
    /** 单一协议类型名 */
    public var protocol: String = ""
    /** 是否显式给出了 protocol */
    public var hasProtocol: Bool = false
}

/**
 * 解析 @actor[...] 的属性 token。
 * 支持的键：cases = [T1, T2, ...]、protocol = T
 */
public func parseActorAttr(attr: Tokens, className: String, macroName: String): ActorAttr {
    let result = ActorAttr()
    let tokens = stripNewlines(attr)
    if (tokens.size == 0) {
        return result
    }
    var i: Int64 = 0
    while (i < tokens.size) {
        // 跳过条目之间的逗号
        while (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
        }
        if (i >= tokens.size) {
            break
        }
        let key = tokens[i]
        if (key.kind != TokenKind.IDENTIFIER) {
            throw ASTException("${macroName}: ${className} 的属性格式错误，期望键名（cases / protocol）")
        }
        let keyName = key.value
        if (keyName != "cases" && keyName != "protocol") {
            throw ASTException(
                "${macroName}: ${className} 不认识属性 ${keyName}（可用：cases = [T1, T2]、protocol = T）")
        }
        // 重复的键不能静默合并或覆盖：cases 写两遍时第二份会被追加，protocol 写两遍时后者覆盖前者
        if ((keyName == "cases" && result.hasCases) || (keyName == "protocol" && result.hasProtocol)) {
            throw ASTException("${macroName}: ${className} 的属性 ${keyName} 重复出现")
        }
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.ASSIGN) {
            throw ASTException("${macroName}: ${className} 的属性 ${keyName} 后面需要 '='")
        }
        i++
        if (i >= tokens.size) {
            throw ASTException("${macroName}: ${className} 的属性 ${keyName} 缺少取值")
        }
        if (keyName == "protocol") {
            if (tokens[i].kind != TokenKind.IDENTIFIER) {
                throw ASTException("${macroName}: ${className} 的 protocol 需要写一个类型名")
            }
            let (name, next) = readQualifiedName(tokens, i)
            result.protocol = normalizeTypeName(name)
            result.hasProtocol = true
            i = next
        } else {
            // cases = [T1, T2, ...]
            if (tokens[i].kind != TokenKind.LSQUARE) {
                throw ASTException("${macroName}: ${className} 的 cases 需要写成 [T1, T2, ...]")
            }
            i++
            result.hasCases = true
            while (i < tokens.size && tokens[i].kind != TokenKind.RSQUARE) {
                if (tokens[i].kind != TokenKind.IDENTIFIER) {
                    throw ASTException("${macroName}: ${className} 的 cases 只能列类型名")
                }
                let (name, next) = readQualifiedName(tokens, i)
                let t = normalizeTypeName(name)
                var dup = false
                for (old in result.cases) {
                    if (old == t) {
                        dup = true
                    }
                }
                if (dup) {
                    throw ASTException("${macroName}: ${className} 的 cases 里 ${t} 重复列出")
                }
                result.cases.add(t)
                i = next
                if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
                    i++
                }
            }
            if (i >= tokens.size) {
                throw ASTException("${macroName}: ${className} 的 cases 缺少右括号 ']'")
            }
            i++ // 跳过 ']'
        }
    }
    return result
}

/** 读取 Ident(.Ident)* 形式的限定名，返回（名字, 下一个位置） */
func readQualifiedName(tokens: Tokens, start: Int64): (String, Int64) {
    var text = tokens[start].value
    var i = start + 1
    while (i + 1 < tokens.size && tokens[i].kind == TokenKind.DOT && tokens[i + 1].kind == TokenKind.IDENTIFIER) {
        text = "${text}.${tokens[i + 1].value}"
        i += 2
    }
    (text, i)
}

/**
 * 归一化类型名：去空白、去外层类型的包名前缀（pkg.Type → Type），便于清单与处理器参数比对。
 * 只处理第一个 '<' 之前的部分：pkg.Box<a.B> → Box<a.B>；整体按 '.' 切分会把它切成 "B>"。
 */
public func normalizeTypeName(text: String): String {
    let t = text.replace(" ", "")
    let (head, tail) = match (t.indexOf("<")) {
        case Some(idx) => (t[..idx], t[idx..])
        case None => (t, "")
    }
    match (head.lastIndexOf(".")) {
        case Some(dot) => head[dot + 1..] + tail
        case None => t
    }
}
