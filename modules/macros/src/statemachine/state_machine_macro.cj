/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - @StateMachine 宏（对照 docs/plan/dsl20261030.md §9 P4.1 / §12 假设 D）
 *
 * 用法：
 *
 *   enum OrderState {
 *       | Idle
 *       | Running
 *       | Stopped
 *   }
 *
 *   @StateMachine[
 *       stateType = OrderState,
 *       states = [Idle, Running, Stopped],
 *       initial = Idle,
 *       transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
 *   ]
 *   class OrderMachine {
 *       // 业务逻辑写在这里
 *   }
 *
 * 为什么清单要显式写出来：
 *   宏只能看到自己被标注的那段 token，无法跨包查到调用方包里的 enum 声明
 *   （dsl20261030.md §12 假设 A 已证伪）。因此状态清单与转换表由调用方给出。
 *
 * 编译期检查（§12 假设 D 的验证点）：
 *   1. 状态名必须是 stateType 的枚举构造器 —— 由生成的穷尽 match 让编译器报错；
 *   2. states 必须穷尽该枚举 —— 生成的 match 不写 `case _`，漏状态时编译器报错；
 *   3. transitions 里的状态必须出现在 states 中 —— 宏直接报错；
 *   4. transitions 不得重复 —— 宏直接报错；
 *   5. initial 必须出现在 states 中 —— 宏直接报错。
 */
macro package cactor_macros.statemachine

import std.ast.*
import std.collection.*
import cactor_macros.common.*

public macro StateMachine(attr: Tokens, input: Tokens): Tokens {
    let decl = parseDecl(input)
    let cls = match (decl) {
        case c: ClassDecl => c
        case _ => throw ASTException("@StateMachine 只能用于 class")
    }
    let className = cls.identifier.value
    let spec = parseStateMachineAttr(attr, className)

    // 转换表里的状态必须出现在清单中
    for (t in spec.transitions) {
        if (!spec.states.contains(t.source)) {
            throw ASTException(
                "@StateMachine: ${className} 的转换 ${t.source} -> ${t.target} 中，${t.source} 不在 states 清单里")
        }
        if (!spec.states.contains(t.target)) {
            throw ASTException(
                "@StateMachine: ${className} 的转换 ${t.source} -> ${t.target} 中，${t.target} 不在 states 清单里")
        }
    }
    // 转换不得重复
    var idx: Int64 = 0
    for (t in spec.transitions) {
        var other = idx + 1
        while (other < spec.transitions.size) {
            let o = spec.transitions[other]
            if (t.source == o.source && t.target == o.target) {
                throw ASTException("@StateMachine: ${className} 的转换 ${t.source} -> ${t.target} 重复声明")
            }
            other++
        }
        idx++
    }
    // 初始状态必须在清单里
    if (!spec.states.contains(spec.initial)) {
        throw ASTException("@StateMachine: ${className} 的初始状态 ${spec.initial} 不在 states 清单里")
    }

    // 类型与构造器都用 token 插值：字符串插值会变成字符串字面量，类型位置不能用
    let typeTok = Token(TokenKind.IDENTIFIER, spec.stateType)
    let classId = cls.identifier

    // 状态名数组（字符串字面量）
    let stateArray = stringListTokens(spec.states)

    // 每个源状态允许的目标
    var allowedArms = quote()
    for (s in spec.states) {
        let targetNames = ArrayList<String>()
        for (t in spec.transitions) {
            if (t.source == s) {
                targetNames.add(t.target)
            }
        }
        let targetArray = stringListTokens(targetNames)
        let ctor = ctorTokens(spec.stateType, s)
        allowedArms = quote($allowedArms
            case $ctor => $targetArray
        )
    }

    // 字符串索引的允许目标（供外部状态机工具对接）
    var allowedNameArms = quote()
    for (s in spec.states) {
        let targetNames = ArrayList<String>()
        for (t in spec.transitions) {
            if (t.source == s) {
                targetNames.add(t.target)
            }
        }
        let targetArray = stringListTokens(targetNames)
        allowedNameArms = quote($allowedNameArms
            case $(s) => $targetArray
        )
    }

    // 穷尽 match：states 少写一个状态，编译器会报"match 不完整"（§12-D 的检查点）
    var stateCases = quote()
    for (s in spec.states) {
        let ctor = ctorTokens(spec.stateType, s)
        stateCases = quote($stateCases
            case $ctor => $(s)
        )
    }

    let initialCtor = ctorTokens(spec.stateType, spec.initial)
    let ds = cls.body.decls
    ds.add(parseDecl(quote(
        public static let stateNames: Array<String> = $stateArray
    )))
    ds.add(parseDecl(quote(
        public static let initialState: $typeTok = $initialCtor
    )))
    ds.add(parseDecl(quote(
        public static func allowedTargets(from: $typeTok): Array<String> {
            match (from) {
                $allowedArms
                case _ => Array<String>()
            }
        }
    )))
    ds.add(parseDecl(quote(
        public static func canTransition(from: $typeTok, to: $typeTok): Bool {
            allowedTargets(from).contains(stateName(to))
        }
    )))
    // 字符串索引版本：便于与外部状态机工具（如 xstate 的 StateMachineBuilder）对接
    ds.add(parseDecl(quote(
        public static func allowedTargetNames(from: String): Array<String> {
            match (from) {
                $allowedNameArms
                case _ => Array<String>()
            }
        }
    )))
    ds.add(parseDecl(quote(
        public static func stateName(s: $typeTok): String {
            match (s) {
                $stateCases
            }
        }
    )))
    ds.add(parseDecl(quote(
        private var __currentState: $typeTok = $initialCtor
    )))
    ds.add(parseDecl(quote(
        public func currentState(): $typeTok {
            __currentState
        }
    )))
    ds.add(parseDecl(quote(
        public func transitionTo(to: $typeTok): Bool {
            if (!$classId.canTransition(__currentState, to)) {
                return false
            }
            __currentState = to
            true
        }
    )))
    cls.body.decls = ds
    cls.toTokens()
}

/** 转换表的一行 */
class StateTransition {
    let source: String
    let target: String

    init(source: String, target: String) {
        this.source = source
        this.target = target
    }
}

/** 解析后的 @StateMachine 属性 */
class StateMachineSpec {
    var stateType: String = ""
    var states: ArrayList<String> = ArrayList<String>()
    var initial: String = ""
    let transitions: ArrayList<StateTransition> = ArrayList<StateTransition>()
}

/**
 * 解析 @StateMachine[...] 属性。
 * 支持：stateType = T、states = [S1, S2]、initial = S、transitions = [(A, B), ...]
 */
func parseStateMachineAttr(attr: Tokens, className: String): StateMachineSpec {
    let spec = StateMachineSpec()
    let tokens = stripNewlines(attr)
    var i: Int64 = 0
    var hasStateType = false
    var hasInitial = false
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
            throw ASTException(
                "@StateMachine: ${className} 的属性格式错误，期望键名（stateType / states / initial / transitions）")
        }
        let keyName = key.value
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.ASSIGN) {
            throw ASTException("@StateMachine: ${className} 的属性 ${keyName} 后面需要 '='")
        }
        i++
        if (i >= tokens.size) {
            throw ASTException("@StateMachine: ${className} 的属性 ${keyName} 缺少取值")
        }
        match (keyName) {
            case "stateType" =>
                let (text, next) = parseTypePath(tokens, i, className, "stateType")
                spec.stateType = text
                hasStateType = true
                i = next
            case "initial" =>
                if (tokens[i].kind != TokenKind.IDENTIFIER) {
                    throw ASTException("@StateMachine: ${className} 的 initial 需要写一个状态名")
                }
                spec.initial = tokens[i].value
                hasInitial = true
                i++
            case "states" =>
                i = parseNameList(tokens, i, spec.states, className, "states")
            case "transitions" =>
                i = parseTransitionList(tokens, i, spec.transitions, className)
            case _ =>
                throw ASTException(
                    "@StateMachine: ${className} 不认识属性 ${keyName}（可用：stateType、states、initial、transitions）")
        }
    }
    if (!hasStateType) {
        throw ASTException("@StateMachine: ${className} 需要写 stateType = <状态枚举类型>")
    }
    if (spec.states.isEmpty()) {
        throw ASTException("@StateMachine: ${className} 需要写 states = [S1, S2, ...]")
    }
    if (!hasInitial) {
        throw ASTException("@StateMachine: ${className} 需要写 initial = <初始状态>")
    }
    spec
}

/** 解析类型路径：pkg.Sub.Type 或 Type */
func parseTypePath(tokens: Tokens, start: Int64, className: String, key: String): (String, Int64) {
    var i = start
    if (tokens[i].kind != TokenKind.IDENTIFIER) {
        throw ASTException("@StateMachine: ${className} 的 ${key} 需要写一个类型名")
    }
    var text = tokens[i].value
    i++
    while (i + 1 < tokens.size && tokens[i].kind == TokenKind.DOT && tokens[i + 1].kind == TokenKind.IDENTIFIER) {
        text = "${text}.${tokens[i + 1].value}"
        i += 2
    }
    (text, i)
}

/** 解析 [A, B, C] 形式的名称列表 */
func parseNameList(tokens: Tokens, start: Int64, out: ArrayList<String>, className: String, key: String): Int64 {
    var i = start
    if (tokens[i].kind != TokenKind.LSQUARE) {
        throw ASTException("@StateMachine: ${className} 的 ${key} 需要写成 [A, B, ...]")
    }
    i++
    while (i < tokens.size && tokens[i].kind != TokenKind.RSQUARE) {
        if (tokens[i].kind != TokenKind.IDENTIFIER) {
            throw ASTException("@StateMachine: ${className} 的 ${key} 只能列状态名")
        }
        out.add(tokens[i].value)
        i++
        if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
        }
    }
    if (i >= tokens.size) {
        throw ASTException("@StateMachine: ${className} 的 ${key} 缺少右括号 ']'")
    }
    i + 1
}

/** 解析 [(A, B), (C, D)] 形式的转换表 */
func parseTransitionList(tokens: Tokens, start: Int64, out: ArrayList<StateTransition>, className: String): Int64 {
    var i = start
    if (tokens[i].kind != TokenKind.LSQUARE) {
        throw ASTException("@StateMachine: ${className} 的 transitions 需要写成 [(A, B), ...]")
    }
    i++
    while (i < tokens.size && tokens[i].kind != TokenKind.RSQUARE) {
        if (tokens[i].kind != TokenKind.LPAREN) {
            throw ASTException("@StateMachine: ${className} 的 transitions 每一项写成 (源状态, 目标状态)")
        }
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.IDENTIFIER) {
            throw ASTException("@StateMachine: ${className} 的 transitions 缺少源状态")
        }
        let source = tokens[i].value
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.COMMA) {
            throw ASTException("@StateMachine: ${className} 的 transitions 每一项需要逗号分隔源状态与目标状态")
        }
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.IDENTIFIER) {
            throw ASTException("@StateMachine: ${className} 的 transitions 缺少目标状态")
        }
        let target = tokens[i].value
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.RPAREN) {
            throw ASTException("@StateMachine: ${className} 的 transitions 每一项需要用括号包起来")
        }
        i++
        out.add(StateTransition(source, target))
        if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
        }
    }
    if (i >= tokens.size) {
        throw ASTException("@StateMachine: ${className} 的 transitions 缺少右括号 ']'")
    }
    i + 1
}

/** 构造枚举构造器 token：<stateType>.<name> */
func ctorTokens(stateType: String, name: String): Tokens {
    Token(TokenKind.IDENTIFIER, stateType) + Token(TokenKind.DOT, ".") + Token(TokenKind.IDENTIFIER, name)
}

/** 把字符串列表拼成数组字面量 token：[a, b, c] */
func stringListTokens(items: ArrayList<String>): Tokens {
    var body = quote()
    var first = true
    for (s in items) {
        if (first) {
            body = quote($body $(s))
            first = false
        } else {
            body = quote($body, $(s))
        }
    }
    Token(TokenKind.LSQUARE, "[") + body + Token(TokenKind.RSQUARE, "]")
}
