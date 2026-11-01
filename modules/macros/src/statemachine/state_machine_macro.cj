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
 *   5. initial 必须出现在 states 中 —— 宏直接报错；
 *   6. states 不得重复、属性键不得重复 —— 宏直接报错；
 *   7. 不同转换拼出的 guard/onTransition 方法名不得相同（如 (AB, C) 与 (A, BC)）—— 宏直接报错。
 *
 * 运行时开销：canTransition / transitionTo 展开为对 (from, to) 的一次枚举 match，
 * 不分配数组、不做字符串比较，与手写的 match 等价。
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

    // 类体已有方法名：命名守卫校验、骨架同名跳过都需要它
    let existingMethods = ArrayList<String>()
    for (d in cls.body.decls) {
        match (d) {
            case f: FuncDecl => existingMethods.add(f.identifier.value)
            case _ => ()
        }
    }

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
        // 命名守卫（守卫表达式）：方法必须在类体里定义，否则生成调用会编译不过且难读
        if (t.guardMethod != "" && !existingMethods.contains(t.guardMethod)) {
            throw ASTException(
                "@StateMachine: ${className} 的转换 ${t.source} -> ${t.target} 指定守卫方法 ${t.guardMethod}()，但类体里没有定义它")
        }
    }
    // 子状态机：复合状态必须在清单里，且不能重复声明
    for (m in spec.submachines) {
        if (!spec.states.contains(m.state)) {
            throw ASTException(
                "@StateMachine: ${className} 的子状态机 ${m.typeName} 委托给状态 ${m.state}，但它不在 states 清单里")
        }
    }
    var smIdx: Int64 = 0
    for (m in spec.submachines) {
        var other = smIdx + 1
        while (other < spec.submachines.size) {
            if (spec.submachines[other].state == m.state) {
                throw ASTException(
                    "@StateMachine: ${className} 的状态 ${m.state} 声明了多个子状态机")
            }
            other++
        }
        smIdx++
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
    // 方法名由状态名直接拼接，不同转换可能拼出同名方法（(AB, C) 与 (A, BC) 都是 guardABC），
    // 放任不管会得到编译器的"重复定义"报错，看不出是哪两条转换撞了
    var ti: Int64 = 0
    while (ti < spec.transitions.size) {
        let t = spec.transitions[ti]
        var tj = ti + 1
        while (tj < spec.transitions.size) {
            let o = spec.transitions[tj]
            if (t.source + t.target == o.source + o.target) {
                throw ASTException(
                    "@StateMachine: ${className} 的转换 ${t.source} -> ${t.target} 与 ${o.source} -> ${o.target} " +
                    "生成的方法名相同（guard${t.source}${t.target}），请调整状态名")
            }
            tj++
        }
        ti++
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
    var canArms = quote()
    for (t in spec.transitions) {
        let fromCtor = ctorTokens(spec.stateType, t.source)
        let toCtor = ctorTokens(spec.stateType, t.target)
        canArms = quote($canArms
            case ($fromCtor, $toCtor) => true
        )
    }
    ds.add(parseDecl(quote(
        public static func canTransition(from: $typeTok, to: $typeTok): Bool {
            match ((from, to)) {
                $canArms
                case _ => false
            }
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
    // 迁移入口：先查转换表，再过守卫（命名守卫优先，否则用骨架），更新状态、
    // 复位子状态机后执行动作。每个转换展开成 (from, to) 上的一个 match 分支：
    // 不在表里的组合落到 case _ 被拒绝。
    var transitionArms = quote()
    for (t in spec.transitions) {
        let fromCtor = ctorTokens(spec.stateType, t.source)
        let toCtor = ctorTokens(spec.stateType, t.target)
        // 命名守卫优先；未指定时用骨架方法
        let guardToken = if (t.guardMethod != "") {
            Token(TokenKind.IDENTIFIER, t.guardMethod)
        } else {
            Token(TokenKind.IDENTIFIER, "guard${t.source}${t.target}")
        }
        let actionName = Token(TokenKind.IDENTIFIER, "onTransition${t.source}${t.target}")
        // 进入复合状态时把子状态机复位到它的初始状态
        var subResets = quote()
        for (m in spec.submachines) {
            if (m.state == t.target) {
                let field = Token(TokenKind.IDENTIFIER, "__sub_${m.state}")
                subResets = quote($subResets
                    this.$field.reset()
                )
            }
        }
        transitionArms = quote($transitionArms
            case ($fromCtor, $toCtor) =>
                if (!this.$guardToken()) {
                    return false
                }
                __currentState = to
                $subResets
                this.$actionName()
                true
        )
    }
    ds.add(parseDecl(quote(
        public func transitionTo(to: $typeTok): Bool {
            match ((__currentState, to)) {
                $transitionArms
                case _ => false
            }
        }
    )))
    // 为每个转换补默认 guard / onTransition（用户已写的跳过；指定了命名守卫的不再生成守卫骨架）
    for (t in spec.transitions) {
        let guardName = "guard${t.source}${t.target}"
        let actionName = "onTransition${t.source}${t.target}"
        if (t.guardMethod == "" && !existingMethods.contains(guardName)) {
            ds.add(parseDecl(quote(
                public func $(Token(TokenKind.IDENTIFIER, guardName))(): Bool {
                    true
                }
            )))
        }
        if (!existingMethods.contains(actionName)) {
            ds.add(parseDecl(quote(
                public func $(Token(TokenKind.IDENTIFIER, actionName))(): Unit {
                    ()
                }
            )))
        }
    }
    // 复位到初始状态（子状态机进入复合状态时由父状态机调用）
    if (!existingMethods.contains("reset")) {
        ds.add(parseDecl(quote(
            public func reset(): Unit {
                __currentState = $initialCtor
            }
        )))
    }
    // 子状态机：复合状态委托给子状态机实例（进入复合状态时自动复位）
    for (m in spec.submachines) {
        let subTypeTok = Token(TokenKind.IDENTIFIER, m.typeName)
        let field = Token(TokenKind.IDENTIFIER, "__sub_${m.state}")
        let accessor = Token(TokenKind.IDENTIFIER, "sub${m.state}")
        ds.add(parseDecl(quote(
            private let $field: $subTypeTok = $subTypeTok()
        )))
        if (!existingMethods.contains("sub${m.state}")) {
            ds.add(parseDecl(quote(
                public func $accessor(): $subTypeTok {
                    $field
                }
            )))
        }
    }
    cls.body.decls = ds
    cls.toTokens()
}

/** 转换表的一行（guardMethod 可选：命名守卫，留空则用 guard<From><To> 骨架） */
class StateTransition {
    let source: String
    let target: String
    var guardMethod: String = ""

    init(source: String, target: String) {
        this.source = source
        this.target = target
    }
}

/** 子状态机的一行：复合状态 -> 委托的子状态机类型 */
class SubMachineEntry {
    let state: String
    let typeName: String

    init(state: String, typeName: String) {
        this.state = state
        this.typeName = typeName
    }
}

/** 解析后的 @StateMachine 属性 */
class StateMachineSpec {
    var stateType: String = ""
    var states: ArrayList<String> = ArrayList<String>()
    var initial: String = ""
    let transitions: ArrayList<StateTransition> = ArrayList<StateTransition>()
    let submachines: ArrayList<SubMachineEntry> = ArrayList<SubMachineEntry>()
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
    let seenKeys = ArrayList<String>()
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
        // 重复的键会让 states / transitions 被静默追加、initial 被静默覆盖
        if (seenKeys.contains(keyName)) {
            throw ASTException("@StateMachine: ${className} 的属性 ${keyName} 重复出现")
        }
        seenKeys.add(keyName)
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
            case "submachines" =>
                i = parseSubMachineList(tokens, i, spec.submachines, className)
            case _ =>
                throw ASTException(
                    "@StateMachine: ${className} 不认识属性 ${keyName}（可用：stateType、states、initial、transitions、submachines）")
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
        if (out.contains(tokens[i].value)) {
            throw ASTException("@StateMachine: ${className} 的 ${key} 里 ${tokens[i].value} 重复列出")
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

/** 解析 [(A, B), (C, D, guardMethod)] 形式的转换表（守卫名可选） */
func parseTransitionList(tokens: Tokens, start: Int64, out: ArrayList<StateTransition>, className: String): Int64 {
    var i = start
    if (tokens[i].kind != TokenKind.LSQUARE) {
        throw ASTException("@StateMachine: ${className} 的 transitions 需要写成 [(A, B), ...]")
    }
    i++
    while (i < tokens.size && tokens[i].kind != TokenKind.RSQUARE) {
        if (tokens[i].kind != TokenKind.LPAREN) {
            throw ASTException("@StateMachine: ${className} 的 transitions 每一项写成 (源状态, 目标状态) 或 (源状态, 目标状态, 守卫方法名)")
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
        let t = StateTransition(source, target)
        // 可选第三项：命名守卫（守卫表达式）——迁移前调用类体里的这个方法
        if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
            if (i >= tokens.size || tokens[i].kind != TokenKind.IDENTIFIER) {
                throw ASTException("@StateMachine: ${className} 的 transitions 第三项需要写守卫方法名")
            }
            t.guardMethod = tokens[i].value
            i++
        }
        if (i >= tokens.size || tokens[i].kind != TokenKind.RPAREN) {
            throw ASTException("@StateMachine: ${className} 的 transitions 每一项需要用括号包起来")
        }
        i++
        out.add(t)
        if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
        }
    }
    if (i >= tokens.size) {
        throw ASTException("@StateMachine: ${className} 的 transitions 缺少右括号 ']'")
    }
    i + 1
}

/** 解析 [(State, SubMachineType), ...] 形式的子状态机清单 */
func parseSubMachineList(tokens: Tokens, start: Int64, out: ArrayList<SubMachineEntry>, className: String): Int64 {
    var i = start
    if (tokens[i].kind != TokenKind.LSQUARE) {
        throw ASTException("@StateMachine: ${className} 的 submachines 需要写成 [(状态, 子状态机类型), ...]")
    }
    i++
    while (i < tokens.size && tokens[i].kind != TokenKind.RSQUARE) {
        if (tokens[i].kind != TokenKind.LPAREN) {
            throw ASTException("@StateMachine: ${className} 的 submachines 每一项写成 (复合状态, 子状态机类型)")
        }
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.IDENTIFIER) {
            throw ASTException("@StateMachine: ${className} 的 submachines 缺少状态名")
        }
        let state = tokens[i].value
        i++
        if (i >= tokens.size || tokens[i].kind != TokenKind.COMMA) {
            throw ASTException("@StateMachine: ${className} 的 submachines 每一项需要逗号分隔状态与类型")
        }
        i++
        let (typeName, next) = parseTypePath(tokens, i, className, "submachines")
        i = next
        if (i >= tokens.size || tokens[i].kind != TokenKind.RPAREN) {
            throw ASTException("@StateMachine: ${className} 的 submachines 每一项需要用括号包起来")
        }
        i++
        out.add(SubMachineEntry(state, typeName))
        if (i < tokens.size && tokens[i].kind == TokenKind.COMMA) {
            i++
        }
    }
    if (i >= tokens.size) {
        throw ASTException("@StateMachine: ${className} 的 submachines 缺少右括号 ']'")
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
