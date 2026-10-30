/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 分层配置
 *
 * 按优先级从低到高合并：内置默认值 < 配置文件（可多个，后加载的覆盖先加载的）< 环境变量 < 代码中的覆盖值。
 * - 配置文件格式：`键 = 值`，`#` 或 `//` 开头为注释，`[段]` 为之后的键加上 "段." 前缀；值两侧可加双引号。
 * - 环境变量：键 "cluster.gossip-interval" 对应 `<前缀>CLUSTER_GOSSIP_INTERVAL`（字母大写，'.' 与 '-' 变成 '_'），
 *   按需查找，不需要列举全部环境变量。
 * - 类型化读取：getString / getInt / getFloat / getBool / getDuration（"500ms"、"3s"、"2m"、"1h"、"250us"），
 *   类型不对时抛 ConfigException，消息里带键、原始值和来源（哪一层、哪个文件第几行），便于排查部署问题。
 * - origin(key) 返回值的来源；requireKeys 一次报告全部缺少的键。
 */
package cactor_foundation.config

import std.fs.*
import std.env.*
import std.time.*
import std.convert.*
import std.collection.*

public class ConfigException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 一个配置值及其来源 */
public class ConfigEntry {
    public let value: String
    public let origin: String

    public init(value: String, origin: String) {
        this.value = value
        this.origin = origin
    }
}

public class LayeredConfig {
    private let defaults = HashMap<String, ConfigEntry>()
    private let files = ArrayList<HashMap<String, ConfigEntry>>()
    private let overrides = HashMap<String, ConfigEntry>()
    private let envPrefix: Option<String>
    private let envLookup: (String) -> Option<String>

    /** envPrefix 为 None 时不读环境变量 */
    public init(envPrefix: Option<String>) {
        this(envPrefix, { name => getVariable(name) })
    }

    /** 可注入环境变量的读取方式（测试用） */
    public init(envPrefix: Option<String>, envLookup: (String) -> Option<String>) {
        this.envPrefix = envPrefix
        this.envLookup = envLookup
    }

    /** CACTOR_ 前缀的环境变量 + 默认值 */
    public static func standard(): LayeredConfig {
        LayeredConfig(Some("CACTOR_"))
    }

    public func withDefault(key: String, value: String): LayeredConfig {
        defaults[key] = ConfigEntry(value, "默认值")
        this
    }

    public func withOverride(key: String, value: String): LayeredConfig {
        overrides[key] = ConfigEntry(value, "代码覆盖")
        this
    }

    /** 加载配置文件；文件不存在时抛 ConfigException */
    public func withFile(path: String): LayeredConfig {
        if (!exists(path)) {
            throw ConfigException("配置文件不存在: ${path}")
        }
        files.add(parse(String.fromUtf8(File.readFrom(path)), path))
        this
    }

    /** 文件存在时才加载 */
    public func withOptionalFile(path: String): LayeredConfig {
        if (exists(path)) {
            withFile(path)
        } else {
            this
        }
    }

    /** 直接加载文本（内嵌配置、测试） */
    public func withText(text: String, name: String): LayeredConfig {
        files.add(parse(text, name))
        this
    }

    public static func envName(prefix: String, key: String): String {
        let sb = StringBuilder(prefix)
        for (r in key.runes()) {
            if (r == r'.' || r == r'-') {
                sb.append(r'_')
            } else if (r >= r'a' && r <= r'z') {
                sb.append(Rune(UInt32(r) - 32))
            } else {
                sb.append(r)
            }
        }
        sb.toString()
    }

    private static func parse(text: String, name: String): HashMap<String, ConfigEntry> {
        let out = HashMap<String, ConfigEntry>()
        var section = ""
        var lineNo = 0
        for (raw in text.split("\n")) {
            lineNo++
            let line = raw.trimAscii()
            if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) {
                continue
            }
            if (line.startsWith("[")) {
                if (!line.endsWith("]") || line.size < 3) {
                    throw ConfigException("${name}:${lineNo}: 段名格式错误: ${line}")
                }
                section = line[1..(line.size - 1)].trimAscii() + "."
                continue
            }
            match (line.indexOf("=")) {
                case Some(i) =>
                    let k = line[0..i].trimAscii()
                    var v = line[(i + 1)..].trimAscii()
                    if (k.isEmpty()) {
                        throw ConfigException("${name}:${lineNo}: 缺少键: ${line}")
                    }
                    if (v.size >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
                        v = v[1..(v.size - 1)]
                    }
                    out[section + k] = ConfigEntry(v, "${name}:${lineNo}")
                case None => throw ConfigException("${name}:${lineNo}: 应为 键 = 值: ${line}")
            }
        }
        out
    }

    /** 最高优先级的值 */
    public func entry(key: String): Option<ConfigEntry> {
        if (let Some(e) <- overrides.get(key)) {
            return Some(e)
        }
        if (let Some(p) <- envPrefix) {
            let name = envName(p, key)
            if (let Some(v) <- envLookup(name)) {
                return Some(ConfigEntry(v, "环境变量 ${name}"))
            }
        }
        var i = files.size - 1
        while (i >= 0) {
            if (let Some(e) <- files[i].get(key)) {
                return Some(e)
            }
            i--
        }
        defaults.get(key)
    }

    public func hasKey(key: String): Bool {
        entry(key).isSome()
    }

    public func origin(key: String): Option<String> {
        match (entry(key)) {
            case Some(e) => Some(e.origin)
            case None => None
        }
    }

    /** 缺少任何一个键时抛 ConfigException，消息列出全部缺少的键 */
    public func requireKeys(keys: Array<String>): Unit {
        let missing = ArrayList<String>()
        for (k in keys) {
            if (!hasKey(k)) {
                missing.add(k)
            }
        }
        if (!missing.isEmpty()) {
            throw ConfigException("缺少配置: ${String.join(missing.toArray(), delimiter: ", ")}")
        }
    }

    private func need(key: String): ConfigEntry {
        match (entry(key)) {
            case Some(e) => e
            case None => throw ConfigException("缺少配置: ${key}")
        }
    }

    private static func bad(key: String, e: ConfigEntry, expected: String): ConfigException {
        ConfigException("配置 ${key} = \"${e.value}\"（来自 ${e.origin}）不是${expected}")
    }

    public func getString(key: String): String {
        need(key).value
    }

    public func getString(key: String, fallback: String): String {
        match (entry(key)) {
            case Some(e) => e.value
            case None => fallback
        }
    }

    public func getInt(key: String): Int64 {
        let e = need(key)
        match (Int64.tryParse(e.value)) {
            case Some(v) => v
            case None => throw bad(key, e, "整数")
        }
    }

    public func getInt(key: String, fallback: Int64): Int64 {
        if (hasKey(key)) { getInt(key) } else { fallback }
    }

    public func getFloat(key: String): Float64 {
        let e = need(key)
        match (Float64.tryParse(e.value)) {
            case Some(v) => v
            case None => throw bad(key, e, "数字")
        }
    }

    public func getFloat(key: String, fallback: Float64): Float64 {
        if (hasKey(key)) { getFloat(key) } else { fallback }
    }

    public func getBool(key: String): Bool {
        let e = need(key)
        match (e.value.toAsciiLower()) {
            case "true" | "on" | "yes" | "1" => true
            case "false" | "off" | "no" | "0" => false
            case _ => throw bad(key, e, "布尔值（true/false/on/off/yes/no/1/0）")
        }
    }

    public func getBool(key: String, fallback: Bool): Bool {
        if (hasKey(key)) { getBool(key) } else { fallback }
    }

    public func getDuration(key: String): Duration {
        let e = need(key)
        match (parseDuration(e.value)) {
            case Some(d) => d
            case None => throw bad(key, e, "时长（例如 250us、500ms、3s、2m、1h）")
        }
    }

    public func getDuration(key: String, fallback: Duration): Duration {
        if (hasKey(key)) { getDuration(key) } else { fallback }
    }

    /** "500ms" / "3s" / "2m" / "1h" / "250us"；数字与单位之间可以有空格；不带单位视为错误 */
    public static func parseDuration(text: String): Option<Duration> {
        let t = text.trimAscii().toAsciiLower()
        var i = 0
        while (i < t.size && t[i] >= 48 && t[i] <= 57) {
            i++
        }
        if (i == 0) {
            return None
        }
        let n = match (Int64.tryParse(t[0..i])) {
            case Some(v) => v
            case None => return None
        }
        let unit = t[i..].trimAscii()
        match (unit) {
            case "us" => Some(Duration.microsecond * n)
            case "ms" => Some(Duration.millisecond * n)
            case "s" => Some(Duration.second * n)
            case "m" | "min" => Some(Duration.minute * n)
            case "h" => Some(Duration.hour * n)
            case _ => None
        }
    }

    /** 某个前缀下的全部键（去掉前缀），合并全部层，用于列出子配置 */
    public func keysUnder(prefix: String): Array<String> {
        let p = if (prefix.endsWith(".")) { prefix } else { prefix + "." }
        let set = HashSet<String>()
        let layers = ArrayList<HashMap<String, ConfigEntry>>()
        layers.add(defaults)
        layers.add(all: files)
        layers.add(overrides)
        for (layer in layers) {
            for ((k, _) in layer) {
                if (k.startsWith(p)) {
                    set.add(k[p.size..])
                }
            }
        }
        set.toArray()
    }
}
