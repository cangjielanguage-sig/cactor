/**
 * CActor DevKit — Pi 扩展
 *
 * 复用仓库现有工具，为 Pi 会话提供：
 * 1. cactor_layer_check 工具：模块分层检查（tools/check_module_deps.py）
 * 2. cactor_build / cactor_test 工具：workspace 构建 / 全量测试（自动 source envsetup）
 * 3. /cactor-ci 命令：一键 build + 分层检查 + 测试（tools/ci.sh）
 * 4. tool_call 防护：编辑 .cj 文件后自动跑分层检查，违规即提示
 *
 * 所有命令仅在 workspace 根目录（存在 cjpm.toml）时生效。
 */

import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { existsSync } from "node:fs";
import { join } from "node:path";

const run = promisify(execFile);

const ENVSETUP = "/home/devbox/.cangjie/1.2.0/cangjie/envsetup.sh";

/** 在 workspace 根目录跑 shell 命令（先 source 仓颉环境） */
async function sh(ctx: ExtensionContext, script: string, timeoutMs = 600_000) {
	const cwd = ctx.cwd;
	if (!existsSync(join(cwd, "cjpm.toml"))) {
		throw new Error(`不在 CActor workspace（${cwd} 下没有 cjpm.toml）`);
	}
	const full = `set -o pipefail; source "${ENVSETUP}" >/dev/null 2>&1; cd "${cwd}" && ${script}`;
	try {
		const { stdout } = await run("bash", ["-c", full], { timeout: timeoutMs, maxBuffer: 64 * 1024 * 1024 });
		return stdout;
	} catch (e: unknown) {
		const err = e as { stdout?: string; stderr?: string; message: string };
		const out = [err.stdout, err.stderr].filter(Boolean).join("\n");
		throw new Error(out ? `${out}\n(exit 非 0)` : err.message);
	}
}

const stripAnsi = (s: string) => s.replace(/\x1b\[[0-9;]*m/g, "");

// ---------- 工具 1：分层检查 ----------
const layerCheckTool = defineTool({
	name: "cactor_layer_check",
	label: "CActor 分层检查",
	description:
		"检查 CActor 模块依赖分层（foundation -> core -> runtime -> patterns -> ... 无环）。" +
		"在 CActor 仓库中新增或修改 import 后、提交前运行。只读、快速。",
	parameters: Type.Object({}),
	async execute(_id, _params, _signal, _onUpdate, ctx) {
		const out = stripAnsi(await sh(ctx, "python3 tools/check_module_deps.py", 60_000));
		const ok = out.includes("通过");
		return {
			content: [{ type: "text", text: out.trim() || "(无输出)" }],
			details: { ok, output: out.trim() },
			structuredContent: { ok, output: out.trim() },
		};
	},
});

// ---------- 工具 2：构建 ----------
const buildTool = defineTool({
	name: "cactor_build",
	label: "CActor 构建",
	description:
		"构建 CActor workspace（cjpm build，-O2，必须从仓库根目录执行）。改完代码后验证编译。" +
		"注意：不要用 cjpm build -m 单模块构建跑基准，不继承 -O2。",
	parameters: Type.Object({}),
	async execute(_id, _params, _signal, _onUpdate, ctx) {
		const out = stripAnsi(await sh(ctx, "cjpm build 2>&1 | tail -20"));
		return { content: [{ type: "text", text: out.trim() || "cjpm build success" }] };
	},
});

// ---------- 工具 3：测试 ----------
const testTool = defineTool({
	name: "cactor_test",
	label: "CActor 测试",
	description:
		"运行 CActor 全量测试（cjpm test，从 workspace 根目录）。较慢（数分钟）。" +
		"失败时返回 FAILED/ERROR 的 CASE 名与编译错误。",
	parameters: Type.Object({}),
	async execute(_id, _params, _signal, _onUpdate, ctx) {
		const out = stripAnsi(await sh(ctx, "cjpm test 2>&1", 1_800_000));
		const lines = out.split("\n");
		const summary = lines.filter((l) => /^(Summary|cjpm test)/.test(l.trim()));
		const failures = [...new Set(lines.filter((l) => /\[ (FAILED|ERROR) \] CASE/.test(l)))];
		const errors = lines.filter((l) => /^error:/.test(l.trim()));
		const pass = /\bcjpm test success\b/.test(out) && failures.length === 0 && errors.length === 0;
		const text = [
			pass ? "测试全部通过" : "测试失败",
			...summary.slice(-2),
			...errors.slice(0, 5),
			...failures.slice(0, 20),
		].join("\n");
		return {
			content: [{ type: "text", text: text || out.slice(-2000) }],
			structuredContent: { pass, failures, errors },
		};
	},
});

export default function cactorExtension(pi: ExtensionAPI) {
	pi.registerTool(layerCheckTool);
	pi.registerTool(buildTool);
	pi.registerTool(testTool);

	// /cactor-ci：一键完整门禁
	pi.registerCommand("cactor-ci", {
		description: "运行 CActor 完整 CI 门禁（tools/ci.sh：构建+分层检查+全部测试+示例自检）",
		handler: async (_args, ctx) => {
			ctx.ui.notify("开始 CActor CI（可能需要数分钟）…", "info");
			try {
				const out = await sh(ctx, "bash tools/ci.sh 2>&1", 2_700_000);
				const tail = stripAnsi(out).split("\n").slice(-30).join("\n");
				ctx.ui.notify(`CI 完成：\n${tail}`, "info");
			} catch (e) {
				ctx.ui.notify(`CI 失败：\n${stripAnsi(String((e as Error).message)).split("\n").slice(-40).join("\n")}`, "warning");
			}
		},
	});

	// 防护：每次 .cj 文件写入后自动跑分层检查（快速、只读）
	pi.on("tool_result", async (event) => {
		if (event.toolName !== "write" && event.toolName !== "edit") return;
		const path = String((event.input as { path?: string }).path ?? (event.input as { file_path?: string }).file_path ?? "");
		if (!path.endsWith(".cj") || !path.includes("modules/")) return;
		try {
			// 注意：检查失败时脚本 exit 非 0，sh() 会抛错，错误输出在 e.stdout/e.stderr 里 —— 必须把它当作“违规”而不是吞掉
			let out: string;
			try {
				out = stripAnsi(await sh({ cwd: process.cwd() } as ExtensionContext, "python3 tools/check_module_deps.py", 60_000));
			} catch (e) {
				const err = e as { message?: string };
				out = stripAnsi(err.message ?? "");
			}
			if (!out.includes("通过")) {
				return {
					content: [
						...event.content,
						{ type: "text", text: `\n⚠️ CActor 分层检查失败（依赖只能自底向上，见 CLAUDE.md）：\n${out.trim()}` },
					],
				};
			}
		} catch (e) {
			// 环境问题（如 cjpm 不存在）时静默跳过，不打扰正常编辑
		}
	});
}
