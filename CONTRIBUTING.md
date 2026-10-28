# 🤝 CActor 贡献指南

感谢您对 CActor 项目的关注和贡献！本指南将帮助您了解如何参与项目开发。

## 📋 贡献方式

### 🐛 报告问题
- 使用 [GitHub Issues](../../issues) 报告 bug
- 提供详细的问题描述和复现步骤
- 包含环境信息（操作系统、仓颉版本等）

### 💡 功能建议
- 使用 [GitHub Discussions](../../discussions) 讨论新功能
- 详细描述功能需求和使用场景
- 考虑功能的实现复杂度和维护成本

### 📝 文档改进
- 修正文档中的错误或不清晰的地方
- 添加示例代码和使用说明
- 翻译文档到其他语言

### 💻 代码贡献
- 修复 bug
- 实现新功能
- 性能优化
- 代码重构

## 🚀 开发流程

### 1. 准备开发环境

```bash
# 1. Fork 项目到您的 GitHub 账户

# 2. 克隆您的 fork
git clone https://github.com/YOUR_USERNAME/cactor.git
cd cactor

# 3. 添加上游仓库
git remote add upstream https://github.com/ORIGINAL_OWNER/cactor.git

# 4. 安装依赖
# 确保已安装仓颉 (Cangjie) 1.2.0+
cjpm build
```

### 2. 创建功能分支

```bash
# 从 main 分支创建新分支
git checkout main
git pull upstream main
git checkout -b feature/your-feature-name

# 或者修复 bug
git checkout -b fix/issue-number-description
```

### 3. 开发和测试

```bash
# 进行代码修改

# 运行测试确保没有破坏现有功能
cjpm build

# 运行性能测试
./target/release/bin/cactor_integration.testing.simple_realistic_benchmark

# 运行功能测试
./target/release/bin/cactor_examples.hello_world
```

### 4. 提交代码

```bash
# 添加修改的文件
git add .

# 提交代码（使用清晰的提交信息）
git commit -m "feat: add new feature description"

# 推送到您的 fork
git push origin feature/your-feature-name
```

### 5. 创建 Pull Request

1. 在 GitHub 上打开您的 fork
2. 点击 "New Pull Request"
3. 选择目标分支（通常是 `main`）
4. 填写 PR 描述，包括：
   - 修改内容概述
   - 相关的 Issue 编号
   - 测试结果
   - 破坏性变更说明（如有）

## 📝 代码规范

### 🎯 仓颉语言规范

```cangjie
// 1. 使用清晰的命名
public class MessageProcessor {
    private let processingQueue: Queue<Message>
    
    public func processMessage(message: Message): MessageResult {
        // 实现逻辑
    }
}

// 2. 添加适当的注释
/**
 * 处理Actor消息的核心类
 * 负责消息的接收、处理和响应
 */
public class ActorMessageHandler {
    // 实现细节
}

// 3. 使用一致的错误处理
public func sendMessage(message: Message): Result<Unit, ActorError> {
    try {
        // 发送逻辑
        return Ok(())
    } catch (e: Exception) {
        return Err(ActorError.SendFailed(e.message))
    }
}
```

### 📁 文件组织

```
src/
├── core/           # 核心Actor功能
├── runtime/        # 运行时实现
├── patterns/       # Actor模式
├── distribution/   # 分布式功能
├── integration/    # 集成和测试
└── api/           # 对外API
```

### 🧪 测试要求

```cangjie
// 为新功能添加测试
public class MessageProcessorTest {
    @Test
    public func testMessageProcessing(): Unit {
        let processor = MessageProcessor()
        let message = TestMessage("test")
        let result = processor.processMessage(message)
        
        assert(result == MessageResult.Handled)
    }
}
```

## 🔍 代码审查

### Pull Request 检查清单

- [ ] 代码遵循项目规范
- [ ] 添加了适当的测试
- [ ] 所有测试通过
- [ ] 文档已更新
- [ ] 提交信息清晰
- [ ] 没有破坏性变更（或已说明）

### 审查标准

1. **功能正确性**: 代码是否正确实现了预期功能
2. **性能影响**: 是否对性能产生负面影响
3. **代码质量**: 代码是否清晰、可维护
4. **测试覆盖**: 是否有足够的测试覆盖
5. **文档完整**: 是否更新了相关文档

## 🏷️ 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### 类型说明

- `feat`: 新功能
- `fix`: bug 修复
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建或辅助工具变动

### 示例

```
feat(core): add message batching support

Implement message batching to improve throughput for high-volume scenarios.
This change allows actors to process multiple messages in a single batch,
reducing overhead and improving performance.

Closes #123
```

## 🎯 开发重点

### 当前优先级

1. **性能优化**: 继续提升各种工作负载下的性能
2. **分布式功能**: 实现集群和远程通信
3. **监控集成**: 完善监控和诊断工具
4. **文档完善**: 改进API文档和示例

### 技术债务

- 优化内存使用
- 改进错误处理
- 增强类型安全
- 完善测试覆盖

## 🙏 致谢

感谢所有为 CActor 项目做出贡献的开发者！您的每一个贡献都让项目变得更好。

### 贡献者

- 核心维护者
- 功能贡献者
- 文档贡献者
- 测试贡献者
- 问题报告者

## 📞 联系方式

- **GitHub Issues**: 技术问题和 bug 报告
- **GitHub Discussions**: 功能讨论和社区交流
- **项目维护者**: 通过 GitHub 联系

---

再次感谢您对 CActor 项目的贡献！🚀
