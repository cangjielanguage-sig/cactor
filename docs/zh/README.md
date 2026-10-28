# CActor 中文文档

本目录是 CActor 的中文文档。项目介绍、构建方法和当前状态请先阅读仓库根目录的 [README](../../README.md)。

> 项目处于早期开发阶段，部分文档还在随代码一起更新，个别内容可能和当前实现不一致。遇到不一致的地方，以代码为准，也欢迎提 Issue 指出。

## 架构设计

CActor采用6层模块化架构，确保清晰的职责分离和高度的可扩展性：

```
┌─────────────────────────────────────────┐
│              API Layer (API层)           │  ← 统一对外接口
├─────────────────────────────────────────┤
│         Integration Layer (集成层)        │  ← 监控、日志、配置
├─────────────────────────────────────────┤
│        Distribution Layer (分布式层)      │  ← 远程、集群、持久化
├─────────────────────────────────────────┤
│          Patterns Layer (模式层)         │  ← Ask、路由、断路器
├─────────────────────────────────────────┤
│          Runtime Layer (运行时层)        │  ← 调度器、邮箱、定时器
├─────────────────────────────────────────┤
│            Core Layer (核心层)           │  ← Actor、消息、系统
├─────────────────────────────────────────┤
│        Foundation Layer (基础设施层)      │  ← 并发、序列化、网络
└─────────────────────────────────────────┘
```

### 层次职责

1. **Foundation Layer (基础设施层)**: 提供并发原语、序列化、网络通信等基础设施
2. **Core Layer (核心层)**: 定义Actor、消息、系统等核心抽象
3. **Runtime Layer (运行时层)**: 提供高性能的执行引擎
4. **Patterns Layer (模式层)**: 实现常用的Actor模式
5. **Distribution Layer (分布式层)**: 提供分布式能力
6. **Integration Layer (集成层)**: 提供监控、日志等集成功能
7. **API Layer (API层)**: 提供统一的对外接口

### 层与模块的对应关系

| 层 | 模块 |
|---|---|
| Foundation | `cactor_foundation`（断路器也下沉到此层） |
| Core | `cactor_core` |
| Runtime | `cactor_runtime` |
| Patterns | `cactor_patterns` |
| Distribution | `cactor_distribution`、`cactor_streams`、`cactor_xstate` |
| Integration | `cactor_integration` |
| API | `cactor`（门面模块） |

详细的模块依赖见 [包与模块结构](package-structure.md)。

## 入门

- [快速开始](QUICK_START.md)
- [入门指南](getting-started.md)
- [功能说明](FEATURES.md)

## 设计与参考

- [架构设计](architecture.md)
- [包与模块结构](package-structure.md)
- [API 参考](api-reference.md)

## 开发与运维

- [最佳实践](BEST_PRACTICES.md)
- [代码风格](CODE_STYLE_GUIDE.md)
- [性能指南](performance.md)
- [监控与日志](MONITORING.md)
- [部署指南](DEPLOYMENT.md)
- [安全指南](SECURITY.md)
- [故障排除](TROUBLESHOOTING.md)
- [迁移指南](MIGRATION.md)

## 规划

- [路线图](roadmap.md)
- [开发计划](../plan/20261028.md)
- [更新日志](../../CHANGELOG.md)

[English](../en/README.md)
