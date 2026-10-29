# CActor Documentation (English)

This directory contains the English documentation for CActor. For an overview of the project, build instructions and current status, start with the [README](../../README_EN.md) at the repository root.

> The project is in early development. Some documents are still being updated alongside the code and may not match the current implementation in every detail. When in doubt, the code is authoritative, and issue reports are welcome.

## Architecture

CActor adopts a 6-layer modular architecture, ensuring clear separation of concerns and high scalability:

```
┌─────────────────────────────────────────┐
│              API Layer                   │  ← Unified External Interface
├─────────────────────────────────────────┤
│         Integration Layer                │  ← Monitoring, Logging, Config
├─────────────────────────────────────────┤
│        Distribution Layer                │  ← Remote, Cluster, Persistence
├─────────────────────────────────────────┤
│          Patterns Layer                  │  ← Ask, Routing, Circuit Breaker
├─────────────────────────────────────────┤
│          Runtime Layer                   │  ← Scheduler, Mailbox, Timer
├─────────────────────────────────────────┤
│            Core Layer                    │  ← Actor, Message, System
├─────────────────────────────────────────┤
│        Foundation Layer                  │  ← Concurrency, Serialization, Network
└─────────────────────────────────────────┘
```

### Layer Responsibilities

1. **Foundation Layer**: Provides concurrency primitives, serialization, network communication infrastructure
2. **Core Layer**: Defines core abstractions like Actor, Message, System
3. **Runtime Layer**: Provides high-performance execution engine
4. **Patterns Layer**: Implements common Actor patterns
5. **Distribution Layer**: Provides distributed capabilities
6. **Integration Layer**: Provides monitoring, logging integration features
7. **API Layer**: Provides unified external interface

### Layer-to-module mapping

| Layer | Module |
|---|---|
| Foundation | `cactor_foundation` (the circuit breaker also lives here) |
| Core | `cactor_core` |
| Runtime | `cactor_runtime` |
| Patterns | `cactor_patterns` |
| Distribution | `cactor_distribution`, `cactor_streams`, `cactor_xstate` |
| Integration | `cactor_integration` |
| API | `cactor` (facade module) |

See [Package and Module Structure](package-structure.md) for module dependencies.

## Getting Started

- [Quick Start](QUICK_START.md)
- [Getting Started Guide](getting-started.md)
- [Features](FEATURES.md)

## Design and Reference

- [Architecture](architecture.md)
- [Package and Module Structure](package-structure.md)
- [API Reference](api-reference.md)

## Development and Operations

- [Best Practices](BEST_PRACTICES.md)
- [Code Style](CODE_STYLE_GUIDE.md)
- [Performance Tuning Guide](performance.md)
- [Monitoring and Logging](MONITORING.md)
- [Deployment](DEPLOYMENT.md)
- [Security](SECURITY.md)
- [Troubleshooting](TROUBLESHOOTING.md)
- [Migration](MIGRATION.md)

## Planning

- [Roadmap](roadmap.md)
- [Changelog](../../CHANGELOG.md)

[中文](../zh/README.md)
