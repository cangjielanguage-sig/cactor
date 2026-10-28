# CActor Package & Module Structure

CActor is a cjpm **workspace** made of 12 Cangjie **modules**. In Cangjie, a module is the tree formed by a root package and all of its subpackages, and the root package name must equal the module name. Cyclic dependencies are forbidden between packages and between modules.

## Layout

```
cactor/
├── cjpm.toml                 # [workspace]: members + shared compile options
└── modules/
    ├── foundation/           # module cactor_foundation
    │   ├── cjpm.toml         # [package] name = "cactor_foundation"
    │   └── src/
    │       ├── pkg.cj        # root package cactor_foundation
    │       └── queue/ ...    # subpackage cactor_foundation.queue
    ├── core/                 # cactor_core
    ├── patterns/             # cactor_patterns
    ├── runtime/              # cactor_runtime
    ├── cactor/               # cactor (facade: cactor / cactor.api.* / cactor.config.*)
    ├── macros/               # cactor_macros (macro package)
    ├── streams/              # cactor_streams
    ├── distribution/         # cactor_distribution
    ├── xstate/               # cactor_xstate
    ├── integration/          # cactor_integration
    ├── tests/                # cactor_tests
    └── examples/             # cactor_examples
```

## Modules and dependencies

| Module | Files / lines | Direct deps | Subpackages |
|---|---|---|---|
| `cactor_foundation` | 35 / 9,350 | — | abstractions, circuit_breaker, config, contracts, di, memory, monitoring, network, queue, serialization, simple_config |
| `cactor_core` | 31 / 6,733 | foundation | actor, config, context, error, guardian, memory, message, supervision, system, typed |
| `cactor_patterns` | 11 / 2,990 | foundation, core | ask, backpressure, routing(.advanced/.factory) |
| `cactor_runtime` | 63 / 16,185 | foundation, core, patterns | actor, context, dispatcher.*, events, execution, guardian, lifecycle, mailbox.*, message, monitoring, registry, scheduler, supervision, system, typed, wrappers |
| `cactor` | 16 / 2,140 | core, runtime | api, api.config, api.extensions, api.public, config, config.file |
| `cactor_macros` | 1 / 695 | — | (the macro package itself) |
| `cactor_streams` | 6 / 2,551 | core, cactor | core, dsl, unified |
| `cactor_distribution` | 20 / 4,993 | foundation, core, cactor | cluster, persistence, remote, streaming |
| `cactor_xstate` | 17 / 4,867 | core, cactor, distribution | core.*, features.*, enterprise.*, lang.macros |
| `cactor_integration` | 28 / 8,138 | foundation, core, runtime, cactor | configuration, logging, monitoring, testing (executables) |
| `cactor_tests` | 37 / 4,812 | core, runtime, cactor | unit.*, integration |
| `cactor_examples` | 61 / 14,015 | all of the above | 38 runnable examples |

Dependency direction (bottom-up, acyclic):

```
foundation ─► core ─► patterns ─► runtime ─► cactor ─┬─► streams
                                                     ├─► distribution ─► xstate
                                                     └─► integration
tests / examples sit on top; macros is standalone and only used by examples
```

## Visibility conventions

- **Module public API** lives in `public` packages (the default) as `public` declarations.
- **Module internals** are declared `protected package`: visible only inside the module, and any `import` from another module is a compile error. Currently protected in `cactor_runtime`: actor, context, guardian, registry, message, monitoring, execution, scheduler, mailbox.lockfree, mailbox.ringbuffer, dispatcher.batch_processing / optimized / work_stealing.
- **Re-export**: a module's root package exposes a curated API with `public import x.y.{A, B}`. For example, `cactor_patterns` re-exports `cactor_foundation.circuit_breaker.*`, so `import cactor_patterns.*` is enough.

## Commands

```bash
cjpm build                                        # build the whole workspace
cjpm test                                         # run @Test in all modules
cjpm run --name cactor_examples.hello_world_demo  # run an example from the workspace root
```
