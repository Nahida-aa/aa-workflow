# 0018 — 删除引擎上的策略参数：`interim_goals` / `reseed_run`

**状态**：已接受（2026-11-06）
**关联**：[0001](0001-strategy-vs-mechanism.md)（本决策的兑现）、
[0016](0016-step-onion.md)（step 层）、[0011](0011-run-meta-and-plugins.md)（meta）

## 问题

`RunWorkflowOptions` 上有两个策略字段：

```rust
pub interim_goals: Option<Vec<String>>,   // 命中即停车
pub reseed_run: Option<ReseedRun>,        // 新 run 导入源 run 的 checkpoint
```

它们是 [0001](0001-strategy-vs-mechanism.md) 明确要删的东西——引擎只提供机制。
但当时删不掉，因为中间件版缺两件事：

1. **`reseed` 拿不到 `imported_from` 溯源**（引擎版把它写进 `STEP_FINISHED.meta`）
2. **`interim_goals` 的泊车时机**依赖引擎内部的「本 drive 已完成」计数

## 决策

两个字段连同 `ReseedRun` 类型一起**从 core 和 runtime 删除**。能力全部由
`aa_workflow_middlewares` 的 step 层承担。

## 为什么现在能删

两个缺口都被 step 层补上了，而且是**同一批改造**的结果：

| 缺口 | 补法 |
| --- | --- |
| `imported_from` 溯源 | `StepCall::skip_with_meta(result, StepMeta)`——短路时同时写事件 meta；`Decision::SkipImported` 是层的第三种处置 |
| 「已完成」判定 | `StepCall::completed`——引擎在构造每次调用时从 `live` 表取快照，层变成纯函数 |

`completed` 能用的两条前提已核实：`STEP_FINISHED` 在包装链返回**之前**落盘；
泊车结束整条 drive，所以同一 drive 不会泊两次。

## 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| 保留字段，标 deprecated | 两条语义路径并存——「引擎泊车」与「层泊车」的结果一致但实现两套，改一处忘另一处就漂移 |
| 保留 `ReseedRun` 类型给中间件用 | 中间件只需要 `redo_set: Vec<String>`；`workflow_dag` 改成自己的 `RedoPlan`，不必让 core 继续持有策略词汇 |
| 只删 `interim_goals` | 两者是同一决策的两半，半删只会延长过渡期 |

## 代价

- **`workflow_dag::DagShape::reseed_plan` 签名变了**：去掉 `source_run_id`，
  返回 `RedoPlan { redo_set }`。源 run id 由调用方（中间件）自己带着，名字不该
  出现在「算名单」这一步。
- **`WorkflowRuntimeStartRunArgs.reseed_run` 消失**：runtime 正门不再能直接表达
  redo。需要走中间件（`reseed` / `dag_policies`）——这正是 [0001](0001-strategy-vs-mechanism.md)
  要的方向，但对已有的 runtime 调用方是破坏性变更。
- **删掉了 6 条引擎级测试**（4 条 `interim_goals`、2 条 `reseed_run`，另有 2 条
  demo）。它们验证的是被删掉的实现；等价语义由
  `aa_workflow_middlewares/tests/{interim_goals,reseed,dag_policies,completed_snapshot}.rs`
  覆盖。

## 证据

- `cargo check --workspace --all-targets`：0 error
- `cargo test --workspace`：291 passed
- `cargo test -p aa_workflow_store_sqlx_postgres`：2 passed（本地 PG）
- 中间件 `reseed` 的 `imported_from` 断言：`tests/reseed.rs`
