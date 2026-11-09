# `EngineRuntime` 字段对照：哪些对齐上游、哪些是本仓扩展

**上游基线**：`tanstack/workflow` 的 `packages/workflow-core/src/engine/run-workflow.ts:787`
（`interface EngineRuntime`）。

**为什么要有这份文档**：本仓改引擎时反复出现过「加了一个上游没有的字段、
或把上游的字段换个名字」的情况（`log_len` / `publish_tx` / `lives` /
`default_step_retry` / `*_counter` 都改过一轮）。留下一张对照表，下次动
`EngineRuntime` 前先查它：**上游有同义字段就用上游的名字和位置**，确实要新增的
必须在这一页写成「本仓扩展」并说明理由。

---

## 一、与上游对齐（同名同义）

| 本仓 | 上游 | 备注 |
| --- | --- | --- |
| `run_id: String` | `runId: string` | |
| `input: Value` | （无独立字段，走 handler 参数） | 本仓把 input 存在引擎上，供 `StepCtx` 与校验读 |
| `workflow: AnyWorkflowDefinition` | `workflow: AnyWorkflowDefinition` | **必须是 `select_workflow_version` 选中的那一版**，不是 `opts.workflow` |
| `store: Arc<dyn RunStore>` | `runStore: RunStore` | |
| `emit: UnboundedSender<WorkflowEvent>` | `emit: (event) => void` | 上游是闭包捕获 `queue` + `resolveWait`；本仓用 channel 顶替，语义相同 |
| `history: Mutex<Vec<WorkflowEvent>>` | `history: ReadonlyArray<WorkflowEvent>` | 内存日志副本，重放短路与「已完成集合」都扫它 |
| `next_log_index: AtomicUsize` | `nextLogIndex: number` | 上游 `++` 自增，本仓 CAS 重定基（并行分支会同时追加） |
| `counters.now / .uuid / .yield_` | `counters.{now, uuid, yield}` | 上游还有 `wait` / `sleep` / `approve` / `goalParks`，见下节 |
| `deadline: Option<i64>` | `deadline?: number` | |
| `min_yield_remaining_ms: u64` | `minYieldRemainingMs: number` | |
| `yield_resume_at: Option<i64>` | `yieldResumeAt?: number` | |
| `prev_state_snapshot` | `prevStateSnapshot` | `STATE_DELTA` 的 diff 基线 |
| `state_mirror` | `state` | 上游是裸对象；本仓多一个 `Arc<Mutex<..>>` 供 handler 的 `StateHandle` 共享 |
| `compensations` | `compensations` | |

## 二、上游有、本仓**没有**（有意）

| 上游 | 为什么不在这里 |
| --- | --- |
| `consumed: Set<number>` | 上游的 `findCheckpoint` 靠它做「一次调用只消费一条同种 checkpoint」；本仓的 `ctx.step` 用 `live(step_id)` 按 id 查，不存在顺序匹配问题 |
| `paused: boolean` | 上游用它让外层 catch 知道别再写终局事件；本仓用 `Err(WorkflowParked)` 这个错误类型表达 |
| `applicationStepIds: Set<string>` | 用于拒绝 `__compensation:` 前缀；本仓在 `register_compensation` 里校验 |
| `compensationError` / `compensating` | 本仓的补偿执行在 `run_compensations` 里用局部变量推进 |
| `interimGoals` / `interimGoalsDone` / `goalsDoneAtDriveStart` | 引擎版临时目标已删除（[0018](../decisions/0018-remove-engine-strategy-params.md)），能力在 `aa_workflow_middlewares::interim_goals` |
| `counters.wait / sleep / approve / goalParks` | 对应原语的 id 在本仓由调用方提供，或该能力已移到中间件 |
| `pluginHooks` / `pluginCtx` | 本仓没有插件系统；钩子能力由中间件承载（[0016](../decisions/0016-step-onion.md)、[0017](../decisions/0017-meta-typed-keys.md)） |
| `telemetry` | 本仓未接 tracing |
| `abortController` | 本仓用 store 里的 run 状态做取消判定（`is_cancelled`） |

## 三、本仓扩展（上游没有）

| 字段 | 为什么必须有 |
| --- | --- |
| `gate: Arc<Gate>` | `StepOptions.resource` 的资源门（按 key 串行）。上游没有这个能力 |
| `step_layers: Mutex<Vec<StepLayer>>` | step 洋葱（[0016](../decisions/0016-step-onion.md)）。上游靠覆盖 `ctx.step`，没有层 |
| `ext_extra: Mutex<Value>` | `ctx.ext` 的命名空间扩展（[0015](../decisions/0015-ext-namespaces.md)） |
| `meta: Option<Value>` | run 级参数（[0011](../decisions/0011-run-meta-and-plugins.md)、[0017](../decisions/0017-meta-typed-keys.md)） |
| `pending_retries` | 耐久重试的待重试状态，驱动启动时从日志折叠 |
| `state_mirror` 的 `Arc` 共享 | 上游 `state` 是裸对象；本仓 handler 的 `StateHandle` 与引擎必须看同一份 |

## 四、判断规则

动 `EngineRuntime` 前依次问：

1. **上游有同名字段吗？** 有 → 用它的名字、位置、语义，不改造。
2. **上游有语义相同但不同名的字段吗？** 有 → 用上游的名字
   （`log_len` → `next_log_index`、`publish_tx` → `emit` 都是这么改的）。
3. **上游把它放在别处吗？** 例如 `defaultStepRetry` 在
   `engine.workflow` 上不在 engine 上 → 照上游放。
4. 都不是 → 才新增，并**在上表第三节登记 + 写理由**。

> 为什么第 1 条这么较真：同名同义让「读一方代码就能懂另一方」成立。
> 反过来，本仓曾把三级结构压成一级（`default_step_retry`）、把两级命名
> 拆成平铺（`*_counter`）、给同义字段起内部实现名（`publish_tx`），
> 每次都导致读代码的人要做一次翻译，而且改完就忘了为什么不同。
