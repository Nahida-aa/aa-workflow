# 0022 — run 是不可变事实：input 不可变、errored 终态、恢复走 reseedRun

**状态**：已接受（2026-11-21）
**关联**：[0021](0021-typestate-middleware-registration.md)（typestate 注册）、
[0020](0020-scope-borrowed-middleware-server.md)（scope-borrowed server）、
`docs/concepts/ctx-state.md`（三通道表）、
`docs/experiments/middleware-protocol.md` §8.1（workflowId 漂移考证）、
`examples/store_file/tests/dub_sf_ocr_dag.rs`（四场景实测）

## 背景

连续三轮追问触发的完整设计审查：

1. 「为什么没有 updateInput」—— errored 之后想恢复，大多时候可能想**改
   input** 再继续；
2. 「多 input 单 log 怎么样」—— 把 input 变更也变成 append-only 事件，
   一个 run 的日志里容纳多种 input；
3. 「run 记录的 workflowId 被定义 id 覆盖，是不是 bug」—— FileExecutionStore
   的 save_run_state 用 core RunState（其 workflow_id = 定义自身 id）投影
   覆写 run 记录，注册键被替换。

## 决策

1. **input 是启动时事实，不可变**：run 记录的 input 在 createRun 一次写入；
   resume 传存储值（不读调用方参数）；store 契约**无** update/patch input。
2. **errored = 终态**：恢复型续驱（同 run 重驱）= 重放日志 + 失败 checkpoint
   rethrow——不是恢复（`errored_run_redrive_rethrows` 测试钉住：闭包不重入、
   下游不推进）。
3. **跑错后的恢复路 = reseedRun**：新 run（**新 input ✓ 新代码 ✓ 新外部状态 ✓**）
   + 源 run 幸存 checkpoint 导入（`imported_from` 溯源）+ 失败 step 真跑
   （缺席即待续）。
4. **否决多 input 单 log**：语义与 reseedRun 完全等价，但需要三样新机制
   （分段 replay / checkpoint 有效性行情 / 审计段边界），且打破「一个 run
   一段事实」的审计模型。
5. **否决 updateInput**：破坏 input「启动时事实」的定位——RunState.input
   与 handler 的 `ctx.input` 的整个语义依赖它不可变。
6. **run 记录的 workflowId = 定义自身 id（首次 drive 后）是上游同构行为，
   非 bug**：上游 `run-workflow.ts:300` `workflowId: workflow.id` +
   `setRunState` 落盘同款；惯例是**注册键 == 定义 id**（同一字符串写两处），
   runtime 的 `load_workflow` 在不一致时告警（容错不拒绝）。

## 理由

三条实测事实（本仓 + 上游）：

1. **input 是 run 级不可变的**：两侧 store 契约均无 update/patch input；
   resume 传存储值（我们 `runtime-driver.rs` resume 传 `run.input.clone()`，
   上游 `runtime-driver.ts:529` 同）；
2. **续驱只达泊车，不达 errored**：上游 guide 原话 "New signals to
   finished, errored, or aborted runs return **not-waiting**"；我们同款
   （`NotWaiting` / `NotClaimable`）；
3. **errored 的恢复型续驱 = rethrow**：`errored_run_redrive_rethrows`
   测试钉住（重放日志、失败 checkpoint rethrow、闭包不重入）。

**Temporal Continue-As-New 先例**：对「input 要变 / 历史超长」的同类需求，
Temporal 的答案是**显式新 run + continue-as-new 链**——不是多 input 单 log。
链可回溯（新 execution 链接旧 execution），保留「一个 execution 一段事实」
的审计模型。reseedRun 是同一决策（多带了幸存导入——Temporal 的重跑由
caller 用同一 workflow id 重跑实现，无跨 run 导入）。

## 被否决的选项

- **多 input 单 log**：INPUT_RECORDED 事件 + 分段 replay + checkpoint
  有效性行情 + 审计段边界——三样新机制，语义与 reseedRun 完全等价；
  且「一个 run 一段事实」的审计模型被打破（run 产物 = 多段拼接）。
- **updateInput**：RunState.input 与 `ctx.input` 的「启动时事实」定位
  崩塌；RunState.input 与 handler 消费的 input 的整个语义依赖不可变；
  且它给「嵌套 durable 操作」开了第二扇门（改 input 续驱 = 变相跳过
  失败 checkpoint 的语义问题）。
- **errored 恢复型续驱**：rethrow 不是恢复（见决策 2）；四方法全不可用
  是刻意设计（对齐上游）。

## 后果

- **「世界变了」 = 新 run（新 input / 新代码 / 新外部状态）**——reseedRun
  的 input 是新 run 的参数，所以「改 input 恢复」已经被覆盖：**新 run 的
  input 就是改了的 input**。
- **run_id 对外稳定的收益 → 宿主别名层**（外部引用指向稳定 job 标识，
  job → 当前 run_id 映射，reseedRun 时更新指向）——不动引擎。
- **定性纪律**：定性 bug 前先查上游（workflowId 漂移的教训：从
  「store bug」修正为「测试键名不一致 + 惯例未成文」）。
- 错误分类学（错误恢复想改什么）：输入数据错 → 改 input ✓；代码 bug →
  新版本 ✓；服务故障恢复 → input 标志翻转 ✓——**全部通过「新 run +
  reseedRun」承载**，同 run 续驱的缺失不构成能力缺口。
