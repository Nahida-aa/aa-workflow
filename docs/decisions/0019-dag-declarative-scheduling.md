# 0019 — DAG 声明式调度：`dag_to_handler(spec)` 自动生成 handler 函数体

**状态**：已接受（2026-11-21；2026-11-21 订正：去掉接管式中间件，统一为 handler 工厂）
**关联**：[0006](0006-dag-dual-drive-and-middleware.md)、
[0007](0007-middleware-compose-order.md)、[0016](0016-step-onion.md)、
`feature-requirements.md` F7、
`learn_ls/wf-demo/packages/workflow-dag/src/index.ts:167-196`、
`learn_ls/wf-demo/packages/workflow-middlewares/src/dag.ts:34-62`

## 问题

[0006](0006-dag-dual-drive-and-middleware.md) 已把「DAG 调度本身由中间件接管、
用户不写 handler」列为已接受的决策，TS 实验线也已有 `dag(spec)` 与
`dagToHandler(spec)` 两个入口。但 Rust 侧只落地了策略层（`reseed` /
`interim_goals` / `dag_policies`）和名单计算（`DagShape`），**调度这一半从未实现**。

实际后果不是「少个语法糖」：`docs/.../dag_policies.rs` 的 handler 必须手写
`for id in ["load", "enrich", "translate", "merge", "publish"]`，把 shape 里
声明过的节点再抄一遍。图和执行没有驱动关系——改图不改 handler 不会报错，
两边不一致也不会被发现。F7 的场景（声明 `DagSpec` 就按依赖自动并发地跑）无从满足。

## 决策

把声明与调度核心放进 `aa_workflow_middlewares::dag::core`（原 `aa_workflow_dag` 包
已迁入并删除，DAG 代码与测试集中一处），对外只暴露一个入口：`dag_to_handler(spec)`
生成 handler 函数体，用户挂到 `.handler(dag_to_handler(spec))`：

```rust
// dag::core::declare
pub type DagRunFn = Arc<
    dyn Fn(BTreeMap<String, Value>, Value) -> BoxFuture<'static, Result<Value>>
        + Send + Sync,
>;
pub struct DagNode { pub deps: Vec<String>, pub run: DagRunFn }
pub struct DagSpec { pub id: String, pub nodes: BTreeMap<String, DagNode> }

// dag::core::scheduler
async fn run_dag(ctx: &WorkflowCtx, spec: &DagSpec) -> Result<BTreeMap<String, Value>>;
pub fn dag_to_handler(spec: Arc<DagSpec>) -> /* impl Fn(WorkflowCtx) -> BoxFuture */;
```

```rust
create_workflow(config)
    .middleware([dag_policies(shape, get_events)])  // 策略照旧挂
    .handler(dag_to_handler(spec));                  // 调度：handler 天然最内层
```

**为什么不保留接管式中间件**：曾做过 `dag_scheduler(spec)` 中间件（`take_over()`
接管 handler），实测证明它与 handler 工厂语义等价但更差——

- `take_over()` 是冗余的：推进到内层的唯一入口是 `sc.next()`，不调它 handler
  自然不执行；去掉 `take_over()` 后 `does_not_run_user_handler` 测试照样通过。
  所谓「Rust 可接管 handler、TS 不行」在本场景没有意义。
- 中间件方案要求用户写空壳 handler，且 `dag_scheduler` 必须排在策略之后
  （[0007](0007-middleware-compose-order.md) 的组合顺序约束），写反了复用静默失效。
- handler 方案里 handler 天然最内层，顺序约束自动满足，更短也更不易错。

删掉中间件后，「Rust 用 `take_over` 接管 vs TS 经 `ctx.dagResults` 转手」的
机制差异不再有实际使用者；两侧统一为 handler 工厂（TS `dagToHandler` ↔ Rust
`dag_to_handler`），反而消除了一个分歧点。

- **调度语义**对齐 TS 参考：每个节点对应一个 future，内部等自己的依赖
  `ensure(dep)` 全部完成，再调 `ctx.step(id, ..)`；依赖结果按 id 装进 `deps`
  传给节点函数，run input 原样透传。合并用 `tokio::task::JoinSet`，不按波
  （Kahn 层）栅栏等待。
- **输入/输出**：run input 就是用户业务 input；output 为
  `{ "results": { 节点 id → 结果 } }`，与 `dagToHandler` 的 `{ results }` 同形。
- **声明期校验**由 `define_dag` 调 `DagShape::build` 完成：重复节点 / 未知依赖 /
  环全部在构建时报错；`__dag*` 前缀的节点 id 保留。
- **组合顺序**：用 `dag_to_handler` 时 handler 天然最内层，[0007](0007-middleware-compose-order.md)
  的顺序约束自动满足——策略中间件照常挂 `.middleware([...])`，无需担心调度器排在哪。

## 被否决的选项

### 1. 让 `dag_policies` 在 `next()` 前「补发」缺失的 step

曾设想：中间件拿到 shape 后，在 handler 前调用 `ctx.step` 把图里的节点跑掉。
探针实测证明它能跑（`exec = ["pre", "handler", "post"]`，run `Finished`），
但有两个致命问题：

- **看不到 handler 内部的 step 点**：中间件眼里 `sc.next()` 是一个不透明的
  `Value`，无法知道 handler 跑了哪些节点，更不可能在节点之间「插入」；
- **绕过包装层**：内层注册的 `wrap_step` 在 `next()` 之后才生效。同一探针里
  记录层只看到 `handler`，没看到 `pre`——即 `reseed` / `interim_goals` 这些
  策略对预发 step 静默失效，重开一个 [0007](0007-middleware-compose-order.md)
  踩过的坑。

要同时满足「全部节点都经过统一包装」，正解是让调度本身跑在 handler 里
（`dag_to_handler`）——handler 天然是洋葱最内层，所有策略的 `wrap_step` 都已注册。
用中间件接管是另一条等价但更绕的路（见「决策」里被否的理由）。

### 2. 按拓扑波（Kahn 层）逐波 `try_join!`

实现更简单，但波栅栏会让「第 1 波里先完成的节点」白等同波最慢的节点才推进
后继。TS 参考实现明确按**每节点就绪**调度（`index.ts:174-176` 注释：
「严格按 DAG 偏序，与手写 try_join! 的链级并发同构，无波栅栏的假等待」）。
`tokio::sync::OnceCell` + `JoinSet` 足以实现同样的**按依赖自动并发**，没有理由接受这层损失。

### 3. 改 core 让引擎持有 DAG 并自动调度

上游主线 core 明确「engine has no list of them and no graph」
（`run-workflow.ts:126-129`），code-as-DAG handler 重放是底座
（`docs/dag-strategies.md` §1 强信号 #2）。改引擎会破坏对齐、牵动 replay 与
store 契约，违反 [0001](0001-strategy-vs-mechanism.md) 的策略/机制分层。
本能力限定在实验线派生层，不动 core。

## 代价

- **两侧统一为 handler 工厂**：TS `dagToHandler` ↔ Rust `dag_to_handler`，都不依赖
  中间件接管，行为同构。曾尝试的 `take_over()` 中间件方案已删（等价但更绕，
  见「决策」）。
- **只支持静态图**：运行时分支 / 动态子图 / 循环不可表达，与 TS `DagSpec`
  的边界一致（见 `wf-demo/workflow-dag/src/index.ts:91-96`）。需要这些时用
  原生 handler。
- **输出形状固定为 `{ results }`**：想要别的形状需在 `dag_to_handler`
  路径外包一层，或用原生 handler。
- **取消语义**：第一个节点失败时 `JoinSet::abort_all()`，避免孤儿任务继续写
  checkpoint。

## 证据

- 探针（已删）：去掉 `dag_scheduler` 中间件的 `take_over()` 后，`does_not_run_user_handler`
  测试照样通过——证明不调 `next()` 就足以跳过 handler，`take_over` 冗余；
  另一探针证明 `next()` 前发的 step 绕过内层包装（记录层只看到 `handler`）。
- TS 参考：`wf-demo/packages/workflow-dag/src/index.ts:167-196`（memo 化
  `ensure` 按依赖调度，`dagToHandler` 入口）。
- ADR 0006 已记录 Rust/TS 中间件返回值差异（`:63-66`）；ADR 0007 已实测
  顺序错误的静默后果（`:30-39`），本决策改用 handler 工厂后该约束自动满足。
