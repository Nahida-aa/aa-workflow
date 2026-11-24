# 功能需求清单

本文件是**唯一的功能需求真相源**：记录「我要的功能」——场景、为什么现有手段不够、
自然形态是什么、两侧（f2-lite TS / aa-workflow Rust）状态。动源码前先读这里；
新增需求先在这里登记，再进任一线实现。

配对纪律：`workflow` 仓 f2-lite 分支（TS 实验线）↔ `aa-workflow`（Rust 产线）。
新原语一般先在 f2-lite 验语义，再镜像到 Rust。

## 状态总表

| # | 功能 | f2-lite (TS) | aa-workflow (Rust) | 状态 |
| --- | --- | --- | --- | --- |
| F1 | 同 run 继续：resume + 失效名单 + 临时目标 | ❌ | ❌ | **设计中，未实现** |
| F2 | redo：新 run 播种幸存 checkpoint | ✅ | ✅ | 已实现（原 `reexecuteFrom`，已改名 `reseedRun`） |
| F3 | 临时目标泊车（interim goals） | ✅ | ✅ | 已实现 |
| F4 | 信号收件箱（buffer_signals） | ✅（基线 taren 分支） | ✅ | 已实现（Rust 侧 `c536e03` 等） |
| F5 | 耐久重试 + 补偿 | ✅ | ✅ | 已实现 |
| F6 | INVALIDATED 耐久失效原语 | ❌ | ❌ | **已评估：不做**（区别于 F2 redo/seed，见 `docs/concepts/redo-vs-invalidation.md`） |
| F7 | DAG 声明式调度（图驱动 handler） | ✅（wf-demo `dag` / `dagToHandler`） | ❌（只有 `dag_policies` 策略层） | **已接受未落地**（ADR 0006/0007） |

## F1. 同 run 继续：resume + 失效名单 + 临时目标

**场景**：一个已经跑完 / 已停下（到达 interim goal 或 error）的 workflow，
resume 时可以选择：

- **失效名单**：哪些 step 作废重跑；
- **临时目标**：跑到哪些 step 停；**目标为空 = 跑到终点**；
- 同一个 run 继续，**不新开 run**。

**为什么现有手段不够**：

- `interim_goals`（F3）只管「停在哪」，不管「哪些重跑」；
- `reseed_run`（F2）能表达失效，但要**开新 run**：新 run id、外部引用断裂、
  input 重喂、审计跨两个 run 拼。为「换目标继续」付这些代价，收益是零——是 hack。

**自然形态**：~~drive 参数 `invalidate_steps: Option<Vec<String>>`，与
`interim_goals` 并列（core 的 `RunWorkflowOptions` 与 runtime 的
`WorkflowRuntimeStartRunArgs` 各加一处，两侧同名同形）~~。

> **2026-11-06 订正**：这个「与 `interim_goals` 并列加参数」的形态**不采纳**。
> 两个参数已经从 core 与 runtime 删除（[0018](../decisions/0018-remove-engine-strategy-params.md)），
> 能力移到 step 层（`wrap_step`）。按 [0001](../decisions/0001-strategy-vs-mechanism.md)，
> 候选形态应是中间件：`reseed` 已有等价实现，未来若要「同一个 run 继续、只失效
> 部分 step」，落点也在中间件，不在引擎。

语义与 `reseed_run.redo_set` 是**同一个 advisory 语义**（引擎**不**校验名单对依赖方向
封闭——不封闭**可能**让下游拿到旧 checkpoint，是否可接受由调用者业务判断：
失效 B 不失效下游 C，C 会拿旧结果短路），只是作用对象是一次 drive 而非新 run 导入。

**机制**（已对 Rust 代码核实）：drive 启动时从折叠出的 `lives` 删掉被点名的
entry，step 回 `Pending` → 重跑 → 追加新 `StepFinished` → 重放时**后者胜**
（`fold_step_states` 覆盖写）。不碰事件系统、不碰 store 契约、不开新 run。

**两个必须处理的细节**：

1. 同时清 `pending_retries`——耐久重试的到期检查在 replay 短路**之前**，
   不清会先在 `__timer` 泊车、根本不重跑；
2. `goals_done_at_drive_start` 用失效**后**的 lives 算——否则「失效的 step 恰好
   是 goal」会被误判为已达成。

**设计细节与判据**：`workflow` 仓 `docs/internal/f2-lite-feature-points.md`
（含官方 ai-orchestration 用法对照、「隐式新开 run」的四条评估标准）。

**落地顺序**：f2-lite 先行加 `invalidateSteps` 并写测试 → Rust 镜像 → 回本文件
更新状态。

## F2. redo：新 run 播种幸存 checkpoint（已实现，实现载体已迁移）

`reseed_run(source_run_id, redo_set)`：全新 run 首驱前导入源 run
幸存的 `STEP_FINISHED`（失败 step 按设计重跑），溯源 `meta.importedFrom`。
两侧都有（f2-lite `reseedRun` / Rust `057a0c6`、runtime 透传 `91111f3`）。

> **2026-11-06 订正**：Rust 侧的**引擎参数**已删除（[0018](../decisions/0018-remove-engine-strategy-params.md)）。
> 等价能力在 `aa_workflow_middlewares::reseed`：`skip_with_meta` 写
> `STEP_FINISHED.meta.imported_from`，与删除前的引擎实现语义一致（有测试锁定）。
> f2-lite 侧仍是引擎参数，两侧载体不同但语义对齐。

**已改名 `reseedRun`**（原 `reexecuteFrom`）：旧名没体现"落到一个新 run 里"，
且 `re-` 前缀保留（增量播种 + redo 词族）。0.x 阶段两侧同步、直接换新名、
不留 alias。论证见 `docs/concepts/redo-vs-invalidation.md` §6。

**定位**：显式新开 run = 常规手段，服务于「换世界重来 + 旧日志原样不动」，
不是继续手段。日志型代价（新 id / 引用断 / 审计跨 run）由它的问题域承担。

> **注意**：F2 的 redo（seed）≠ F6 的 INVALIDATED。redo/seed 机制轻（零新事件、
> 零 store 契约变更，已落地），**保留**；INVALIDATED 持久失效原语才是不做的对象。
> "redo 不进 core" 若被听成"seed 也不做"是误读——seed 是失败续跑的正确默认路径。

## F3. 临时目标泊车（已实现，实现载体已迁移）

`interim_goals`：名单内 step 全部 checkpoint 后泊车（保留信号 `__interim-goal`），
宿主续驱 = 同名信号投递（Rust）或 `recover: true` drive（f2-lite core）。
per-drive 参数，不落盘。转移检查防重挂。

> **2026-11-06 订正**：Rust 侧的**引擎参数**已删除（[0018](../decisions/0018-remove-engine-strategy-params.md)）。
> 能力在 `aa_workflow_middlewares::interim_goals`（step 层），「本 drive 已完成」
> 读 `StepCall::completed` 快照。

## F4. 信号收件箱（已实现）

早到信号：`buffer_signals` 开关（默认关，对齐上游）+ store 收件箱
（signalId 去重、终态与 `__timer` 拒收、一次性消费）+ core 等待点消费后落账。
Rust 侧四个事件类型、`signal_id` 进日志、投递过闸均已对齐基线 `0971b4c`。
TS 侧基线 `taren/durable-recovery` 原生有 `bufferSignals`。

## F5. 耐久重试 + 补偿（已实现）

`retry.durable`（`StepRetryScheduled` 泊车 + sweep 续驱 + `StepAttemptStarted`
事件）与 `ctx.compensate`（`CompensationRegistered/Started/Completed/Failed` +
逆序 undo + 重复 id 报错）。两侧对齐基线。

## F6. INVALIDATED 耐久失效原语（已评估：不做）

与 F1 的区别：F1 的失效是 **drive 参数**（不传就不失效）；INVALIDATED 要求
失效**持久生效**（下次 resume 不传参也作废）且日志可独立读出「这步被作废过」，
需要新事件 + 重放规则 + 状态转换。

**不做**的理由：当前没有「跨 drive 持久失效」的场景；F1 的 drive 期过滤 +
日志两条 `StepFinished` 后者胜已覆盖需求。若将来出现该场景（例：审计要求
「作废决定本身是耐久事实」），按配对纪律两遍实现，**先写设计文档**。

更详尽 rationale（engine complexity、redo trade-offs、decision tree、upstream
supporting points）见 `docs/concepts/redo-vs-invalidation.md`（ADR 式，编号与本仓
F1/F2/F6 对齐）。

## F7. DAG 声明式调度（图驱动 handler）

**状态**：设计已接受（[ADR 0006](decisions/0006-dag-dual-drive-and-middleware.md) /
[0007](decisions/0007-middleware-compose-order.md)），**Rust 侧未实现**。
TS 实验线 `learn_ls/wf-demo` 已有 `dag(spec)` 与 `dagToHandler(spec)` 两个入口；
本仓目前只有 `DagShape`（名单计算）和 `dag_policies`（策略层），**调度那一半缺失**——
handler 里跑哪些 step 仍由用户手写（测试里就是把 shape 的节点名抄一遍）。

**场景**：用户声明 `DagSpec { id, nodes: { id → { deps, run } } }`，希望 workflow
按依赖关系自动并发执行全部节点（有依赖的按先后、无依赖的同时跑）：依赖结果按节点 id 传给下游，节点函数只通过
`ctx.step` 做耐久副作用，用户**不写 handler**。

**为什么现有手段不够**：

- `DagShape` 只算 `reseed_plan` 的名单（`from ∪ descendants`），不发起 step；
- `dag_policies` 的 `Decision` 只有 `Pass / Skip / SkipImported / Park`
  （`workflow_core::middleware::step_layer`），只能处置 handler 已发出的 step，无法发起节点；
- 因此图对**执行**没有任何驱动力：声明与 handler 不一致时不会报错，
  改图还要同步改 handler 两处（ADR 0006 称之为「调度是 handler 的职责」）。

**自然形态**（两侧同构，对齐 wf-demo）：

- `dag_to_handler(spec)` 工厂：同一套调度逻辑做成 handler 函数体，用户挂到
  `.handler(dag_to_handler(spec))` 即可，id / input / meta 等元数据仍自己控制。
  另有一个被否的 `dag_scheduler(spec)` 中间件（`take_over` 接管）：等价但更绕，
  已删除，理由见 ADR 0019。

调度语义（对齐 `wf-demo/workflow-dag/src/index.ts:167-196`）：每个节点一个任务，
**自身依赖的 future 全部 resolve 后立即执行**（按依赖自动并发、无波栅栏的假等待），
memo 去重保证每节点恰好一个任务；节点函数收到 `(deps, input)`，`deps` 的键是
直接依赖的节点 id。

**与上游主线的关系**：TanStack 主线 core 无图、无调度（code-as-DAG handler
重放，见 `docs/dag-strategies.md` §1 强信号 #2）。本能力来自本仓 f2-lite/wf-demo
实验线，是**有意偏离**；偏离点与代价在
[ADR 0019](decisions/0019-dag-declarative-scheduling.md) 记录，不并进主线对齐口径。

**组合约束**：handler 天然是最内层，[0007](decisions/0007-middleware-compose-order.md)
的顺序约束自动满足——策略中间件照常挂 `.middleware([...])`，调度调到的 step
必经过策略的 `wrap_step`，无需担心"调度器排在哪"。

**通用约束核对**：不涉及新开 run；不碰 store 契约；命名沿用 wf-demo
（`dag_to_handler` / `DagSpec`）；「用户不写 handler 函数体」正是要消除的负担。

## F8. 带参数中间件的零仪式捕获（scope-borrowed server）

**状态**：已实现（ADR 0020）。

**场景**：中间件在构造期捕获参数（日志读取器、图形状、配置、连接句柄），
在每次 drive 的 server 调用里使用。参数化中间件是 tower/axum 生态的常态形态
（`TimeoutLayer::new(dur)`、`from_fn_with_state(state, f)`）。

**为什么现有手段不够**：server 合同是 `Fn(Args) -> Fut + 'static`，其中
`Fut: 'static` 让返回的 future **不能借闭包状态**——非 Copy 捕获（`Arc<dyn Fn>`
这类句柄）必须在 `async move` 前手动 clone 一份（`let get_events = get_events.clone();`）。
这是三条件联立的必然（`Fn` 可重入 × future 自持 × 捕获非 Copy），tower 生态
的作者写的是同一行；每带一个参数就多一行仪式，且容易被误写成在 async 块内
clone（借不到，编不过）。

**为什么不是 async 闭包能解的**：`AsyncFn*` 的语义（future 借 `&mut`/`&` 闭包
状态）恰好是解药，但擦除位用不上——`AsyncFnMut`/`AsyncFn` 非 dyn compatible、
`dyn AsyncFnOnce` 被 `CallOnceFuture` GAT 卡死（E0191）、GAT 投影写 Send bound
撞 E0658（`async_fn_traits` 未稳定）。编译器实测，见 ADR 0020。

**自然形态**（两层）：

- **擦除 trait + lifetime-tied 返回**（tower `Service` 同款）：`MiddlewareServer`
  trait 的 `fn server<'a>(&'a self, args) -> BoxFuture<'a, Result<Value>>`——
  返回 future 借 `&self`，struct 形态的中间件状态是**字段**（tower 首选形态），
  按引用使用、零克隆零仪式；
- **state 适配器**（axum `from_fn_with_state` 同款）：`server_state(state, f)`——
  框架每次调用 clone 一份 state（Arc 场景=引用计数）传作闭包首参，轻量闭包
  形态的仪式收进框架。

旧闭包形态（`Fn(Args) -> 'static future`）经 blanket impl 继续可用，现有
调用点零改动；future 需要逃逸作用域（spawn）时 clone 回到语义上诚实的位置。

**两侧状态**：Rust 已实现（`create_middleware.rs`）；上游 TS 无对应物
（TS 闭包原生捕获、无此问题）。

## F9. 中间件注册 typestate 化（静态栈零箱、擦除逃生门）

**状态**：已实现（ADR 0021）。

**场景**：中间件注册的常态是**逐个**（`.middleware(a).middleware(b)`）；
传列表（`.middleware([..])`）是少数需求——低代码/运行时动态决定集合。
静态注册的洋葱应该在**类型层**组合（tower `ServiceBuilder` 同构），
擦除只留给动态。

**为什么现有手段不够**：列表形态把洋葱压成定义期逐层擦除（N+1 次箱）+
drive 期逐层箱（2N 次）——静态注册本可以零箱到终点擦一次（tower/axum
同款）。`next(ext)` 的 CPS 续延（每层每 drive 一次 pipeline 擦除）是协议
结构性成本，不在消除范围。

**自然形态**：`MiddlewareServer`（RPITIT，静态门，`Clone` 纪律）+
`MiddlewareStack`（元组递归栈，`Append` 类型级追加）+ builder 第 5 类型
参数 `TMw`；`.middleware(m)` 单件追加、`.middleware((a, b, c))` 静态列表、
`.middleware_any(iter)` 擦除逃生门。`DynMiddlewareServer`（dyn）承接动态
元素，任何静态中间件可按需擦除（blanket 互转）。

**两侧状态**：Rust 已实现；上游 TS 无此分层（JS 闭包无此问题）。

## F10. 中间件直读事件日志（`MiddlewareServerFnArgs::get_events`）

**状态**：已实现。

**场景**：中间件需要「本 run 已落盘的完整事件日志」——判断开局目标是否达成
（`interim_goals`）、策略层按历史 step 决策、把日志尾部信息写进 ctx 扩展。

**为什么现有手段不够**：`WorkflowCtx` 上没有 store（`engine` 是 `pub(crate)`，
对齐上游「engine 藏在闭包」）。中间件原先只能在构造期注入 `Arc<GetEvents>`
（F8 的形态），多一处「必须传同一个 store 实例」的仪式，且注入的句柄与引擎
实际使用的 store 可能在装配时错配。因此内置的 `interim_goals` / `reseed` /
`dag_policies` 已全部改为直接走该方法（前两者去参，后者只留 `shape`）；
`reseed` 与 `dag_policies` 共用 `step_results_from_events` 解析源日志，读源失败
统一传播错误（不再一处静默降级、一处 `Errored`）。

**自然形态**：`MiddlewareServerFnArgs::get_events(&self, run_id)`——引擎自己持有的
store，中间件直接 await；本 run 传 `ctx.run_id`，reseed 类策略传源 run 的 id。
作用域刻意只在中间件：handler 侧仍无 store（读日志会引入隐藏的 replay 依赖，
破坏确定性契约）。

**两侧状态**：Rust 已实现；上游 TS 无对应方法（中间件从闭包拿 store，
不经过 ctx）。

## 通用约束（本轮明确过的，新增功能都要过）

1. **隐式新开 run 默认禁止**。引擎在 resume 时内部创建新 run 而调用方不直接
   指定，会让 attach / sweep / lease 全部无法回答「操作的是哪个 run」。只有当
   全部满足才可考虑：(a) 调用方可见 id 不变或映射落 store；(b) 新旧日志可拼接；
   (c) attach 旧 run 与驱动新 run 事实一致；(d) 穷尽同 run 手段后仍无法表达。
2. **语义对齐优先于代码移植**（本仓既有原则）：能用自己的机制达成上游可观察
   行为时，不照搬机制；但**结构差异要写进文档**，不留在暗处。
3. **命名 / 行为对齐上游**是为降低长期维护成本（用户 2026-10 明确）：每次上游
   改动都要重新翻译、每个新人要先学分歧，是会复利的成本。与上游不同的地方，
   在 `docs/tanstack-alignment.md` 记档。
4. **功能以自然形态落地**：如果实现需要调用方做「本来不该由它做」的事
   （如手工拼新 run 续跑），先回来改引擎，不要在上层堆补丁。

---

## 设计取舍记录 → 已迁至 `docs/decisions/`

关于「临时目标与 redo 放哪一层」「TS/Rust 中间件能力差异」「被否决的选项」等
结论性记录，见：

- [`docs/decisions/0001`](decisions/0001-strategy-vs-mechanism.md) 策略与机制分层
- [`docs/decisions/0002`](decisions/0002-rust-step-hooks.md) Rust 中间件能力钩子
- [`docs/decisions/README.md`](decisions/README.md) 全部 ADR 索引

详细的论证过程与实测数据仍保留在：
`feature/_02`（TS 实现）、`feature/_03`（redo）、
`feature/_04`（不进引擎的替代实现）、`feature/_09`（插件预测）。

---

## 中间件相关决策 → 已迁至 `docs/decisions/`

- [0010](decisions/0010-ts-middleware-capabilities.md) TS 中间件的能力事实
  （inputSchema 不可改 / input 不可变 / `input.__interimGoals` 载体与四条约定）
- [0002](decisions/0002-rust-step-hooks.md) Rust 中间件能力钩子
- [0001](decisions/0001-strategy-vs-mechanism.md) 策略与机制分层


