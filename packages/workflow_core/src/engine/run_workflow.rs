//! 单次 invocation 的驱动入口：把 workflow **跑起来 / 续跑** 的顶层编排。
//!
//! 引擎本体（`EngineRuntime` 驱动态、`exec_step` / `exec_pause` / `deliver_signal`）
//! 在 [`super`](crate::engine) 中；这里只负责一次 `run_workflow` 调用内部：
//! 从 store 构造 run state、算 per-invocation state、造 `EngineRuntime`、跑 handler、
//! 收尾写终态事件与 RunState 信封。每次调用（start / resume）都独立走完整条路径。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use crate::define::{AnyWorkflowDefinition, WorkflowDefinition};
use crate::engine::{DEFAULT_MIN_YIELD_REMAINING_MS, EngineRuntime, WorkflowCancelled, WorkflowParked, now_ms};
use crate::error::{RunError, RunErrorCode, WorkflowError};
use crate::event::{RunStatus, WorkflowEvent};
use crate::resource::Gate;
use crate::run_store::{
    ApprovalDecision, DeliveryOutcome, PendingApproval, RunState, RunStore, SignalDelivery,
    WaitForState,
};

/// 事件回调的**已擦除**类型：返回 future 而非直接调用，所以宿主可以给异步实现。
/// 参数与上游 `publish?: (runId, event) => void | Promise<void>`
/// （`run-workflow.ts:41`）**逐个对应**，顺序也一致。
///
/// **`run_id` 是补上的那个参数。** 之前回调只收事件，宿主想按 run 归集就得从
/// `event.run_id()` 里反解——而 `WorkflowEvent::Custom` 不带 run_id（自定义事件
/// 是宿主的，引擎无从填），那条路径上宿主根本拿不到 run 身份。上游把 runId 当
/// 独立参数传就是这个原因。
///
/// **事件按值传 `WorkflowEvent`**，不是 `&WorkflowEvent`。这不是口味问题：
/// `publish()` 走队列时本来就要克隆一份（调用点只有 `&self`），drain task 已经
/// 持有一份完整副本。传引用就得让返回的 future 借用它，于是被迫用
/// `BoxFuture<'static, _>` + HRTB 兜（`for<'a> Fn(&'a E) -> BoxFuture<'a, ()>`
/// 在 `dyn` 上是噩梦），代价是 async block **不能借用事件**。
/// 按值传则一次克隆都不浪费、生命周期问题直接消失。
/// `run_id` 相反**按引用**：它 `&'static`（spawn 时克隆的那份），且真要在 async
/// block 里长期持有，按值只会多一次 clone。
///
/// ```ignore
/// .async_publish(|run_id, ev| async move { sink.send(run_id, ev).await })
/// ```
/// run 标识（上游 core 写 `string`，这里给个具名，和 runtime 的 `RunId` 同义）。
///
/// 只用在**公开入口**上——`PublisherFn` 的回调契约、`RunWorkflowOptions.run_id`。
/// 事件与 store 结构体里那几百处仍写 `String`：它们是 serde 面对的形状，
/// 上游也一律 `string`，改名的文档收益抵不上 290 处 churn。
pub type RunId = String;

/// 事件 fan-out 回调。**按引用**收 run id（上游按值，但 TS 传引用不拷贝，
/// Rust 按值就得多 clone 一次）；事件按值收，因为 core 每次都要 clone 出去
/// （`RunEventStream` 要在 poll 里重放同一份）。
pub type PublisherFn =
    Arc<dyn Fn(&RunId, WorkflowEvent) -> crate::define::BoxFuture<'static, ()> + Send + Sync>;

/// `run_workflow` / `run_workflow_sync` 的全部入参（对齐上游
/// `runWorkflow(options)` 的**按值**收法，见 `engine/run-workflow.ts:34-72`）。
///
/// # 为什么把 `workflow` / `run_store` 也收进来
///
/// 之前是 4 个位置参数 `run_workflow(&wf, store, &opts, publisher)`——容易传错位。
/// 上游把**全部**入参放在一个结构体里，`workflow` / `runStore` 是**必填字段**。
/// 这里照做：必填项由 [`RunWorkflowOptions::new`] 强制（构造完就一定齐了），
/// 可选项走 builder 链。
///
/// # 与上游的字段差异
///
/// | 上游 `RunWorkflowOptions` | 本结构 | 说明 |
/// | --- | --- | --- |
/// | `workflow` / `runStore` | ✅ `workflow` / `run_store` | 必填 |
/// | `input` / `runId` / `deadline` / `minYieldRemainingMs` / `yieldResumeAt` | ✅ | 同名同义 |
/// | `publish` | ✅ `publish` | 位置从参数移进结构体 |
/// | `signalDelivery` / `approval` | — | 我们走 `deliver_signal` / `deliver_approval` 先落盘再 drive（D3 的形态差异） |
/// | `attach` | ✅ `attach` | 只读回放，见 [`RunWorkflowOptions::attach`] |
/// | `recover` / `signal` / `threadId` / `outputSink` / `telemetry` | — | **暂无**；未做，不是不做 |
/// | — | ➕ `interimGoals` | **本地扩展**（对齐 TS f2-lite 泊车语义） |
///
/// # 为什么字段类型是 `AnyWorkflowDefinition`（newtype）而不是裸 `WorkflowDefinition`
///
/// 上游这个字段是 `AnyWorkflowDefinition`，而它**本身就是别名**
/// `WorkflowDefinition<any, any, any>`（`types.ts:466`）——TS 的 `any` 双向可赋值，
/// 一个类型同时充当「带类型的声明」和「擦除的运行站点」。Rust 做不到：
/// `WorkflowDefinition<ChargeInput, Draft>` 与 `WorkflowDefinition<Value, Value>`
/// 是两个互不 coercion 的类型，所以擦除态必须是**独立的类型**，也就是这个 newtype。
/// 它的名字沿用上游，于是本字段与上游一字不差。
///
/// newtype 内部持 `Arc<WorkflowDefinition>`：`previous_versions` 是 `Vec`、另有
/// 若干 `Arc`，按值持有会让每次 run 都 deep-copy 一遍；`Arc` 让 clone 变引用计数自增，
/// 也避免给结构体引入生命周期参数（那会让 builder 链很难写）。

pub struct RunWorkflowOptions<W = AnyWorkflowDefinition>
where
    W: crate::define::WorkflowMetaSpec + Into<AnyWorkflowDefinition> + Clone,
{
    /// 要驱动的 workflow。**必填**（由 [`Self::new`] 保证）。
    ///
    /// 用上游的名字，让本字段与 TanStack 的 `RunWorkflowOptions.workflow` 一字不差。
    /// `AnyWorkflowDefinition` = 擦除态的 [`WorkflowDefinition`]（上游是
    /// `WorkflowDefinition<any,any,any>` 的别名；Rust 没有 `any` 的双向可赋值，
    /// 所以擦除态是独立类型）。内部持 `Arc`，clone 只加引用计数。
    ///
    /// **`W` 决定 [`Self::meta`] 的类型**：传具体的 `WorkflowDefinition<.., M>`
    /// 时，`meta` 必须是 `MetaBag<M>`（缺字段编译报错）；传擦除态
    /// `AnyWorkflowDefinition` 时不约束（声明已在擦除时丢失）。
    pub workflow: W,
    /// 事件日志 / run 元数据的落盘位置。**必填**（由 [`Self::new`] 保证）。
    pub run_store: Arc<dyn RunStore>,

    /// 复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。
    pub run_id: Option<RunId>,
    /// run 输入。默认 `Value::Null`。
    pub input: serde_json::Value,
    /// **Run 级参数**（本仓有意扩展；上游无此字段）。
    ///
    /// 与 `input` 的分工：`input` 是**业务数据**（handler 消费、persist 到
    /// `RunState`、resume 不可变）；`meta` 是**策略/扩展参数**——中间件在
    /// `wrap` 前置与 step 钩子里读取（见 [`BaseCtx::meta`] 与
    /// [`StepFinishedEvent::run_meta`]），不参与 handler、不落 `RunState`。
    ///
    /// 存在的理由：Rust 的中间件无法覆盖 `ctx.step`（方法是 `&self` 上的），
    /// 钩子也拿不到 `ctx`——策略参数需要一个钩子能读到的通道，见
    /// `docs/decisions/0011-run-meta-and-plugins.md`。
    ///
    /// 构造：`MetaBag::new().set::<DagPlanKey>(plan)`——按 key 类型化写入，
    /// 与 `ctx.meta.dag_plan()` 的读取对称（见 `docs/decisions/0017`）。
    ///
    /// 类型由 [`Self::workflow`] 决定：声明了 meta 需求的 workflow 要求这里
    /// 提供对应字段，**缺了在构造处编译报错**。
    pub meta: <W as crate::define::WorkflowMetaSpec>::Meta,
    /// 只读回放一个已存在的 run（上游 `attach`，`run-workflow.ts:48` / `:371`）。
    ///
    /// 不碰 handler、不写日志、不改 `RunState`——把日志里已有的事件原样重放
    /// 一遍，让调用方拿到和"从头 drive 一次"同样形状的事件包。run 不存在时报
    /// `run_lost`（上游 `attachRun` 的第一条分支）。
    ///
    /// **只认显式**：不照抄上游 `startRun` 的隐式幂等 redirect
    /// （`run-workflow.ts:194-200`），因为我们没有 `signalDelivery` 标志位去区分
    /// start 和 resume——照抄会把每次 resume 都变成 attach；另外重驱动已完成的 run
    /// 在本仓是**故意的**（`replay_shape_mismatch_fails_instead_of_yielding_null`
    /// 靠它做日志形状漂移自检）。理由详见 [`attach_decision`] 的文档。
    pub attach: bool,
    /// 本次 drive 的绝对 UTC ms 预算（上游 `deadline`）。设了之后
    /// `time_remaining()` / `should_yield()` / `ctx.yield_()` 才生效；
    /// 每次 resume 都可以给一个新的。
    pub deadline: Option<u64>,
    /// 剩余预算低于此值时 `should_yield()` 翻真（上游 `minYieldRemainingMs`，默认 1000）。
    pub min_yield_remaining_ms: Option<u64>,
    /// `ctx.yield_()` 的重新唤醒时刻（上游 `yieldResumeAt`；默认每次调用「now+1ms」）。
    pub yield_resume_at: Option<u64>,
    /// 客户端关联用的线程标识（上游 `threadId`，`run-workflow.ts:52`）。
    ///
    /// 只写进 `RUN_STARTED` 事件，**不落 store**、不参与 resume 判定。上游
    /// 把它当纯客户端关联（"Thread ID for client-side correlation"）。
    pub thread_id: Option<String>,

    /// **随本次 drive 一起投递的信号**（上游 `RunWorkflowOptions.signalDelivery`，
    /// `run-workflow.ts:50`）。
    ///
    /// 为什么要放进 options 而不是单独写日志：`SIGNAL_RESOLVED` 该落在哪个
    /// `SIGNAL_AWAITED` 后面、要不要复用已有的一条、`signal_id` 是否重复——
    /// 这些判定属于引擎（只有它知道日志里等待点的配对规则）。暴露成独立函数
    /// 就是把内部逻辑泄露给调用方，还得让调用方自己保证「先写日志再 drive」
    /// 的顺序。
    ///
    /// 设置后 `drive` 的开头会先跑 `append_seed`：定位等待点、去重、追加
    /// `SignalResolved`，然后照常重放 handler。
    pub signal_delivery: Option<crate::run_store::SignalDelivery>,
    /// **随本次 drive 一起投递的审批决定**（上游 `approval`，
    /// `run-workflow.ts:51`）。语义同 [`Self::signal_delivery`]。
    pub approval: Option<crate::run_store::ApprovalDecision>,
    /// 每个事件都会回调（上游 `publish`）——host 可以接到 Redis / Durable Streams
    /// 之类的扇出通道，让别的节点能 tail 这个 run。
    ///
    /// # 这是「可观测」与「耐久」的分界
    ///
    /// 引擎只把**事实**落盘（`append`）：`STEP_FINISHED` / `STEP_FAILED` /
    /// `STEP_PAUSED` / `NOW_RECORDED` / `UUID_RECORDED`。其余事件——`STEP_PROGRESS`、
    /// `STEP_STARTED`、`CUSTOM`、`STATE_DELTA`——**只**走到这里，**从不**写盘
    /// （`EngineRuntime::publish` 没有任何落盘路径）。
    ///
    /// 「要不要把 progress 落盘」是**宿主的策略**，引擎不替所有人决定：devtools
    /// 不需要，产品 UI 可能需要。所以要留就自己在这里写：
    ///
    /// ```ignore
    /// .publish(Some(Arc::new(move |run_id: &str, e: WorkflowEvent| {
    ///     if let WorkflowEvent::Custom { name, value, .. } = e {
    ///         if name == "progress" {
    ///             my_db.insert_progress(run_id, value.clone());
    ///         }
    ///     }
    /// })))
    /// ```
    ///
    /// ## publish 不拖慢引擎（对齐上游的 async generator）
    ///
    /// 投递是「同步调用点 + 独立 drain task」两段：调用点只做一次
    /// `UnboundedSender::send`（非阻塞），真正 await publish 的是 drain task。
    ///
    /// 上游是同一个形状：`runWorkflow` 是个 `async function*`，它在
    /// `queue.shift()` 之后 `await publish` 再 `yield`，而**执行在另一个 task**
    /// 里继续往那个 queue 推（`run-workflow.ts:85-134`）。所以「publish 慢」
    /// 在两边都只拖慢**消费端吞吐**，不拖慢**引擎进度**。
    ///
    /// 队列是**无界**的（上游就是个裸数组），所以慢 publish 涨内存而不是卡住
    /// run；要背压就在宿主自己的 publish 里做。
    ///
    /// **「返回前终态已投递」是守住的旧语义**：收尾发 shutdown 并 join drain，
    /// 所以 `RUN_FINISHED` / `RUN_ERRORED` / `STEP_PAUSED` 一定在
    /// `run_workflow` 返回前送达（Paused 早退路径也排空）。代价是慢 publish 会
    /// 延迟**返回**，但执行早已结束。
    ///
    /// 死锁风险与改造前相同：publish 若 `await` 依赖本次 run 完成的东西，仍会
    /// 挂——旧语义下内联调用时也会挂。不是回归。
    ///
    /// ## 坑 1：进程内回调 = 有丢失窗口
    ///
    /// 崩溃时最后一批事件就没了。所以「自己落盘」得到的是**被观测到的那部分**
    /// 耐久，不是「全部」耐久。拿它当审计日志会得到一份有洞的审计日志——
    /// 审计要耐久就别走这里，该让引擎 append。
    ///
    /// ## 坑 2：publish panic 会被吞掉（与上游一致，刻意如此）
    ///
    /// 宿主 publish 里的 panic **不会**掀掉你的 run —— `publish()` 用
    /// `catch_unwind` 兜住。上游同形（*"A misbehaving publish must not break
    /// the run — swallow and continue."*，`run-workflow.ts:128-134`）：宿主代码
    /// 不该有能力损毁已经 append 了 checkpoint 的耐久状态。
    ///
    /// 代价是**静默** —— 本 crate 没有日志依赖，所以拿不到「publish 炸了」这
    /// 条信息。要诊断就在**你自己的 publish 内部** catch + 记日志，日志策略和
    /// 依赖都留在宿主那侧。
    /// 可选的事件回调（上游 `publish`）。多数宿主用同步的 [`Self::publish`]；
    /// 需要 `await` 落盘/发网络的用 [`Self::async_publish`]。
    pub publish: Option<PublisherFn>,
}

// 手写而不是 `derive`：`meta` 的类型是关联类型 `<W as WorkflowMetaSpec>::Meta`，
// `derive` 不会给它加 `Clone` 约束。
impl<W> Clone for RunWorkflowOptions<W>
where
    W: crate::define::WorkflowMetaSpec + Into<AnyWorkflowDefinition> + Clone,
    <W as crate::define::WorkflowMetaSpec>::Meta: Clone,
{
    fn clone(&self) -> Self {
        Self {
            workflow: self.workflow.clone(),
            run_store: Arc::clone(&self.run_store),
            run_id: self.run_id.clone(),
            input: self.input.clone(),
            meta: self.meta.clone(),
            attach: self.attach,
            deadline: self.deadline,
            min_yield_remaining_ms: self.min_yield_remaining_ms,
            yield_resume_at: self.yield_resume_at,
            thread_id: self.thread_id.clone(),
            signal_delivery: self.signal_delivery.clone(),
            approval: self.approval.clone(),
            publish: self.publish.clone(),
        }
    }
}

impl<W> RunWorkflowOptions<W>
where
    W: crate::define::WorkflowMetaSpec + Into<AnyWorkflowDefinition> + Clone,
    <W as crate::define::WorkflowMetaSpec>::Meta: crate::define::EmptyMeta,
{
    /// **必填项在这里**：`workflow` + `run_store`。构造完这两个就一定齐了。
    ///
    /// `workflow` 的类型 `W` 会**保留**（不擦除），因为 `meta` 的类型由它决定：
    /// 声明了 meta 需求的 workflow 必须配对应的 `MetaBag`（见 [`Self::meta`]）。
    ///
    /// 擦除态的 `AnyWorkflowDefinition` 也能传——那时 `meta` 不受约束。
    pub fn new(workflow: W, run_store: Arc<dyn RunStore>) -> Self {
        Self {
            workflow,
            run_store,
            run_id: None,
            input: serde_json::Value::Null,
            meta: <<W as crate::define::WorkflowMetaSpec>::Meta as crate::define::EmptyMeta>::empty_meta(),
            attach: false,
            deadline: None,
            min_yield_remaining_ms: None,
            yield_resume_at: None,
            thread_id: None,
            signal_delivery: None,
            approval: None,
            publish: None,
        }
    }
}

impl<W> RunWorkflowOptions<W>
where
    W: crate::define::WorkflowMetaSpec + Into<AnyWorkflowDefinition> + Clone,
    <W as crate::define::WorkflowMetaSpec>::Meta: crate::define::EmptyMeta,
{
    /// **Run 级参数**（本仓有意扩展）——中间件的 wrap 前置与策略钩子读取。
    /// 与 `input` 的分工见 [`RunWorkflowOptions::meta`] 字段文档。
    ///
    /// 类型化写入用 [`MetaBag::set`]：
    /// `opts.meta(MetaBag::new().set::<DagPlanKey>(plan))`。
    pub fn meta(mut self, v: <W as crate::define::WorkflowMetaSpec>::Meta) -> Self {
        self.meta = v;
        self
    }

    /// 客户端关联标识（上游 `threadId`），只写进 `RUN_STARTED`。
    pub fn thread_id(mut self, v: Option<String>) -> Self {
        self.thread_id = v;
        self
    }

    /// run 输入。
    pub fn input(mut self, v: serde_json::Value) -> Self {
        self.input = v;
        self
    }

    /// 复用该 run_id ⇒ resume。
    pub fn run_id(mut self, v: impl Into<String>) -> Self {
        self.run_id = Some(v.into());
        self
    }

    /// 随本次 drive 投递一个信号（上游 `signalDelivery`）。
    ///
    /// `deliver_signal` 是「先写日志、之后某次 drive 才看到」的两步写法；
    /// 这里是**一步**：drive 开头定位等待点、去重、追加 `SignalResolved`，
    /// 然后照常重放。上游只有这一种写法（`appendSeed` 在 `runWorkflow` 内部）。
    pub fn signal_delivery(mut self, v: crate::run_store::SignalDelivery) -> Self {
        self.signal_delivery = Some(v);
        self
    }

    /// 随本次 drive 投递一个审批决定（上游 `approval`）。
    pub fn approval(mut self, v: crate::run_store::ApprovalDecision) -> Self {
        self.approval = Some(v);
        self
    }


    /// 只读回放已存在的 run（上游 `attach`）。见 [`RunWorkflowOptions::attach`]。
    pub fn attach(mut self) -> Self {
        self.attach = true;
        self
    }

    /// 设置本次 drive 的绝对 UTC ms 预算。
    pub fn deadline(mut self, v: u64) -> Self {
        self.deadline = Some(v);
        self
    }

    /// 剩余预算低于此值时允许让出（上游 `minYieldRemainingMs`）。
    pub fn min_yield_remaining(mut self, v: u64) -> Self {
        self.min_yield_remaining_ms = Some(v);
        self
    }

    /// `ctx.yield_()` 的重新唤醒时刻。
    pub fn yield_resume_at(mut self, v: u64) -> Self {
        self.yield_resume_at = Some(v);
        self
    }

    /// 事件回调（上游 `publish`）。收 `Option`，便于直接对接旧的四参数签名。
    ///
    /// 同步版：内部包装成一个**立即完成**的 future，实际投递发生在 drain task
    /// 上（见字段文档「publish 不拖慢引擎」）。要真正 `await` 请用
    /// [`Self::async_publish`]。
    ///
    /// 事件**按值**传入，与 `async_publish` 同一套所有权语义（只有投递时机
    /// 不同），这样两条路径的心智模型是一致的：`ev` 归你，随便 move 进
    /// `async move`、随便丢给线程、随便 `join()`。
    pub fn publish(mut self, v: Option<Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync>>) -> Self {
        self.publish = v.map(|f| {
            Arc::new(move |run_id: &RunId, ev: WorkflowEvent| -> crate::define::BoxFuture<'static, ()> {
                let out = f(run_id, ev);
                Box::pin(async move { out })
            }) as PublisherFn
        });
        self
    }

    /// 异步事件回调（上游 `publish` 的 `Promise<void>` 那一支）。
    ///
    /// 与 [`Self::publish`] 的差别只是**允许 `await`**：投递在 drain task 上
    /// 串行进行，但引擎执行不受它阻塞。要落盘/发网络而不想卡住引擎，就用这个。
    ///
    /// 事件**按值**传入，所以 async block 可以直接 `async move` 整个事件 ——
    /// 不需要「先取值再进 async」那套：
    ///
    /// ```ignore
    /// .async_publish(|run_id, ev| async move { sink.send(run_id, ev).await })
    /// ```
    ///
    /// 想留一份自己用就克隆（事件不大，且 `publish()` 已经为了入队克隆过一次，
    /// 这里不会再多一次引擎侧克隆）。
    pub fn async_publish<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&RunId, WorkflowEvent) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.publish = Some(Arc::new(move |run_id: &RunId, ev: WorkflowEvent| {
            Box::pin(f(run_id, ev)) as crate::define::BoxFuture<'static, ()>
        }));
        self
    }

    /// 清掉事件回调。
    pub fn no_publish(mut self) -> Self {
        self.publish = None;
        self
    }
}

#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub output: Option<serde_json::Value>,
    /// 与 [`RunState::error`](crate::run_store::RunState) 是**同一个类型**——
    /// 一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。
    ///
    /// `RunError` 实现了 [`Display`](std::fmt::Display)（转发 `message`），
    /// 所以 `warn!("{e}")` / `e.to_string()` 照常可用；想要错误名用 `.name`。
    pub error: Option<RunError>,
}

impl RunOutcome {
    /// 从 **store 里的 [`RunState`]** 读出 run 结果，而不是从事件流末事件推。
    ///
    /// # 为什么不能看事件
    ///
    /// 上游把 iterable 定义成「本次 drive **append 到日志**的事件」，不是状态
    /// 通道（`engine/run-workflow.ts:76-84`：*"an `AsyncIterable` of every event
    /// the engine appends to the run's log […] the log IS the transport"*）。
    /// runtime 因此把两者分开：`collectWorkflowEvents` 允许 `includeEvents:
    /// false`（`events: []`）和 `maxEvents` 截断，而 `classifyRun` **只看
    /// store**——`events: []` 时 `kind` 依然正确，所以状态不可能由事件推出
    /// （`runtime-driver.ts:968-977`、`:1179-1214`、`types.ts:449-457`）。
    /// 我们的 [`RunEventStream::outcome`] 就是这套顺序的直译：drain 完再读
    /// `RunState`。
    ///
    /// # 幂等重放为什么让「看末事件」必然出错
    ///
    /// 重 drive 一个已挂起的 run 时，primitive 短路、**一条新日志都不写**
    /// （上游：*"primitives short-circuit via `findCheckpoint` lookup in
    /// history"*，commit `4f64b9c`；本仓对应 `exec_pause_with` 的
    /// `already_paused` 门），流里只剩 `RUN_STARTED`——凭末事件分不出「挂起」
    /// 和「结束」。`Paused` 只能从 `RunState.status` 拿。
    ///
    /// 完整决策记录见 `docs/tanstack-alignment.md` 的「决策（2026-09-30）」。
    pub fn from_run_state(state: RunState) -> Self {
        Self {
            run_id: state.run_id,
            status: state.status,
            output: state.output,
            error: state.error,
        }
    }

    /// [`Self::from_run_state`] 的 `Result` 版——`RunState` 读不出来才算错。
    pub fn try_from_run_state(state: Option<RunState>) -> Result<Self, WorkflowError> {
        state.map(Self::from_run_state).ok_or_else(|| {
            WorkflowError::Internal("run state not found after drive".into())
        })
    }
}

/// Resolves which workflow definition drives a run.
///
/// 真正的实现在 [`crate::registry::select_workflow_version`]（对齐上游
/// `registry/select-version.ts` 的文件组织）；此处仅再导出。
pub use crate::registry::select_workflow_version;

/// Runs (or resumes) a workflow by driving its async handler.
///
/// 策略类参数（泊车目标 `interim_goals`、redo 导入 `reseed_run`）**不在引擎上**：
/// 它们是 step 层（`wrap_step`）的能力，见 `docs/decisions/0001`。
///
/// On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
/// `run` not re-executed) and rethrows failed ones. Multiplex step results
/// however you like — the log is the only source of truth.
pub fn run_workflow<W>(mut opts: RunWorkflowOptions<W>) -> RunEventStream
where
    W: crate::define::WorkflowMetaSpec + Into<AnyWorkflowDefinition> + Clone,
{
    // run_id 必须**只在这里定一次**并写回 `opts`：stream 自己要按它回 store 读
    // 结果（[`Self::outcome`]），drive 也要按它写日志。两处各生成一次的话，
    // `opts.run_id` 为 `None` 时跨过一个 ms 边界就会拿到两个不同的 id。
    // 定下来之后 `publish` 也能直接拿到它——上游得在循环里等 `RUN_STARTED`
    // 出现才敢用（`runIdForPublish`，`run-workflow.ts:116-119`）。
    let run_id = opts
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run_{}", 0));
    opts.run_id = Some(run_id.clone());
    let publish = opts.publish.clone();
    // `outcome()` 要在 drain 之后回 store 读状态，所以这条 clone 得活过引擎。
    let store = opts.run_store.clone();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WorkflowEvent>();

    // **擦除**：`W` 只服务于编译期的 meta 校验，运行期一律看擦除态。
    // meta 的值原样搬过去（类型层丢了，值还在）。
    let mut opts = RunWorkflowOptions {
        workflow: opts.workflow.clone().into(),
        run_store: opts.run_store.clone(),
        run_id: opts.run_id.clone(),
        input: opts.input.clone(),
        meta: crate::define::MetaBag::<crate::define::Nil>::from_value(
            crate::define::MetaValue::erased(&opts.meta).clone(),
        ),
        attach: opts.attach,
        deadline: opts.deadline,
        min_yield_remaining_ms: opts.min_yield_remaining_ms,
        yield_resume_at: opts.yield_resume_at,
        thread_id: opts.thread_id.clone(),
        signal_delivery: opts.signal_delivery.clone(),
        approval: opts.approval.clone(),
        publish: opts.publish.clone(),
    };
    let _ = &mut opts;

    // 入口分派（上游 `run-workflow.ts:165-168` 的 attach 分支）。attach 是
    // **只读**的：不 drive、不写日志、不改 RunState，直接把日志重放进流。
    // 事件仍会过 `poll_next` 里的 publish —— 与上游一致（上游 publish 挂在生成器
    // 的 yielding loop 上，attachRun emit 的事件也走同一个 loop）。
    //
    // 无论 attach 还是正常 drive，引擎逻辑都在后台 async task 里跑（store 操作
    // 现在是 async 的）。`run_workflow` 本身保持同步、只返回流——对齐上游
    // `runWorkflow(): AsyncIterable<WorkflowEvent>`（真正的 async 在迭代时发生）。
    let task_run_id = run_id.clone();
    let task_store = store.clone();
    tokio::spawn(async move {
        if let Some(ev) = attach_decision(&opts, &task_store, &task_run_id).await {
            for e in ev {
                let _ = tx.send(e);
            }
            // tx 随本 task 结束而 drop → poll_recv 排空后自然收尾。
            return;
        }
        if let Err(ev) = drive(opts, tx.clone()).await {
            // **错误也是事件**（上游 `drive().catch()` 吞掉异常、所有失败都
            // `emit(RUN_ERRORED)`，`run-workflow.ts:96-99`）。宿主只面对一种
            // 失败通道。
            let _ = tx.send(ev);
        }
    });

    RunEventStream {
        rx,
        publish,
        run_id,
        store,
        inflight: None,
        pending: None,
        finished: false,
    }
}

/// [`run_workflow`] 返回的事件流 —— 上游 `AsyncIterable<WorkflowEvent>` 的对应物
/// （`run-workflow.ts:74-83`）。
///
/// ```ignore
/// let mut stream = run_workflow(opts);
/// while let Some(event) = stream.next().await {
///     // event 是统一的 WorkflowEvent 联合类型 —— 耐久的 + 仅观测的
/// }
/// let outcome = run_workflow(opts).outcome().await?;    // drain 完回 store 读
/// ```
///
/// # 与上游同一套机制
///
/// 上游是个 `async function*`：`queue` 是裸数组，`emit` push 进去并唤醒一个
/// park 着的 promise，生成器循环 `shift()` → `await publish` → `yield`
/// （`run-workflow.ts:85-134`）。Rust 没有生成器语法，所以这里是同一套东西的
/// poll 机版本：
///
/// | 上游 | 这里 |
/// | --- | --- |
/// | `queue: Array<WorkflowEvent>` | `tokio::sync::mpsc::UnboundedReceiver` |
/// | `emit` = `queue.push` + 唤醒 | `EngineRuntime::publish` = `tx.send`（非阻塞）|
/// | `const exec = drive(...)` 后台跑 | [`run_workflow`] 里 `tokio::spawn(drive(...))` |
/// | `await new Promise(r => resolveWait = r)` | `poll_recv` 返回 `Pending`，由 waker 唤醒 |
/// | `if (executionDone) break` | `poll_recv` 返回 `None`（sender 全 drop 了）|
/// | `await options.publish(...)` 然后 `yield` | [`Self::inflight`] + [`Self::pending`] 两个槽 |
///
/// 顺序是刻意的：**先 await publish，再把事件交给消费者**，跟上游一致。所以慢
/// publisher 会拖慢消费端，但不会拖慢引擎——引擎只做一次非阻塞 `send`。
///
/// # 错误不在流里
///
/// 引擎的所有失败都变成 `RUN_ERRORED` 事件（上游 `drive().catch()` 同形），
/// 所以这个流**没有** `Err` 变体：宿主只面对一种失败通道。
///
/// # 成败不在流里
///
/// 流里**只有本次 drive 写进日志的事件**（上游：*"every event the engine
/// appends to the run's log"*）。run 现在什么状态要问 store——重放幂等，
/// 重 drive 一个已挂起的 run 一条新事件都不写，凭末事件分不出「挂起」和
/// 「结束」。[`Self::outcome`] 就是「drain 完 → `store.get_run_state`」，
/// 对齐上游 `collectWorkflowEvents` + `loadRun` + `classifyRun` 的顺序。
/// 详见 [`RunOutcome::from_run_state`]。
pub struct RunEventStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<WorkflowEvent>,
    /// 上游 `options.publish`：yield 之前 await 它。
    publish: Option<PublisherFn>,
    /// 正在 await 的 publish future。`PublisherFn` 返回 `BoxFuture<'static>`，
    /// 所以它不借用 `self`——手写 poll 机里这是唯一能存的中间态。
    inflight: Option<crate::define::BoxFuture<'static, ()>>,
    /// 「已取到、正在等 publish 落地」的那条事件。对应上游
    /// `await options.publish(ev)` 之后 `yield ev` 里那个还没交出去的 `ev`
    /// ——poll 机里必须显式留住它，否则 await 完就不知道该 yield 哪条了。
    pending: Option<WorkflowEvent>,
    /// 建流时就算出来了，喂给 publish（上游得等 `RUN_STARTED` 出现才敢用）。
    run_id: String,
    /// drain 完之后用来读 run 结果的 store。上游 runtime 是 `collectWorkflowEvents`
    /// 之后 `store.loadRun` 再 `classifyRun`（`runtime-driver.ts:783-795`），
    /// 顺序一模一样——流负责「本次写了哪些日志」，store 负责「run 现在什么状态」。
    store: Arc<dyn RunStore>,
    finished: bool,
}

impl RunEventStream {
    /// 排干事件流，然后从 store 读出 [`RunOutcome`]。
    ///
    /// 这是「只要结果、不留事件」那条路的落点，也是流化之后 `run_workflow` 的
    /// 等价替代（旧的 `run_workflow(opts).outcome().await -> Result<RunOutcome>`）。
    ///
    /// 两步的分工逐行对齐上游 runtime：先 `collectWorkflowEvents` 排干
    /// （`runtime-driver.ts:1179-1214`），再 `store.loadRun` 拿状态
    /// （`:783-795`）——**状态不从末事件推**。原因见
    /// [`RunOutcome::from_run_state`]：重放幂等，重 drive 一个已挂起的 run
    /// 一条新事件都不写，凭末事件分不出「挂起」和「结束」。
    pub async fn outcome(self) -> Result<RunOutcome, WorkflowError> {
        let store = self.store.clone();
        let run_id = self.run_id.clone();
        self.drain().await;
        RunOutcome::try_from_run_state(store.get_run_state(&run_id).await?)
    }

    /// 把流排干收成 `Vec`——`StreamExt::collect` 的零依赖版（`futures-core` 只有
    /// `Stream` 本体，`.collect()` 在 `futures-util` 里，而那一个包只为了这一个
    /// 方法不值）。
    ///
    /// 要逐条事件时用它；只要结果用 [`Self::outcome`]（它还会去 store 读状态）：
    ///
    /// ```ignore
    /// let events = run_workflow(opts).collect().await;
    /// let outcome = run_workflow(opts).outcome().await?;
    /// ```
    pub async fn collect(self) -> Vec<WorkflowEvent> {
        let mut this = self;
        let mut out = Vec::new();
        while let Some(ev) = this.drain_one().await {
            out.push(ev);
        }
        out
    }

    /// 排干但不留事件，回吐条数。对齐上游 `collectWorkflowEvents` 的
    /// `includeEvents: false` + `maxEvents: 0` 那一档——事件照样计数、照样走
    /// publish，只是数组是空的。
    pub async fn drain(self) -> usize {
        let mut this = self;
        let mut n = 0;
        while this.drain_one().await.is_some() {
            n += 1;
        }
        n
    }

    /// 取下一条事件，`None` 表示流结束。`poll_fn` 把 `Poll` 拆开，只把值交出来。
    async fn drain_one(&mut self) -> Option<WorkflowEvent> {
        std::future::poll_fn(|cx| {
            futures_core::Stream::poll_next(std::pin::Pin::new(&mut *self), cx)
        })
        .await
    }
}

impl std::fmt::Debug for RunEventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunEventStream")
            .field("run_id", &self.run_id)
            .field("has_publish", &self.publish.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}

impl futures_core::Stream for RunEventStream {
    type Item = WorkflowEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<WorkflowEvent>> {
        use std::task::Poll;
        let me = self.get_mut();
        if me.finished {
            return Poll::Ready(None);
        }
        loop {
            // ① 上一次留下的 publish 还没 await 完？先 await 它——
            //    对应上游 `await options.publish(...)` 在 `yield` 之前。
            //    `take` 出来再放回去，免得和下面的 `me.rx` 撞借用。
            if let Some(mut fut) = me.inflight.take() {
                if fut.as_mut().poll(cx).is_pending() {
                    me.inflight = Some(fut);
                    return Poll::Pending;
                }
                // await 完了 → 把留住的那条事件交出去（`yield ev`）。
                if let Some(ev) = me.pending.take() {
                    return Poll::Ready(Some(ev));
                }
            }
            // ② 取下一个事件。对应上游 `while (queue.length > 0) queue.shift()`。
            match me.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                // 所有 sender 都 drop 了 = 引擎结束 = 流结束。上游是
                // `executionDone` 标志，语义一样。
                Poll::Ready(None) => {
                    me.finished = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(ev)) => {
                    if let Some(publish) = me.publish.clone() {
                        // 逐个兜住：宿主 panic 不得掀掉 run（上游 `try/catch`，
                        // *"A misbehaving publisher must not break the run"*)。
                        // future 在**构造**时才 panic，所以只能连 future 一起 catch。
                        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            publish(&me.run_id, ev.clone())
                        }));
                        match caught {
                            Ok(fut) => {
                                me.pending = Some(ev);
                                me.inflight = Some(fut);
                                // 回到 ①：await 完 publish 才交出这条事件。
                                continue;
                            }
                            // publisher 构造就炸了：吞掉，照常 yield（上游同策略）。
                            Err(_) => return Poll::Ready(Some(ev)),
                        }
                    }
                    return Poll::Ready(Some(ev));
                }
            }
        }
    }
}

/// 把本次 drive 随行投递的信号 / 审批落成事件（上游 appendSeed，
/// run-workflow.ts:1755）。
///
/// 放在 drive 开头、**重放 handler 之前**：ctx.wait_for_event 的重放短路是
/// 「扫日志找 SignalResolved」，事件必须先落盘，重放才看得到。
///
/// `waiting` / `pending` 是**清除前**的 run 状态快照——判「是不是真的在等」
/// 只能看它；日志里出现过 SIGNAL_AWAITED 不代表当前还在等（可能已经投过）。
async fn append_seed(
    store: &dyn RunStore,
    run_id: &str,
    waiting: Option<&WaitForState>,
    pending: Option<&PendingApproval>,
    signal_delivery: Option<&SignalDelivery>,
    approval: Option<&ApprovalDecision>,
) -> Result<DeliveryOutcome, WorkflowError> {
    let events = store.get_events(run_id).await?;

    if let Some(d) = signal_delivery {
        // 给了 step_id 就按它认——那时 name 无关（`signal_run` 的调用方不知道
        // 信号名，由下面的日志扫描取）。没给才按 name 比。
        let waiting_ok = waiting.is_some_and(|w| match &d.step_id {
            Some(sid) => w.step_id.as_deref() == Some(sid.as_str()),
            None => w.signal_name == d.name,
        });
        if !waiting_ok {
            return Ok(DeliveryOutcome::NotWaiting);
        }
        // 幂等：同一 signal_id 已投递过就不重复写。
        if let Some(sid) = &d.signal_id
            && events.iter().any(|ev| {
                matches!(ev, WorkflowEvent::SignalResolved { signal_id: Some(x), .. } if x == sid)
            })
        {
            return Ok(DeliveryOutcome::Duplicate);
        }
        // 定位等待点：显式给了 step_id 就按它，否则按 name 找最近一条。
        let target = match &d.step_id {
            Some(step_id) => events.iter().rev().find_map(|ev| match ev {
                WorkflowEvent::SignalAwaited { step_id: id, name, .. } if id == step_id => {
                    Some((id.clone(), name.clone()))
                }
                _ => None,
            }),
            None => events.iter().rev().find_map(|ev| match ev {
                WorkflowEvent::SignalAwaited { step_id, name: n, .. } if n == &d.name => {
                    Some((step_id.clone(), n.clone()))
                }
                _ => None,
            }),
        };
        let Some((step_id, name)) = target else {
            return Ok(DeliveryOutcome::NotWaiting);
        };
        crate::engine::append_with_cas(
            store,
            run_id,
            WorkflowEvent::SignalResolved {
                ts: now_ms(),
                run_id: run_id.to_string(),
                step_id,
                name,
                signal_id: d.signal_id.clone(),
                payload: d.payload.clone(),
                meta: d.meta.clone(),
            },
        ).await?;
        return Ok(DeliveryOutcome::Delivered);
    }

    if let Some(a) = approval {
        let waiting_ok = pending.is_some_and(|p| p.approval_id == a.approval_id);
        if !waiting_ok {
            return Ok(DeliveryOutcome::NotWaiting);
        }
        crate::engine::append_with_cas(
            store,
            run_id,
            WorkflowEvent::ApprovalResolved {
                ts: now_ms(),
                run_id: run_id.to_string(),
                step_id: a.approval_id.clone(),
                approval_id: a.approval_id.clone(),
                approved: a.approved,
                feedback: a.feedback.clone(),
                meta: None,
            },
        ).await?;
        return Ok(DeliveryOutcome::Delivered);
    }

    Ok(DeliveryOutcome::NotWaiting)
}

/// run_workflow 的内部驱动：**只吐事件，不返回错误**。
///
/// 每条失败路径都 `Err(RunErrored{..})` 出去，由 [`run_workflow`] 转成事件投递。
/// store 写失败时那条终局事件进不了日志（append 本身失败了），但仍会出现在流里
/// ——和上游一样，流是内存里的队列，事件与日志不是同一件事。
async fn drive(
    opts: RunWorkflowOptions,
    publish_tx: tokio::sync::mpsc::UnboundedSender<WorkflowEvent>,
) -> Result<(), WorkflowEvent> {
    // `opts.workflow` 是构造时 `Into<AnyWorkflowDefinition>` 转好的擦除态；
    // 保留那份 `AnyWorkflowDefinition`（`Arc`），版本路由命中当前版本时
    // 直接复用，不用 deep-copy 整个定义。
    let workflow_any: &AnyWorkflowDefinition = &opts.workflow;
    let workflow: &WorkflowDefinition = workflow_any;
    let store = Arc::clone(&opts.run_store);

    let run_id = opts.run_id.clone().unwrap_or_else(|| format!("run_{}", 0));
    let ts = now_ms();

    // A run that was cancelled while parked must not be resurrected by a drive:
    // `cancel_run` flipped `status` to `Aborted` and nothing is watching a
    // parked run, so this is the first place it can be observed. Report it
    // instead of resetting the run to `Running`.
    //
    // (`Finished` / `Errored` are likewise terminal — re-driving those replays
    // from the log, which is how `resumed_run_replays_from_log_without_rewaiting`
    // asserts replay-safety; only `Aborted` is latched here, because a cancelled
    // run's handler has unfinished business and must not run again.)
    let persisted = match store.get_run_state(&run_id).await {
        Ok(st) => st,
        Err(e) => return Err(err_event(&run_id, e, RunErrorCode::Error)),
    };
    if let Some(st) = &persisted
        && st.status == RunStatus::Aborted
    {
        // `cancel_run` 只翻 `RunState.status`、**不 append 终局事件**（它不是
        // 引擎路径），所以这条事实只能 emit 到流里。日志保持原样——重复 append
        // 一条终局事件会把「被取消」记成两次。
        return Err(WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: st
                .error
                .clone()
                .unwrap_or_else(|| RunError::cancelled()),
            code: RunErrorCode::Aborted,
        });
    }

    // append_seed 要判「run 现在是不是真的在等这个信号/审批」——那是清除前的
    // 事实，所以先快照（下面的 run_state 会把这两个投影清空）。
    let waiting_before: Option<WaitForState> = persisted.as_ref().and_then(|st| st.waiting_for.clone());
    let pending_before: Option<PendingApproval> =
        persisted.as_ref().and_then(|st| st.pending_approval.clone());

    let run_state = match persisted {
        Some(mut st) => {
            st.status = RunStatus::Running;
            st.error = None;
            // 清除上一次 drive 留下的等待投影（上游 `run-workflow.ts:336-342`
            // 在 resume 前导里做同一件事）。挂起由 `exec_pause` 写、
            // **在下次 drive 开头清**——不是在被唤醒的那一刻清，因为挂起后
            // 进程已经退出，没有「那一刻」。
            st.waiting_for = None;
            st.pending_approval = None;
            st.updated_at = ts;
            st
        }
        None => RunState {
            run_id: run_id.clone(),
            workflow_id: workflow.id.clone(),
            workflow_version: workflow.version.clone(),
            status: RunStatus::Running,
            input: opts.input.clone(),
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            awaiting: vec![],
            created_at: ts,
            updated_at: ts,
        },
    };
    if let Err(e) = store.set_run_state(&run_id, &run_state).await {
        return Err(err_event(&run_id, e, RunErrorCode::Error));
    }

    // 随本次 drive 投递的信号 / 审批（上游 appendSeed，run-workflow.ts:409）。
    // 必须在重放 handler 之前：waitForEvent 的重放短路靠扫日志找 SignalResolved。
    // 「没人在等」在这里变成 RunErrored——与其它引擎失败同一条通道，
    // 而不是静默什么都不做（那样调用方会看到 run 停在 Paused 却不知道为什么）。
    if opts.signal_delivery.is_some() || opts.approval.is_some() {
        match append_seed(
            store.as_ref(),
            &run_id,
            waiting_before.as_ref(),
            pending_before.as_ref(),
            opts.signal_delivery.as_ref(),
            opts.approval.as_ref(),
        )
        .await
        {
            Ok(DeliveryOutcome::Delivered | DeliveryOutcome::Duplicate) => {}
            Ok(DeliveryOutcome::NotWaiting) => {
                let what = match (&opts.signal_delivery, &opts.approval) {
                    (Some(d), _) => format!("signal `{}`", d.name),
                    (_, Some(a)) => format!("approval `{}`", a.approval_id),
                    _ => unreachable!("上面已判二者至少有一个"),
                };
                // 走 error_persisted：它会把 RunState 置为 Errored 再回事件。
                // 直接构造 RunErrored 只会 emit，state 仍停在 Running——调用方
                // 从 store 读到的状态与事件流对不上。
                return Err(error_persisted(
                    &store,
                    run_state,
                    &run_id,
                    &anyhow::anyhow!("run `{run_id}` is not waiting for {what} (delivery lost)"),
                    RunErrorCode::Error,
                )
                .await);
            }
            Ok(DeliveryOutcome::NotFound) => {
                return Err(error_persisted(
                    &store,
                    run_state,
                    &run_id,
                    &anyhow::anyhow!("run `{run_id}` does not exist"),
                    RunErrorCode::Error,
                )
                .await);
            }
            Err(e) => return Err(err_event(&run_id, e, RunErrorCode::Error)),
        }
    }

    // Version routing: resume against the definition whose `version` the run
    // persisted (workflow or one of its `previous_versions`). 版本化 run 匹配
    // 不上时**不回退**——那会把 v1 的 run 路由进当前版本的代码，是确定性违规
    // （上游 `select-version.ts` 注释原话）。这里以终局错误结束。
    let persisted_version = run_state
        .workflow_version
        .as_deref()
        .or(workflow.version.as_deref());
    let Some(active) = select_workflow_version(workflow_any, persisted_version) else {
        let e = anyhow::anyhow!(
            "run `{run_id}` was started under version {persisted_version:?}, \
             which is not the current version nor in `previous_versions`"
        );
        // 必须落盘：状态是 store 的事（[`RunOutcome::from_run_state`]），只 emit
        // 不写的话 `outcome()` 会读回 `Running`——一个永远不会自己结束的 run。
        return Err(error_persisted(
            &store,
            run_state,
            &run_id,
            &e,
            RunErrorCode::WorkflowVersionMismatch,
        )
        .await);
    };

    // Input shape-check **before** `initialize`: upstream validates the input
    // then builds state in one guarded block (`run-workflow.ts:209-213`), and a
    // bad input is a `validation_error`, not a handler crash. This has to live
    // here rather than in the typed handler because `TInput` is erased by the
    // time the engine sees the workflow (`AnyWorkflowDefinition`).
    if let Some(validate) = &active.input_validator
        && let Err(e) = validate(&opts.input)
    {
        return Err(error_persisted(&store, run_state, &run_id, &e, RunErrorCode::Validation).await);
    }

    // Per-invocation state: re-derived from `initialize(input)` on every
    // start and resume (mirrors TanStack, where state is rebuilt from
    // `initialize({ input })` and never persisted). The handler input the
    // workflow sees is `opts.input` (see `EngineRuntime.input`), so initialize
    // shares that source for consistency.
    let state = match (active.initialize)(&opts.input) {
        Ok(s) => s,
        Err(e) => return Err(error_persisted(&store, run_state, &run_id, &e, RunErrorCode::Validation).await),
    };
    if let Some(validate) = &active.state_validator
        && let Err(e) = validate(&state)
    {
        return Err(error_persisted(&store, run_state, &run_id, &e, RunErrorCode::Validation).await);
    }

    let events = match store.get_events(&run_id).await {
        Ok(ev) => ev,
        Err(e) => return Err(err_event(&run_id, e, RunErrorCode::Error)),
    };
    // F2-lite 耐久重试：折叠待重试状态（每 step 取最后一份
    // STEP_RETRY_SCHEDULED）——exec_step 入口据此泊车或续跑。
    let mut pending_retries: std::collections::HashMap<String, crate::engine::PendingRetry> =
        std::collections::HashMap::new();
    for ev in &events {
        if let WorkflowEvent::StepRetryScheduled {
            step_id,
            attempt,
            next_attempt_at,
            attempts,
            ..
        } = ev
        {
            pending_retries.insert(
                step_id.clone(),
                crate::engine::PendingRetry {
                    attempts: attempts.clone(),
                    last_attempt: *attempt,
                    next_attempt_at: *next_attempt_at,
                },
            );
        }
    }
    let next_log_index = events.len();


    let state_mirror: Arc<Mutex<serde_json::Value>> = Arc::new(Mutex::new(state.clone()));

    // 事件扇出只有一个动作：`send` 进无界通道。`publish` **不在这里**——它在
    // [`RunEventStream::poll_next`] 里、yield 之前被 await，与上游
    // `await options.publish(...)` 然后 `yield event`（`run-workflow.ts:120-129`）
    // 同一个位置。慢 publish 因此只拖慢**消费端吞吐**，不拖慢引擎进度：引擎只做
    // 一次非阻塞 `send`。
    let inner = Arc::new(EngineRuntime {
        run_id: run_id.clone(),
        // 选中的那一版（可能是 previous_versions 里的旧版本）。
        workflow: active.clone(),
        input: opts.input.clone(),
        meta: Some(opts.meta.as_value().clone()),
        state_mirror,
        prev_state_snapshot: Mutex::new(state),
        store: store.clone(),
        gate: Arc::new(Gate::new()),
        next_log_index: AtomicUsize::new(next_log_index),
        // 内存日志副本，对应上游 `history: [...history]`（`run-workflow.ts:561`）。
        // 重放短路与「已完成集合」都扫它，没有第二份派生状态。
        history: Mutex::new(events),
        appended: Mutex::new(Vec::new()),
        consumed: Mutex::new(std::collections::HashSet::new()),

        emit: publish_tx.clone(),
        counters: crate::engine::Counters::default(),
        deadline: opts.deadline,
        min_yield_remaining_ms: opts
            .min_yield_remaining_ms
            .unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS),
        yield_resume_at: opts.yield_resume_at,
        compensations: std::sync::Mutex::new(Vec::new()),
        pending_retries: Mutex::new(pending_retries),
        // 收集中间件的 step 边界钩子（注册顺序 = 声明顺序）。
        step_layers: Mutex::new(Vec::new()),
        ext_extra: Mutex::new(serde_json::Value::Null),
    });
    inner.publish(&WorkflowEvent::RunStarted {
        ts,
        run_id: run_id.clone(),
        thread_id: opts.thread_id.clone(),
    });

    let ctx = crate::define::WorkflowCtx::untyped(inner.clone());
    let handler_result = (active.handler)(ctx).await;
    // **清空 step 包装链**：drive 结束即丢弃，与中间件里捕获的策略状态
    // （`done` / `lookup` 等局部变量）同一生命周期。
    //
    // 这一步也是必需的：包装层经常捕获 `ctx.engine`（泊车要调 pause 原语），
    // 而链本身存在 `EngineRuntime` 上——不清空就形成 `engine → wrappers →
    // engine` 的引用环，engine 永不 drop，它的 publish sender 也就永不 drop，
    // 流不会结束（`drain()` 永久等待）。
    if let Ok(mut list) = inner.step_layers.lock() {
        list.clear();
    }
    // 尾段 state delta：ctx 在 handler 内 drop 时已把最终 state 同步进
    // mirror，这里统一 diff 发射（对齐上游 handler 返回 / catch 处的
    // flushStateDelta）。
    inner.flush_state_delta();

    // `failure` 是结构化的（`RunState.error`、`RUN_ERRORED`、`RunOutcome.error`
    // 三处同类型）。
    //
    // `WorkflowParked` 走**单独一条路**：它不是失败，也不是正常结束——run
    // 停在挂起点，等外部驱动器投递。`exec_pause` 已经把 checkpoint 与 RunState
    // 投影写好了，这里**不 append 终态事件、不覆盖 RunState**，直接以 `Paused`
    // 返回（对齐上游 `if (engine.paused) return`，`run-workflow.ts:529`）。
    let (status, output, failure) = match handler_result {
        Ok(output) => (RunStatus::Finished, Some(output), None),
        // 挂起不是失败、也不是正常结束：run 停在挂起点等外部投递，
        // `exec_pause` 已经把 checkpoint 与 RunState 投影写好了。**不 append
        // 终局事件**（对齐上游 `if (engine.paused) return`，`run-workflow.ts:529`），
        // 流到此自然结束。注意这次 drive **可能一条新事件都没写**（重 drive 一个
        // 已挂起的 run 时 `already_paused` 短路），所以 `Paused` 判不出来——它
        // 已经由 `exec_pause_with` 写进 RunState 了，宿主从 store 读。
        Err(e) if e.downcast_ref::<WorkflowParked>().is_some() => return Ok(()),
        Err(e) if e.downcast_ref::<WorkflowCancelled>().is_some() => (
            RunStatus::Aborted,
            None,
            Some((RunError::cancelled(), RunErrorCode::Aborted)),
        ),
        Err(e) => {
            // Phase-1 补偿（undo 轴）：未捕获失败 → 逆序执行已登记撤销。
            // 每个补偿一个 COMPENSATION_* checkpoint；某个补偿耗尽重试 →
            // 链就地停止（后续补偿留给人工），run 仍以原始失败 errored 结束
            // （对齐 #19 durable-recovery 语义）。
            if let Err((cid, ce)) =
                crate::engine::run_compensations(&inner, &run_id, &e).await
            {
                eprintln!("[compensation] {cid} 失败：{ce}");
            }
            (
                RunStatus::Errored,
                None,
                Some((RunError::from_anyhow(&e), RunErrorCode::Error)),
            )
        }
    };

    let terminal = match &failure {
        Some((err, code)) => WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: err.clone(),
            code: *code,
        },
        None => WorkflowEvent::RunFinished {
            ts: now_ms(),
            run_id: run_id.clone(),
            output: output.clone(),
        },
    };
    if let Err(e) = inner.append(&terminal).await {
        return Err(err_event(&run_id, e, RunErrorCode::Error));
    }

    let mut st = run_state;
    st.status = status;
    st.output = output.clone();
    st.error = failure.as_ref().map(|(e, _)| e.clone());
    st.updated_at = now_ms();
    if let Err(e) = store.set_run_state(&run_id, &st).await {
        return Err(err_event(&run_id, e, RunErrorCode::Error));
    }

    Ok(())
}

/// 把一条 store 层失败包成 `RUN_ERRORED` 事件（**不 append**——落盘失败时
/// 再写日志大概率也会失败，且 store 状态可能已经不一致）。
/// 该不该走 attach（只读回放），以及回放哪些事件。对齐上游 `attachRun`
/// （`run-workflow.ts:371-430`）那五步。返回 `None` = 正常 drive。
///
/// # 为什么**只**认显式 `attach`，不照抄上游的隐式 redirect
///
/// 上游 `startRun` 有个幂等检查（`run-workflow.ts:194-200`）：给了 `runId` 而
/// 该 id 已有 run，就 redirect 到 attach，「instead of a second start」。看着
/// 很该抄，但抄过来会踩两个坑，**其中一个是本仓有意的能力**：
///
/// 1. **上游分得清 start 和 resume，我们分不清。** 它的 resume 入口带
///    `signalDelivery || approval` 标志（`:173`），所以「同 runId 再来一次」
///    必然是误触发。我们的 resume 就是「同一 run_id + input」
///    （`runtime_driver.rs` 的 `drive_claimed_run`），**没有标志位**——照抄那条
///    redirect 会把每一次正常 resume 都变成 attach，workflow 直接不再推进。
/// 2. **重驱动已完成的 run 在这里是故意的，不只是"能跑"。**
///    `replay_shape_mismatch_fails_instead_of_yielding_null` 就是靠再 drive 一次
///    来发现「日志里的 step 结果已经反序列化不回 handler 声明的类型」——
///
/// 这是**日志形状漂移的自检**。redirect 会把它变成只读回放，漂移就永远查不出来了
/// （实测：改完之后该测试 `left: Finished, right: Errored`）。
///
/// 所以差异是自觉的：上游「重复 start 自动降级为 attach」，我们要求调用方显式
/// `.attach()`。重复 start 的防护留给调用点判断，而不是悄悄改掉 `run_id` 的含义。
/// 真要重跑一个已完成的 run，仍然照旧直接重驱动。
async fn attach_decision(
    opts: &RunWorkflowOptions,
    store: &Arc<dyn RunStore>,
    run_id: &str,
) -> Option<Vec<WorkflowEvent>> {
    if !opts.attach {
        return None;
    }
    let persisted = match store.get_run_state(run_id).await {
        Ok(st) => st,
        // 读不到状态就按正常 drive 走，让 `drive` 报它自己的错。
        Err(_) => return None,
    };
    // 显式 `attach` 指向不存在的 run 才是 `run_lost`；隐式 redirect 到这里时
    // 状态刚读出来过，不可能不存在。
    let Some(state) = persisted else {
        return Some(vec![WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.to_string(),
            error: RunError {
                name: "RunLost".to_string(),
                message: format!("Run {run_id} not found."),
            },
            code: RunErrorCode::RunLost,
        }]);
    };

    // ① 合成的 `RUN_STARTED` 当信封头（上游 `:393-398`）——日志里那条是上一次
    //    drive 写的，attach 要给订阅方一个"这次回放现在开始"的边界。
    let mut out = vec![WorkflowEvent::RunStarted {
        ts: now_ms(),
        run_id: run_id.to_string(),
        thread_id: opts.thread_id.clone(),
    }];
    // ② 整份日志重放（上游 `:401`）。
    match store.get_events(run_id).await {
        Ok(evs) => out.extend(evs),
        Err(e) => return Some(vec![err_event(run_id, e, RunErrorCode::Error)]),
    }
    // ③ 日志里没有终态事件时，用持久化的 status 补一条（上游 `:404-428`）。
    //    `cancel_run` 只翻 status 不写事件，所以 aborted 的 run 必然走这条。
    let has_terminal = out.iter().any(|e| {
        matches!(e, WorkflowEvent::RunFinished { .. } | WorkflowEvent::RunErrored { .. })
    });
    if !has_terminal {
        match state.status {
            RunStatus::Finished => out.push(WorkflowEvent::RunFinished {
                ts: now_ms(),
                run_id: run_id.to_string(),
                output: state.output.clone(),
            }),
            RunStatus::Errored | RunStatus::Aborted => out.push(WorkflowEvent::RunErrored {
                ts: now_ms(),
                run_id: run_id.to_string(),
                error: state.error.clone().unwrap_or(RunError {
                    name: "Unknown".to_string(),
                    message: "Run ended in non-terminal state".to_string(),
                }),
                code: if state.status == RunStatus::Aborted {
                    RunErrorCode::Aborted
                } else {
                    RunErrorCode::Error
                },
            }),
            // `paused` / `running`：快照已经在上面的回放里了，要跟后续事件
            // 就得靠 `publish` 钩子（上游 `:429-430` 同一句）。
            RunStatus::Paused | RunStatus::Running => {}
        }
    }
    Some(out)
}

fn err_event(run_id: &str, e: impl std::fmt::Display, code: RunErrorCode) -> WorkflowEvent {
    WorkflowEvent::RunErrored {
        ts: now_ms(),
        run_id: run_id.to_string(),
        error: RunError {
            name: "StoreError".to_string(),
            message: e.to_string(),
        },
        code,
    }
}

/// 把一条**引擎在跑起来之前**就知道的失败落盘，并返回对应的 `RUN_ERRORED` 事件。
///
/// 覆盖两类：pre-handler 初始化失败（state `initialize` 报错、`state_schema`
/// 形状检查拒绝——对应 TanStack zod `.safeParse` 失败，报错而不是悬着），以及
/// 版本路由失配。
///
/// **落盘是必须的，不是可选的**：状态归 store 管（[`RunOutcome::from_run_state`]），
/// 只 emit 不写的话 `outcome()` 会读回上一个状态（通常是 `Running`）——一个看起来
/// 永远不会自己结束的 run。对齐上游 `drive` 的 catch 分支：`status='errored'` +
/// `error` + `setRunState`，然后 `emitAndAppend(RUN_ERRORED)`
/// （`run-workflow.ts:555-570`）。
///
/// **只走流，不 append**：引擎还没开始跑，不该在日志里留下半条记录——TanStack
/// 同理，他们的 validation 失败走 `emit(...)`（只进内存队列），不是
/// `emitAndAppend`（`run-workflow.ts:215`）。
async fn error_persisted(
    store: &Arc<dyn RunStore>,
    mut run_state: RunState,
    run_id: &str,
    err: &anyhow::Error,
    code: RunErrorCode,
) -> WorkflowEvent {
    let run_err = RunError::from_anyhow(err);
    run_state.status = match code {
        RunErrorCode::Aborted => RunStatus::Aborted,
        _ => RunStatus::Errored,
    };
    run_state.error = Some(run_err.clone());
    run_state.updated_at = now_ms();
    // 落盘失败不改变这条事件：流是内存队列，与日志是两件事（上游同形）。
    let _ = store.set_run_state(run_id, &run_state).await;
    WorkflowEvent::RunErrored {
        ts: now_ms(),
        run_id: run_id.to_string(),
        error: run_err,
        code,
    }
}

/// Sync convenience over a local multi-thread runtime for callers that are
/// not async themselves (e.g. LocalDub's CLI entrypoint).
///
/// 事件流在这里被**排干**：本函数的契约是「跑到终态并交出 outcome」，中间事件
/// 不外泄。要逐条看事件就用 async 的 [`run_workflow`]。`publish` 回调仍然逐条
/// 触发（它在 [`RunEventStream`] 里被 await，与 async 路径同一处）。
pub fn run_workflow_sync(opts: RunWorkflowOptions) -> Result<RunOutcome, WorkflowError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|e| WorkflowError::Internal(format!("tokio runtime: {e}")))?;
    rt.block_on(async move { run_workflow(opts).outcome().await })
}
#[cfg(test)]
mod tests {
    use crate::define::{Backoff, RetryPolicy};
    /// `threadId` 只挂在 `RUN_STARTED` 上（上游 `types.ts:76` / `run-workflow.ts:52`），
    /// 且 start / resume / attach 三条路径都要带上（上游 `:242` / `:353` / `:391`）。
    ///
    /// attach 这条尤其容易漏：它发的是**全新**的 `RUN_STARTED`（带 attach 调用者
    /// 自己的 threadId），然后才重放整份日志——所以订阅者会看到两个 `RUN_STARTED`。
    #[tokio::test]
    async fn thread_id_lands_on_run_started_in_all_three_paths() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.step("s", |_sc: StepCtx| async move { Ok(serde_json::json!({"v": 1})) })
                .await
        });

        // 1) start
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("t1")
                .thread_id(Some("thread-start".into())),
        )
        .collect()
        .await;
        let started: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::RunStarted { thread_id, .. } => Some(thread_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(started, vec![Some("thread-start".into())]);

        // 2) resume（同一个 run_id 再 drive 一次）
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("t1")
                .thread_id(Some("thread-resume".into())),
        )
        .collect()
        .await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                WorkflowEvent::RunStarted { thread_id: Some(t), .. } if t == "thread-resume"
            )),
            "resume 路径的 RUN_STARTED 应带新 threadId"
        );

        // 3) attach：合成头带 attach 自己的 threadId，与日志里那条并存
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("t1")
                .attach()
                .thread_id(Some("thread-attach".into())),
        )
        .collect()
        .await;
        let attached_head = match events.first() {
            Some(WorkflowEvent::RunStarted { thread_id, .. }) => thread_id.clone(),
            other => panic!("attach 首事件应是合成的 RUN_STARTED，实际：{other:?}"),
        };
        assert_eq!(attached_head, Some("thread-attach".into()));

        // 4) 不给 threadId 时字段整个消失（serde skip），与上游 `threadId: undefined`
        //    在 JSON 里被丢掉一致
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(WorkflowDefinition::new("w2").handler(
                |ctx: WorkflowCtx| async move {
                    ctx.step("s", |_sc: StepCtx| async move { Ok(serde_json::json!({})) })
                        .await
                },
            )), store)
                .input(serde_json::json!({}))
                .run_id("t2"),
        )
        .collect()
        .await;
        match events.first() {
            Some(WorkflowEvent::RunStarted { thread_id, .. }) => {
                assert_eq!(*thread_id, None, "未给 threadId 应为 None");
            }
            other => panic!("应为 RUN_STARTED，实际：{other:?}"),
        }
        let json = serde_json::to_string(&events[0]).unwrap();
        assert!(
            !json.contains("thread_id"),
            "None 时不该序列化出 thread_id：{json}"
        );
    }

    /// attach 是**只读**的：不重跑 handler、不写日志、不改 RunState。
    ///
    /// 三件事分别钉住：事件包形状（合成 RUN_STARTED 头 + 整份日志重放）、
    /// handler 没被再跑、日志长度不变。
    #[tokio::test]
    async fn attach_replays_log_without_reexecuting_or_writing() {
        let store = Arc::new(InMemoryStore::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let r = runs.clone();
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                ctx.step("s", |_sc: StepCtx| async move { Ok(serde_json::json!({"v": 1})) })
                    .await
            }
        });
        let first = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("att"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(first.status, RunStatus::Finished);
        let log_before = store.get_events("att").await.unwrap().len();
        let state_before = store.get_run_state("att").await.unwrap().unwrap();

        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("att")
                .attach(),
        )
        .collect()
        .await;

        // 合成头 + 日志重放（头不在原日志里，所以比原日志多一条）。
        assert!(
            matches!(events.first(), Some(WorkflowEvent::RunStarted { .. })),
            "attach 要先给一个合成的 RUN_STARTED 当信封头，实际：{:?}",
            events.first()
        );
        assert_eq!(events.len(), log_before + 1, "attach 应重放整份日志再加一个头");
        assert!(
            events.iter().any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "重放里要有原来的终态事件"
        );
        // 只读：handler 没跑、日志没长、状态没动。
        assert_eq!(runs.load(Ordering::SeqCst), 1, "attach 不得重跑 handler");
        assert_eq!(store.get_events("att").await.unwrap().len(), log_before, "attach 不得写日志");
        assert_eq!(store.get_run_state("att").await.unwrap().unwrap().status, state_before.status);
    }

    /// attach 一个不存在的 run ⇒ `run_lost`（上游 `attachRun` 第一条分支，
    /// `run-workflow.ts:376-385`）。这是**唯一**产得出 `run_lost` 的地方。
    #[tokio::test]
    async fn attach_to_missing_run_is_run_lost() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::Value::Null) });
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store)
                .run_id("nope")
                .attach(),
        )
        .collect()
        .await;
        match events.as_slice() {
            [WorkflowEvent::RunErrored { code, error, .. }] => {
                assert_eq!(*code, RunErrorCode::RunLost);
                assert_eq!(error.name, "RunLost");
            }
            other => panic!("应只报 run_lost，实际：{other:?}"),
        }
    }

    /// `cancel_run` 只翻 status、**不写终态事件**（`drive` 里那条注释说得对）。
    /// 所以 attach 一个被取消的 run 时，日志里没有终态事件，信封得靠持久化状态
    /// 补一条 `RUN_ERRORED` —— 走的就是上游 `:414-428` 那个 `!hasPersistedTerminal`
    /// 分支，否则订阅方永远等不到收尾。
    #[tokio::test]
    async fn attach_synthesizes_terminal_event_for_status_only_runs() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.approve("gate", "ok?").await?;
            Ok(serde_json::Value::Null)
        });
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("ab"),
        )
        .outcome()
        .await
        .unwrap();
        crate::engine::cancel_run(store.as_ref(), "ab").await.unwrap();

        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .run_id("ab")
                .attach(),
        )
        .collect()
        .await;
        match events.last() {
            Some(WorkflowEvent::RunErrored { run_id, code, .. }) => {
                assert_eq!(run_id, "ab");
                assert_eq!(
                    *code,
                    RunErrorCode::Aborted,
                    "cancel_run 不写终态事件，attach 得按持久化 status 补一条 aborted"
                );
            }
            other => panic!("末尾应是合成的 RUN_ERRORED，实际：{other:?}"),
        }
        // 补的这条只在流里，日志仍然没有终态事件（attach 只读）。
        assert!(
            !store
                .get_events("ab").await
                .unwrap()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. } | WorkflowEvent::RunErrored { .. })),
            "attach 不得往日志里补写终态事件"
        );
    }

    /// 没有 `signalDelivery` 这类标志位时，**重驱动一个已完成的 run 必须仍然是
    /// 重驱动**，不能被 attach 吃掉 —— 上游靠标志位区分 start/resume，我们靠
    /// 「同一 run_id + input」这个形状区分。少一个标志位就得靠日志形状自检来兜底
    /// （见 `replay_shape_mismatch_fails_instead_of_yielding_null`）。
    #[tokio::test]
    async fn redrive_of_finished_run_is_still_a_redi_rive_not_an_attach() {
        let store = Arc::new(InMemoryStore::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let r = runs.clone();
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                ctx.step("s", |_sc: StepCtx| async move { Ok(serde_json::json!({"v": 1})) })
                    .await
            }
        });
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("rd"),
        )
        .outcome()
        .await
        .unwrap();
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("rd"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 2, "没写 attach 就该照常重驱动");
    }

    use super::*;
    use crate::define::{BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, WorkflowCtx, create_workflow};
    use crate::engine::testkit::TestLog;
    use crate::run_store::InMemoryStore;
    use serde_json::json;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    #[derive(serde::Deserialize, Clone, serde::Serialize)]
    struct StrictState {
        n: u64,
    }

    /// 未显式给 `run_id` 时引擎生成 `run_<now_ms>`，并照常写 RunState 信封。
    #[tokio::test]
    async fn run_id_defaults_to_generated_when_absent() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("gen-id").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { Ok(json!({ "ok": true })) })
                .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({ "x": 1 })),
        )
        .outcome().await
        .unwrap();
        assert!(
            out.run_id.starts_with("run_"),
            "缺省 run_id 由引擎生成，实际为 {:?}",
            out.run_id
        );
        let st = store
            .get_run_state(&out.run_id).await
            .unwrap()
            .expect("RunState 应已写入");
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.input, json!({ "x": 1 }), "input 落在 RunState 信封上");
    }

    /// `initialize` 失败 → `init_failed`：run 记 Errored，且**不 append 任何事件**
    /// （什么都没跑）。对齐 TanStack zod `.safeParse` 失败的语义。
    #[tokio::test]
    async fn initialize_failure_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("cannot build state")))
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("never", |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await
            });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("bad-init:r"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("cannot build state"));
        assert!(
            store.get_events("bad-init:r").await.unwrap().is_empty(),
            "init 失败不应留下任何事件"
        );
        let st = store.get_run_state("bad-init:r").await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
        assert!(st.output.is_none());
    }

    /// `state_schema` 形状不匹配走同一条 `init_failed` 路径。
    #[tokio::test]
    async fn state_shape_mismatch_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("bad-state")
                .input::<serde_json::Value>()
                .initialize(|_| Ok(json!({ "n": "not-a-number" })))
                .state::<StrictState>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value, StrictState>| async move {
            Ok(json!({ "n": ctx.state.n }))
        });
        let wf: AnyWorkflowDefinition = wf.into();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("bad-state:r"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            out.error.unwrap().message.contains("invalid type"),
            "错误应来自 state 的 serde 形状校验"
        );
        assert!(
            store.get_events("bad-state:r").await.unwrap().is_empty(),
            "state 校验失败同样不落事件"
        );
        let st = store.get_run_state("bad-state:r").await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
    }

    /// `RUN_ERRORED` 带机器可读的 `code`（TanStack `RUN_ERRORED.code`），
    /// host 该用它分支，而不是去匹配 `error.message`。
    #[tokio::test]
    async fn handler_failure_errored_with_error_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("bad").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { anyhow::bail!("boom") })
                .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:err"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        let (err, code) = store
            .get_events("code:err").await
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Error);
        assert_eq!(code.as_str(), "error");
        assert_eq!(err.message, "boom");
    }

    #[tokio::test]
    async fn cancelled_run_errored_with_aborted_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("c").handler(|_ctx: WorkflowCtx| async move {
            Err(crate::engine::WorkflowCancelled.into())
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:abort"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Aborted);
        let (err, code) = store
            .get_events("code:abort").await
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Aborted);
        assert_eq!(err.name, "Aborted");
    }

    /// publisher 收 `&RunId`（具名别名）而**不是** `&str`，且这不该带来 clone。
    ///
    /// 这条容易被"顺手优化"掉：`RunId = String`，所以 `&RunId` 就是 `&String`，
    /// 看着比 `&str` 重。但 `RunEventStream` 本来就持有一个 owned
    /// `run_id: String`（`run_workflow.rs:496`），poll 里传的是 `&me.run_id`——
    /// 换成 `&RunId` 后**零分配**，因为签名要的是 `&String` 而它手上就是 `&String`。
    ///
    /// 反过来若把签名退回 `&str`，runtime 侧要拿 `&RunId` 喂 publisher 就得
    /// 每个事件 `String::from` 一次——白付一次分配，就为省下一个名字。
    #[tokio::test]
    async fn publisher_run_id_is_named_and_borrowed() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        // 收 &RunId 的回调能拿到具体类型（不是靠 deref 蒙混）
        let got: Arc<Mutex<Option<RunId>>> = Arc::new(Mutex::new(None));
        let g = got.clone();
        let publish: Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync> = {
            Arc::new(move |run_id: &RunId, _ev: WorkflowEvent| {
                *g.lock().unwrap() = Some(run_id.clone());
            })
        };
        let wf = WorkflowDefinition::new("named").handler(|ctx: WorkflowCtx| async move {
            ctx.step("s", |_sc: StepCtx| async move { Ok(serde_json::json!({})) })
                .await
        });
        run_workflow(
            RunWorkflowOptions::new(wf, store)
                .input(serde_json::json!({}))
                .run_id("named:1")
                .publish(Some(publish)),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(*got.lock().unwrap(), Some(RunId::from("named:1")));
    }


    /// `publish` 收到的 `run_id` 就是 run 身份本身（上游把 runId 单列成一个参数，
    /// `run-workflow.ts:41`）。这里特意用 `Custom` 事件验：`Custom` 的
    /// `event.run_id()` 字段是宿主自己填的，引擎无从保证它对——所以 run 身份
    /// 必须能从**回调参数**拿到，而不是从事件里反解。
    #[tokio::test]
    async fn publish_receives_run_id_including_custom_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let publish: Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync> = {
            let seen = seen.clone();
            Arc::new(move |run_id: &RunId, ev: WorkflowEvent| {
                seen.lock()
                    .unwrap()
                    .push((run_id.to_string(), ev.type_name().to_string()))
            })
        };
        let wf = WorkflowDefinition::new("rid")
            .handler(|ctx: WorkflowCtx| async move {
                ctx.emit("ping", serde_json::json!({ "v": 1 }));
                Ok(serde_json::json!({}))
            });
        run_workflow(
            RunWorkflowOptions::new(wf, store)
                .input(serde_json::json!({}))
                .run_id("rid:1")
                .publish(Some(publish)),
        )
        .outcome().await
        .unwrap();

        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "publish 一次都没被调用？");
        assert!(
            seen.iter().all(|(rid, _)| rid == "rid:1"),
            "每条事件的 run_id 都该是 'rid:1'，实际：{seen:?}"
        );
        assert!(
            seen.iter().any(|(_, ty)| ty == "CUSTOM"),
            "Custom 事件也该带上 run_id：{seen:?}"
        );
    }

    /// `initialize` 失败：publish `validation_error`，但**不进事件日志**（对齐
    /// TanStack 的 `emit` 而非 `emitAndAppend`）。
    #[tokio::test]
    async fn init_failure_publishes_validation_code_without_appending() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen: Arc<Mutex<Vec<WorkflowEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let publish: Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync> = {
            let seen = seen.clone();
            Arc::new(move |_run_id: &RunId, ev| seen.lock().unwrap().push(ev))
        };
        let wf = WorkflowDefinition::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("nope")))
            .handler(|_ctx: WorkflowCtx| async move { Ok(json!({ "unreachable": true })) });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:validation")
            .publish(Some(publish)),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            store.get_events("code:validation").await.unwrap().is_empty(),
            "什么都没跑，不该留下事件"
        );

        let (err, code) = seen
            .lock()
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("应 publish RunErrored");
        assert_eq!(code, RunErrorCode::Validation);
        assert_eq!(code.as_str(), "validation_error");
        assert_eq!(err.message, "nope");
    }

    /// 一次失败的 run，在四处描述必须是**同一个** `RunError`：
    /// `RunOutcome.error`（返回值）、`RunState.error`（信封）、
    /// `RUN_ERRORED.error`（日志）、`StepAttempt.error`（step 级）。
    /// 任何一处退回字符串都是退步，这个测试钉住它。
    ///
    /// 顺带验证 `RunError` 的 `Display` 转发 `message`——所以改成结构化之后
    /// 调用方 `warn!("{e}")` 的写法不受影响。
    #[tokio::test]
    async fn one_failure_is_one_run_error_everywhere() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("one-error").handler(|ctx: WorkflowCtx| async move {
            ctx.step(
                "a",
                |_sc: StepCtx| async move { anyhow::bail!("same failure") },
            )
            .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("one:r"),
        )
        .outcome().await
        .unwrap();

        let from_outcome = out.error.expect("RunOutcome.error");
        let from_state = store
            .get_run_state("one:r").await
            .unwrap()
            .unwrap()
            .error
            .expect("RunState.error");

        let events = store.get_events("one:r").await.unwrap();
        let from_event = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("RUN_ERRORED.error");
        let from_attempt = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFailed { attempts, .. } => {
                    attempts.first().and_then(|a| a.error.clone())
                }
                _ => None,
            })
            .expect("StepAttempt.error");

        assert_eq!(
            from_outcome, from_state,
            "RunOutcome 与 RunState 应同类型同值"
        );
        assert_eq!(from_outcome, from_event, "与 RUN_ERRORED 应同类型同值");
        assert_eq!(from_outcome, from_attempt, "与 StepAttempt 应同类型同值");
        assert_eq!(from_outcome.message, "same failure");

        // Display 转发 message —— 调用方 `format!("{e}")` 不受影响。
        assert_eq!(from_outcome.to_string(), from_outcome.message);
    }

    /// 版本化 run 的 `workflow_version` 匹配不上 → 终局错误，**不回退**。
    /// 回退会把旧版 run 路由进新版代码（确定性违规，见 registry 模块文档）。
    #[tokio::test]
    async fn version_mismatch_errors_instead_of_falling_back() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());

        // 先以 v1 起一个 run（挂起在 approve，保持非终态以便后续 resume）。
        let v1 = WorkflowDefinition::new("ver")
            .version("v1")
            .handler(|ctx: WorkflowCtx| async move {
                ctx.approve("gate", "hold").await?;
                Ok(serde_json::Value::Null)
            });
        let task = tokio::spawn({
            let store = store.clone();
            let v1 = v1.clone();
            async move {
                run_workflow(
                    RunWorkflowOptions::new(Arc::new(v1.clone()), store)
                        .input(serde_json::json!({}))
                    .run_id("ver:mismatch"),
                )
                .outcome().await
            }
        });
        // 等挂起（`RunState` 信封此时已记下 `workflow_version = "v1"`）。
        for _ in 0..500 {
            if let Some(st) = store.get_run_state("ver:mismatch").await.unwrap()
                && st.status == RunStatus::Paused
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        task.abort();
        let _ = task.await;

        // v2 **不带** v1 作 previous_versions → 应终局报错而非回退到 v2。
        // 失败走**事件**（对齐上游：他们的版本失配也是 `emit(RUN_ERRORED{code:
        // 'workflow_version_mismatch'})`，`run-workflow.ts:286-297`），不是
        // `Err`——run_workflow 的流没有 Err 变体。
        let v2 = WorkflowDefinition::new("ver")
            .version("v2")
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::Value::Null) });
        let events = run_workflow(
            RunWorkflowOptions::new(Arc::new(v2.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:mismatch"),
        )
        .collect()
        .await;
        match events.last() {
            Some(WorkflowEvent::RunErrored { code, error, .. }) => {
                assert_eq!(
                    *code,
                    RunErrorCode::WorkflowVersionMismatch,
                    "错误码应为 workflow_version_mismatch"
                );
                assert!(
                    error.message.contains("version"),
                    "消息应说明是版本问题，实际 {error:?}"
                );
            }
            other => panic!("末事件应为 RunErrored，实际 {other:?}"),
        }
        // 派生出的 outcome 是 Errored（不是 Err）——状态从 store 读
        let state = store.get_run_state("ver:mismatch").await.unwrap().unwrap();
        let outcome = RunOutcome::from_run_state(state);
        assert_eq!(outcome.status, RunStatus::Errored);
    }

    #[tokio::test]
    async fn handler_output_is_run_output() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({"x": 1}))
                })
                .await?;
            Ok(serde_json::json!({ "out": v }))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "out": { "x": 1 } })));
        let st = store.get_run_state(&out.run_id).await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.output, Some(serde_json::json!({ "out": { "x": 1 } })));
    }

    /// resume 按持久化 `workflow_version` 路由到 previous version 的 handler；
    /// 全新 run 用当前版本。
    /// 回归护栏：**重 drive 一个已挂起的 run，流里一条新事件都不写，但
    /// `outcome()` 仍然报 `Paused`**。
    ///
    /// 这是把状态改成「从 store 读」的根本原因。`exec_pause_with` 的
    /// `already_paused` 门让重放幂等（对齐上游 `findCheckpoint` 短路，
    /// commit `4f64b9c`），代价是第二次 drive 的流只有 `RUN_STARTED`——
    /// 凭末事件分不出「挂起」和「结束」。`Paused` 只能问 store。
    ///
    /// 顺带钉住 wait 投影的清除点：drive 前导会清掉上一轮挂起留下的
    /// `pending_approval`（对齐上游 `run-workflow.ts:336-342`），重挂起再投影一次，
    /// 所以跑完之后它必须还在——否则「清干净了但没重投影」和「没清」就分不开了。
    #[tokio::test]
    async fn redrive_of_parked_run_writes_no_new_events_but_still_reports_paused() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("park").handler(|ctx: WorkflowCtx| async move {
            ctx.approve("gate", "等审批").await?;
            Ok(serde_json::json!({ "ok": true }))
        });
        let opts = |store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(Arc::new(wf.clone()), store)
                .input(serde_json::json!({}))
                .run_id("park:1")
        };

        // 第一次 drive：真的挂起，写下 STEP_PAUSED。
        let first = run_workflow(opts(store.clone())).collect().await;
        assert!(
            first.iter().any(|e| matches!(e, WorkflowEvent::SignalAwaited { .. } | WorkflowEvent::ApprovalRequested { .. })),
            "首次挂起应写 STEP_PAUSED，实际 {:?}",
            first.iter().map(|e| e.type_name()).collect::<Vec<_>>()
        );
        assert_eq!(
            run_workflow(opts(store.clone())).outcome().await.unwrap().status,
            RunStatus::Paused
        );

        // 第二次 drive：primitive 短路 → **零条新事件**（连 STEP_PAUSED 都不重写）。
        let second = run_workflow(opts(store.clone())).collect().await;
        assert!(
            !second.iter().any(|e| matches!(e, WorkflowEvent::SignalAwaited { .. } | WorkflowEvent::ApprovalRequested { .. })),
            "重挂起不该重复 append STEP_PAUSED（日志幂等），实际 {:?}",
            second.iter().map(|e| e.type_name()).collect::<Vec<_>>()
        );
        assert!(
            !second.iter().any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "重 drive 没跑完，不该有 RUN_FINISHED"
        );

        // 但状态仍然问得到，而且必须是 Paused——不是「流结束了所以不知道」。
        assert_eq!(
            run_workflow(opts(store.clone())).outcome().await.unwrap().status,
            RunStatus::Paused,
            "Paused 只能从 RunState 来"
        );
        let st = store.get_run_state("park:1").await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        // 审批类挂起投影到 `pending_approval`（`waiting_for` 是 signal/sleep 那档，
        // 见 `exec_pause_with` 里的 `WaitKind` 分支）。
        assert!(
            st.pending_approval.is_some(),
            "重挂起后审批投影应重新写上，实际 {st:?}"
        );
    }

    #[tokio::test]
    async fn resume_routes_by_persisted_workflow_version() {
        let store = Arc::new(InMemoryStore::new());
        let v1: AnyWorkflowDefinition = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v1")
                .input::<serde_json::Value>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v1" }))
            })
            .await
        })
        .into();

        let out1 = run_workflow(
            RunWorkflowOptions::new(Arc::new(v1.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ver:r"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out1.output, Some(serde_json::json!({ "ver": "v1" })));

        let v2: AnyWorkflowDefinition = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v2")
                .input::<serde_json::Value>(),
        )
        .previous_versions(vec![v1])
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v2" }))
            })
            .await
        })
        .into();

        let out2 = run_workflow(
            RunWorkflowOptions::new(v2.clone(), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:r"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(
            out2.output,
            Some(serde_json::json!({ "ver": "v1" })),
            "resume 路由到持久化的 previous version"
        );

        let out3 = run_workflow(
            RunWorkflowOptions::new(v2.clone(), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:r2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(
            out3.output,
            Some(serde_json::json!({ "ver": "v2" })),
            "全新 run 用当前版本"
        );
    }

    // ========================================================
    // Phase-1 补偿（undo 轴）——对齐 #19 durable-recovery 语义
    // ========================================================

    type UndoLog = Arc<Mutex<Vec<&'static str>>>;

    type UndoHandler = Arc<dyn Fn(StepCtx) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>,
    > + Send + Sync>;

    fn make_undo(log: &UndoLog, name: &'static str) -> UndoHandler {
        let calls = Arc::clone(log);
        Arc::new(move |_sc: StepCtx| {
            let calls = Arc::clone(&calls);
            Box::pin(async move {
                calls.lock().unwrap().push(name);
                Ok(())
            })
        })
    }

    async fn compensation_events(store: &InMemoryStore, run_id: &str) -> Vec<(String, String)> {
        store
            .get_events(run_id).await
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::CompensationStarted { .. } => {
                    Some(("COMPENSATION_STARTED".into(), String::new()))
                }
                WorkflowEvent::CompensationCompleted { compensation_id, .. } => {
                    Some(("COMPENSATION_COMPLETED".into(), compensation_id.clone()))
                }
                WorkflowEvent::CompensationFailed { compensation_id, .. } => {
                    Some(("COMPENSATION_FAILED".into(), compensation_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// 补偿**登记**是耐久的（对齐上游 \`COMPENSATION_REGISTERED\`）：落一条事件，
    /// 且 handler 重放时不会重复追加。
    #[tokio::test]
    async fn compensation_registration_is_durable_and_replay_idempotent() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let undo_log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let undo_in = undo_log.clone();

        let wf = WorkflowDefinition::new("reg").handler(move |ctx: WorkflowCtx| {
            let undo_in = undo_in.clone();
            async move {
                ctx.compensate(
                    "undo-a",
                    move |_sc: StepCtx| {
                        let undo_in = undo_in.clone();
                        Box::pin(async move {
                            undo_in.lock().unwrap().push("undo-a".into());
                            Ok(())
                        })
                    },
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                Err::<serde_json::Value, _>(anyhow::anyhow!("boom"))
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("reg:1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        let registered = store
            .get_events("reg:1").await
            .unwrap()
            .iter()
            .filter(|e| matches!(
                e,
                WorkflowEvent::CompensationRegistered { compensation_id, .. }
                    if compensation_id == "undo-a"
            ))
            .count();
        assert_eq!(registered, 1, "登记应落一条 COMPENSATION_REGISTERED");
        assert_eq!(
            undo_log.lock().unwrap().as_slice(),
            &["undo-a".to_string()],
            "undo 应执行一次"
        );

        // 重放（同 run_id 再驱一次）：登记事件不得重复追加。
        let again = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("reg:1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(again.status, RunStatus::Errored);
        let registered_again = store
            .get_events("reg:1").await
            .unwrap()
            .iter()
            .filter(|e| matches!(
                e,
                WorkflowEvent::CompensationRegistered { compensation_id, .. }
                    if compensation_id == "undo-a"
            ))
            .count();
        assert_eq!(
            registered_again, 1,
            "重放不得重复追加登记事件（findCheckpoint 那一步的对端）"
        );
    }

    /// 重复补偿 id 直接报错（对齐上游 \`Duplicate compensation id "x"\`）。
    ///
    /// 不检查的后果不只是"名单里多一项"：失败时逆序 undo 会把它执行两遍，
    /// 而第二次的撤销对象可能已被第一次消掉。
    #[tokio::test]
    async fn duplicate_compensation_id_is_rejected() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let undo_log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let undo_in = undo_log.clone();

        let wf = WorkflowDefinition::new("dup").handler(move |ctx: WorkflowCtx| {
            let undo_in = undo_in.clone();
            async move {
                ctx.compensate(
                    "same-id",
                    move |_sc: StepCtx| {
                        let undo_in = undo_in.clone();
                        Box::pin(async move {
                            undo_in.lock().unwrap().push("same-id".into());
                            Ok(())
                        })
                    },
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                // 同 id 第二次登记应报错，且错误信息可读。
                let dup = ctx.compensate(
                    "same-id",
                    move |_sc: StepCtx| Box::pin(async move { Ok(()) }),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await;
                assert!(
                    dup.is_err(),
                    "重复 id 应报错，实际 {:?}",
                    dup.as_ref().err().map(|e| e.to_string())
                );
                Err::<serde_json::Value, _>(anyhow::anyhow!("boom"))
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("dup:1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        // 名单里只有一项 ⇒ undo 只跑一次。
        assert_eq!(
            undo_log.lock().unwrap().as_slice(),
            &["same-id".to_string()],
            "重复登记被拒后，逆序 undo 只应执行一次"
        );
    }

    #[tokio::test]
    async fn compensation_runs_in_reverse_on_failure() {
        let store = Arc::new(InMemoryStore::new());
        let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
        let undo_a = make_undo(&undo_log, "undo-a");
        let undo_b = make_undo(&undo_log, "undo-b");

        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            let undo_b = undo_b.clone();
            async move {
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                ctx.compensate(
                    "undo-b",
                    move |sc: StepCtx| undo_b(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                let _ = ctx
                    .step("boom", move |_sc: StepCtx| async move {
                        Err::<serde_json::Value, _>(anyhow::anyhow!("boom"))
                    })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Errored);
        assert_eq!(
            *undo_log.lock().unwrap(),
            vec!["undo-b", "undo-a"],
            "逆登记序执行（后登记先撤销）"
        );
        let comp = compensation_events(&store, &out.run_id).await;
        assert_eq!(comp.len(), 3, "STARTED + COMPLETED ×2");
        assert_eq!(comp[0].0, "COMPENSATION_STARTED");
        assert_eq!(
            comp.iter().filter(|(t, _)| t == "COMPENSATION_COMPLETED").count(),
            2
        );
    }

    #[tokio::test]
    async fn compensation_failure_stops_chain() {
        let store = Arc::new(InMemoryStore::new());
        let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
        let undo_a = make_undo(&undo_log, "undo-a");
        let undo_b = make_undo(&undo_log, "undo-b");

        let handler_log = Arc::clone(&undo_log);
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            let _undo_b = undo_b.clone();
            let calls = Arc::clone(&handler_log);
            async move {
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                // undo-b 的撤销动作本身失败（耗尽即停）
                ctx.compensate(
                    "undo-b",
                    move |_sc: StepCtx| {
                        calls.lock().unwrap().push("undo-b");
                        Box::pin(async {
                            Err::<(), _>(anyhow::anyhow!("undo-b 自身故障"))
                        })
                    },
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                let _ = ctx
                    .step("boom", move |_sc: StepCtx| async move {
                        Err::<serde_json::Value, _>(anyhow::anyhow!("boom"))
                    })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Errored);
        assert_eq!(*undo_log.lock().unwrap(), vec!["undo-b"], "链就地停止：undo-a 未执行");
        let comp = compensation_events(&store, &out.run_id).await;
        assert!(comp.iter().any(|(t, _)| t == "COMPENSATION_FAILED"));
        assert_eq!(comp.iter().filter(|(t, _)| t == "COMPENSATION_COMPLETED").count(), 0);
    }

    #[tokio::test]
    async fn no_compensation_on_success() {
        let store = Arc::new(InMemoryStore::new());
        let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
        let undo_a = make_undo(&undo_log, "undo-a");

        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            async move {
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                let _ = ctx
                    .step("ok", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Finished);
        assert!(
            compensation_events(&store, &out.run_id).await.is_empty(),
            "成功不触发补偿"
        );
    }

    #[tokio::test]
    async fn completed_compensations_skip_across_restart() {
        let store = Arc::new(InMemoryStore::new());
        let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
        let undo_a = make_undo(&undo_log, "undo-a");
        let undo_b = make_undo(&undo_log, "undo-b");

        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            let undo_b = undo_b.clone();
            async move {
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                ctx.compensate(
                    "undo-b",
                    move |sc: StepCtx| undo_b(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                let _ = ctx
                    .step("boom", move |_sc: StepCtx| async move {
                        Err::<serde_json::Value, _>(anyhow::anyhow!("boom"))
                    })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let opts = || {
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("comp:restart")
        };
        let first = run_workflow(opts()).outcome().await.unwrap();
        assert_eq!(first.status, RunStatus::Errored);
        let after_first = undo_log.lock().unwrap().len();

        // 第二次驱动（同 run_id 同 store）：重放短路已完成的补偿
        let second = run_workflow(opts()).outcome().await.unwrap();
        assert_eq!(second.status, RunStatus::Errored);
        assert_eq!(
            undo_log.lock().unwrap().len(),
            after_first,
            "已完成的补偿跨重启不重复执行"
        );
    }

    #[tokio::test]
    async fn durable_retry_parks_then_succeeds() {
        let store = Arc::new(InMemoryStore::new());
        let attempts_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = Arc::clone(&attempts_seen);
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let seen = Arc::clone(&handler_seen);
            async move {
                let opts = crate::define::StepOptions {
                    retry: Some(RetryPolicy {
                        max_attempts: 2,
                        backoff: Backoff::Fixed { base_ms: 30 },
                        durable: true,
                    }),
                    ..Default::default()
                };
                let _ = ctx
                    .step_with(
                        "flaky",
                        opts,
                        move |sc: StepCtx| {
                            let seen = Arc::clone(&seen);
                            let attempt = sc.attempt;
                            seen.lock().unwrap().push(attempt);
                            async move {
                                if attempt == 1 {
                                    Err::<serde_json::Value, _>(anyhow::anyhow!("flaky"))
                                } else {
                                    Ok(serde_json::json!("ok"))
                                }
                            }
                        },
                    )
                    .await?;
                Ok(serde_json::json!({ "retried": true }))
            }
        });

        // 第一次驱动：attempt 1 失败 → 耐久泊车（run 暂停，等下次重试）
        let first = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("durable:1"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(first.status, RunStatus::Paused, "durable 失败 → 泊车等重试");
        // 等重试 deadline（now + 30ms）过期——泊车语义：到期后由重驱续跑
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        // 日志里应有 STEP_RETRY_SCHEDULED（attempt 1，deadline 已登记）
        let scheduled = store
            .get_events("durable:1").await
            .unwrap()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::StepRetryScheduled { step_id, .. } if step_id == "flaky"))
            .count();
        assert_eq!(scheduled, 1, "STEP_RETRY_SCHEDULED 应持久化");
        // 耐久重试逐 attempt 落 STEP_ATTEMPT_STARTED（对齐上游）：两次驱动
        // 共两次 attempt（第一次失败、第二次成功），应各有一条。
        eprintln!("DEBUG events={:?}", store.get_events("durable:1").await.unwrap().iter().map(|e| (e.type_name().to_string(), e.step_id().map(str::to_string))).collect::<Vec<_>>());

        // 第二次驱动（deadline 已过）：attempt 2 成功 → Finished
        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("durable:1"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        assert_eq!(
            attempts_seen.lock().unwrap().as_slice(),
            &[1, 2],
            "attempts 跨重启携带"
        );

        // STEP_FAILED 未落日志（耐久路径只有 RETRY_SCHEDULED）——重放规则验证
        let failed = store
            .get_events("durable:1").await
            .unwrap()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::StepFailed { step_id, .. } if step_id == "flaky"))
            .count();
        assert_eq!(failed, 0, "耐久路径耗尽前不落 STEP_FAILED");

        // 耐久重试逐 attempt 落 STEP_ATTEMPT_STARTED（对齐上游）：两次驱动
        // 共两次 attempt（第一次失败、第二次成功），应各有一条。
        let attempt_starts = store
            .get_events("durable:1").await
            .unwrap()
            .iter()
            .filter(|e| matches!(
                e,
                WorkflowEvent::StepAttemptStarted { step_id, .. } if step_id == "flaky"
            ))
            .count();
        assert_eq!(
            attempt_starts, 2,
            "每次 attempt 前都应落 STEP_ATTEMPT_STARTED，实际 {attempt_starts}"
        );
    }

    #[tokio::test]
    async fn durable_retry_exhausts_to_errored() {
        let store = Arc::new(InMemoryStore::new());
        let attempts_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = Arc::clone(&attempts_seen);
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let seen = Arc::clone(&handler_seen);
            async move {
                let opts = crate::define::StepOptions {
                    retry: Some(RetryPolicy {
                        max_attempts: 2,
                        backoff: Backoff::Fixed { base_ms: 30 },
                        durable: true,
                    }),
                    ..Default::default()
                };
                let _ = ctx
                    .step_with(
                        "always-fails",
                        opts,
                        move |sc: StepCtx| {
                            let seen = Arc::clone(&seen);
                            let attempt = sc.attempt;
                            seen.lock().unwrap().push(attempt);
                            async move {
                                Err::<serde_json::Value, _>(anyhow::anyhow!("always"))
                            }
                        },
                    )
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let opts = || {
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("durable:ex")
        };
        let first = run_workflow(opts()).outcome().await.unwrap();
        assert_eq!(first.status, RunStatus::Paused, "attempt 1 失败 → 泊车");
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let second = run_workflow(opts()).outcome().await.unwrap();
        assert_eq!(second.status, RunStatus::Errored, "attempt 2 = 最后一次 → 耗尽即终态");
        let run_err = second.error.as_ref().map(|e| e.to_string()).unwrap_or_default();
        assert!(run_err.contains("always"), "原始错误保留：{run_err}");
        assert_eq!(attempts_seen.lock().unwrap().len(), 2, "恰好两次 attempt（跨重启携带）");
    }

    #[tokio::test]
    async fn reserved_compensation_prefix_rejected() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.compensate(
                "__compensation:steal",
                move |_sc: StepCtx| Box::pin(async { Ok(()) }),
                RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
            ).await?;
            Ok(serde_json::Value::Null)
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Errored, "保留前缀登记 → run 失败");
    }

    // ========================================================
    // DAG 恢复 demo（三个已实现原语各一个可读场景）：
    //   ① interimGoals 泊车 + 续驱  ② compensate 逆序撤销
    //   ③ retry.durable 泊车/续跑
    // ========================================================

    /// ② 补偿（undo 轴）：三步各登记撤销，c 抛错 → 逆序撤销 [c, b, a]；
    ///    全部撤销成功 run 仍 errored（携带原始错误）——补偿修复外部世界，
    ///    不修改 run 的失败定性。
    #[tokio::test]
    async fn demo_compensate_reverse_undo() {
        let store = Arc::new(InMemoryStore::new());
        let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
        let undo_a = make_undo(&undo_log, "undo-a");
        let undo_b = make_undo(&undo_log, "undo-b");
        let undo_c = make_undo(&undo_log, "undo-c");

        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            let undo_b = undo_b.clone();
            let undo_c = undo_c.clone();
            async move {
                // 登记先于效果：撤销幂等（容忍"尚无效果"），逆序触发
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                ctx.compensate(
                    "undo-b",
                    move |sc: StepCtx| undo_b(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                ctx.compensate(
                    "undo-c",
                    move |sc: StepCtx| undo_c(sc),
                    RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
                ).await?;
                let _ = ctx
                    .step("a", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                    .await?;
                let _ = ctx
                    .step("b", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                    .await?;
                let _ = ctx
                    .step("c", move |_sc: StepCtx| async move {
                        Err::<serde_json::Value, _>(anyhow::anyhow!("c 失败"))
                    })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Errored, "全部撤销成功仍 errored");
        assert_eq!(
            *undo_log.lock().unwrap(),
            vec!["undo-c", "undo-b", "undo-a"],
            "逆登记序撤销"
        );
    }

    /// ③ 耐久重试：flaky 步骤 attempt 1 失败 → STEP_RETRY_SCHEDULED 泊车；
    ///    重驱 attempt 2 成功 → Finished；attempts [1, 2] 跨驱动携带。
    #[tokio::test]
    async fn demo_durable_retry_park_and_resume() {
        let store = Arc::new(InMemoryStore::new());
        let attempts_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = Arc::clone(&attempts_seen);
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let seen = Arc::clone(&handler_seen);
            async move {
                let opts = crate::define::StepOptions {
                    retry: Some(RetryPolicy {
                        max_attempts: 2,
                        backoff: Backoff::Fixed { base_ms: 30 },
                        durable: true,
                    }),
                    ..Default::default()
                };
                let _ = ctx
                    .step_with(
                        "flaky",
                        opts,
                        move |sc: StepCtx| {
                            let attempt = sc.attempt;
                            seen.lock().unwrap().push(attempt);
                            async move {
                                if attempt == 1 {
                                    Err::<serde_json::Value, _>(anyhow::anyhow!("flaky"))
                                } else {
                                    Ok(serde_json::json!("ok"))
                                }
                            }
                        },
                    )
                    .await?;
                Ok(serde_json::Value::Null)
            }
        });

        let first = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("demo:retry"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(first.status, RunStatus::Paused, "attempt 1 失败 → 泊车");
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("demo:retry"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished, "重驱续跑至成功");
        assert_eq!(
            *attempts_seen.lock().unwrap(),
            vec![1, 2],
            "attempt 序号跨驱动携带"
        );
    }

    // ========================================================
    // reseedRun（redo 侧）——镜像 f2-lite engine.rerun.test.ts 的
    // 两个场景 + 本仓补的源缺失用例
    // ========================================================

    /// a → b → c，每步返回自己的 id，汇成 output；执行计数进 `log`。
    fn abc_wf(log: &Arc<Mutex<TestLog>>) -> WorkflowDefinition {
        WorkflowDefinition::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    let mut out = std::collections::BTreeMap::new();
                    for id in ["a", "b", "c"] {
                        let log = log.clone();
                        let id = id.to_string();
                        let id_c = id.clone();
                        let v = ctx
                            .step(&id, move |_sc: StepCtx| {
                                let (log, id) = (log.clone(), id_c.clone());
                                async move {
                                    log.lock().unwrap().note_start(&id);
                                    log.lock().unwrap().note_finish(&id);
                                    Ok(serde_json::json!(id))
                                }
                            })
                            .await?;
                        out.insert(id, v);
                    }
                    Ok(serde_json::to_value(out).unwrap())
                }
            }
        })
    }

    fn finished_meta(events: &[WorkflowEvent], step_id: &str) -> Option<Option<crate::event::StepMeta>> {
        events.iter().find_map(|e| match e {
            WorkflowEvent::StepFinished { step_id: s, meta, .. } if s == step_id => {
                Some(meta.clone())
            }
            _ => None,
        })
    }

    /// a→b→c→d 链；`fail_at` 指定的 step 抛错。
    fn chain_wf(log: &Arc<Mutex<TestLog>>, fail_at: Option<&'static str>) -> WorkflowDefinition {
        WorkflowDefinition::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    for id in ["a", "b", "c", "d"] {
                        let log = log.clone();
                        let id_s = id.to_string();
                        let fail = fail_at == Some(id);
                        ctx.step(&id_s, move |_sc: StepCtx| {
                            let (log, id) = (log.clone(), id.to_string());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                if fail {
                                    return Err::<serde_json::Value, _>(anyhow::anyhow!("{id} 失败"));
                                }
                                Ok(serde_json::json!(id))
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        })
    }

    /// 场景 2（基线）：errored 的 run 直接重驱 = 失败 checkpoint rethrow——
    /// a/b 短路、c 闭包**不重入**，run 再次 errored。失败是终态，就地重试
    /// 不存在（重试在 step 内 / redo / 新 run）。
    #[tokio::test]
    async fn errored_run_redrive_rethrows_without_reclosures() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));

        let first = run_workflow(
            RunWorkflowOptions::new(chain_wf(&log, Some("c")), store.clone())
                .input(serde_json::json!({}))
                .run_id("err:redrive"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(first.status, RunStatus::Errored);
        assert_eq!(log.lock().unwrap().runs["c"], 1);

        let second = run_workflow(
            RunWorkflowOptions::new(chain_wf(&log, Some("c")), store.clone())
                .input(serde_json::json!({}))
                .run_id("err:redrive"),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Errored, "失败 checkpoint rethrow");
        let l = log.lock().unwrap();
        assert_eq!(l.runs["c"], 1, "c 闭包不重入");
        assert_eq!(l.runs["a"], 1, "a 短路");
        assert_eq!(l.runs.get("d").copied().unwrap_or(0), 0, "下游不推进");
    }
}
