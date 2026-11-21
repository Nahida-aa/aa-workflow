//! `aa_workflow_middlewares` —— **内置中间件清单**。
//!
//! 定位与 TS 侧 `@wf-demo/workflow-middlewares` 一致：这不是「中间件机制」的
//! 实现（机制在 [`aa_workflow_core::middleware`]），而是在它之上提供几个
//! **现成的策略中间件**。
//!
//! # 能力边界（与 TS 的差异只剩两处，均已论证）
//!
//! 中间件形状已经**与 TS 对齐**：一个 `server` 函数，最内层是 handler，外层
//! 靠 `next()` 推进（洋葱）。TS 的 `server` 能做三件事，Rust 一一对应：
//!
//! | TS `server` 里做的事 | Rust 对应 |
//! | --- | --- |
//! | `next({ context: {...} })` 扩展 ctx | [`MiddlewareServerFnArgs::next`]`(ext)`（一次调用，同构） |
//! | 在 `next` 前后加行为（计时 / 日志） | `server` 闭包里 `args.next(..).await` 前后 |
//! | `ctx.step = wrapped` 拦截 step | [`MiddlewareServerFnArgs::wrap_step`]（tower `Layer`，可叠加） |
//!
//! 剩下的差异是**语言必然**，不是设计取舍：
//!
//! 1. `TCtxExt` 的类型交集：TS 编译期 `UnionToIntersection`，Rust 运行期
//!    serde 反序列化（同 axum extensions）。
//! 2. `ctx.step` 是方法不是字段：擦除下移到包装层（`engine` 上的属性），
//!    `T` 的推导留在方法签名上。所以「拦截」是 `wrap_step` +
//!    不调 `next()`，**不是**赋值覆盖。
//!
//! Rust 侧还**多**一项能力：包装层能 `park`（在 step 边界停下整条 drive），见
//! `docs/decisions/0016-step-onion.md`。
//!
//! DAG 调度走 [`dag_to_handler`](crate::dag_to_handler)（handler 工厂），不是
//! 中间件：handler 天然最内层，所有策略的 `wrap_step` 都已注册；TS 侧同构
//! （`dagToHandler`）。见 `docs/decisions/0019`。
//!
//! # 使用者不用写 `<Ext>`（内置中间件固定 `Middleware<(), ()>`）
//!
//! 内置中间件都是**不写扩展**的策略（只读 `meta` / `input`），所以它们的构造函数
//! 返回统一的 `Middleware` 而不是泛型未知的 `Middleware`。好处是调用点
//! **完全不用写类型参数**：
//!
//! ```rust,ignore
//! // 中间件：挂一次，不含 run 级参数
//! .middleware(interim_goals(get_events))
//! .middleware(reseed(load_results))
//! .middleware(dag_policies(shape, get_events))   // shape 是静态图，不是 run 级参数
//!
//! // run 级参数：每次 drive 各自给
//! RunWorkflowOptions::new(wf, store)
//!     .meta(MetaBag::new()
//!         .with::<InterimGoalsKey>(vec!["b".into()])
//!         .with::<ReseedKey>(ReseedPlan { .. }))
//! ```
//!
//! DAG 调度不用中间件，直接用 [`dag_to_handler`] 作 handler：
//!
//! ```rust,ignore
//! create_workflow(config)
//!     .middleware(dag_policies(shape, get_events))  // 策略照旧挂
//!     .handler(dag_to_handler(spec));                  // handler 天然最内层
//! ```
//!
//! **给中间件作者的建议**：如果你的中间件不写 `ctx.ext`，函数返回值就写成
//! `Middleware<(), ()>`，使用者就能省掉 `.middleware::<Ext>(..)`。
//! 只有「确实要写扩展」的中间件才需要使用者手写 `<Ext>`——`Ext` 是 handler 侧
//! `ctx.ext` 的类型，Rust 没有 TS 的 `UnionToIntersection`（见 `docs/decisions/0013`）。
//!
//! # 策略参数走 `RunWorkflowOptions.meta`
//!
//! 三个中间件都从 [`ctx.meta`](aa_workflow_core::define::BaseCtx) 读策略参数
//! （`__interimGoals` / `__reseedPlan` / `__dagPlan`），而不是往引擎加字段——
//! 引擎上曾经有过 `interim_goals` / `reseed_run` 两个策略参数，已于 2026-11-06
//! 删除（`docs/decisions/0001`、`0018`），本包就是它们的归宿。
//!
//! **中间件的构造参数只放静态配置**（`get_events` / `load_results` / `shape`），
//! 不放「每次 run 可能不同」的东西——那些都走 `ctx.meta` 的类型化 key
//! （`InterimGoalsKey` / `ReseedKey` / `DagPlan`）。
mod dag;
mod interim_goals;
mod reseed;

// DAG 代码集中在 dag 模块，在此 re-export 让调用方路径简洁。
pub use dag::core::{
    DagNode, DagRunFn, DagShape, DagSpec, RedoPlan, ShapeError, dag_to_handler, define_dag,
    run_dag,
};
pub use dag::policies::{
    DagPoliciesMw, DagPlan, DagPlanAccess, DagPlanKey, GetEvents, INTERIM_GOAL_SIGNAL,
    dag_policies,
};
pub use interim_goals::{InterimGoalsAccess, InterimGoalsKey, InterimGoalsMw, interim_goals};
pub use reseed::{LoadResults, ReseedAccess, ReseedKey, ReseedPlan, ReseedMw, SourceStep, reseed};

/// 供文档链接用：把常用名字提上来，避免调用方再导一遍 core。
///
/// ```ignore
/// use aa_workflow_middlewares::prelude::*;
/// ```
pub mod prelude {
    pub use aa_workflow_core::middleware::{
        Middleware, MiddlewareServerFn, MiddlewareServerFnArgs, StepRequest,
    };
}
