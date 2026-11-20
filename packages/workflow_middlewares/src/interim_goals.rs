//! 临时目标的中间件版——**对齐 TS `server` 形状**。
//!
//! TS 版包装 `ctx.step`（它是可覆盖的函数属性）来「每个 step 完成后」检查目标；
//! Rust 对齐为 [`MiddlewareServerFnArgs::wrap_step`]——可叠加的 tower `Layer`，
//! 不调 `next()` 即短路，用 [`StepCall::park`] 在边界泊车。
//!
//! # 行为
//!
//! 目标全部 checkpoint 后，在**目标之后的 step 入口**泊车，后面的 step 不跑。
//!
//! 注意时机：拦截器在 step **入口**调用，所以「goals 全部达成」是在
//! `goals` 里最后一个 step 完成**之后**、下一个 step 入口处判定并泊车。
//!
//! # 转移检查
//!
//! 拦截器只在**真实执行**的 step 入口被调用——重放短路的 step 不会到达它。
//! 所以续驱时已完成的 step 不会被重复计入 → 不会重复泊车。
//! 开局即已达成（续驱 / reseed 导入）的情况由 `goals_done_at_start` 显式挡掉。
//!
//! # 参数只从 `RunWorkflowOptions.meta` 来
//!
//! 中间件是**提前配置**的（挂一次、跑很多次），目标却是**每次 drive 可能不同**的。
//! 所以目标走 `ctx.meta` 的 [`InterimGoalsKey`]，构造时不接收——
//! 与 `ctx.ext`（中间件运行期写）/ `ctx.meta`（调用方每次 run 写）的分工一致
//! （见 `define/meta.rs` 的模块文档）。
//!
//! # 需要 store
//!
//! 判断「开局目标是否已达成」要读日志，构造时注入 `get_events`。
use anyhow::Result;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use aa_workflow_core::define::{BoxFuture, MetaKey};
use aa_workflow_core::middleware::{
    Middleware, MiddlewareServer, MiddlewareServerFnArgs, StepRequest, step_layer, Decision,
};

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// `RunWorkflowOptions.meta` 里的目标列表。
///
/// 构造：`MetaBag::new().with::<InterimGoalsKey>(vec!["b".into()])`。
pub struct InterimGoalsKey;

impl MetaKey for InterimGoalsKey {
    /// 与既有调用点（及 TS 侧）保持的键名，故保留旧值。
    const KEY: &'static str = "__interimGoals";
    type Value = Vec<String>;
}

aa_workflow_core::impl_meta_accessors!(
    InterimGoalsKey => InterimGoalsAccess,
    interim_goals,
    try_interim_goals
);

/// 读某个 run 的日志（引擎事件数组）。
pub type GetEvents =
    dyn Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>> + Send + Sync;

/// 目标全部 checkpoint 后在下一个 step 入口泊车（后续 step 不执行）。
///
/// 目标本身**不在这里给**——它属于 run 级参数，调用方每次 drive 通过
/// `RunWorkflowOptions.meta` 传：
///
/// ```ignore
/// .middleware([interim_goals(get_events)])          // 挂一次
/// RunWorkflowOptions::new(wf, store)
///     .meta(MetaBag::new().with::<InterimGoalsKey>(vec!["b".into()]))   // 每次可不同
/// ```
///
/// 没传 `InterimGoalsKey` 时中间件不装层（等价于「本次 drive 不设临时目标」），
/// 而不是泊在空目标上。
pub fn interim_goals<F>(get_events: F) -> Middleware
where
    F: Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>>
        + Send
        + Sync
        + 'static,
{
    Middleware::from_impl(InterimGoalsMw {
        get_events: Arc::new(get_events),
    })
}

/// struct 形态（tower `Service` 同款）：参数是**字段**，`serve<'a>(&'a self)`
/// 的 future 借 `&self`——零捕获仪式、零 per-call 克隆。
struct InterimGoalsMw {
    get_events: Arc<GetEvents>,
}

impl MiddlewareServer for InterimGoalsMw {
    fn serve<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>> {
        Box::pin(async move {
            let mut sc = args;
            // 目标只在 meta 里；缺失 = 本次 drive 不设临时目标。
            let goals: Vec<String> = match sc.ctx.meta.try_get::<InterimGoalsKey>() {
                Some(g) if !g.is_empty() => g,
                _ => return sc.next(None).await,
            };

            // 开局已完成的目标（续驱 / reseed 导入）——不该为它们泊车。
            // `&self.get_events` 直接借字段——server_state/闭包形态里这曾是
            // 「async move 前 clone 一行」的仪式位。
            let finished_at_start: HashSet<String> = (self.get_events)(&sc.ctx.run_id)?
                .iter()
                .filter_map(|e| match e {
                    aa_workflow_core::event::WorkflowEvent::StepFinished { step_id, .. } => {
                        Some(step_id.clone())
                    }
                    _ => None,
                })
                .collect();
            let goals_done_at_start = goals.iter().all(|g| finished_at_start.contains(g));

            // **无状态层**：判定全部读 `req.completed`（引擎给的
            // 「本 drive 已成功 step」快照），不自己累积。
            //
            // 为什么可以不累积：进入某个 step 时，此前所有 step 的
            // `STEP_FINISHED` 都已落盘（在它们自己的链返回前），引擎在
            // 构造请求时取的快照就是准确的。
            //
            // 为什么不用 `parked` 标记：泊车会整条 drive 结束
            // （`WorkflowParked`），不可能在同一 drive 内触发第二次。
            sc.wrap_step(step_layer(move |req: &StepRequest| {
                if goals_done_at_start {
                    return Decision::Pass;
                }
                // 入口判定：goals 是否已全齐（不含本 step——它还没跑）。
                if goals.iter().all(|g| req.completed.contains(g)) {
                    return Decision::Park {
                        signal: INTERIM_GOAL_SIGNAL.to_string(),
                        meta: Some(serde_json::json!({ InterimGoalsKey::KEY: goals })),
                    };
                }
                Decision::Pass
            }));

            sc.next(None).await
        })
    }
}
