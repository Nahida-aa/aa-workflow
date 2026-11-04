//! 临时目标的中间件版——**对齐 TS `server` 形状**。
//!
//! TS 版包装 `ctx.step`（它是可覆盖的函数属性）来「每个 step 完成后」检查目标；
//! Rust 对齐为 [`MiddlewareServerFnArgs::push_step_wrapper`]——可叠加的包装层，
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
//! # 需要 store
//!
//! 判断「开局目标是否已达成」要读日志，构造时注入 `get_events`。
use std::collections::HashSet;
use std::sync::Arc;

use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs, StepCall, StepWrapper};

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// `RunWorkflowOptions.meta` 里的目标字段。
pub const INTERIM_GOALS_FIELD: &str = "__interimGoals";

/// 读某个 run 的日志（引擎事件数组）。
pub type GetEvents =
    dyn Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>> + Send + Sync;

/// 目标全部 checkpoint 后在下一个 step 入口泊车（后续 step 不执行）。
///
/// `goals` 由调用方在构造时给出；若 `meta[INTERIM_GOALS_FIELD]` 存在则**以
/// meta 为准**（每次 drive 可换，对齐 TS 的 per-drive 参数）。
pub fn interim_goals<F>(goals: Vec<String>, get_events: F) -> Middleware
where
    F: Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>>
        + Send
        + Sync
        + 'static,
{
    let base_goals = Arc::new(goals);
    let get_events: Arc<GetEvents> = Arc::new(get_events);

    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let base_goals = base_goals.clone();
        let get_events = get_events.clone();

        Box::pin(async move {
            // meta 优先（每次 drive 可换目标）；否则用构造时给的。
            let goals: Vec<String> = sc
                .ctx
                .meta
                .as_ref()
                .and_then(|m| m.get(INTERIM_GOALS_FIELD))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_else(|| (*base_goals).clone());

            if !goals.is_empty() {
                // 开局已完成的目标（续驱 / reseed 导入）——不该为它们泊车。
                let finished_at_start: HashSet<String> = get_events(&sc.ctx.run_id)?
                    .iter()
                    .filter_map(|e| match e {
                        aa_workflow_core::event::WorkflowEvent::StepFinished { step_id, .. } => {
                            Some(step_id.clone())
                        }
                        _ => None,
                    })
                    .collect();
                let goals_done_at_start = goals.iter().all(|g| finished_at_start.contains(g));

                // **无状态包装层**：判定全部读 `call.completed`（引擎给的
                // 「本 drive 已成功 step」快照），不再自带 `done` 累积集合。
                //
                // 为什么可以不累积：进入某个 step 时，此前所有 step 的
                // `STEP_FINISHED` 都已落盘（在它们自己的链返回前），引擎在
                // 构造 `StepCall` 时取的快照就是准确的。
                //
                // 为什么不用 `parked` 标记：泊车会整条 drive 结束
                // （`WorkflowParked`），不可能在同一 drive 内触发第二次。
                let wrapper: StepWrapper = Arc::new(move |call: StepCall| {
                    let goals = goals.clone();
                    Box::pin(async move {
                        if goals_done_at_start {
                            return call.next().await;
                        }
                        // 入口判定：goals 是否已全齐（不含本 step——它还没跑）。
                        let reached = goals.iter().all(|g| call.completed.contains(g));
                        if reached {
                            return call
                                .park(
                                    INTERIM_GOAL_SIGNAL,
                                    Some(serde_json::json!({ INTERIM_GOALS_FIELD: goals })),
                                )
                                .await;
                        }
                        call.next().await
                    })
                });
                sc.push_step_wrapper(wrapper);
            }

            sc.next(None).await
        })
    })
}
