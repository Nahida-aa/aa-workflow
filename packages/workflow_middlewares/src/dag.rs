//! DAG 策略中间件——**Rust 版**：redo（复用）+ 临时目标（泊车），二合一。
//!
//! # 与 `interim_goals` / `reseed` 的分工
//!
//! `interim_goals` 只认 `meta.__interimGoals`，`reseed` 的计划在**构造时**固定。
//! 本中间件是它们的「读图版」：每次 drive 从 `meta.__dagPlan` 取策略，并用
//! [`DagShape`] 把 `from` 闭包展开成重跑名单（缺省种子计算也要图）。
//!
//! 单独用时：[`interim_goals`](crate::interim_goals) /
//! [`reseed`](crate::reseed) 更直接；要「一次给了 from + goals」就用这个。
//!
//! # 行为（读 `RunWorkflowOptions.meta.__dagPlan`）
//!
//! ```json
//! { "__dagPlan": {
//!     "source_run_id": "rev1",   // redo：播种来源
//!     "from": ["translate"],     // redo：失效起点（闭包展开成重跑名单）
//!     "redo_set": ["x"],         // redo：裸名单（与 from 展开取并集）
//!     "goals": ["merge"]         // 临时目标：全 checkpoint 后泊车
//! } }
//! ```
//!
//! - **redo**：不在重跑名单里的 step，拦截器返回 `Skip` 直接提交源结果
//!   （业务闭包不执行）；引擎照常落 `STEP_FINISHED`——重放可短路。
//!   闭包展开由 [`DagShape::reseed_plan`] 完成（库有图）。
//! - **临时目标**：在 step **入口**判定——上一个 step 完成后 goals 若已全齐，
//!   就泊车（后续 step 不跑）。被复用的 step 也算达成（`Skip` 分支同时记入）。
//! - 都未配置 → 中间件不产生任何行为。
//!
//! # 转移语义（无需显式转移检查）
//!
//! 拦截器只在**真实执行**的 step 入口被调用——重放短路的 step 不会到达它。
//! 所以续驱时已完成的 step 不会被重复计入 → 不会重复泊车。`parked` 仅作防御。
//!
//! # 需要 store
//!
//! `reseed_plan` 要读源 run 日志——构造时注入 `get_events`。
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aa_workflow_core::middleware::{
    Middleware, MiddlewareServerFnArgs, StepRequest,
};
use aa_workflow_dag::DagShape;

use crate::step_layer::{step_layer, Decision};

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// `RunWorkflowOptions.meta` 里的计划信封字段。
pub const DAG_PLAN_FIELD: &str = "__dagPlan";

/// 读某个 run 的日志（引擎事件数组）。
pub type GetEvents =
    dyn Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>> + Send + Sync;

/// drive 开始时从 meta 解析出的计划（**不可变**，整条 drive 内只读）。
///
/// 「哪些 step 已完成」不在
/// 这里——那是引擎给的事实，见 `StepCall::completed`。
#[derive(Default)]
struct PlanState {
    /// 临时目标（`goals`）。
    goals: Vec<String>,
    /// redo 复用表：step_id → 源结果。空 = 无 redo。
    lookup: HashMap<String, serde_json::Value>,
}

/// DAG 策略中间件：redo 复用 + 临时目标泊车，读 `meta.__dagPlan`。
///
/// `shape` 提供图（`from` 闭包展开、缺省种子计算）；`get_events` 提供日志读取。
pub fn dag_policies(shape: Arc<DagShape>, get_events: Arc<GetEvents>) -> Middleware {
    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let get_events = get_events.clone();
        let shape = shape.clone();

        Box::pin(async move {
            // 前置：解析 RunWorkflowOptions.meta.__dagPlan → PlanState。
            let plan = sc
                .ctx
                .meta
                .as_value()
                .get(DAG_PLAN_FIELD)
                .and_then(|v| v.as_object())
                .cloned();

            let mut st = PlanState::default();
            if let Some(obj) = plan {
                if let Some(goals) = obj.get("goals").and_then(|v| v.as_array()) {
                    st.goals = goals
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect();
                }

                // redo：source_run_id 存在才启用；from 闭包展开交给
                // DagShape::reseed_plan（库有图，含缺省种子计算）。
                if let Some(source_run_id) = obj.get("source_run_id").and_then(|v| v.as_str()) {
                    let source_log = get_events(source_run_id)?;
                    let from = obj.get("from").and_then(|v| v.as_array()).map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect::<Vec<_>>()
                    });
                    let reseed_run = shape.reseed_plan(source_run_id, &source_log, from)?;
                    let mut redo: HashSet<String> =
                        reseed_run.redo_set.iter().cloned().collect();
                    if let Some(extra) = obj.get("redo_set").and_then(|v| v.as_array()) {
                        for v in extra {
                            if let Some(id) = v.as_str() {
                                redo.insert(id.to_string());
                            }
                        }
                    }

                    // 源结果 → lookup（排除重跑名单）。
                    for e in &source_log {
                        if let aa_workflow_core::event::WorkflowEvent::StepFinished {
                            step_id,
                            result: Some(result),
                            ..
                        } = e
                        {
                            if !redo.contains(step_id) {
                                st.lookup.insert(step_id.clone(), result.clone());
                            }
                        }
                    }
                }
            }

            // 无任何策略 → 不装拦截器，纯透传。
            if st.goals.is_empty() && st.lookup.is_empty() {
                return sc.next(None).await;
            }

            // 计划是不变的，直接按值捕获；无锁、无状态。
            let plan = Arc::new(st);

            sc.wrap_step(step_layer(move |req: &StepRequest| {
                // ① 临时目标：入口判定——此前所有 step 完成后 goals 已全齐
                //    则泊车。当前 step 尚未执行，所以 `completed` 不含它。
                if !plan.goals.is_empty()
                    && plan.goals.iter().all(|g| req.completed.contains(g))
                {
                    // 泊车会整条 drive 结束，不可能在同一 drive 内触发第二次
                    // （不需要额外的 `parked` 标记）。
                    return Decision::Park {
                        signal: INTERIM_GOAL_SIGNAL.to_string(),
                        meta: Some(serde_json::json!({ "goals": plan.goals })),
                    };
                }

                // ② redo：有源结果 → 复用（`skip` 会落 STEP_FINISHED，
                //    所以它同样进入下一次调用的 `completed`）。
                if let Some(result) = plan.lookup.get(&req.step_id).cloned() {
                    return Decision::Skip(result);
                }

                // ③ redoSet 里的 step（或源里没有的）真跑。
                Decision::Pass
            }));

            sc.next(None).await
        })
    })
}
