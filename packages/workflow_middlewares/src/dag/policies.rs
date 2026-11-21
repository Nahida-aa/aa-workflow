//! DAG 策略中间件——**Rust 版**：redo（复用）+ 临时目标（泊车），二合一。
//!
//! # 与 `interim_goals` / `reseed` 的分工
//!
//! 三者都从 `ctx.meta` 读 run 级策略（[`DagPlanKey`] /
//! [`InterimGoalsKey`](crate::InterimGoalsKey) / [`ReseedKey`](crate::ReseedKey)）。
//! 本中间件是它们的「读图版」：用 [`DagShape`] 把 `from` 闭包展开成重跑名单
//! （缺省种子计算也要图）。
//!
//! 单独用时：[`interim_goals`](crate::interim_goals) /
//! [`reseed`](crate::reseed) 更直接；要「一次给了 from + goals」就用这个。
//!
//! # 行为（读 `RunWorkflowOptions.meta` 里的 [`DagPlanKey`]）
//!
//! ```ignore
//! MetaBag::new().with::<DagPlanKey>(DagPlan {
//!     source_run_id: Some("rev1".into()),  // redo：播种来源
//!     from: Some(vec!["translate".into()]),// redo：失效起点（闭包展开成重跑名单）
//!     redo_set: Some(vec!["x".into()]),    // redo：裸名单（与 from 展开取并集）
//!     goals: vec!["merge".into()],         // 临时目标：全 checkpoint 后泊车
//! })
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
use anyhow::Result;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aa_workflow_core::define::BoxFuture;
use aa_workflow_core::middleware::{
    Middleware, MiddlewareServer, MiddlewareServerFnArgs, StepRequest, step_layer, Decision,
};
use crate::dag::core::DagShape;

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// `RunWorkflowOptions.meta` 里的 redo + 临时目标计划。
///
/// 与 [`InterimGoalsKey`](crate::InterimGoalsKey) / [`ReseedKey`](crate::ReseedKey)
/// 同构：作为 `MetaBag::new().with::<DagPlanKey>(..)` 传入，**不是**中间件的构造参数。
///
/// 字段全 `Option`（`goals` 用 `Vec` + serde default）：四个维度可以只给其中几个。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DagPlan {
    /// redo：播种来源 run。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    /// redo：失效起点（[crate::dag] 用 shape 闭包展开成重跑名单）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<Vec<String>>,
    /// redo：裸重跑名单（与 `from` 展开取并集）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redo_set: Option<Vec<String>>,
    /// 临时目标：全 checkpoint 后泊车。
    #[serde(default)]
    pub goals: Vec<String>,
}

/// `ctx.meta` 里的 DAG 策略 key。
pub struct DagPlanKey;

impl aa_workflow_core::define::MetaKey for DagPlanKey {
    /// 与既有调用点（及 TS 侧）保持的键名，故保留旧值。
    const KEY: &'static str = "__dagPlan";
    type Value = DagPlan;
}

aa_workflow_core::impl_meta_accessors!(DagPlanKey => DagPlanAccess, dag_plan, try_dag_plan);


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
    Middleware::from_impl(DagPoliciesMw { shape, get_events })
}

/// struct 形态（tower `Service` 同款）：两个构造期参数都是**字段**，
/// `serve<'a>(&'a self)` 的 future 借 `&self`——零捕获仪式、零 per-call 克隆。
#[derive(Clone)]
struct DagPoliciesMw {
    shape: Arc<DagShape>,
    get_events: Arc<GetEvents>,
}

impl MiddlewareServer for DagPoliciesMw {
    async fn serve(&self, args: MiddlewareServerFnArgs) -> Result<Value> {
        let mut sc = args;

        // 前置：解析 RunWorkflowOptions.meta 里的 DagPlan → PlanState。
        // 缺失 = 本次 run 不做任何策略（与另两个中间件一致）。
        let Some(plan) = sc.ctx.meta.try_get::<DagPlanKey>() else {
            return sc.next(None).await;
        };

        let mut st = PlanState {
            goals: plan.goals.clone(),
            ..Default::default()
        };

        {
            // redo：source_run_id 存在才启用；from 闭包展开交给
            // DagShape::reseed_plan（库有图，含缺省种子计算）。
            if let Some(source_run_id) = plan.source_run_id.as_deref() {
                let source_log = (self.get_events)(source_run_id)?;
                let from = plan.from.clone();
                let reseed_run = self.shape.reseed_plan(&source_log, from)?;
                let mut redo: HashSet<String> = reseed_run.redo_set.iter().cloned().collect();
                if let Some(extra) = &plan.redo_set {
                    for id in extra {
                        redo.insert(id.clone());
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
            if !plan.goals.is_empty() && plan.goals.iter().all(|g| req.completed.contains(g)) {
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
    }
}
