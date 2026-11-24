//! redo（reseed run）的中间件版——**对齐 TS `server` 形状**。
//!
//! 语义与 TS 版（`@wf-demo/workflow-middlewares` 的 `reseed`）**等价**：
//! 不在 `redoSet` 里的 step **不执行闭包**，直接用源 run 的结果提交
//! `STEP_FINISHED`。
//!
//! # 与已删除的引擎参数 `reseed_run` 的差别
//!
//! | | 引擎 `reseed_run` | 本中间件 |
//! | --- | --- | --- |
//! | 结果复用 | ✅ | ✅ |
//! | 重放能短路 | ✅ | ✅（照常落 checkpoint） |
//! | `meta.importedFrom` 溯源 | ✅ | ❌（可从 plan + 源日志推算） |
//! | 读源时机 | 首次 drive（`startRun` 里） | 首次 drive（首个真实 step 入口） |
//! | 源在首驱前没了 | 导入 0 条 → 真跑 | 查不到 → 真跑（**相同**） |
//!
//! # 计划走 `RunWorkflowOptions.meta`
//!
//! 中间件是**提前配置**的（挂一次、跑很多次），而「从哪个源 run 播种、重跑哪些
//! step」每次 run 都可能不同。所以 plan 走 `ctx.meta` 的 [`ReseedKey`]，
//! 构造时不接收——与 `interim_goals` 同一条规则（见 `lib.rs` 的模块文档）。
//!
//! # 为什么是 step 包装层
//!
//! 要对「即将执行的 step」在**执行之前**介入；TS 版靠覆盖 `ctx.step` 表达
//! 同一件事，Rust 版对齐为可叠加的
//! [`MiddlewareServerFnArgs::wrap_step`]——装一个 tower `Layer`，
//! 用 [`StepCall::skip`] 把源结果作为本 step 的 checkpoint 落盘。
//!
//! # 读源日志
//!
//! 引擎自带 store：`args.get_events(&plan.source_run_id)` 直接读源 run 的
//! 事件日志，不需要构造期注入读闭包。事件 → 结果的转换见
//! [`step_results_from_events`]（与 [`crate::dag_policies`] 共用一份）。
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use aa_workflow_core::middleware::{MiddlewareServer, MiddlewareServerFnArgs, step_layer, Decision};

/// 一次 redo 的计划：从哪里播种、哪些 step 重跑。
///
/// 作为 [`RunWorkflowOptions.meta`](aa_workflow_core::engine::RunWorkflowOptions::meta)
/// 里的 [`ReseedKey`] 传入——**不是**中间件的构造参数。理由同
/// `interim_goals`：中间件是提前配置的（挂一次、跑很多次），而「从哪个源 run
/// 播种、重跑哪些 step」每次 run 都可能不同。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReseedPlan {
    /// 源 run id。
    pub source_run_id: String,
    /// 强制重跑的 stepId 名单（**不**复用它们的旧结果）。
    pub redo_set: Vec<String>,
}

/// `ctx.meta` 里的 redo 计划。
///
/// 构造：`MetaBag::new().with::<ReseedKey>(ReseedPlan { .. })`。
pub struct ReseedKey;

impl aa_workflow_core::define::MetaKey for ReseedKey {
    const KEY: &'static str = "__reseedPlan";
    type Value = ReseedPlan;
}

aa_workflow_core::impl_meta_accessors!(ReseedKey => ReseedAccess, reseed_plan, try_reseed_plan);

/// 从源 run 读「step_id → (结果, 源日志下标)」的读日志闭包。
///
/// 内置 [`reseed`] 已改走 [`MiddlewareServerFnArgs::get_events`] +
/// [`step_results_from_events`]；这个别名留给**自写中间件**做构造期注入
/// （仍是合法写法，与 [`crate::GetEvents`] 同款逃生门），输出形状应与
/// [`step_results_from_events`] 一致。
pub type LoadResults = dyn for<'a> Fn(&'a str) -> aa_workflow_core::define::BoxFuture<'a, anyhow::Result<HashMap<String, SourceStep>>>
    + Send
    + Sync;

/// 源 run 里一个 step 的结果与出处。
#[derive(Clone, Debug)]
pub struct SourceStep {
    /// step 的结果。
    pub result: serde_json::Value,
    /// 该 `STEP_FINISHED` 在源 run 日志里的下标。
    pub index: usize,
}

/// 把源 run 的事件日志收成「step_id → 结果 + 源下标」。
///
/// 只取 `STEP_FINISHED`；`redo_set` 过滤由调用方负责。
///
/// `result: None` 记为 `Value::Null`——有些工作流（如 LocalDub）刻意让节点
/// 返回 Null、数据走盘上文件，复用只表示「这步跑过了、不入边数据」，消费方
/// 不读 deps 即可。
///
/// 内置的 [`reseed`] 与 [`crate::dag_policies`] 共用这一份，避免两处解析漂移。
pub fn step_results_from_events(
    events: &[aa_workflow_core::event::WorkflowEvent],
) -> HashMap<String, SourceStep> {
    let mut m = HashMap::with_capacity(events.len());
    for (index, e) in events.iter().enumerate() {
        if let aa_workflow_core::event::WorkflowEvent::StepFinished {
            step_id, result, ..
        } = e
        {
            m.insert(
                step_id.clone(),
                SourceStep {
                    result: result.clone().unwrap_or(Value::Null),
                    index,
                },
            );
        }
    }
    m
}

/// 按 `ctx.meta` 里的 [`ReseedKey`] 复用源 run 的 step 结果。
///
/// 计划本身**不在这里给**——它属于 run 级参数，调用方每次 drive 通过
/// `RunWorkflowOptions.meta` 传：
///
/// ```ignore
/// .middleware(reseed())            // 挂一次
/// RunWorkflowOptions::new(wf, store)
///     .meta(MetaBag::new().with::<ReseedKey>(ReseedPlan {
///         source_run_id: "rev1".into(),
///         redo_set: vec!["translate".into()],
///     }))
/// ```
///
/// 没传 `ReseedKey` 时不装层（= 本次 run 不做播种，全部真跑）。
pub fn reseed() -> ReseedMw {
    ReseedMw
}

/// struct 形态（tower `Service` 同款）：无构造期状态，`call<'a>(&'a self)`
/// 的 future 借 `&self`——零捕获仪式、零 per-call 克隆。
#[derive(Clone)]
pub struct ReseedMw;

impl MiddlewareServer for ReseedMw {
    async fn call(&self, mut args: MiddlewareServerFnArgs) -> Result<Value> {

        // 计划只在 meta 里；缺失 = 本次 run 不做播种。
        let Some(plan) = args.ctx.meta.try_get::<ReseedKey>() else {
            return args.next(None).await;
        };
        let redo: std::collections::HashSet<String> = plan.redo_set.iter().cloned().collect();

        // 本 run 就是源 run？不做任何事（禁止自我播种）。
        if plan.source_run_id != args.ctx.run_id {
            // **drive 开始时加载一次**，而不是在包装层里懒加载 + 缓存：
            // 省掉可变状态后层就是纯的（只读 plan）。
            //
            // 源 run 不存在 → store 返回空日志 → 空表 → 全部真跑
            // （与引擎版「导入 0 条」同义）；**读失败**则传播错误，与
            // `dag_policies` 一致，不再静默降级。
            let events = args.get_events(&plan.source_run_id).await?;
            let lookup = Arc::new(step_results_from_events(&events));

            args.wrap_step(step_layer({
                let redo = redo.clone();
                let lookup = lookup.clone();
                let source_run_id = plan.source_run_id.clone();
                move |req: &aa_workflow_core::middleware::StepRequest| {
                    // redoSet 里的 step 要真跑。
                    if redo.contains(&req.step_id) {
                        return Decision::Pass;
                    }
                    match lookup.get(&req.step_id).cloned() {
                        // 短路：源结果作为本 step 的 checkpoint 落盘，**带溯源**。
                        Some(src) => Decision::SkipImported {
                            result: src.result,
                            from: aa_workflow_core::event::ImportedFrom {
                                run_id: source_run_id.clone(),
                                index: src.index,
                            },
                        },
                        // 源里没有这个 step（新加的 / 上次失败的）→ 真跑。
                        None => Decision::Pass,
                    }
                }
            }));
        }

        args.next(None).await
    }
}

