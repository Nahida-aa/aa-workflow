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

/// 从源 run 读「step_id → 结果」。
///
/// 返回的 map 应只包含该 run 日志里 `STEP_FINISHED` 的结果；`redo_set` 过滤由本
/// 中间件负责（保持读函数简单）。
/// 从源 run 读「step_id → (结果, 该事件在源日志里的下标)」。
///
/// 下标用于写 `STEP_FINISHED.meta.imported_from.index`——与引擎版
/// `reseed_run` 的记录方式一致，丢了就只能事后重算。
pub type LoadResults =
    dyn Fn(&str) -> anyhow::Result<HashMap<String, SourceStep>> + Send + Sync;

/// 源 run 里一个 step 的结果与出处。
#[derive(Clone, Debug)]
pub struct SourceStep {
    /// step 的结果。
    pub result: serde_json::Value,
    /// 该 `STEP_FINISHED` 在源 run 日志里的下标。
    pub index: usize,
}

/// 按 `ctx.meta` 里的 [`ReseedKey`] 复用源 run 的 step 结果。
///
/// 计划本身**不在这里给**——它属于 run 级参数，调用方每次 drive 通过
/// `RunWorkflowOptions.meta` 传：
///
/// ```ignore
/// .middleware(reseed(load_results))            // 挂一次
/// RunWorkflowOptions::new(wf, store)
///     .meta(MetaBag::new().with::<ReseedKey>(ReseedPlan {
///         source_run_id: "rev1".into(),
///         redo_set: vec!["translate".into()],
///     }))
/// ```
///
/// 没传 `ReseedKey` 时不装层（= 本次 run 不做播种，全部真跑）。
pub fn reseed<F>(load_results: F) -> ReseedMw
where
    F: Fn(&str) -> anyhow::Result<HashMap<String, SourceStep>> + Send + Sync + 'static,
{
    ReseedMw {
        load_results: Arc::new(load_results),
    }
}

/// struct 形态（tower `Service` 同款）：参数是**字段**，`serve<'a>(&'a self)`
/// 的 future 借 `&self`——零捕获仪式、零 per-call 克隆。
#[derive(Clone)]
pub struct ReseedMw {
    load_results: Arc<LoadResults>,
}

impl MiddlewareServer for ReseedMw {
    async fn serve(&self, args: MiddlewareServerFnArgs) -> Result<Value> {
        let mut sc = args;

        // 计划只在 meta 里；缺失 = 本次 run 不做播种。
        let Some(plan) = sc.ctx.meta.try_get::<ReseedKey>() else {
            return sc.next(None).await;
        };
        let redo: std::collections::HashSet<String> = plan.redo_set.iter().cloned().collect();

        // 本 run 就是源 run？不做任何事（禁止自我播种）。
        if plan.source_run_id != sc.ctx.run_id {
            // **drive 开始时加载一次**，而不是在包装层里懒加载 + 缓存：
            // `load_results` 是同步函数，放在这里不会阻塞；省掉可变状态后
            // 层就是纯的（只读 plan）。
            //
            // 源读不到 → 空表，「没有可复用结果」→ 全部真跑
            // （与引擎版「导入 0 条」同义）。
            let lookup = Arc::new((self.load_results)(&plan.source_run_id).unwrap_or_default());

            sc.wrap_step(step_layer({
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

        sc.next(None).await
    }
}

