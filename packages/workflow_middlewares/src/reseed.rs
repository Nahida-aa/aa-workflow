//! redo（reseed run）的中间件版——**对齐 TS `server` 形状**。
//!
//! 语义与 TS 版（`@wf-demo/workflow-middlewares` 的 `reseed`）**等价**：
//! 不在 `redoSet` 里的 step **不执行闭包**，直接用源 run 的结果提交
//! `STEP_FINISHED`。
//!
//! # 与引擎参数 `reseed_run` 的差别
//!
//! | | 引擎 `reseed_run` | 本中间件 |
//! | --- | --- | --- |
//! | 结果复用 | ✅ | ✅ |
//! | 重放能短路 | ✅ | ✅（照常落 checkpoint） |
//! | `meta.importedFrom` 溯源 | ✅ | ❌（可从 plan + 源日志推算） |
//! | 读源时机 | 首次 drive（`startRun` 里） | 首次 drive（首个真实 step 入口） |
//! | 源在首驱前没了 | 导入 0 条 → 真跑 | 查不到 → 真跑（**相同**） |
//!
//! # 为什么是 step 包装层
//!
//! 要对「即将执行的 step」在**执行之前**介入；TS 版靠覆盖 `ctx.step` 表达
//! 同一件事，Rust 版对齐为可叠加的
//! [`MiddlewareServerFnArgs::push_step_wrapper`]——不调 `next()` 即短路，
//! 用 [`StepCall::skip`] 把源结果作为本 step 的 checkpoint 落盘。
use std::collections::HashMap;
use std::sync::Arc;

use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs, StepCall, StepWrapper};

/// 一次 redo 的计划：从哪里播种、哪些 step 重跑。
#[derive(Debug, Clone)]
pub struct ReseedPlan {
    /// 源 run id。
    pub source_run_id: String,
    /// 强制重跑的 stepId 名单（**不**复用它们的旧结果）。
    pub redo_set: Vec<String>,
}

/// 从源 run 读「step_id → 结果」。
///
/// 返回的 map 应只包含该 run 日志里 `STEP_FINISHED` 的结果；`redo_set` 过滤由本
/// 中间件负责（保持读函数简单）。
pub type LoadResults =
    dyn Fn(&str) -> anyhow::Result<HashMap<String, serde_json::Value>> + Send + Sync;

/// 按 plan 复用源 run 的 step 结果。
///
/// `plan` 在**构造时**给出（Rust 的中间件是 `Arc<dyn Fn>`，没有「运行时读 input」
/// 的默认行为；需要从 input 读，就自己在 server 闭包里读 `ctx.input` 再决定
/// 是否装拦截器——见 `dag_policies`）。
///
/// # 例
///
/// ```ignore
/// let mw = reseed(
///     ReseedPlan { source_run_id: "rev1".into(), redo_set: vec!["translate".into()] },
///     |run_id| { /* 从 store 读 STEP_FINISHED → HashMap<step_id, result> */ Ok(Default::default()) },
/// );
/// ```
pub fn reseed<F>(plan: ReseedPlan, load_results: F) -> Middleware
where
    F: Fn(&str) -> anyhow::Result<HashMap<String, serde_json::Value>> + Send + Sync + 'static,
{
    let redo: std::collections::HashSet<String> = plan.redo_set.iter().cloned().collect();
    let plan = Arc::new(plan);
    let load_results = Arc::new(load_results);

    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let plan = plan.clone();
        let load_results = load_results.clone();
        let redo = redo.clone();

        Box::pin(async move {
            // 本 run 就是源 run？不做任何事（禁止自我播种）。
            if plan.source_run_id != sc.ctx.run_id {
                // **drive 开始时加载一次**，而不是在包装层里懒加载 + 缓存：
                // `load_results` 是同步函数，放在这里不会阻塞；省掉
                // `Arc<Mutex<Option<...>>>` 之后包装层就是纯的（只读 plan）。
                //
                // 源读不到 → 空表，「没有可复用结果」→ 全部真跑
                // （与引擎版「导入 0 条」同义）。
                let lookup = Arc::new(load_results(&plan.source_run_id).unwrap_or_default());

                let wrapper: StepWrapper = Arc::new(move |call: StepCall| {
                    let lookup = lookup.clone();
                    let redo = redo.clone();
                    Box::pin(async move {
                        // redoSet 里的 step 要真跑。
                        if redo.contains(&call.step_id) {
                            return call.next().await;
                        }
                        match lookup.get(&call.step_id).cloned() {
                            // 短路：源结果作为本 step 的 checkpoint 落盘。
                            Some(result) => call.skip(result).await,
                            // 源里没有这个 step（新加的 / 上次失败的）→ 真跑。
                            None => call.next().await,
                        }
                    })
                });
                sc.push_step_wrapper(wrapper);
            }

            sc.next(None).await
        })
    })
}
