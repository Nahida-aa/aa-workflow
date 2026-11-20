//! DAG 声明式调度中间件：接管 handler，按 `DagSpec` 的依赖偏序驱动 `ctx.step`。
//!
//! 与同目录的 `dag_policies`（策略层）分工：策略只处置 handler 已发出的 step，
//! 本中间件**发起** step。组合时它必须是 `.middleware([...policies, dag_scheduler(..)])`
//! 的最后一个（最内层），否则策略的 `wrap_step` 还没注册就已经被调度调用
//! （ADR 0007 实测的静默失效）。
//!
//! 调度核心在本包 `dag::core::run_dag`，与 `dag_to_handler` 共用。
use std::sync::Arc;

use aa_workflow_core::middleware::Middleware;
use crate::dag::core::{DagSpec, run_dag};

/// 接管式 DAG 调度中间件。run output = `{ "results": { 节点 id → 结果 } }`。
///
/// ```ignore
/// let spec = define_dag(DagSpec::new("p").node("a", [], |_, _| async { Ok(json!(1)) }))?;
/// let wf = create_workflow(..)
///     .middleware([dag_policies(shape, get_events), dag_scheduler(spec)])  // 调度最后
///     .handler(|_| async { Ok(json!(null)) });                   // 空壳，不会执行
/// ```
pub fn dag_scheduler(spec: Arc<DagSpec>) -> Middleware {
    Middleware::new().server(move |mut sc| {
        let spec = spec.clone();
        Box::pin(async move {
            let ctx = sc.ctx.clone();
            let results = run_dag(&ctx, &spec).await?;
            // Rust 一等能力：不调 next，handler 不跑，本返回值就是 run output。
            // TS 没有这个能力（composeMiddlewares 丢弃 server 返回值），
            // 那边靠 ctx.dagResults 转手。见 ADR 0019 / tanstack-alignment。
            sc.take_over();
            Ok(serde_json::json!({ "results": results }))
        })
    })
}
