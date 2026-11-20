//! DAG 调度器：按依赖关系自动并发地驱动 `ctx.step`。
//!
//! 语义对齐 `wf-demo/workflow-dag/src/index.ts:167-196`：每个节点一个任务，
//! **自己的依赖一全部就绪就立即执行**——即按依赖关系自动并发，没有波次
//! （Kahn 层）栅栏造成的假等待。`OnceCell` memo 去重保证 diamond 里被多条
//! 路径依赖的节点只执行一次 `ctx.step`；方案与边界见 ADR 0019。
use std::collections::BTreeMap;
use std::sync::Arc;

use aa_workflow_core::define::{BoxFuture, StepCtx};
use aa_workflow_core::WorkflowCtx;
use anyhow::{anyhow, Result};
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use super::declare::DagSpec;

/// 按依赖关系自动并发地调度全部节点，返回 `节点 id → 结果`。
///
/// 每个节点 spawn 一个任务，`OnceCell::get_or_try_init` 做 memo 去重，
/// 使 diamond 里被多条路径依赖的节点只执行一次 `ctx.step`。
/// 首个失败会 `abort_all()`，避免孤儿任务继续写 checkpoint。
pub async fn run_dag(ctx: &WorkflowCtx, spec: &DagSpec) -> Result<BTreeMap<String, Value>> {
    let cells: BTreeMap<String, Arc<OnceCell<Value>>> = spec
        .nodes
        .keys()
        .map(|id| (id.clone(), Arc::new(OnceCell::new())))
        .collect();
    let ctx = ctx.clone();
    let input = ctx.input.clone();

    let mut set = JoinSet::new();
    for id in spec.nodes.keys() {
        let id = id.clone();
        let spec = spec.clone();
        let cells = cells.clone();
        let ctx = ctx.clone();
        let input = input.clone();
        set.spawn(async move {
            let value = ensure(&id, &spec, &cells, &ctx, &input).await?;
            Ok::<_, anyhow::Error>((id, value))
        });
    }

    let mut results = BTreeMap::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok((id, value))) => {
                results.insert(id, value);
            }
            Ok(Err(err)) => {
                set.abort_all();
                return Err(err);
            }
            Err(err) => {
                set.abort_all();
                return Err(anyhow!("dag task panicked: {err}"));
            }
        }
    }
    Ok(results)
}

/// 取一个节点的结果（必要时先执行）。返回 boxed future 以支持 DAG 递归。
fn ensure<'a>(
    id: &'a str,
    spec: &'a DagSpec,
    cells: &'a BTreeMap<String, Arc<OnceCell<Value>>>,
    ctx: &'a WorkflowCtx,
    input: &'a Value,
) -> BoxFuture<'a, Result<Value>> {
    Box::pin(async move {
        let cell = cells
            .get(id)
            .ok_or_else(|| anyhow!("unknown dag node: {id}"))?;
        cell.get_or_try_init(|| async {
            let node = spec
                .nodes
                .get(id)
                .ok_or_else(|| anyhow!("unknown dag node: {id}"))?;
            let mut deps = BTreeMap::new();
            for dep in &node.deps {
                let value = ensure(dep, spec, cells, ctx, input).await?;
                deps.insert(dep.clone(), value);
            }
            let run = node.run.clone();
            let input = input.clone();
            let step_id = id.to_string();
            ctx.step(&step_id, move |_sc: StepCtx| {
                let run = run.clone();
                let deps = deps.clone();
                let input = input.clone();
                async move { run(deps, input).await }
            })
            .await
        })
        .await
        .cloned()
    })
}

/// 工厂入口：同一套调度逻辑做成 handler，用户自己控制 id / input / meta。
pub fn dag_to_handler(
    spec: Arc<DagSpec>,
) -> impl Fn(WorkflowCtx) -> BoxFuture<'static, Result<Value>> + Clone + Send + Sync + 'static {
    move |ctx: WorkflowCtx| {
        let spec = spec.clone();
        Box::pin(async move {
            let results = run_dag(&ctx, &spec).await?;
            Ok(serde_json::json!({ "results": results }))
        })
    }
}
