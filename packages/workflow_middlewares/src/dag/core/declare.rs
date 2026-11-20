//! DAG 声明式调度：把 `DagSpec` 按数据依赖偏序驱动成 `ctx.step`。
//!
//! 与 [`DagShape`](super::DagShape) 的分工：`DagShape` 只算名单（redo / 目标闭包），
//! 本模块才**发起** step。方案与边界见 `docs/decisions/0019`。
//!
//! 调度语义对齐 `wf-demo/workflow-dag/src/index.ts:167-196`：每个节点一个任务，
//! 自身依赖全部 resolve 后立即执行（严格偏序、无波栅栏），memo 去重保证每节点
//! 恰好一个任务；依赖结果按 id 传给节点函数。
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;

use aa_workflow_core::define::{BoxFuture, StepCtx};
use aa_workflow_core::WorkflowCtx;
use anyhow::{anyhow, Result};
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use super::DagShape;

/// 节点执行体：收到「直接依赖的结果（键 = 节点 id）」与 run input，返回 JSON 结果。
pub type DagRunFn = Arc<
    dyn Fn(BTreeMap<String, Value>, Value) -> BoxFuture<'static, Result<Value>> + Send + Sync,
>;

/// 一个 DAG 节点：直接依赖列表 + 执行体。
#[derive(Clone)]
pub struct DagNode {
    pub deps: Vec<String>,
    pub run: DagRunFn,
}

impl DagNode {
    pub fn new<F, Fut, I, S>(deps: I, run: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(BTreeMap<String, Value>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        Self {
            deps: deps.into_iter().map(Into::into).collect(),
            run: Arc::new(move |deps, input| Box::pin(run(deps, input))),
        }
    }
}

/// 静态 DAG 声明。`BTreeMap` 保证遍历与 output 的键顺序确定（replay 可重放）。
#[derive(Clone)]
pub struct DagSpec {
    pub id: String,
    pub nodes: BTreeMap<String, DagNode>,
}

impl DagSpec {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: BTreeMap::new(),
        }
    }

    pub fn node<F, Fut, I, S>(mut self, id: impl Into<String>, deps: I, run: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(BTreeMap<String, Value>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.nodes.insert(id.into(), DagNode::new(deps, run));
        self
    }
}

/// 声明期校验：重复节点 / 未知依赖 / 环，以及 `__dag*` 保留前缀。
/// 图本身交给 [`DagShape::build`] 把关；先做拓扑排序以满足它「先声明后引用」的要求。
pub fn define_dag(spec: DagSpec) -> Result<Arc<DagSpec>> {
    for id in spec.nodes.keys() {
        if id.starts_with("__dag") {
            return Err(anyhow!("dag \"{}\": node id \"{id}\" uses reserved __dag* prefix", spec.id));
        }
    }
    let order = topo_order(&spec)?;
    // 把拓扑序转成 DagShape::build 要的 (&str, &[&str])。
    let steps: Vec<(String, Vec<String>)> = order
        .iter()
        .map(|id| {
            let deps = spec.nodes.get(id).map(|n| n.deps.clone()).unwrap_or_default();
            (id.clone(), deps)
        })
        .collect();
    let dep_refs: Vec<Vec<&str>> = steps
        .iter()
        .map(|(_, deps)| deps.iter().map(String::as_str).collect())
        .collect();
    let declared: Vec<(&str, &[&str])> = steps
        .iter()
        .zip(dep_refs.iter())
        .map(|((id, _), deps)| (id.as_str(), deps.as_slice()))
        .collect();
    DagShape::build(&declared)?;
    Ok(Arc::new(spec))
}

fn topo_order(spec: &DagSpec) -> Result<Vec<String>> {
    fn visit(
        id: &str,
        spec: &DagSpec,
        state: &mut HashMap<String, u8>,
        stack: &mut Vec<String>,
        out: &mut Vec<String>,
    ) -> Result<()> {
        match state.get(id) {
            Some(&1) => return Ok(()),
            Some(&0) => return Err(anyhow!("dag cycle: {}", stack.join(" -> "))),
            _ => {}
        }
        let node = spec
            .nodes
            .get(id)
            .ok_or_else(|| anyhow!("unknown dag node: {id}"))?;
        state.insert(id.to_string(), 0);
        stack.push(id.to_string());
        for dep in &node.deps {
            visit(dep, spec, state, stack, out)?;
        }
        stack.pop();
        state.insert(id.to_string(), 1);
        out.push(id.to_string());
        Ok(())
    }

    let mut state = HashMap::new();
    let mut out = Vec::with_capacity(spec.nodes.len());
    for id in spec.nodes.keys() {
        visit(id, spec, &mut state, &mut Vec::new(), &mut out)?;
    }
    Ok(out)
}

/// 按偏序调度全部节点，返回 `节点 id → 结果`。
///
/// 每个节点一个 spawn 任务；`OnceCell::get_or_try_init` 做 memo 去重，
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
