//! DAG 声明：`DagSpec` 的节点声明与校验。`DagSpec` 的调度执行在
//! [`scheduler`](super::scheduler)——本模块只负责"图长什么样"和"声明是否合法"。
//!
//! 与 [`DagShape`](super::DagShape) 的分工：`DagShape` 只算名单（redo / 目标闭包），
//! 这里声明的 `DagSpec` 才是可调度的图。方案与边界见 `docs/decisions/0019`。
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;

use aa_workflow_core::define::BoxFuture;
use anyhow::{anyhow, Result};
use serde_json::Value;

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
