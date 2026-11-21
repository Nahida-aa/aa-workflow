//! DAG 声明：`DagSpec` 的节点声明与校验。`DagSpec` 的调度执行在
//! [`scheduler`](super::scheduler)——本模块只负责"图长什么样"和"声明是否合法"。
//!
//! 与 [`DagShape`](super::DagShape) 的分工：`DagShape` 只算名单（redo / 目标闭包），
//! 这里声明的 `DagSpec` 才是可调度的图。方案与边界见 `docs/decisions/0019`。
//!
//! # 类型边界：input / state 精确、deps 仍是 `Value`
//!
//! `DagSpec<I, S>` 的 `I` / `S` 是 **run input / 工作流 state 的精确类型**
//! （run 级各只有一种，可以泛型直通——节点闭包经 `sc.ctx.input` /
//! `sc.ctx.state` 读到类型本身，不过 JSON）。**deps 仍是 `Value`**，这不是
//! 偷懒而是结构使然：每个节点吃不同的上游产物（异构），`DagNode.run` 是
//! `Arc<dyn Fn>`——擦除后的闭包只能有一个签名，异构 deps 的公共签名只有
//! JSON；而且 `ctx.step` 的耐久边界本来就是 JSON（checkpoint 存 `Value`，
//! replay 反序列化），deps 过 Value 与耐久边界同构，不产生额外损失。
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;

use aa_workflow_core::define::{BoxFuture, StepCtx};
use anyhow::{anyhow, Result};
use serde_json::Value;

use super::DagShape;

/// 节点执行体：收到 step 上下文（[`StepCtx`]——`sc.ctx.input` / `sc.ctx.state`
/// 都是 **typed**：input 与 state 是 run 级事实，run 级视图不擦除；`sc.ctx`
/// 同时给出 emit / is_cancelled / attempt / 预算探针）与直接依赖的结果
/// （键 = 节点 id，JSON），返回 JSON 结果（进 checkpoint）。
///
/// input / state **没有参数**——经 `sc.ctx` 读；deps 仍是 `Value`（异构上游
/// 产物 + 耐久边界的公共形状，理由见模块文档）。
pub type DagRunFn<I, S = Value> = Arc<
    dyn Fn(StepCtx<I, S>, BTreeMap<String, Value>) -> BoxFuture<'static, Result<Value>> + Send + Sync,
>;

/// 一个 DAG 节点：直接依赖列表 + 执行体。
#[derive(Clone)]
pub struct DagNode<I = Value, S = Value> {
    pub deps: Vec<String>,
    pub run: DagRunFn<I, S>,
}

impl<I: Clone + Send + Sync + 'static, S: Clone + Send + Sync + 'static> DagNode<I, S> {
    pub fn new<F, Fut, D, N>(deps: D, run: F) -> Self
    where
        D: IntoIterator<Item = N>,
        N: Into<String>,
        F: Fn(StepCtx<I, S>, BTreeMap<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        Self {
            deps: deps.into_iter().map(Into::into).collect(),
            run: Arc::new(move |sc, deps| Box::pin(run(sc, deps))),
        }
    }
}

/// 静态 DAG 声明。`I` / `S` = run input / 工作流 state 的精确类型（缺省
/// `Value`）。`BTreeMap` 保证遍历与 output 的键顺序确定（replay 可重放）。
#[derive(Clone)]
pub struct DagSpec<I = Value, S = Value> {
    pub id: String,
    pub nodes: BTreeMap<String, DagNode<I, S>>,
}

impl<I: Clone + Send + Sync + 'static, S: Clone + Send + Sync + 'static> DagSpec<I, S> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: BTreeMap::new(),
        }
    }

    pub fn node<F, Fut, D, N>(mut self, id: impl Into<String>, deps: D, run: F) -> Self
    where
        D: IntoIterator<Item = N>,
        N: Into<String>,
        F: Fn(StepCtx<I, S>, BTreeMap<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.nodes.insert(id.into(), DagNode::new(deps, run));
        self
    }

    /// spec → 形状图（[`DagShape`]）：调度执行与策略中间件的名单计算共用
    /// **同一张图**——声明式的单一真相源，没有「手写编排与形状各编码一遍」
    /// 的漂移面。校验与 [`define_dag`] 同源（拓扑排序 + 未知依赖 / 环）。
    pub fn shape(&self) -> Result<DagShape> {
        let order = topo_order(self)?;
        // 把拓扑序转成 DagShape::build 要的 (&str, &[&str])。
        let steps: Vec<(String, Vec<String>)> = order
            .iter()
            .map(|id| {
                let deps = self
                    .nodes
                    .get(id)
                    .map(|n| n.deps.clone())
                    .unwrap_or_default();
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
        Ok(DagShape::build(&declared)?)
    }
}

/// 声明期校验：重复节点 / 未知依赖 / 环，以及 `__dag*` 保留前缀。
/// 图本身交给 [`DagShape::build`] 把关（经 [`DagSpec::shape`]）；先做拓扑
/// 排序以满足它「先声明后引用」的要求。
pub fn define_dag<I: Clone + Send + Sync + 'static, S: Clone + Send + Sync + 'static>(
    spec: DagSpec<I, S>,
) -> Result<Arc<DagSpec<I, S>>> {
    for id in spec.nodes.keys() {
        if id.starts_with("__dag") {
            return Err(anyhow!("dag \"{}\": node id \"{id}\" uses reserved __dag* prefix", spec.id));
        }
    }
    spec.shape()?;
    Ok(Arc::new(spec))
}

fn topo_order<I, S>(spec: &DagSpec<I, S>) -> Result<Vec<String>> {
    fn visit<I, S>(
        id: &str,
        spec: &DagSpec<I, S>,
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
