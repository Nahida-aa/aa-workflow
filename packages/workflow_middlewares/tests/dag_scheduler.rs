//! `dag` 中间件：用户声明 DAG 即可运行；Rust 侧完全接管 handler（TS 没有这个能力）。
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStatus,
    RunWorkflowOptions, WorkflowCtx,
};
use aa_workflow_dag::{DagSpec, define_dag};
use aa_workflow_middlewares::dag;
use serde_json::{Value, json};

/// a → b，b 消费 a 的结果。两个测试共用这份声明。
fn two_node_spec() -> Arc<DagSpec> {
    define_dag(
        DagSpec::new("m")
            .node("a", Vec::<&str>::new(), |_, _| async { Ok(json!(1)) })
            .node("b", ["a"], |deps, _input| async move {
                Ok(json!(deps["a"].as_i64().unwrap() + 1))
            }),
    )
    .unwrap()
}

/// **正常使用**：用户声明 DAG、挂上 `dag(spec)`，run 的 output 就是各节点结果。
/// handler 只是构建器要求的空壳——用户不需要在里面写任何编排逻辑。
#[tokio::test]
async fn dag_runs_declared_nodes_and_returns_results() {
    let wf = create_workflow(CreateWorkflowConfig::new("m").input::<Value>())
        .middleware([dag(two_node_spec())])
        .handler(|_ctx: WorkflowCtx| async move { Ok(json!(null)) });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).run_id("mw:1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(out.output.unwrap(), json!({ "results": { "a": 1, "b": 2 } }));
}

/// 机制验收（正常使用之上）：handler 确实没跑——Rust 的 `take_over` 语义。
/// 这个断言不是功能本身，而是钉住与 TS 的机制差异（见 ADR 0019）。
#[tokio::test]
async fn dag_does_not_run_user_handler() {
    let handler_ran = Arc::new(AtomicUsize::new(0));
    let ran = handler_ran.clone();
    let wf = create_workflow(CreateWorkflowConfig::new("m2").input::<Value>())
        .middleware([dag(two_node_spec())])
        .handler(move |_ctx: WorkflowCtx| {
            let ran = ran.clone();
            async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(json!("user handler should not run"))
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).run_id("mw:2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(out.output.unwrap(), json!({ "results": { "a": 1, "b": 2 } }));
    assert_eq!(handler_ran.load(Ordering::SeqCst), 0);
}
