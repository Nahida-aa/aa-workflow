//! `dag` 中间件：Rust 侧完全接管 handler（TS 没有这个能力，机制差异见 ADR 0019）。
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStatus,
    RunWorkflowOptions, WorkflowCtx,
};
use aa_workflow_dag::{DagSpec, define_dag};
use aa_workflow_middlewares::dag;
use serde_json::{Value, json};

#[tokio::test]
async fn dag_middleware_takes_over_handler_and_returns_results() {
    let handler_ran = Arc::new(AtomicUsize::new(0));
    let spec = define_dag(
        DagSpec::new("m")
            .node("a", Vec::<&str>::new(), |_, _| async { Ok(json!(1)) })
            .node("b", ["a"], |deps, _input| async move {
                Ok(json!(deps["a"].as_i64().unwrap() + 1))
            }),
    )
    .unwrap();

    let ran = handler_ran.clone();
    let wf = create_workflow(CreateWorkflowConfig::new("m").input::<Value>())
        .middleware([dag(spec)])
        .handler(move |_ctx: WorkflowCtx| {
            let ran = ran.clone();
            async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(json!("user handler should not run"))
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).run_id("mw:1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(out.output.unwrap(), json!({ "results": { "a": 1, "b": 2 } }));
    assert_eq!(handler_ran.load(Ordering::SeqCst), 0);
}
