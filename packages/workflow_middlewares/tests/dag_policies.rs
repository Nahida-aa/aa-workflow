//! 验证 DAG 策略中间件：redo 复用 + 临时目标泊车，计划走 `meta` 的 `DagPlanKey`。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::StepCtx;
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStatus, RunStore,
    RunWorkflowOptions, WorkflowCtx,
};
use aa_workflow_dag::{DagNode, DagShape, DagSpec, define_dag};
use aa_workflow_middlewares::{
    DagPlan, DagPlanKey, INTERIM_GOAL_SIGNAL, dag_policies, dag_scheduler,
};
use serde_json::{Value, json};

fn shape() -> Arc<DagShape> {
    // 菱形：load → (enrich, translate) → merge → publish
    let load: &[&str] = &[];
    let with_load: &[&str] = &["load"];
    let with_pair: &[&str] = &["enrich", "translate"];
    let with_merge: &[&str] = &["merge"];
    Arc::new(
        DagShape::build(&[
            ("load", load),
            ("enrich", with_load),
            ("translate", with_load),
            ("merge", with_pair),
            ("publish", with_merge),
        ])
        .unwrap(),
    )
}

fn loader(store: Arc<InMemoryStore>) -> Arc<aa_workflow_middlewares::GetEvents> {
    Arc::new(move |run_id: &str| {
        use aa_workflow_core::RunStore;
        Ok(store.get_events(run_id)?)
    })
}

/// 每个节点把 id 记进 `exec`——不再由 handler 手写 step 列表。
fn recording_node(exec: &Arc<Mutex<Vec<String>>>, deps: &[&str], id: &'static str) -> DagNode {
    let exec = exec.clone();
    DagNode::new(deps.iter().copied(), move |_deps, _input: Value| {
        let exec = exec.clone();
        async move {
            exec.lock().unwrap().push(id.to_string());
            Ok(json!({ "id": id }))
        }
    })
}

#[tokio::test]
async fn redo_reuse_and_goal_park_via_dag_plan() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // **一个 definition 跑多次 run**——plan 走 meta，中间件不含它。
    // 调度按图自动进行：handler 只是空壳（dag 中间件 take_over 后不会执行）。
    let spec = define_dag(DagSpec {
        id: "dag-pol".into(),
        nodes: BTreeMap::from([
            ("load".into(), recording_node(&exec, &[], "load")),
            ("enrich".into(), recording_node(&exec, &["load"], "enrich")),
            ("translate".into(), recording_node(&exec, &["load"], "translate")),
            (
                "merge".into(),
                recording_node(&exec, &["enrich", "translate"], "merge"),
            ),
            ("publish".into(), recording_node(&exec, &["merge"], "publish")),
        ]),
    })
    .unwrap();
    let wf = Arc::new(
        create_workflow(CreateWorkflowConfig::new("dag-pol").input::<serde_json::Value>())
            // dag 必须最后（最内层）：策略的 wrap_step 要先注册。
            .middleware([dag_policies(shape(), loader(store.clone())), dag_scheduler(spec)])
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!(null)) }),
    );

    // r1：不给 plan → 全跑
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("r1"),
    )
    .outcome()
    .await
    .unwrap();
    // enrich / translate 是 fan-out，彼此先后不定；排序后比集合。
    let mut got = exec.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec!["enrich", "load", "merge", "publish", "translate"]
    );

    // rev2：from=translate（重跑 translate/merge/publish），goals=[merge]
    // 期望：load/enrich 复用（不执行），translate/merge 真跑，merge 达成后泊车
    //（publish 不执行）。
    exec.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .meta(
                aa_workflow_core::define::MetaBag::new().with::<DagPlanKey>(DagPlan {
                    source_run_id: Some("r1".into()),
                    from: Some(vec!["translate".into()]),
                    redo_set: None,
                    goals: vec!["merge".into()],
                }),
            )
            .run_id("r2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["translate".to_string(), "merge".to_string()],
        "load/enrich 复用、merge 达成即停、publish 不跑"
    );
    // 真断言 Paused（同 interim_goals 的测试：`!= Finished` 会放过 Errored）。
    assert_eq!(out.status, RunStatus::Paused, "应泊车");
    assert!(
        store
            .get_events("r2")
            .unwrap()
            .iter()
            .any(|e| matches!(
                e,
                WorkflowEvent::SignalAwaited { name, .. } if name == INTERIM_GOAL_SIGNAL
            )),
        "应有 __interim-goal 的 SignalAwaited"
    );

    // rev2 日志里 load/enrich 也有 checkpoint（复用提交的）→ 重放可短路
    let mut finished: Vec<String> = store
        .get_events("r2")
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.clone()),
            _ => None,
        })
        .collect();
    finished.sort();
    assert_eq!(
        finished,
        vec!["enrich", "load", "merge", "translate"],
        "load/enrich/translate/merge 都有 checkpoint"
    );

    // 续驱：publish 补跑（load/enrich/translate/merge 短路）
    exec.lock().unwrap().clear();
    let out3 = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .run_id("r2"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out3.status), "Finished");
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["publish".to_string()],
        "续驱只补跑 publish"
    );
}

#[tokio::test]
async fn no_plan_means_no_behavior() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = create_workflow(CreateWorkflowConfig::new("no-plan").input::<serde_json::Value>())
        .middleware([dag_policies(shape(), loader(store.clone()))])
        .handler({
            let exec = exec.clone();
            move |ctx: WorkflowCtx| {
                let exec = exec.clone();
                async move {
                    ctx.step("a", move |_sc: StepCtx| async move {
                        exec.lock().unwrap().push("a".to_string());
                        Ok(serde_json::json!(1))
                    })
                    .await?;
                    Ok(serde_json::json!({ "ok": true }))
                }
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("np:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out.status), "Finished", "无 plan = 无行为");
    assert_eq!(exec.lock().unwrap().len(), 1);
}
