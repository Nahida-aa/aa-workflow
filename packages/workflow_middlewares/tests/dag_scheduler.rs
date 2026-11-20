//! F7：DAG 声明式调度（`define_dag` / `dag_to_handler`）。
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStatus,
    RunWorkflowOptions,
};
use aa_workflow_middlewares::{DagSpec, dag_to_handler, define_dag};
use serde_json::{Value, json};

#[tokio::test]
async fn dag_schedules_by_dependency_order_and_fans_out() {
    let exec: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let e_a = exec.clone();
    let e_b = exec.clone();
    let e_c = exec.clone();
    let e_d = exec.clone();
    let bar_b = barrier.clone();
    let bar_c = barrier.clone();

    let spec = define_dag(
        DagSpec::new("pipe")
            .node("a", Vec::<&str>::new(), move |_deps, input| {
                let exec = e_a.clone();
                async move {
                    exec.lock().unwrap().push("a");
                    Ok(json!({ "v": input["base"] }))
                }
            })
            .node("b", ["a"], move |deps, _input| {
                let exec = e_b.clone();
                let barrier = bar_b.clone();
                async move {
                    exec.lock().unwrap().push("b");
                    // b / c 必须同时在跑，否则屏障等到超时。
                    tokio::time::timeout(Duration::from_secs(2), barrier.wait())
                        .await
                        .map_err(|_| anyhow::anyhow!("b/c 没有并发执行"))?;
                    Ok(json!(deps["a"]["v"].as_i64().unwrap() * 2))
                }
            })
            .node("c", ["a"], move |deps, _input| {
                let exec = e_c.clone();
                let barrier = bar_c.clone();
                async move {
                    exec.lock().unwrap().push("c");
                    tokio::time::timeout(Duration::from_secs(2), barrier.wait())
                        .await
                        .map_err(|_| anyhow::anyhow!("b/c 没有并发执行"))?;
                    Ok(json!(deps["a"]["v"].as_i64().unwrap() * 3))
                }
            })
            .node("d", ["b", "c"], move |deps, _input| {
                let exec = e_d.clone();
                async move {
                    exec.lock().unwrap().push("d");
                    Ok(json!(
                        deps["b"].as_i64().unwrap() + deps["c"].as_i64().unwrap()
                    ))
                }
            }),
    )
    .unwrap();

    let wf = create_workflow(CreateWorkflowConfig::new("pipe").input::<Value>())
        .handler(dag_to_handler(spec));
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(json!({ "base": 7 }))
            .run_id("dag:1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Finished);
    let got = exec.lock().unwrap().clone();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0], "a");
    assert_eq!(got[3], "d");
    assert!(got[1..3].contains(&"b") && got[1..3].contains(&"c"));
    assert_eq!(
        out.output.unwrap(),
        json!({ "results": { "a": { "v": 7 }, "b": 14, "c": 21, "d": 35 } })
    );
}

#[tokio::test]
async fn dag_replay_short_circuits_checkpointed_nodes() {
    let runs: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let spec = {
        let runs_a = runs.clone();
        let runs_b = runs.clone();
        define_dag(
            DagSpec::new("chain")
                .node("a", Vec::<&str>::new(), move |_, _| {
                    let runs = runs_a.clone();
                    async move {
                        *runs.lock().unwrap().entry("a".into()).or_insert(0) += 1;
                        Ok(json!(1))
                    }
                })
                .node("b", ["a"], move |_deps, _| {
                    let runs = runs_b.clone();
                    async move {
                        *runs.lock().unwrap().entry("b".into()).or_insert(0) += 1;
                        Ok(json!(2))
                    }
                }),
        )
        .unwrap()
    };
    let wf = create_workflow(CreateWorkflowConfig::new("chain").input::<Value>())
        .handler(dag_to_handler(spec.clone()));
    let store = Arc::new(InMemoryStore::new());
    for run_id in ["replay:1", "replay:1"] {
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone()).run_id(run_id),
        )
        .outcome()
        .await
        .unwrap();
    }
    // 第二次 drive 命中重放短路：节点闭包一次也没再跑。
    assert_eq!(runs.lock().unwrap().clone(), BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 1)]));
}

#[test]
fn define_dag_rejects_unknown_dep_and_cycle() {
    let bad = DagSpec::new("bad").node("a", ["nope"], |_, _| async { Ok(json!(1)) });
    assert!(define_dag(bad).is_err());

    let cyclic = DagSpec::new("cyc")
        .node("a", ["b"], |_, _| async { Ok(json!(1)) })
        .node("b", ["a"], |_, _| async { Ok(json!(1)) });
    assert!(define_dag(cyclic).is_err());

    let reserved = DagSpec::new("r").node("__dagx", Vec::<&str>::new(), |_, _| async { Ok(json!(1)) });
    assert!(define_dag(reserved).is_err());
}
