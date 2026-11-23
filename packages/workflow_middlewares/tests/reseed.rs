//! 验证 redo：不在 redoSet 里的 step 不执行闭包、复用源 run 的结果。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::{MetaBag, StepCtx};
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{ReseedKey, ReseedPlan, SourceStep, reseed};

/// redo 的 plan 走 run 级参数（`RunWorkflowOptions.meta`），不在中间件构造处。
fn reseed_meta(plan: ReseedPlan) -> MetaBag {
    MetaBag::new().with::<ReseedKey>(plan)
}

/// 源 run 的结果提取器：把日志里的 STEP_FINISHED 收成 map（带日志下标，
/// 供 `STEP_FINISHED.meta.imported_from.index` 使用）。
fn loader(
    store: Arc<InMemoryStore>,
) -> impl for<'a> Fn(&'a str) -> aa_workflow_core::define::BoxFuture<'a, anyhow::Result<HashMap<String, SourceStep>>> + Send + Sync + 'static {
    move |run_id: &str| {
        let store = store.clone();
        Box::pin(async move {
            use aa_workflow_core::RunStore;
            let mut m = HashMap::new();
            for (index, e) in store.get_events(run_id).await?.into_iter().enumerate() {
                if let WorkflowEvent::StepFinished {
                    step_id,
                    result: Some(r),
                    ..
                } = e
                {
                    m.insert(step_id, SourceStep { result: r, index });
                }
            }
            Ok(m)
        }) as aa_workflow_core::define::BoxFuture<'_, _>
    }
}

/// 造一个挂了 `reseed` 的 workflow。**同一个定义跑多次 run**——
/// plan 由每次 run 的 `meta` 决定，不给就不播种。
fn make_wf(
    exec: Arc<Mutex<Vec<String>>>,
    mw: impl aa_workflow_core::middleware::MiddlewareServer + 'static,
) -> aa_workflow_core::define::WorkflowDefinition<
    serde_json::Value,
    serde_json::Value,
    serde_json::Value,
> {
    create_workflow(CreateWorkflowConfig::new("redo").input::<serde_json::Value>())
        .middleware(mw)
        .handler(move |ctx: WorkflowCtx| {
            let exec = exec.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let exec = exec.clone();
                    let sid = id.to_string();
                    ctx.step(id, move |_sc: StepCtx| async move {
                        exec.lock().unwrap().push(sid.clone());
                        Ok(serde_json::json!({ "id": sid }))
                    })
                    .await?;
                }
                Ok(serde_json::json!({ "ok": true }))
            }
        })
}

#[tokio::test]
async fn redo_reuses_steps_outside_redo_set() {
    let store = Arc::new(InMemoryStore::new());
    let exec = Arc::new(Mutex::new(Vec::new()));

    // **一个 workflow 定义跑两次 run**——这才是 redo 的真实形态。
    // 中间件挂在上面（挂一次），plan 走 meta（每次 run 各自给）。
    let wf = Arc::new(make_wf(exec.clone(), reseed(loader(store.clone()))));

    // rev1：不给 plan → 中间件不装层 → 全跑
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("rev1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // rev2：给 plan → 只重跑 c，a/b 复用
    exec.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "rev1".into(),
                redo_set: vec!["c".into()],
            }))
            .run_id("rev2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(format!("{:?}", out.status), "Finished");
    // 关键：只有 c 真正执行过
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["c".to_string()],
        "a/b 应被复用、闭包不执行"
    );

    // 但 rev2 的日志里 a/b/c 都有 checkpoint（重放可短路）
    let finished: Vec<String> = store
        .get_events("rev2")
        .await.unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(finished, vec!["a", "b", "c"]);

    // 复用的 checkpoint 必须带**导入溯源**（与引擎版 `reseed_run` 一致）
    let imported: Vec<(String, String, usize)> = store
        .get_events("rev2")
        .await.unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished {
                step_id,
                meta: Some(m),
                ..
            } => m
                .imported_from
                .as_ref()
                .map(|f| (step_id.clone(), f.run_id.clone(), f.index)),
            _ => None,
        })
        .collect();
    assert_eq!(
        imported.len(),
        2,
        "只有 a/b 被复用，它们才应有 imported_from"
    );
    for (step_id, from_run, _index) in &imported {
        assert!(step_id == "a" || step_id == "b", "只该是 a/b，得到 {step_id}");
        assert_eq!(from_run, "rev1", "溯源应指向源 run");
    }
    // 真跑的 c 不应有溯源
    assert!(
        !imported.iter().any(|(id, _, _)| id == "c"),
        "真执行的 step 不该有 imported_from"
    );
}

#[tokio::test]
async fn redo_with_empty_redo_set_reuses_everything() {
    let store = Arc::new(InMemoryStore::new());
    let exec = Arc::new(Mutex::new(Vec::new()));

    // 一个 workflow 定义跑两次 run（同上一个测试）。
    let wf = Arc::new(make_wf(exec.clone(), reseed(loader(store.clone()))));

    // s1：不给 plan → 全跑
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("s1"),
    )
    .outcome()
    .await
    .unwrap();

    // s2：空 redoSet → 全部复用
    exec.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "s1".into(),
                redo_set: vec![],
            }))
            .run_id("s2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(format!("{:?}", out.status), "Finished");
    assert!(
        exec.lock().unwrap().is_empty(),
        "空 redoSet = 全部复用，零业务执行"
    );
}

/// 源 run 不存在 → 查不到任何结果 → 全部真跑（与引擎参数行为一致）。
#[tokio::test]
async fn redo_with_missing_source_runs_everything() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_in = ran.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("redo2").input::<serde_json::Value>())
        .middleware(reseed(loader(store.clone())))
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran_in.clone();
            async move {
                ctx.step("a", move |_sc: StepCtx| {
                    let ran = ran.clone();
                    async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(serde_json::json!(1))
                    }
                })
                .await?;
                Ok(serde_json::json!({ "ok": true }))
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "ghost".into(),
                redo_set: vec![],
            }))
            .run_id("m1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out.status), "Finished");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "源不存在 → 业务函数真跑");
    void(exec);
}

fn void<T>(_: T) {}
