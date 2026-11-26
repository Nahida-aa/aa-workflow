//! 验证 redo：不在 redoSet 里的 step 不执行闭包、复用源 run 的结果。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::{MetaBag, StepCtx};
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{ReseedKey, ReseedPlan, reseed};

/// redo 的 plan 走 run 级参数（`RunWorkflowOptions.meta`），不在中间件构造处。
fn reseed_meta(plan: ReseedPlan) -> MetaBag {
    MetaBag::new().with::<ReseedKey>(plan)
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
    let wf = Arc::new(make_wf(exec.clone(), reseed()));

    // rev1：不给 plan → 中间件不装层 → 全跑
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("rev1"),
    )
    .collect()
    .await
    ;
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // rev2：给 plan → 只重跑 c，a/b 复用
    exec.lock().unwrap().clear();
    let events = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "rev1".into(),
                redo_set: vec!["c".into()],
            }))
            .run_id("rev2"),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
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
    let wf = Arc::new(make_wf(exec.clone(), reseed()));

    // s1：不给 plan → 全跑
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("s1"),
    )
    .collect()
    .await
    ;

    // s2：空 redoSet → 全部复用
    exec.lock().unwrap().clear();
    let events = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "s1".into(),
                redo_set: vec![],
            }))
            .run_id("s2"),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
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
        .middleware(reseed())
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

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "ghost".into(),
                redo_set: vec![],
            }))
            .run_id("m1"),
    )
    .collect()
    .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
    assert_eq!(ran.load(Ordering::SeqCst), 1, "源不存在 → 业务函数真跑");
    void(exec);
}

/// 只在读**指定源 run** 的日志时失败；本 run 的正常读写仍委托 `InMemoryStore`。
///
/// 这样引擎自己的 replay 读取不受影响，故障只落在 reseed 读源那一步。
struct FailsOnSourceRun {
    inner: InMemoryStore,
    fail_for: String,
}

#[async_trait::async_trait]
impl RunStore for FailsOnSourceRun {
    async fn get_run_state(
        &self,
        run_id: &str,
    ) -> Result<Option<aa_workflow_core::RunState>, aa_workflow_core::StoreError> {
        self.inner.get_run_state(run_id).await
    }

    async fn set_run_state(
        &self,
        run_id: &str,
        state: &aa_workflow_core::RunState,
    ) -> Result<(), aa_workflow_core::StoreError> {
        self.inner.set_run_state(run_id, state).await
    }

    async fn delete_run(
        &self,
        run_id: &str,
        reason: aa_workflow_core::DeleteReason,
    ) -> Result<(), aa_workflow_core::StoreError> {
        self.inner.delete_run(run_id, reason).await
    }

    async fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &WorkflowEvent,
    ) -> Result<(), aa_workflow_core::StoreError> {
        self.inner
            .append_event(run_id, expected_next_index, event)
            .await
    }

    async fn get_events(
        &self,
        run_id: &str,
    ) -> Result<Vec<WorkflowEvent>, aa_workflow_core::StoreError> {
        if run_id == self.fail_for {
            return Err(aa_workflow_core::StoreError::Io(
                "source log unavailable".into(),
            ));
        }
        self.inner.get_events(run_id).await
    }
}

/// 读源日志**失败**（非「源不存在」）→ 传播错误、run `Errored`，
/// 不再静默降级成「全部真跑」——与 `dag_policies` 的 `args.get_events(..)?` 一致。
#[tokio::test]
async fn source_read_failure_is_propagated() {
    let store: Arc<dyn RunStore> = Arc::new(FailsOnSourceRun {
        inner: InMemoryStore::new(),
        fail_for: "ghost".into(),
    });
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_in = ran.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("redo3").input::<serde_json::Value>())
        .middleware(reseed())
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

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store)
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "ghost".into(),
                redo_set: vec![],
            }))
            .run_id("m2"),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunErrored { .. })));
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "读源失败不该被吞掉、静默降级为真跑"
    );
    let error = events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::RunErrored { error, .. } => Some(error.clone()),
            _ => None,
        })
        .expect("应记录错误");
    assert!(
        error.message.contains("source log unavailable"),
        "错误应来自 store 读失败"
    );
}

/// 源 run 里「已完成但事件没带结果」的 step 也要复用——**完成即成功**，
/// 结果为空不等于「没跑过」。复用后本 run 的 checkpoint 结果记为 `Null`。
#[tokio::test]
async fn completed_step_without_result_is_reused_as_null() {
    let store = Arc::new(InMemoryStore::new());
    // 源 run n1：一条 STEP_FINISHED，但 result 是 None（外部/历史写入的形状）。
    store
        .append_event(
            "n1",
            0,
            &WorkflowEvent::StepFinished {
                ts: 1,
                step_id: "a".into(),
                result: None,
                attempts: Vec::new(),
                meta: None,
            },
        )
        .await
        .unwrap();

    let ran = Arc::new(AtomicUsize::new(0));
    let ran_in = ran.clone();
    let wf = create_workflow(CreateWorkflowConfig::new("redo4").input::<serde_json::Value>())
        .middleware(reseed())
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran_in.clone();
            async move {
                ctx.step("a", move |_sc: StepCtx| {
                    let ran = ran.clone();
                    async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(serde_json::json!("真的跑了"))
                    }
                })
                .await?;
                Ok(serde_json::json!({ "ok": true }))
            }
        });

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .meta(reseed_meta(ReseedPlan {
                source_run_id: "n1".into(),
                redo_set: vec![],
            }))
            .run_id("n2"),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "已完成（即使事件没带结果）的 step 不该重跑"
    );

    let results: Vec<Option<serde_json::Value>> = store
        .get_events("n2")
        .await
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished {
                step_id, result, ..
            } if step_id == "a" => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        vec![Some(serde_json::Value::Null)],
        "复用的空结果记为 Null"
    );
}

fn void<T>(_: T) {}
