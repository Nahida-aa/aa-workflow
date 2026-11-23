//! 组合测试：两个内置策略中间件同时挂载。
//!
//! 现有测试都是单中间件；这里验证「各自的 meta key 互不干扰」以及
//! 「reseed 写入的 StepFinished 能被 interim_goals 的完成判定看到」。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::{MetaBag, StepCtx};
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    CreateWorkflowConfig, InMemoryStore, RunStatus, RunStore, RunWorkflowOptions, WorkflowCtx,
    create_workflow, run_workflow,
};
use aa_workflow_middlewares::{
    DagPlan, DagPlanKey, DagShape, INTERIM_GOAL_SIGNAL, InterimGoalsKey, ReseedKey, ReseedPlan,
    SourceStep, dag_policies, interim_goals, reseed,
};

/// 把源 run 的 STEP_FINISHED 收成 map（带日志下标）。
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

/// 一个把 a/b/c 串起来的 workflow，挂上调用方给的两个中间件。
fn make_wf(
    ran: Arc<Mutex<Vec<String>>>,
    a: impl aa_workflow_core::middleware::MiddlewareServer + 'static,
    b: impl aa_workflow_core::middleware::MiddlewareServer + 'static,
) -> aa_workflow_core::define::WorkflowDefinition<
    serde_json::Value,
    serde_json::Value,
    serde_json::Value,
> {
    create_workflow(CreateWorkflowConfig::new("combined").input::<serde_json::Value>())
        .middleware(a)
        .middleware(b)
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let ran = ran.clone();
                    let sid = id.to_string();
                    ctx.step(id, move |_sc: StepCtx| {
                        let ran = ran.clone();
                        let sid = sid.clone();
                        async move {
                            ran.lock().unwrap().push(sid.clone());
                            Ok(serde_json::json!({ "id": sid }))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::json!({ "ok": true }))
            }
        })
}

/// `reseed` 在外层（先注册 = 外层）：它把 c 入口 `SkipImported` 短路了，
/// 内层 `interim_goals` 根本没机会看到 → goals 达成也不泊车。
///
/// 顺带验证两件事：`MetaBag` 链式 `with` 让两个 key 共存，各自读到自己的；
/// `reseed` 的 `SkipImported` 会写 `StepFinished`（被复用的 a 在日志里可见）。
#[tokio::test]
async fn outer_reseed_short_circuits_before_interim_goals_can_park() {
    let store = Arc::new(InMemoryStore::new());
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = Arc::new(make_wf(
        ran.clone(),
        reseed(loader(store.clone())),
        interim_goals({
            let store = store.clone();
            move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
        }),
    ));

    // run1：不给任何 plan → 两个中间件都不装层 → 全跑。
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("c1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "没有 plan 时两个中间件都应透传"
    );

    // run2：reseed 复用 a，interim_goals 在 b 达成后泊车。
    // 期望：a 被复用（不执行）→ b 真跑 → b 入口检查 goals=[b] 未达成 → b 跑完
    //       → c 入口发现 goals 已达成 → 泊车，c 不跑。
    ran.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .meta(
                MetaBag::new()
                    .with::<ReseedKey>(ReseedPlan {
                        source_run_id: "c1".into(),
                        // b 要真跑（goals 是 b），a/c 复用。
                        redo_set: vec!["b".into()],
                    })
                    .with::<InterimGoalsKey>(vec!["b".to_string()]),
            )
            .run_id("c2"),
    )
    .outcome()
    .await
    .unwrap();

    // **层序的后果**：`reseed` 在外层，c 入口被它 `SkipImported` 短路，
    // 内层 `interim_goals` 根本没被调用 → goals 看不到 c 的入口 → 不泊车。
    // 这不是 bug，是「先注册的在外层」的直接结果（反序见下一个测试）。
    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["b".to_string()],
        "只有 b 真跑（在 redo_set）；a/c 被复用"
    );
    assert_eq!(
        out.status,
        RunStatus::Finished,
        "reseed 在外层时 c 被短路，interim_goals 看不到入口 → 不泊车"
    );

    // 没有泊车 → 不该有 __interim-goal 的等待点。
    assert!(
        !store
            .get_events("c2")
            .await.unwrap()
            .iter()
            .any(|e| matches!(
                e,
                WorkflowEvent::SignalAwaited { name, .. } if name == INTERIM_GOAL_SIGNAL
            )),
        "reseed 在外层时不该产生泊车点"
    );

    // a 在 c2 的日志里有 checkpoint（被复用），且带溯源。
    let imported_a = store.get_events("c2").await.unwrap().into_iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::StepFinished { step_id, meta: Some(m), .. }
                if step_id == "a" && m.imported_from.is_some()
        )
    });
    assert!(imported_a, "a 的 checkpoint 应带 imported_from 溯源");
}

/// 换过来注册（`interim_goals` 在外层）：它先看到每个 step 入口，
/// 于是 b 完成后在 c 入口泊车成功——`reseed` 根本没轮到短路 c。
///
/// **与上一个测试对照**：同一对中间件、同一样的 meta，只因注册顺序不同而
/// 一个不泊车、一个泊车。这是「先注册的在外层」的直接后果，不是 bug。
#[tokio::test]
async fn outer_interim_goals_parks_before_reseed_short_circuits() {
    let store = Arc::new(InMemoryStore::new());
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // 注意顺序反了：interim_goals 在外层。
    let wf = Arc::new(make_wf(
        ran.clone(),
        interim_goals({
            let store = store.clone();
            move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
        }),
        reseed(loader(store.clone())),
    ));

    run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(serde_json::json!({}))
            .run_id("d1"),
    )
    .outcome()
    .await
    .unwrap();

    ran.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(
                MetaBag::new()
                    .with::<InterimGoalsKey>(vec!["b".to_string()])
                    .with::<ReseedKey>(ReseedPlan {
                        source_run_id: "d1".into(),
                        redo_set: vec!["b".into()],
                    }),
            )
            .run_id("d2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["b".to_string()],
        "注册顺序不影响两个 meta key 的读取"
    );
    assert_eq!(
        out.status,
        RunStatus::Paused,
        "反序（interim_goals 在外层）应能泊车；实际 err={:?}",
        store.get_run_state("d2").await.unwrap().unwrap().error
    );
}
/// 两个**都能泊车**的中间件同时挂：外层先判定，内层被遮蔽。
///
/// 两者都用 `InterimGoalSignal` 停下来，但 park 时带的 `meta` 不同——
/// 一个带 `{__interimGoals: ..}`、一个带 `{goals: ..}`，正好能看出来是谁触发。
fn linear_shape() -> Arc<DagShape> {
    let none: &[&str] = &[];
    let a: &[&str] = &["a"];
    let ab: &[&str] = &["a", "b"];
    Arc::new(DagShape::build(&[("a", none), ("b", a), ("c", ab)]).unwrap())
}

/// 读出泊车事件的 meta（`None` = 没泊车）。
async fn park_meta(store: &InMemoryStore, run_id: &str) -> Option<serde_json::Value> {
    use aa_workflow_core::RunStore;
    store.get_events(run_id).await.ok()?.into_iter().find_map(|e| match e {
        WorkflowEvent::SignalAwaited { name, meta, .. } if name == INTERIM_GOAL_SIGNAL => meta,
        _ => None,
    })
}

/// 外层 `interim_goals` 先看到 b 入口 → 它 park，`dag_policies` 没轮到。
#[tokio::test]
async fn outer_interim_goals_wins_the_park_race() {
    let store = Arc::new(InMemoryStore::new());
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = Arc::new(make_wf(
        ran.clone(),
        interim_goals({
            let store = store.clone();
            move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
        }),
        dag_policies(
            linear_shape(),
            Arc::new({
                let store = store.clone();
                move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
            }),
        ),
    ));

    let out = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(
                MetaBag::new()
                    .with::<InterimGoalsKey>(vec!["a".to_string()])
                    .with::<DagPlanKey>(DagPlan {
                        goals: vec!["a".to_string()],
                        ..Default::default()
                    }),
            )
            .run_id("p1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(ran.lock().unwrap().clone(), vec!["a".to_string()]);
    assert_eq!(out.status, RunStatus::Paused);
    let m = park_meta(&store, "p1").await.expect("应泊车");
    assert!(
        m.get("__interimGoals").is_some(),
        "应由 interim_goals 触发（meta 带 __interimGoals），实际 {m}"
    );
}

/// 换过来：外层 `dag_policies` 先看到 b 入口 → 它 park，`interim_goals` 没轮到。
#[tokio::test]
async fn outer_dag_policies_wins_the_park_race() {
    let store = Arc::new(InMemoryStore::new());
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = Arc::new(make_wf(
        ran.clone(),
        dag_policies(
            linear_shape(),
            Arc::new({
                let store = store.clone();
                move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
            }),
        ),
        interim_goals({
            let store = store.clone();
            move |run_id: &str| { let store = store.clone(); Box::pin(async move { use aa_workflow_core::RunStore; Ok(store.get_events(run_id).await?) }) as aa_workflow_core::define::BoxFuture<'_, _> }
        }),
    ));

    let out = run_workflow(
        RunWorkflowOptions::new(wf, store.clone())
            .input(serde_json::json!({}))
            .meta(
                MetaBag::new()
                    .with::<DagPlanKey>(DagPlan {
                        goals: vec!["a".to_string()],
                        ..Default::default()
                    })
                    .with::<InterimGoalsKey>(vec!["a".to_string()]),
            )
            .run_id("q1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(ran.lock().unwrap().clone(), vec!["a".to_string()]);
    assert_eq!(out.status, RunStatus::Paused);
    let m = park_meta(&store, "q1").await.expect("应泊车");
    assert!(
        m.get("goals").is_some() && m.get("__interimGoals").is_none(),
        "应由 dag_policies 触发（meta 带 goals），实际 {m}"
    );
}