//! 验证 `StepCall::completed` 快照在**并行分支**下也准确。
//!
//! 这是「策略无状态」改造的关键前提：`interim_goals` / `dag` 不再自己累积
//! `done` 集合，而是读引擎给的快照。若快照在并发下看到的完成集不完整，
//! 「目标全齐」的判定就会错——要么早泊（少跑 step），要么不泊（该停不停）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::middleware::{
    Middleware, MiddlewareServerFnArgs, StepRequest, StepService, StepServiceError, StepFuture,
};
use tower::{Layer, Service};
use aa_workflow_core::{
    CreateWorkflowConfig, InMemoryStore, RunWorkflowOptions, WorkflowCtx, create_workflow,
    run_workflow,
};

/// 记录每次进入某个 step 时「已完成 step」的快照。
fn recorder(seen: Arc<Mutex<Vec<(String, Vec<String>)>>>) -> Middleware {
    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let seen = seen.clone();
        Box::pin(async move {
            sc.wrap_step(RecordLayer { seen });
            sc.next(None).await
        })
    })
}

#[derive(Clone)]
struct RecordLayer {
    seen: Arc<Mutex<Vec<(String, Vec<String>)>>>,
}

impl Layer<StepService> for RecordLayer {
    type Service = RecordService;
    fn layer(&self, inner: StepService) -> Self::Service {
        RecordService {
            inner,
            seen: self.seen.clone(),
        }
    }
}

#[derive(Clone)]
struct RecordService {
    inner: StepService,
    seen: Arc<Mutex<Vec<(String, Vec<String>)>>>,
}

impl Service<StepRequest> for RecordService {
    type Response = serde_json::Value;
    type Error = StepServiceError;
    type Future = StepFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: StepRequest) -> Self::Future {
        let mut list: Vec<String> = req.completed.iter().cloned().collect();
        list.sort();
        self.seen.lock().unwrap().push((req.step_id.clone(), list));
        let mut inner = self.inner.clone();
        Box::pin(async move {
            std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
            inner.call(req).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_snapshot_sees_prior_steps_in_order() {
    let seen: Arc<Mutex<Vec<(String, Vec<String>)>>> = Arc::new(Mutex::new(Vec::new()));
    let wf = create_workflow(CreateWorkflowConfig::new("snap").input::<serde_json::Value>())
        .middleware([recorder(seen.clone())])
        .handler(|ctx: WorkflowCtx| async move {
            for id in ["a", "b", "c"] {
                let sid = id.to_string();
                ctx.step(id, move |_sc: StepCtx| async move {
                    Ok::<_, anyhow::Error>(serde_json::json!(sid))
                })
                .await?;
            }
            Ok(serde_json::json!({}))
        });

    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(serde_json::json!({}))
            .run_id("snap:1"),
    )
    .outcome()
    .await
    .unwrap();

    let seen = seen.lock().unwrap().clone();
    // 串行情形：每个 step 入口看到此前全部完成
    assert_eq!(seen[0], ("a".to_string(), vec![]));
    assert_eq!(seen[1], ("b".to_string(), vec!["a".to_string()]));
    assert_eq!(
        seen[2],
        ("c".to_string(), vec!["a".to_string(), "b".to_string()])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_snapshot_never_contains_self() {
    let counter = Arc::new(AtomicUsize::new(0));
    let seen: Arc<Mutex<Vec<(String, Vec<String>)>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = create_workflow(CreateWorkflowConfig::new("snap-self").input::<serde_json::Value>())
        .middleware([recorder(seen.clone())])
        .handler({
            let counter = counter.clone();
            move |ctx: WorkflowCtx| {
                let counter = counter.clone();
                async move {
                    // 并行分支：三条同时开跑
                    let (a, b, c) = tokio::join!(
                        ctx.step("a", {
                            let counter = counter.clone();
                            move |_sc: StepCtx| {
                                let counter = counter.clone();
                                async move {
                                    counter.fetch_add(1, Ordering::SeqCst);
                                    Ok::<_, anyhow::Error>(serde_json::json!("a"))
                                }
                            }
                        }),
                        ctx.step("b", {
                            let counter = counter.clone();
                            move |_sc: StepCtx| {
                                let counter = counter.clone();
                                async move {
                                    counter.fetch_add(1, Ordering::SeqCst);
                                    Ok::<_, anyhow::Error>(serde_json::json!("b"))
                                }
                            }
                        }),
                        ctx.step("c", {
                            let counter = counter.clone();
                            move |_sc: StepCtx| {
                                let counter = counter.clone();
                                async move {
                                    counter.fetch_add(1, Ordering::SeqCst);
                                    Ok::<_, anyhow::Error>(serde_json::json!("c"))
                                }
                            }
                        }),
                    );
                    a?;
                    b?;
                    c?;
                    Ok(serde_json::json!({}))
                }
            }
        });

    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(serde_json::json!({}))
            .run_id("snap:2"),
    )
    .outcome()
    .await
    .unwrap();

    let seen = seen.lock().unwrap().clone();
    // 核心不变量：任何 step 入口看到的「已完成集」都不包含它自己
    // （在它跑完之前不该被计入）。
    for (id, completed) in &seen {
        assert!(
            !completed.contains(id),
            "step `{id}` 的快照里出现了自己：{completed:?}"
        );
    }
    // 三条都真的跑了
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

/// 续驱语义：`completed` 跨 drive 累积（由日志投影重建）。
///
/// 上一个 drive 完成的目标，在续驱时应当**立刻**被判为达成、不再重跑也不再
/// 泊车。这条锁定「策略无状态」改造后与改造前一致的行为。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_accumulates_across_drives() {
    use aa_workflow_core::RunStore;
    use aa_workflow_core::define::MetaBag;
    use aa_workflow_middlewares::{interim_goals, InterimGoalsKey, INTERIM_GOAL_SIGNAL};

    let store = Arc::new(InMemoryStore::new());
    let store_for_hook = store.clone();
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let ran_in = ran.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("acc").input::<serde_json::Value>())
        .middleware([interim_goals(move |run_id| {
            Ok(store_for_hook.get_events(run_id)?)
        })])
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran_in.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let sid = id.to_string();
                    let ran = ran.clone();
                    ctx.step(id, move |_sc: StepCtx| {
                        let ran = ran.clone();
                        let sid = sid.clone();
                        async move {
                            ran.lock().unwrap().push(sid.clone());
                            Ok::<_, anyhow::Error>(serde_json::json!(sid))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::json!({}))
            }
        });

    // 第一次 drive：a 是目标 → a 完成后、b 入口泊车
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .meta(MetaBag::new().with::<InterimGoalsKey>(vec!["a".to_string()]))
            .run_id("acc:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(ran.lock().unwrap().clone(), vec!["a".to_string()]);

    // 续驱：a 已在日志里 → goals_done_at_start 成立 → 不重复泊车，b/c 跑完。
    //
    // **不需要投递信号**：这条测试验的正是「开局已达成 → 不泊车」，所以 run
    // 不需要被唤醒，直接 drive 就过去了。旧写法投了一个 SignalResolved，那是
    // 多余的（而且一步写法下会被 append_seed 判成 delivery lost）。
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .meta(MetaBag::new().with::<InterimGoalsKey>(vec!["a".to_string()]))
            .run_id("acc:1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "续驱只补跑 b/c，a 不重跑、也不再泊车"
    );
}
