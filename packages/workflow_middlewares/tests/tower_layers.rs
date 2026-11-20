//! 验证：`wrap_step` 接受的是**真正的 `tower::Layer`**。
//!
//! 这是把 step 拦截改成 tower 服务的唯一实质收益——tower 生态里请求类型无关
//! 的层（超时、限流、重试、tracing）可以直接装在 `ctx.step` 上，不必自己实现
//! 一份私有的等价物。
//!
//! 注意：为了能装这些层，链的错误类型统一成了 `tower::BoxError`
//! （`Box<dyn Error + Send + Sync>`）——`timeout` / `retry` / `limit` 都是
//! 以它作为输出错误类型。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::middleware::{
    Middleware, MiddlewareServerFnArgs, StepRequest, StepService, StepServiceError,
};
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunWorkflowOptions,
    WorkflowCtx,
};

/// 把一个 tower 原生 `Layer` 装到 step 上。
fn with_layer<L>(layer: L) -> Middleware
where
    L: tower::Layer<StepService> + Clone + Send + Sync + 'static,
    L::Service: tower::Service<StepRequest, Response = serde_json::Value, Error = StepServiceError>
        + Clone
        + Send
        + 'static,
    <L::Service as tower::Service<StepRequest>>::Future: Send + 'static,
{
    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let layer = layer.clone();
        Box::pin(async move {
            sc.wrap_step(layer);
            sc.next(None).await
        })
    })
}

fn workflow_with(
    id: &'static str,
    mw: Middleware,
    ran: Arc<Mutex<Vec<String>>>,
) -> aa_workflow_core::define::WorkflowDefinition<
    serde_json::Value,
    serde_json::Value,
    serde_json::Value,
> {
    create_workflow(CreateWorkflowConfig::new(id).input::<serde_json::Value>())
        .middleware(mw)
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran.clone();
            async move {
                for s in ["a", "b"] {
                    let ran = ran.clone();
                    let sid = s.to_string();
                    ctx.step(s, move |_sc: StepCtx| {
                        let ran = ran.clone();
                        let sid = sid.clone();
                        async move {
                            ran.lock().unwrap().push(sid.clone());
                            Ok::<_, anyhow::Error>(serde_json::json!(sid))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::json!({ "ok": true }))
            }
        })
}

/// `tower::timeout::TimeoutLayer` — 之前完全用不上的现成层。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tower_timeout_layer_wraps_steps() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let wf = workflow_with(
        "tower-timeout",
        with_layer(tower::timeout::TimeoutLayer::new(Duration::from_secs(10))),
        ran.clone(),
    );
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(serde_json::json!({}))
            .run_id("tl:1"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string()],
        "超时层不改变正常路径"
    );
}

/// 超时真的会生效（闭包 sleep 超过上限）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tower_timeout_layer_actually_times_out() {
    let wf = create_workflow(
        CreateWorkflowConfig::new("tower-timeout-hit").input::<serde_json::Value>(),
    )
    .middleware(with_layer(tower::timeout::TimeoutLayer::new(
        Duration::from_millis(50),
    )))
    .handler(|ctx: WorkflowCtx| async move {
        ctx.step("slow", |_sc: StepCtx| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<_, anyhow::Error>(serde_json::json!("too late"))
        })
        .await?;
        Ok(serde_json::json!({}))
    });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(serde_json::json!({}))
            .run_id("tl:2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_ne!(
        format!("{:?}", out.status),
        "Finished",
        "慢 step 应被 tower 的超时层掐断"
    );
}

/// `tower::limit::ConcurrencyLimitLayer` — 另一类现成层，验证不是特例。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tower_concurrency_limit_layer_wraps_steps() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let wf = workflow_with(
        "tower-limit",
        with_layer(tower::limit::ConcurrencyLimitLayer::new(4)),
        ran.clone(),
    );
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
            .input(serde_json::json!({}))
            .run_id("tl:3"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(ran.lock().unwrap().len(), 2);
}
