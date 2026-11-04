//! 验证 step 包装层的**叠加**：这是把单值拦截器改成洋葱链的目的。
//!
//! 旧实现只有一个槽位——`reseed`（复用）与 `interim_goals`（泊车）同时装，
//! 后装的静默覆盖先装的。改成链后两层都在，**注册顺序决定嵌套层级**，
//! 谁说了算可从代码直接读出。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::middleware::{
    Middleware, MiddlewareServerFnArgs, StepCall, StepWrapper,
};
use aa_workflow_core::{
    create_workflow, event::WorkflowEvent, run_workflow, CreateWorkflowConfig, InMemoryStore,
    RunStore, RunWorkflowOptions, WorkflowCtx,
};

/// 装一个只做观测的包装层，记录每次 before / after。
fn observer(tag: &'static str, log: Arc<Mutex<Vec<String>>>) -> Middleware {
    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let log = log.clone();
        Box::pin(async move {
            let wrapper: StepWrapper = Arc::new(move |call: StepCall| {
                let log = log.clone();
                Box::pin(async move {
                    let step_id = call.step_id.clone();
                    log.lock().unwrap().push(format!("{tag}:before:{step_id}"));
                    let r = call.next().await;
                    log.lock().unwrap().push(format!("{tag}:after:{step_id}"));
                    r
                })
            });
            sc.push_step_wrapper(wrapper);
            sc.next(None).await
        })
    })
}

/// 装一个对指定 step 短路的包装层。
fn skipper(id: &'static str, value: &'static str) -> Middleware {
    Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move {
            let wrapper: StepWrapper = Arc::new(move |call: StepCall| {
                Box::pin(async move {
                    if call.step_id == id {
                        return call.skip(serde_json::json!(value)).await;
                    }
                    call.next().await
                })
            });
            sc.push_step_wrapper(wrapper);
            sc.next(None).await
        })
    })
}

fn workflow(
    middlewares: Vec<Middleware>,
    ran: Arc<Mutex<Vec<String>>>,
) -> aa_workflow_core::define::WorkflowDefinition<
    serde_json::Value,
    serde_json::Value,
    serde_json::Value,
> {
    create_workflow(CreateWorkflowConfig::new("step-onion").input::<serde_json::Value>())
        .middleware(middlewares)
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
                            Ok(serde_json::json!(sid))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::json!({ "ok": true }))
            }
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_wrappers_stack_in_registration_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let ran = Arc::new(Mutex::new(Vec::new()));

    let wf = workflow(
        vec![
            observer("outer", log.clone()),
            observer("inner", log.clone()),
        ],
        ran.clone(),
    );
    let store = Arc::new(InMemoryStore::new());
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("onion:order"),
    )
    .outcome()
    .await
    .unwrap();

    // 先注册的在最外层：outer:before → inner:before → inner:after → outer:after
    let a: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.ends_with(":a"))
        .cloned()
        .collect();
    assert_eq!(
        a,
        vec![
            "outer:before:a".to_string(),
            "inner:before:a".to_string(),
            "inner:after:a".to_string(),
            "outer:after:a".to_string(),
        ],
        "洋葱：先注册的在外层"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inner_skip_short_circuits_base_but_outer_still_wraps() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let ran = Arc::new(Mutex::new(Vec::new()));

    // outer 观测；inner 对 b 短路。
    let wf = workflow(
        vec![observer("outer", log.clone()), skipper("b", "B-CACHED")],
        ran.clone(),
    );
    let store = Arc::new(InMemoryStore::new());
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("onion:skip"),
    )
    .outcome()
    .await
    .unwrap();

    // b 的闭包没跑
    assert_eq!(
        ran.lock().unwrap().clone(),
        vec!["a".to_string(), "c".to_string()],
        "inner 短路 → b 的闭包不执行"
    );
    // 但 outer 仍然包住了它（after 也在）
    let b: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.ends_with(":b"))
        .cloned()
        .collect();
    assert_eq!(
        b,
        vec!["outer:before:b".to_string(), "outer:after:b".to_string()],
        "外层包装层照常执行——短路只跳过内层"
    );
    // 短路仍然落 checkpoint（replay / 续驱可复用）
    let events = store.get_events("onion:skip").unwrap();
    assert!(
        events.iter().any(|e| matches!(
            e,
            WorkflowEvent::StepFinished { step_id, result: Some(r), .. }
                if step_id == "b" && r == &serde_json::json!("B-CACHED")
        )),
        "skip 结果必须作为 b 的 STEP_FINISHED 落盘"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrapper_order_decides_who_wins() {
    let counter = Arc::new(AtomicUsize::new(0));

    // 两个包装层都想对 b 短路：**先注册的在外层**，所以它先决定。
    let mk = |value: &'static str| -> Middleware {
        let counter = counter.clone();
        Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
            let counter = counter.clone();
            Box::pin(async move {
                let wrapper: StepWrapper = Arc::new(move |call: StepCall| {
                    let counter = counter.clone();
                    Box::pin(async move {
                        if call.step_id != "b" {
                            return call.next().await;
                        }
                        counter.fetch_add(1, Ordering::SeqCst);
                        call.skip(serde_json::json!(value)).await
                    })
                });
                sc.push_step_wrapper(wrapper);
                sc.next(None).await
            })
        })
    };

    let store = Arc::new(InMemoryStore::new());
    let wf = workflow(vec![mk("FIRST"), mk("SECOND")], Arc::new(Mutex::new(Vec::new())));
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("onion:win"),
    )
    .outcome()
    .await
    .unwrap();

    let events = store.get_events("onion:win").unwrap();
    let b_result = events.iter().find_map(|e| match e {
        WorkflowEvent::StepFinished { step_id, result, .. } if step_id == "b" => result.clone(),
        _ => None,
    });
    assert_eq!(
        b_result,
        Some(serde_json::json!("FIRST")),
        "先注册的在外层 → 它的短路生效"
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "只有一个包装层真的短路了（外层决定后内层没机会）"
    );
}
