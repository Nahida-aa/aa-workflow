//! 对齐上游 `packages/workflow-core/tests/middleware.test.ts` 的四条基础用例。
//!
//! 上游原文（`learn_ls/workflow/packages/workflow-core/tests/middleware.test.ts`）：
//!
//! 1. extends ctx with middleware-added fields
//! 2. composes multiple middlewares in order, accumulating ctx fields
//! 3. wraps the handler so middleware can run code before AND after
//! 4. rejects calling next() more than once in a middleware
//!
//! 本文件逐条对应，用例名保持英文原文，便于与上游并排阅读。Rust 侧的形状差异
//! （读扩展走 `ctx.ext.xxx()` 而不是 `ctx.xxx` 字段）写在每个用例的注释里。

use std::sync::{Arc, Mutex};

use aa_workflow_core::define::{DefExt, ExtNamespace};
use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs};
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, StepCtx, WorkflowCtx, WorkflowEvent,
};
use serde_json::{json, Value};

// ── 两个「外部开发者」各自定义的扩展命名空间 ──────────────────

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct User {
    id: String,
    name: String,
}

struct UserNs;
impl ExtNamespace for UserNs {
    const KEY: &'static str = "user";
    type Value = User;
}
aa_workflow_core::impl_ext_accessors!(UserNs => UserAccess, user, try_user);

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct Counts {
    a: i64,
    b: i64,
}

struct CountsNs;
impl ExtNamespace for CountsNs {
    const KEY: &'static str = "counts";
    type Value = Counts;
}
aa_workflow_core::impl_ext_accessors!(CountsNs => CountsAccess, counts, try_counts);

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct Extra {
    b: i64,
}

/// 第二个命名空间：模拟「来自另一个开发者」的中间件扩展。
struct ExtraNs;
impl ExtNamespace for ExtraNs {
    const KEY: &'static str = "extra";
    type Value = Extra;
}
aa_workflow_core::impl_ext_accessors!(ExtraNs => ExtraAccess, extra, try_extra);

/// 直接返回 `Middleware<(), Ext>` 的写法（中间件作者固定扩展类型）。
fn require_user() -> Middleware {
    Middleware::new().server(|mut sc: MiddlewareServerFnArgs| async move {
        sc.next(Some(
            DefExt::<UserNs>::new(User {
                id: "u-1".into(),
                name: "Alice".into(),
            })
            .into_value(),
        ))
        .await
    })
}

/// 对齐上游用例 1：`extends ctx with middleware-added fields`
///
/// 上游 `ctx.user.id`；Rust `ctx.ext.user().id`（命名空间由中间件包定义，
/// 与 `@tanstack/ai-orchestration` 的 `ctx.ai` 同构）。
#[tokio::test]
async fn extends_ctx_with_middleware_added_fields() {
    let wf = create_workflow(CreateWorkflowConfig::new("mw-extends").input::<Value>())
        .middleware(require_user())
        .handler(|ctx: WorkflowCtx| async move {
            let user = ctx.ext.user();
            Ok(json!({ "userId": user.id, "userName": user.name }))
        });

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
    let output = events.iter().find_map(|e| match e {
        WorkflowEvent::RunFinished { output, .. } => Some(output.clone()),
        _ => None,
    });
    assert_eq!(
        output.flatten(),
        Some(json!({ "userId": "u-1", "userName": "Alice" }))
    );
}

/// 对齐上游用例 2：`composes multiple middlewares in order, accumulating ctx fields`
///
/// 上游 m1/m2 各加一个字段、handler `ctx.a + ctx.b`。
/// Rust：两个**不同命名空间**（来自不同开发者）各写各的 JSON key，handler 分别读
/// `ctx.ext.counts()` / `ctx.ext.extra()` 求和——使用者不需要定义任何「合并后的类型」
/// （0015 的核心场景）。与上游的一个差异：中间件**之间**不通过 `sc.ctx.ext` 读前序
/// 扩展（那是 handler 才有的能力，见本用例末尾说明），所以 m2 不依赖 m1 的值。
#[tokio::test]
async fn composes_multiple_middlewares_in_order_accumulating_ctx_fields() {
    // m1 写 counts = { a: 1, b: 0 }
    let m1 = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| async move {
        sc.next(Some(
            DefExt::<CountsNs>::new(Counts { a: 1, b: 0 }).into_value(),
        ))
        .await
    });

    // m2 写自己的 extra = { b: 10 }（独立命名空间，来自另一个开发者）
    // 与上游用例 2 的差异：上游 m2 读前序 `ctx.a`；本仓 `sc.ctx.ext` 是最早构造的
    // 快照，读不到 m1 经 next(ext) 写进 engine.ext_extra 的值——只有 handler 的
    // ctx 会在调用时重读 ext_extra。故 m2 不读前序扩展（见 ADR 0015 的能力边界）。
    let m2 = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| async move {
        sc.next(Some(DefExt::<ExtraNs>::new(Extra { b: 10 }).into_value()))
            .await
    });

    let wf = create_workflow(CreateWorkflowConfig::new("mw-chain").input::<Value>())
        .middleware(m1).middleware(m2)
        .handler(|ctx: WorkflowCtx| async move {
            // 两个来源的扩展分别读，无需合并类型
            let sum = ctx.ext.counts().a + ctx.ext.extra().b;
            Ok(json!({ "sum": sum }))
        });

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .collect()
    .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
    let output = events.iter().find_map(|e| match e {
        WorkflowEvent::RunFinished { output, .. } => Some(output.clone()),
        _ => None,
    });
    assert_eq!(output.flatten(), Some(json!({ "sum": 11 })));
}

/// 对齐上游用例 3：`wraps the handler so middleware can run code before AND after`
#[tokio::test]
async fn wraps_the_handler_so_middleware_can_run_code_before_and_after() {
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log = events.clone();

    let m1 = Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let log = log.clone();
        async move {
            log.lock().unwrap().push("m1-before".into());
            let out = sc.next(None).await;
            log.lock().unwrap().push("m1-after".into());
            out
        }
    });

    let log2 = events.clone();
    let wf = create_workflow(CreateWorkflowConfig::new("mw-wrap").input::<Value>())
        .middleware(m1)
        .handler(move |_ctx: WorkflowCtx| {
            let log = log2.clone();
            async move {
                log.lock().unwrap().push("handler".into());
                Ok(json!({}))
            }
        });

    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .collect()
    .await
    ;

    assert_eq!(
        *events.lock().unwrap(),
        vec!["m1-before", "handler", "m1-after"]
    );
}

/// 对齐上游用例 4：`rejects calling next() more than once in a middleware`
///
/// 上游断言 `RUN_ERRORED` 的 message 匹配 `/at most once/`；这里断言
/// run 变 `Errored` 且 message 含同样的字串。
#[tokio::test]
async fn rejects_calling_next_more_than_once_in_a_middleware() {
    let broken = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| async move {
        sc.next(None).await?;
        sc.next(None).await // second call — should error
    });

    let wf = create_workflow(CreateWorkflowConfig::new("mw-broken").input::<Value>())
        .middleware(broken)
        .handler(|_ctx: WorkflowCtx| async move { Ok(json!({})) });

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunErrored { .. })));
    let err = events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::RunErrored { error, .. } => Some(error.clone()),
            _ => None,
        })
        .expect("应记录错误");
    assert!(
        err.message.contains("at most once"),
        "错误信息应含 `at most once`，实际: {}",
        err.message
    );
}

/// 中间件直读 event 日志（`sc.get_events(&sc.ctx.run_id)`）。
///
/// 引擎自己持有 store 与 `run_id`，中间件不必在构造期注入 `Arc<GetEvents>`
/// （那种注入仍合法，但不是唯一路径）。
/// 上游无对应方法：TS 的 engine 藏在闭包里，中间件只能自己捕获 store。
///
/// 验证点：读到的就是引擎用的那个 store，且只含**本次 drive 之前已落盘**的事件。
#[tokio::test]
async fn middleware_reads_the_run_event_log() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    // 预置一条 checkpoint：先于本次 drive，中间件应能读到它。
    store
        .append_event(
            "mw-events:1",
            0,
            &WorkflowEvent::StepFinished {
                ts: 1,
                step_id: "pre".into(),
                result: Some(json!(1)),
                attempts: Vec::new(),
                meta: None,
            },
        )
        .await
        .unwrap();

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let mw = Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let log = log.clone();
        async move {
            let ids: Vec<String> = sc
                .get_events(&sc.ctx.run_id)
                .await?
                .iter()
                .filter_map(|e| match e {
                    WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.clone()),
                    _ => None,
                })
                .collect();
            log.lock().unwrap().extend(ids);
            sc.next(None).await
        }
    });

    let wf = create_workflow(CreateWorkflowConfig::new("mw-events").input::<Value>())
        .middleware(mw)
        .handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async { Ok(json!(2)) })
                .await
        });

    let events = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store)
            .input(json!({}))
            .run_id("mw-events:1"),
    )
    .collect()
    .await;

    assert!(events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })));
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["pre".to_string()],
        "中间件应读到引擎 store 里已落盘的 checkpoint"
    );
}
