//! meta 的类型检查（`docs/decisions/0017`）。
//!
//! # 这里能测什么、不能测什么
//!
//! 「**忘了传 meta 会编译失败**」属于编译期错误，运行期测试锁不住。它由
//! [`meta_is_required_by_declaring_middleware`] 的 `compile_fail` 文档测试锁定。
//!
//! 这里测的是**正向路径**：声明了 meta 的 workflow 配上正确的 `MetaBag`，
//! 运行时值确实到达 `ctx.meta`；以及擦除态不约束 meta。

use std::sync::Arc;

use aa_workflow_core::define::{Cons, MetaBag, MetaKey, Nil};
use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs};
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, StepCtx, WorkflowCtx,
};

/// 一个「消费 meta」的中间件包会定义的 key。
#[derive(Clone)]
pub struct LoopLimitKey;
impl MetaKey for LoopLimitKey {
    const KEY: &'static str = "__loopLimit";
    type Value = u32;
}
aa_workflow_core::impl_meta_accessors!(LoopLimitKey => LoopLimitAccess, loop_limit, try_loop_limit);

/// 声明它消费 `__loopLimit` 的中间件。
fn limiter() -> aa_workflow_core::define::MetaMiddleware<LoopLimitKey> {
    let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move {
            // 中间件读 meta：可能没传，用 try_
            let limit = sc.ctx.meta.try_loop_limit();
            let extra = limit.map(|l| serde_json::json!({ "__seen": l }));
            sc.next(extra).await
        })
    });
    aa_workflow_core::define::MetaMiddleware::new(mw)
}

fn wf_with_meta() -> aa_workflow_core::define::WorkflowDefinition<
    serde_json::Value,
    serde_json::Value,
    serde_json::Value,
    Cons<LoopLimitKey, Nil>,
> {
    create_workflow(CreateWorkflowConfig::new("meta-typed").input::<serde_json::Value>())
        .meta_middleware(limiter())
        .handler(|ctx: WorkflowCtx| async move {
            // handler 读 meta：编译期已保证有值，不需要 Option
            let limit = ctx.meta.loop_limit();
            Ok(serde_json::json!({ "limit": limit }))
        })
}

#[tokio::test]
async fn declared_meta_is_required_and_typed() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());

    // 正确写法：workflow 要 LoopLimit，meta 就提供它
    let opts = RunWorkflowOptions::new(wf_with_meta(), store.clone())
        .input(serde_json::json!({}))
        .meta(MetaBag::<Nil>::new().set::<LoopLimitKey>(7))
        .run_id("meta-typed:1");

    let out = run_workflow(opts).outcome().await.unwrap();
    assert_eq!(
        out.output,
        Some(serde_json::json!({ "limit": 7 })),
        "handler 读到的应当是调用方提供的值"
    );
}

#[tokio::test]
async fn erased_workflow_does_not_constrain_meta() {
    // 擦除态：声明已在擦除时丢失，meta 不受约束（可以空）
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let wf = create_workflow(
        CreateWorkflowConfig::new("meta-erased").input::<serde_json::Value>(),
    )
    .handler(|ctx: WorkflowCtx| async move {
        let limit = ctx.meta.try_loop_limit();
        Ok(serde_json::json!({ "limit": limit }))
    });

    let opts = RunWorkflowOptions::new(Arc::new(wf), store.clone())
        .input(serde_json::json!({}))
        .run_id("meta-erased:1");

    let out = run_workflow(opts).outcome().await.unwrap();
    assert_eq!(out.output, Some(serde_json::json!({ "limit": null })));
}

/// **编译期保证**：声明了 meta 的 workflow，`meta` 少提供字段就不通过。
///
/// 这里的 workflow 需要 `LoopLimitKey`，但 `meta` 是空的 `MetaBag<Nil>`。
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use aa_workflow_core::define::{MetaBag, MetaKey, Nil};
/// # use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs};
/// # use aa_workflow_core::{
/// #     create_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
/// #     RunWorkflowOptions, WorkflowCtx,
/// # };
/// # struct LoopLimitKey;
/// # impl MetaKey for LoopLimitKey {
/// #     const KEY: &'static str = "__loopLimit";
/// #     type Value = u32;
/// # }
/// let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
///     Box::pin(async move { sc.next(None).await })
/// });
/// let wf = create_workflow(
///     CreateWorkflowConfig::new("need-meta").input::<serde_json::Value>(),
/// )
/// .meta_middleware(aa_workflow_core::define::MetaMiddleware::<LoopLimitKey>::new(mw))
/// .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!({})) });
///
/// let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
/// // ← 这里应当编译失败：meta 是空的，但 workflow 要求 __loopLimit
/// let _ = RunWorkflowOptions::new(wf, store)
///     .input(serde_json::json!({}))
///     .meta(MetaBag::<Nil>::new());
/// ```
#[allow(dead_code)]
fn meta_is_required_by_declaring_middleware() {}
