//! 声明了 meta 需求的 workflow，`meta` 少提供字段 → 编译失败。
//!
//! 这是「忘了传值也编译报错」的锁定测试：如果哪天校验失效、这段能编过，
//! trybuild 会报错提醒。
use std::sync::Arc;

use aa_workflow_core::define::{MetaBag, MetaKey, Nil};
use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs};
use aa_workflow_core::{
    create_workflow, CreateWorkflowConfig, InMemoryStore, RunStore, RunWorkflowOptions,
    WorkflowCtx,
};

#[derive(Clone)]
struct LoopLimitKey;
impl MetaKey for LoopLimitKey {
    const KEY: &'static str = "__loopLimit";
    type Value = u32;
}

fn main() {
    let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move { sc.next(None).await })
    });
    let wf = create_workflow(CreateWorkflowConfig::new("need-meta").input::<serde_json::Value>())
        .meta_middleware(aa_workflow_core::define::MetaMiddleware::<LoopLimitKey>::new(mw))
        .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!({})) });

    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    // ← 这里应当编译失败：workflow 要求 __loopLimit，但 meta 是空的
    let _ = RunWorkflowOptions::new(wf, store)
        .input(serde_json::json!({}))
        .meta(MetaBag::<Nil>::new());
}
