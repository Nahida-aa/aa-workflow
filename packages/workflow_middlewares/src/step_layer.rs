//! 把「一个判断函数」变成 tower `Layer` 的小工具。
//!
//! 三个内置策略的形状都是「看请求 → 决定 放行 / 短路 / 泊车」，差别只在判断
//! 逻辑。tower 的 `Service` 样板（`poll_ready`、泛型约束、future 装箱）写三遍
//! 没有意义，收成 [`step_layer`]。
//!
//! ```ignore
//! let layer = step_layer(move |req: StepRequest| {
//!     if req.step_id == "a" { Decision::Pass } else { Decision::Skip(cached) }
//! });
//! ```
use std::sync::Arc;

use aa_workflow_core::middleware::{
    StepRequest, StepService, StepServiceError,
};
use tower::{Layer, Service};

/// 一层对这个 step 的处置。
pub enum Decision {
    /// 正常执行（把请求交给内层）。
    Pass,
    /// 短路：不执行闭包，把 `result` 作为 checkpoint 落盘。
    Skip(serde_json::Value),
    /// 短路 + **导入溯源**：与 [`Skip`](Self::Skip) 相同，但 `STEP_FINISHED.meta`
    /// 里记下这条 checkpoint 来自哪个 run 的哪条事件。
    SkipImported {
        /// step 的结果。
        result: serde_json::Value,
        /// 出处。
        from: aa_workflow_core::event::ImportedFrom,
    },
    /// 在边界泊车（整条 drive 结束）。
    Park {
        signal: String,
        meta: Option<serde_json::Value>,
    },
}

/// 判断函数：拿到请求，返回处置。
pub type StepPolicy = Arc<dyn Fn(&StepRequest) -> Decision + Send + Sync>;

/// 用判断函数构造一层 tower `Layer`。
///
/// 与手写 `impl Service` 等价——纯包装层（超时、限流）照样可以用 tower 自带的。
pub fn step_layer(policy: impl Fn(&StepRequest) -> Decision + Send + Sync + 'static) -> PolicyLayer {
    PolicyLayer {
        policy: Arc::new(policy),
    }
}

/// [`step_layer`] 的产物。
#[derive(Clone)]
pub struct PolicyLayer {
    policy: StepPolicy,
}

impl Layer<StepService> for PolicyLayer {
    type Service = PolicyService;

    fn layer(&self, inner: StepService) -> Self::Service {
        PolicyService {
            inner,
            policy: self.policy.clone(),
        }
    }
}

/// [`PolicyLayer`] 包出来的 service。
#[derive(Clone)]
pub struct PolicyService {
    inner: StepService,
    policy: StepPolicy,
}

impl Service<StepRequest> for PolicyService {
    type Response = serde_json::Value;
    type Error = StepServiceError;
    type Future = aa_workflow_core::middleware::StepFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: StepRequest) -> Self::Future {
        match (self.policy)(&req) {
            Decision::Pass => {
                // 标准 tower：交给内层。
                let mut inner = self.inner.clone();
                Box::pin(async move {
                    std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
                    inner.call(req).await
                })
            }
            // 短路 / 泊车都需要 engine（落 checkpoint、pause）——请求里带着。
            Decision::Skip(result) => req.skip(result),
            Decision::SkipImported { result, from } => req.skip_with_meta(
                result,
                aa_workflow_core::event::StepMeta {
                    imported_from: Some(from),
                },
            ),
            Decision::Park { signal, meta } => req.park(signal, meta),
        }
    }
}
