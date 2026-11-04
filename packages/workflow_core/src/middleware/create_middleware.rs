//! Middleware：对齐 TanStack `createMiddleware`（`middleware/create-middleware.ts`）。
//!
//! # 形状
//!
//! TS 只有一个 `server` 函数：`async ({ ctx, next }) => { ...; next({ context }) }`。
//! Rust 是**同一个形状**：
//!
//! ```ignore
//! let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
//!     Box::pin(async move {
//!         sc.set_step_interceptor(intercept);                              // ① 拦截 step
//!         sc.next(Some(DefExt::<AuthNs>::new(auth).into_value())).await    // ② 扩展 + 推进
//!     })
//! });
//! ```
//!
//! # 类型对照
//!
//! | TS | Rust |
//! | --- | --- |
//! | `Middleware<TCtxIn, TExtension>` | [`Middleware`]（无泛型） |
//! | `MiddlewareServerFn<TCtxIn, TExtension>` | [`MiddlewareServerFn`] |
//! | `({ ctx, next }) => …` | [`MiddlewareServerFnArgs`]（`ctx` 字段 + `next` 方法） |
//! | `next({ context })` | [`MiddlewareServerFnArgs::next`]（一次调用：合并 + 推进） |
//! | `ctx.step = wrapped` | [`MiddlewareServerFnArgs::set_step_interceptor`] |
//! | `AnyMiddleware = Middleware<any, any>` | [`AnyMiddleware`]（别名，可混装） |
//!
//! # 为什么没有 `TCtxIn` / `TExtension` 两个泛型
//!
//! 上游的两个泛型都服务于**类型交集**：`TCtxIn` 让中间件读到前序扩展，
//! `TExtension` 让 handler 的 ctx 多出字段。
//!
//! Rust 里两者都不需要了：
//!
//! - 读扩展走 `ctx.ext.<ns>()`（[`ExtNamespace`](crate::define::ExtNamespace)），
//!   不需要把 ctx 窄化成「已带前序扩展的 ctx」；
//! - 写扩展走 `next(Some({ KEY: value }))`，handler 侧按命名空间取，
//!   **不需要任何合并类型**——使用者不必定义 `Combined`。
//!
//! 详细论证见 `docs/decisions/0015`。
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::define::{BoxFuture, WorkflowCtx};

/// 中间件 `server` 函数的签名（对齐上游 `MiddlewareServerFn`）。
pub type MiddlewareServerFn =
    Arc<dyn Fn(MiddlewareServerFnArgs) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// 后续管线：吃一个扩展对象、返回 future（对齐上游 `next(opts)`）。
///
/// `Some`/`take()` 承载「至多一次」，对齐上游的 `advanced` 标志。
pub type NextFn = Arc<dyn Fn(Option<Value>) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// `MiddlewareServerFn` 的入参对象 —— 上游那个**匿名**的函数参数
/// （`types.ts:458` 的 `(args: { ctx: TCtxIn; next: ... }) => ...`）。
///
/// `ctx` 是字段、`next` 是方法（借 `&mut self` 才能保证「至多一次」）。
pub struct MiddlewareServerFnArgs {
    /// 完整的 workflow ctx（`input` / `state` / `ext` / `meta`）。
    pub ctx: WorkflowCtx,
    /// 擦除层的完整 ctx（clone 廉价：内部全是 `Arc`）。
    pub(crate) erased: WorkflowCtx,
    /// 上游 `next` 参数的**实现**（不是 `next` 本身）。
    pub(crate) pipeline: Option<NextFn>,
}

impl MiddlewareServerFnArgs {
    /// 包裹 `ctx.step`：对**本次 drive 内每个真实执行的 step**包一层。
    ///
    /// 对齐 TS 的 `ctx.step = A(ctx.step)`——可多次注册，**先注册的在最外层**，
    /// 与中间件本身的洋葱顺序一致（见 `docs/decisions/0016`）。
    ///
    /// ```ignore
    /// let w: StepWrapper = Arc::new(|call: StepCall| {
    ///     Box::pin(async move {
    ///         if should_reuse(&call.step_id) {
    ///             return Ok(cached);       // 不调 next = 短路
    ///         }
    ///         call.next().await            // 正常推进
    ///     })
    /// });
    /// sc.push_step_wrapper(w);
    /// ```
    pub fn push_step_wrapper(&self, w: StepWrapper) {
        self.erased.engine.push_step_wrapper(w);
    }

    /// 推进到内层中间件与 handler —— 对齐上游 `next(opts)`（`run-workflow.ts:1502`）。
    ///
    /// 一次调用做两件事：`Object.assign(ctx, ext)` 与 `compose(index + 1)`。
    ///
    /// ```ignore
    /// let out = sc.next(Some(json!({ "user": "u1" }))).await?;   // TS: await next({ context: { user } })
    /// let out = sc.next(None).await?;                            // 不写扩展，直接推进
    /// ```
    ///
    /// # 为什么参数是 `Option<Value>`
    ///
    /// TS 的 `next` 参数必填（只能写 `next({ context: {} })`）。Rust 里 `None`
    /// 表示「这次不贡献扩展」，等价于传空对象，且不会往 `ext_extra` 塞空袋。
    ///
    /// # Errors
    ///
    /// 第二次调用报错（对齐 `next() must be called at most once per invocation`）。
    pub fn next(&mut self, ext: impl Into<Option<Value>>) -> BoxFuture<'static, Result<Value>> {
        let Some(next) = self.pipeline.take() else {
            return Box::pin(async move {
                anyhow::bail!("middleware.next() must be called at most once per invocation")
            });
        };
        next(ext.into())
    }

    /// 完全接管：跳过后续管线，直接给 output。
    ///
    /// Rust 特有（TS 丢弃 `m.server()` 的返回值）——「中间件就是 handler」时用，
    /// 例如 DAG 中间件。
    pub fn take_over(&mut self) {
        self.pipeline = None;
    }

    /// 擦除层的完整 ctx。
    pub fn erased(&self) -> &WorkflowCtx {
        &self.erased
    }
}

/// 一次 step 调用的上下文（wrapper 看到的）。
pub struct StepCall {
    /// 本次 step 的 id。
    pub step_id: String,
    /// 所属 run 的 id（策略按 run 去重时用）。
    pub run_id: String,
    /// Run 级参数（`RunWorkflowOptions.meta`，策略输入）。
    pub run_meta: Option<Value>,
    /// **本 drive 内已成功的 step 快照**（含本次之前的全部，不含本次）。
    ///
    /// 这是策略判断「哪些目标已达成」的唯一真值源——包装层因此**不需要**
    /// 自带可变状态（`Arc<Mutex<HashSet>>`），并发分支之间也就不会出现
    /// 「各记各的、互相看不见」。
    ///
    /// 数据来自引擎的 `live` 表（日志的内存投影），在本次调用构造时取快照。
    pub completed: std::sync::Arc<std::collections::HashSet<String>>,
    /// 进入下一层（含真实执行）。**不调即短路**。
    pub(crate) next: NextStep,
    /// 落 checkpoint / 泊车用。
    pub(crate) engine: std::sync::Arc<crate::engine::EngineRuntime>,
}

impl StepCall {
    /// 推进到下一层。**至多一次**——第二次调用报错。
    ///
    /// 不调 `next` 即为短路：后面的包装层与真实执行都不跑。
    pub fn next(self) -> BoxFuture<'static, Result<Value>> {
        self.next.take_and_call()
    }

    /// 短路并**落 checkpoint**：不执行闭包，把 `result` 作为本 step 的
    /// `STEP_FINISHED` 写入日志，随后原样返回。
    ///
    /// 这是「复用已有结果」（reseed / redo 之外的重放）的正确写法——
    /// 直接 `Ok(cached)` 只是让本次 drive 看到值，日志里没有记录，
    /// 续驱 / 并行分支会重新计算，产生不一致。
    pub fn skip(self, result: Value) -> BoxFuture<'static, Result<Value>> {
        let engine = self.engine;
        let step_id = self.step_id;
        Box::pin(async move {
            crate::engine::record_skipped_step(&engine, &step_id, result).await
        })
    }

    /// **在本 step 边界泊车**：落 `SIGNAL_AWAITED` + 投影 RunState，随后整条
    /// drive 以「已挂起」结束（调用方看到的是 `Err(WorkflowParked)`）。
    ///
    /// 不调 `next`——泊车意味着后续 step 与闭包都不执行。
    /// `signal_name` 用保留名（`__interim-goal` / `__timer` / `__approval`）时
    /// 由宿主侧续驱；自定义名字则等外部 `signal_run` 投递。
    pub fn park(self, signal_name: impl Into<String>, meta: Option<Value>) -> BoxFuture<'static, Result<Value>> {
        let engine = self.engine;
        let step_id = self.step_id;
        let signal_name = signal_name.into();
        Box::pin(async move {
            let _ = crate::engine::exec_pause_with(
                &engine,
                &step_id,
                &signal_name,
                "parked at a middleware step boundary",
                None,
                meta,
                None,
            )
            .await;
            Err(crate::engine::WorkflowParked.into())
        })
    }
}

/// `StepCall::next` 的实现（**至多一次**：用 `take()` 保证只推进一次）。
#[derive(Clone)]
pub(crate) struct NextStep(
    pub(crate) Arc<
        std::sync::Mutex<
            Option<Box<dyn FnOnce() -> BoxFuture<'static, Result<Value>> + Send + Sync>>,
        >,
    >,
);

impl NextStep {
    pub(crate) fn new(
        f: impl FnOnce() -> BoxFuture<'static, Result<Value>> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(std::sync::Mutex::new(Some(Box::new(f)))))
    }

    /// 取走并执行。第二次调用返回错误（对齐 `next()` 的至多一次语义）。
    pub(crate) fn take_and_call(&self) -> BoxFuture<'static, Result<Value>> {
        let taken = self.0.lock().ok().and_then(|mut g| g.take());
        match taken {
            Some(f) => f(),
            None => Box::pin(async {
                anyhow::bail!("step wrapper called next() more than once for the same step")
            }),
        }
    }
}

/// step 包装层：返回本次 step 的结果。**可叠加**，先注册的在最外层。
///
/// 替代旧的单值 `StepInterceptor`（见 `docs/decisions/0016`）：
/// - 短路（原 `Skip`）= 不调 `next`，直接返回缓存值；
/// - 泊车（原 `Park`）= 不调 `next`，改调引擎的 pause 原语后返回 `WorkflowParked`。
pub type StepWrapper =
    Arc<dyn Fn(StepCall) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// 中间件（对齐 TS `Middleware`：一个 server 函数）。
///
/// 不含泛型：扩展走 `next(Some({ KEY: value }))` 写入、`ctx.ext.<ns>()` 读取，
/// 所以「中间件类型 → handler ctx 类型」这条编译期通道在 Rust 里不需要
/// （见模块文档「为什么没有两个泛型」）。
pub struct Middleware {
    pub server: MiddlewareServerFn,
}

impl Clone for Middleware {
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
        }
    }
}

impl Default for Middleware {
    /// 空中间件：**直接透传**（对齐 TS「不设 server 则什么也不做」）。
    fn default() -> Self {
        Self {
            server: Arc::new(|mut sc: MiddlewareServerFnArgs| {
                Box::pin(async move { sc.next(None).await })
            }),
        }
    }
}

impl Middleware {
    /// 空中间件：**直接透传**。
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the server function（对齐 TS 的 `createMiddleware().server(fn)`）。
    ///
    /// ```ignore
    /// let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
    ///     Box::pin(async move { sc.next(None).await })
    /// });
    /// ```
    pub fn server<F>(mut self, f: F) -> Self
    where
        F: Fn(MiddlewareServerFnArgs) -> BoxFuture<'static, Result<Value>> + Send + Sync + 'static,
    {
        self.server = Arc::new(f);
        self
    }
}

/// 类型已擦除的中间件 —— 对齐上游 `AnyMiddleware = Middleware<any, any>`。
///
/// 因为 [`Middleware`] 已经没有类型参数，这就是它本身；保留别名是为了让
/// `Vec<AnyMiddleware>` / `[AnyMiddleware; N]` 的调用点读起来与上游一致。
pub type AnyMiddleware = Middleware;
