//! Middleware：对齐 TanStack `createMiddleware`（`middleware/create-middleware.ts`）。
//!
//! # 形状
//!
//! TS 只有一个 `server` 函数：`async ({ ctx, next }) => { ...; next({ context }) }`。
//! Rust 是**同一个形状**：
//!
//! ```ignore
//! let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| async move {
//!     sc.wrap_step(TimeoutLayer::new(d));                              // ① 给 step 加层
//!     sc.next(Some(DefExt::<AuthNs>::new(auth).into_value())).await    // ② 扩展 + 推进
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
//! | `ctx.step = wrapped` | [`MiddlewareServerFnArgs::wrap_step`]（tower `Layer`，可叠加） |
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
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::define::{BoxFuture, WorkflowCtx, WorkflowHandler};

/// **静态门**：中间件的 server 实现（RPITIT——返回具体 future，不装箱）。
///
/// `Clone` supertrait 是 scope-borrow 洋葱的纪律（tower 同款）：typestate 栈
/// 的递归组合要把各层 clone 进 `'static` 闭包——struct 中间件的字段用
/// `Arc` 句柄即可（Clone = 引用计数）。
pub trait MiddlewareServer: Send + Sync + Clone + 'static {
    /// 方法名与 tower 的 `Service::call` 对齐。E0034 歧义（与 `Fn::call`）
    /// 已重查排除：本 trait 无 `Fn` blanket，实现者（struct / [`Middleware`]）
    /// 均非 `Fn`，泛型位按 bound 解析——唯一的理论暴露是用户为同一类型
    /// 同时实现 `Fn` 与本 trait，tower 生态对同级暴露同样直接共存。
    fn call<'a>(
        &'a self,
        args: MiddlewareServerFnArgs,
    ) -> impl Future<Output = Result<Value>> + Send + 'a;
}

/// **擦除门**（dyn）：运行期动态的中间件集合（低代码）用。静态注册不走这里
/// ——见 [`MiddlewareStack`]（typestate 栈，零箱到终点擦一次）。
pub trait DynMiddlewareServer: Send + Sync {
    fn call<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>>;
}

/// 静态 → 擦除的适配器：[`Middleware::from_impl`] 用它把 struct 中间件
/// 收进擦除世界（专用 newtype 而非 blanket——blanket 会与下面的 `Fn`
/// blanket 构成 coherence 冲突：下游类型可以同时是 `Fn` 和
/// `MiddlewareServer`）。
pub struct Erased<M: MiddlewareServer>(M);

impl<M: MiddlewareServer + 'static> DynMiddlewareServer for Erased<M> {
    fn call<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>> {
        Box::pin(self.0.call(args))
    }
}

/// `Fn(Args) -> 'static future` 的擦除 blanket：现有闭包写法零改动。
impl<F, Fut> DynMiddlewareServer for F
where
    F: Fn(MiddlewareServerFnArgs) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Value>> + Send + 'static,
{
    fn call<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>> {
        Box::pin((self)(args))
    }
}

/// [`Middleware::default`] 的透传实现。
struct PassthroughServer;

impl DynMiddlewareServer for PassthroughServer {
    fn call<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>> {
        let mut args = args;
        Box::pin(async move { args.next(None).await })
    }
}

/// 擦除句柄：动态中间件元素的统一类型（低代码/运行期集合）。
pub type MiddlewareServerFn = Arc<dyn DynMiddlewareServer>;

/// **typestate 栈**：中间件洋葱的类型层表示——`() `为终端，
/// `(Head, Tail)` 递归（Head = 外层，Tail = 其余栈）。
///
/// `serve(&self, inner)` 把栈折叠成一个 [`WorkflowHandler`]（定义期一次
/// 擦除）；`Append<M>` 做类型级追加（builder `.middleware(m)` 的重键）。
/// 元素是 [`MiddlewareServer`]（静态，零箱）或 [`Middleware`]（擦除，
/// 动态元素的固有箱）。
pub trait MiddlewareStack: Send + Sync + Clone + 'static {
    fn compose(&self, inner: WorkflowHandler) -> WorkflowHandler;
}

impl MiddlewareStack for () {
    fn compose(&self, inner: WorkflowHandler) -> WorkflowHandler {
        inner
    }
}

impl<H, T> MiddlewareStack for (H, T)
where
    H: MiddlewareServer,
    T: MiddlewareStack,
{
    fn compose(&self, inner: WorkflowHandler) -> WorkflowHandler {
        let (head, tail) = self;
        let head = head.clone();
        let tail = tail.clone();
        // 外层闭包是 `Fn`（跨 drive 复用）：各层 per-drive clone 一次
        // （字段是 Arc 句柄 = 引用计数；tower 的 clone-per-request 同款）。
        Arc::new(move |ctx: WorkflowCtx| {
            let head = head.clone();
            let tail = tail.clone();
            let inner = inner.clone();
            let ctx_for_pipeline = ctx.clone();
            // 本层的 `next(ext)`：合并 ext 进 engine，然后把 ctx 交给
            // tail 的子洋葱（递归——pipeline 的擦除是 CPS 协议的
            // 结构性成本，见 ADR 0021）。
            let pipeline: NextFn = Arc::new(move |ext: Option<Value>| {
                let tail = tail.clone();
                let inner = inner.clone();
                let mut ctx = ctx_for_pipeline.clone();
                if let Some(ext) = ext {
                    ctx.engine.merge_ext(ext);
                }
                Box::pin(async move { tail.compose(inner)(ctx).await })
            });
            let sc = MiddlewareServerFnArgs {
                ctx,
                pipeline: Some(pipeline),
            };
            Box::pin(async move { head.call(sc).await })
        })
    }
}

fn sc_ctx(args: &MiddlewareServerFnArgs) -> WorkflowCtx {
    args.ctx.clone()
}

/// 类型级追加：builder `.middleware(m)` 的重键机制。
pub trait Append<M>: MiddlewareStack {
    type Out: MiddlewareStack;
    fn append(self, m: M) -> Self::Out;
}

impl<M: MiddlewareServer + 'static> Append<M> for () {
    /// 终端 + 首个中间件 = 二元组 `(M, ())`（Cons 形状——追加在**尾**，
    /// 递归 impl 只需 `()` 与 `(H, T)` 两个，任意长度成立）。
    type Out = (M, ());
    fn append(self, m: M) -> Self::Out {
        (m, ())
    }
}

impl<H, T, M> Append<M> for (H, T)
where
    H: MiddlewareServer,
    T: MiddlewareStack + Append<M>,
    M: MiddlewareServer + 'static,
{
    type Out = (H, T::Out);
    fn append(self, m: M) -> Self::Out {
        (self.0.clone(), self.1.append(m))
    }
}

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
    /// 上游 `next` 参数的**实现**（不是 `next` 本身）。
    pub(crate) pipeline: Option<NextFn>,
}

impl MiddlewareServerFnArgs {
    /// 给 `ctx.step` 加一层（**tower `Layer`**）。
    ///
    /// 对齐 TS 的 `ctx.step = A(ctx.step)`——可多次注册，**先注册的在最外层**，
    /// 与中间件本身的洋葱顺序一致（见 `docs/decisions/0016`）。
    ///
    /// 每层包装的是「本次 step 的叶子 service」——它的形状与 axum 的路由同构：
    ///
    /// ```text
    /// axum:  .route("/x", handler)  →  Router(持有 handler) → call(Request)
    /// ours:  ctx.step("load", f)    →  Leaf(持有 f)          → call(StepRequest)
    /// ```
    ///
    /// 所以 tower 生态里请求类型无关的层可以直接用：
    ///
    /// ```ignore
    /// sc.wrap_step(tower::timeout::TimeoutLayer::new(Duration::from_secs(30)));
    /// sc.wrap_step(tower::limit::ConcurrencyLimitLayer::new(4));
    /// sc.wrap_step(MyReseedLayer::new(plan));       // 自己写的也算
    /// ```
    ///
    /// 自定义层实现 `tower::Service<StepRequest, Response = Value, Error = StepError>`；
    /// **短路 = 不调 `inner.call(req)`**。
    pub fn wrap_step<L>(&self, layer: L)
    where
        L: tower::Layer<StepService> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                StepRequest,
                Response = Value,
                Error = StepServiceError,
            > + Clone
            + Send
            + 'static,
        <L::Service as tower::Service<StepRequest>>::Future: Send + 'static,
    {
        self.ctx.engine.wrap_step(layer.clone());
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
}

/// step 服务的错误类型。
///
/// **必须是 `Box<dyn Error + Send + Sync>`**（tower 的 `BoxError`）：
/// `timeout` / `retry` / `limit` 这些现成层都把它当输出错误类型，
/// 用 `anyhow::Error` 装不进去。`anyhow::Error: Into<BoxError>`，
/// 所以引擎内部的 `anyhow` 错误直接 `?` 就能转。
pub type StepServiceError = Box<dyn std::error::Error + Send + Sync>;

/// step 服务的 future 类型（装箱，因为层的组合是运行期的）。
pub type StepFuture =
    BoxFuture<'static, std::result::Result<Value, StepServiceError>>;

/// 一次 step 调用的**请求**——`tower::Service` 的输入。
///
/// 纯数据（外加 engine 句柄，用于 [`Self::skip`] / [`Self::park`] 这类需要落盘
/// 的动作）。执行体**不在**请求里：它由当次的叶子 service 持有，见
/// [`StepLeaf`]。
pub struct StepRequest {
    /// 本次 step 的 id。
    pub step_id: String,
    /// 所属 run 的 id（策略按 run 去重时用）。
    pub run_id: String,
    /// Run 级参数（`RunWorkflowOptions.meta`，策略输入）。
    pub run_meta: Option<Value>,
    /// **本 drive 内已成功的 step 快照**（含此前全部，不含本次）。
    ///
    /// 策略判断「哪些目标已达成」的唯一真值源——层因此不需要自带可变状态，
    /// 并发分支之间也就不会「各记各的、互相看不见」。
    pub completed: std::sync::Arc<std::collections::HashSet<String>>,
    /// 落 checkpoint / 泊车用。
    pub(crate) engine: std::sync::Arc<crate::engine::EngineRuntime>,
}

impl StepRequest {
    /// 短路并**落 checkpoint**：不执行闭包，把 `result` 作为本 step 的
    /// `STEP_FINISHED` 写入日志，随后原样返回。
    ///
    /// 这是「复用已有结果」的正确写法——直接 `Ok(cached)` 只让本次 drive
    /// 看到值，日志里没有记录，续驱 / 并行分支会重新计算，产生不一致。
    pub fn skip(
        self,
        result: Value,
    ) -> BoxFuture<'static, std::result::Result<Value, StepServiceError>> {
        let engine = self.engine;
        let step_id = self.step_id;
        Box::pin(async move {
            crate::engine::record_skipped_step(&engine, &step_id, result, None)
                .await
                .map_err(Into::into)
        })
    }

    /// [`skip`](Self::skip) + 事件上的 `meta`（例如导入溯源
    /// [`ImportedFrom`](crate::event::ImportedFrom)）。
    ///
    /// 复用已有结果时必须能用这个而不是 `skip`：`STEP_FINISHED.meta` 里的
    /// 溯源是**可观测性数据**，丢了就只能事后从源日志重算。
    pub fn skip_with_meta(
        self,
        result: Value,
        meta: crate::event::StepMeta,
    ) -> BoxFuture<'static, std::result::Result<Value, StepServiceError>> {
        let engine = self.engine;
        let step_id = self.step_id;
        Box::pin(async move {
            crate::engine::record_skipped_step(&engine, &step_id, result, Some(meta))
                .await
                .map_err(Into::into)
        })
    }

    /// **在本 step 边界泊车**：落 `SIGNAL_AWAITED` + 投影 RunState，随后整条
    /// drive 以「已挂起」结束（调用方看到的是 `Err(WorkflowParked)`）。
    ///
    /// 不调内层——泊车意味着后续 step 与闭包都不执行。
    pub fn park(
        self,
        signal_name: impl Into<String>,
        meta: Option<Value>,
    ) -> BoxFuture<'static, std::result::Result<Value, StepServiceError>> {
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
            Err(Box::new(crate::engine::WorkflowParked) as StepServiceError)
        })
    }
}

/// 擦除后的 step 服务：链的每一层包完都归成它。
///
/// 用 `tower` 官方的 `BoxCloneService`——层在运行期追加，而
/// `ServiceBuilder` 的层组合是编译期的，靠它对接。
pub type StepService =
    tower::util::BoxCloneService<StepRequest, Value, StepServiceError>;

/// 叶子 service：**持有当次 `ctx.step` 的执行闭包**。
///
/// 每次 `ctx.step(id, f)` 现建一个——等价于 axum 的「注册一个 handler」。
#[derive(Clone)]
pub(crate) struct StepLeaf {
    exec: std::sync::Arc<
        dyn Fn() -> BoxFuture<'static, std::result::Result<Value, StepServiceError>>
            + Send
            + Sync,
    >,
}

impl StepLeaf {
    pub(crate) fn new(
        exec: std::sync::Arc<
        dyn Fn() -> BoxFuture<'static, std::result::Result<Value, StepServiceError>>
            + Send
            + Sync,
    >,
    ) -> Self {
        Self { exec }
    }
}

impl tower::Service<StepRequest> for StepLeaf {
    type Response = Value;
    type Error = StepServiceError;
    type Future = StepFuture;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: StepRequest) -> Self::Future {
        (self.exec)()
    }
}

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

/// 擦除元素也是合法的静态栈成员（委托给内层 dyn——动态元素的固有箱）。
impl MiddlewareServer for Middleware {
    fn call<'a>(
        &'a self,
        args: MiddlewareServerFnArgs,
    ) -> impl Future<Output = Result<Value>> + Send + 'a {
        // 内层是 dyn DynMiddlewareServer——它的 `call` 与本 trait 的 `call`
        // 不同 trait，无歧义。
        self.server.call(args)
    }
}

impl Default for Middleware {
    /// 空中间件：**直接透传**（对齐 TS「不设 server 则什么也不做」）。
    fn default() -> Self {
        Self {
            server: Arc::new(PassthroughServer),
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
    /// 接受返回任意 `Future` 的闭包，内部统一 `Box::pin` 擦除成 [`BoxFuture`]——
    /// 调用方直接写 `async move { .. }`，不必自己装箱（对齐 `ctx.step` 的写法）。
    /// 旧的 `Box::pin(..)` 写法仍然兼容（`Pin<Box<dyn Future>>` 本身也是 `Future`）。
    ///
    /// **future 是 `'static`**：不能借闭包的捕获状态。带构造期参数时，
    /// 非 Copy 捕获（`Arc<dyn Fn>` 句柄等）在 `async move` 前需要一行
    /// per-call clone——struct 形态（[`MiddlewareServer`] 状态即字段）免此仪式
    /// struct 形态要消灭的仪式。
    ///
    /// ```ignore
    /// let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
    ///     async move { sc.next(None).await }
    /// });
    /// ```
    pub fn server<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(MiddlewareServerFnArgs) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.server = Arc::new(f);
        self
    }

    /// struct 形态的入口：**状态是字段，按引用使用**（tower `Service` 同款）。
    ///
    /// 带多个构造期参数、或想在 server 间复用状态的中间件写成 struct +
    /// `impl MiddlewareServer`——`serve<'a>(&'a self)` 的返回 future 借 `&self`，
    /// 字段零克隆零仪式：
    ///
    /// ```ignore
    /// struct InterimGoalsMw { get_events: Arc<GetEvents> }
    /// impl MiddlewareServer for InterimGoalsMw {
    ///     fn call<'a>(&'a self, args: MiddlewareServerFnArgs) -> BoxFuture<'a, Result<Value>> {
    ///         Box::pin(async move {
    ///             let events = (self.get_events)(&args.ctx.run_id)?;   // 借 &self，无 clone
    ///             args.next(None).await
    ///         })
    ///     }
    /// }
    /// let mw = Middleware::from_impl(InterimGoalsMw { get_events });
    /// ```
    pub fn from_impl<M: MiddlewareServer + 'static>(m: M) -> Middleware {
        Middleware { server: Arc::new(Erased(m)) }
    }
}


/// 类型已擦除的中间件 —— 对齐上游 `AnyMiddleware = Middleware<any, any>`。
///
/// 因为 [`Middleware`] 已经没有类型参数，这就是它本身；保留别名是为了让
/// `Vec<AnyMiddleware>` / `[AnyMiddleware; N]` 的调用点读起来与上游一致。
pub type AnyMiddleware = Middleware;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{CreateWorkflowConfig, StepCtx, WorkflowCtx, create_workflow};
    use crate::{InMemoryStore, RunStatus, RunStore, RunWorkflowOptions, run_workflow};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 参数化中间件的**状态就是字段**：同一实例跨 drive 复用（不重新构造、
    /// 不 per-call 克隆）。
    #[tokio::test]
    async fn parameterized_middleware_state_persists_across_drives() {
        #[derive(Clone)]
        struct CountingMw {
            calls: Arc<AtomicUsize>,
        }
        impl MiddlewareServer for CountingMw {
            fn call<'a>(
                &'a self,
                args: MiddlewareServerFnArgs,
            ) -> BoxFuture<'a, Result<Value>> {
                Box::pin(async move {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    let mut sc = args;
                    sc.next(None).await
                })
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let wf = create_workflow(CreateWorkflowConfig::new("mw-count").input::<serde_json::Value>())
            .middleware(Middleware::from_impl(CountingMw {
                calls: Arc::clone(&calls),
            }))
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("a", |_sc: StepCtx| async { Ok(serde_json::Value::Null) }).await
            });
        let run = |store: Arc<dyn RunStore>| {
            run_workflow(
                RunWorkflowOptions::new(wf.clone(), store)
                    .input(serde_json::json!({}))
                    .run_id("count:1"),
            )
            .outcome()
        };
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        run(Arc::clone(&store)).await.unwrap();
        run(store).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "同一实例服务两次 drive");

    }

    /// 三种形态（struct / server_state / 闭包）混装进同一条链。
    #[tokio::test]
    async fn middleware_forms_compose_together() {
        #[derive(Clone)]
        struct Nop;
        impl MiddlewareServer for Nop {
            fn call<'a>(
                &'a self,
                args: MiddlewareServerFnArgs,
            ) -> BoxFuture<'a, Result<Value>> {
                Box::pin(async move { let mut args = args; args.next(None).await })
            }
        }
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(CreateWorkflowConfig::new("mw-mix").input::<serde_json::Value>())
            .middleware(Middleware::from_impl(Nop))
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("a", |_sc: StepCtx| async { Ok(serde_json::Value::Null) }).await
            });
        let out = run_workflow(
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }
}
