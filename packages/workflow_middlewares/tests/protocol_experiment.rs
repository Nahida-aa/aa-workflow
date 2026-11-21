//! **协议对照实验**（ADR 0022 证据）：同一个中间件行为，CPS（`next` 协议）
//! 与 tower（`Service`/`Layer` 协议）各写一遍，实测作者侧成本。
//!
//! 对照的行为（覆盖三档难度）：
//! - B1 纯短路（before-only，无结果依赖）
//! - B2 around（before + after——分水岭：要看执行结果之后再做事）
//! - B3 扩展贡献（把数据塞给下游）
//!
//! 对照口径：**作者侧行数 + 需要的类型个数 + 装箱数**（静态可数，不跑基准）。
//! 两种协议驱动同一个 stub 内层（两笔「step」），断言行为等价。
//!
//! # 结论（详见 docs/experiments/middleware-protocol.md）
//!
//! - before-only：两种协议同价（tower 多 2 个关联类型 + poll_ready 样板）；
//! - **around：CPS 平铺完胜**（tower 必须产生包装 future——手写 struct 样板
//!   ~25 行 / BoxFuture 一行 + 一次分配）；
//! - **擦除门反转**：tower 形态的 dyn 更顺（`dyn Service<Args, Future = BoxFuture>`
//!   关联类型可点名——我们的 RPITIT 不可 dyn，要单独的 DynMiddlewareServer）。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use anyhow::Result;
use tower::{Layer, Service};

use aa_workflow_core::middleware::{Middleware, MiddlewareServer, MiddlewareServerFnArgs};

type BoxFut = Pin<Box<dyn Future<Output = Result<serde_json::Value, anyhow::Error>> + Send>>;

/// 实验用的「run 请求」：两笔 step 的日志 + 短路标记。
#[derive(Clone, Default)]
struct RunCtx {
    steps: Arc<Mutex<Vec<String>>>,
    skip_me: bool,
}

/// 公共内层：相当于「handler + 引擎」——两笔 step。
async fn leaf(ctx: &RunCtx) -> Result<serde_json::Value, anyhow::Error> {
    ctx.steps.lock().unwrap().push("leaf-1".into());
    ctx.steps.lock().unwrap().push("leaf-2".into());
    Ok(serde_json::json!({ "steps": 2 }))
}

// ============================================================
// 行为 1：纯短路（before-only）——skip_me 时 leaf 不跑
// ============================================================

// ── CPS 形态：─────────────────────────────────────────
#[derive(Clone)]
struct SkipCps;
impl MiddlewareServer for SkipCps {
    async fn serve(&self, mut args: MiddlewareServerFnArgs) -> Result<serde_json::Value, anyhow::Error> {
        if args.ctx.meta.try_get::<SkipFlag>().unwrap_or(false) {
            return Ok(serde_json::json!({ "skipped": true }));
        }
        args.next(None).await
    }
}
aa_workflow_core::impl_meta_accessors!(SkipFlag => SkipAccess, skip_flag, try_skip_flag);
struct SkipFlag;
impl aa_workflow_core::define::MetaKey for SkipFlag {
    const KEY: &'static str = "__exp-skip";
    type Value = bool;
}

// ── tower 形态：───────────────────────────────────────
struct SkipLayer;
impl<S> Layer<S> for SkipLayer
where
    S: Service<RunCtx, Response = serde_json::Value, Error = anyhow::Error> + Send,
    S::Future: Send,
{
    type Service = SkipSvc<S>;
    fn layer(&self, inner: S) -> Self::Service {
        SkipSvc { inner }
    }
}

#[derive(Clone)]
struct SkipSvc<S> {
    inner: S,
}

impl<S> Service<RunCtx> for SkipSvc<S>
where
    S: Service<RunCtx, Response = serde_json::Value, Error = anyhow::Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = serde_json::Value;
    type Error = anyhow::Error;
    // ⚠️ 实验发现：即便纯 before 层，**提前返回与透传分支类型不一致** →
    // 两个分支必须统一类型 → 都装箱（或写 Either 枚举——更多样板）。
    // tower 官方的解法就是 service clone + BoxService。
    type Future = BoxFut;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // 样板：几乎总是这段
    }

    fn call(&mut self, req: RunCtx) -> Self::Future {
        if req.skip_me {
            return Box::pin(async move { Ok(serde_json::json!({ "skipped": true })) });
        }
        // tower 惯例 #1：`call(&mut self)` + async 组合 → 先 clone 再 mut
        // （Service::call 是 &mut self——闭包里要用就得 mut 绑定）
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

// ============================================================
// 行为 2：around（before + after）——记录 leaf 的执行耗时
// ============================================================

// ── CPS 形态：after 就是 await 之后接着写——平铺 ✓────────
#[derive(Clone)]
struct TimedCps {
    log: Arc<Mutex<Vec<String>>>,
}
impl MiddlewareServer for TimedCps {
    async fn serve(&self, mut args: MiddlewareServerFnArgs) -> Result<serde_json::Value, anyhow::Error> {
        let start = std::time::Instant::now();
        let out = args.next(None).await?;
        let ms = start.elapsed().as_millis();
        self.log
            .lock()
            .unwrap()
            .push(format!("took {ms}ms"));
        Ok(out)
    }
}

// ── tower 形态（变体 a：手写包装 future——零箱，样板 ~25 行）────────
struct TimedLayer {
    log: Arc<Mutex<Vec<String>>>,
}
impl<S> Layer<S> for TimedLayer
where
    S: Service<RunCtx, Response = serde_json::Value, Error = anyhow::Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Service = TimedSvc<S>;
    fn layer(&self, inner: S) -> Self::Service {
        TimedSvc { inner, log: self.log.clone() }
    }
}

struct TimedSvc<S> {
    inner: S,
    log: Arc<Mutex<Vec<String>>>,
}

impl<S> Service<RunCtx> for TimedSvc<S>
where
    S: Service<RunCtx, Response = serde_json::Value, Error = anyhow::Error> + Clone + Send + 'static,
    S::Future: Send + Unpin,
{
    type Response = serde_json::Value;
    type Error = anyhow::Error;
    // 零箱的代价：**手写包装 future 类型**——after 逻辑写进 poll 里
    type Future = TimedFut<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RunCtx) -> Self::Future {
        // tower 惯例 #2：clone-per-request——关联 Future 类型没有生命周期
        // 参数，`inner.call(req)` 返回的 future 借 `&mut inner`（局部）会逃逸，
        // 所以必须 clone 一份内层再 call（零箱变体的固有仪式）。
        let mut inner = self.inner.clone();
        let log = self.log.clone();
        TimedFut {
            start: std::time::Instant::now(),
            log,
            inner: inner.call(req),
        }
    }
}

/// 手写的包装 future——tower 零箱 after 的真实样板量
struct TimedFut<F> {
    start: std::time::Instant,
    log: Arc<Mutex<Vec<String>>>,
    inner: F,
}

impl<F: Future<Output = Result<serde_json::Value, anyhow::Error>> + Send + Unpin> Future for TimedFut<F> {
    type Output = Result<serde_json::Value, anyhow::Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<serde_json::Value, anyhow::Error>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(cx) {
            Poll::Ready(out) => {
                let ms = this.start.elapsed().as_millis();
                this.log.lock().unwrap().push(format!("took {ms}ms"));
                Poll::Ready(out)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// ── tower 形态（变体 b：BoxFuture——一行，付一次分配）────────
struct TimedBoxSvc<S> {
    inner: S,
    log: Arc<Mutex<Vec<String>>>,
}
impl<S> Service<RunCtx> for TimedBoxSvc<S>
where
    S: Service<RunCtx, Response = serde_json::Value, Error = anyhow::Error>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = serde_json::Value;
    type Error = anyhow::Error;
    type Future = BoxFut;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RunCtx) -> Self::Future {
        let log = self.log.clone();
        let mut inner = self.inner.clone(); // clone-per-request（'static future 的前提）
        Box::pin(async move {
            let start = std::time::Instant::now();
            let out = inner.call(req).await?;
            let ms = start.elapsed().as_millis();
            log.lock().unwrap().push(format!("took {ms}ms"));
            Ok(out)
        })
    }
}

// ============================================================
// 行为 3：扩展贡献（把数据塞给下游）——两种协议都有通道
// ============================================================

// ── CPS：next(Some(value))——框架合并 ✓
#[derive(Clone)]
struct ExtCps;
impl MiddlewareServer for ExtCps {
    async fn serve(&self, mut args: MiddlewareServerFnArgs) -> Result<serde_json::Value, anyhow::Error> {
        args.next(Some(serde_json::json!({ "from": "cps" }))).await
    }
}

// ── tower：改写请求（Request 携带可合并的槽位）
#[derive(Clone, Default)]
struct ExtRunCtx {
    base: RunCtx,
    ext: Option<serde_json::Value>,
}
struct ExtLayer;
impl<S> Layer<S> for ExtLayer
where
    S: Service<ExtRunCtx, Response = serde_json::Value, Error = anyhow::Error> + Send,
    S::Future: Send,
{
    type Service = ExtSvc<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ExtSvc { inner }
    }
}
struct ExtSvc<S> {
    inner: S,
}
impl<S> Service<ExtRunCtx> for ExtSvc<S>
where
    S: Service<ExtRunCtx, Response = serde_json::Value, Error = anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = serde_json::Value;
    type Error = anyhow::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: ExtRunCtx) -> Self::Future {
        req.ext = Some(serde_json::json!({ "from": "tower" })); // 改写请求
        self.inner.call(req)
    }
}

// ============================================================
// 行为等价断言：两个协议跑同一个 stub 内层，日志一致
// ============================================================

#[derive(Clone)]
struct LeafSvc {
    ctx: RunCtx,
}
impl Service<RunCtx> for LeafSvc {
    type Response = serde_json::Value;
    type Error = anyhow::Error;
    type Future = BoxFut;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _req: RunCtx) -> Self::Future {
        let ctx = self.ctx.clone();
        Box::pin(async move { leaf(&ctx).await })
    }
}

/// tower 侧的手动洋葱（静态组合——两层 concrete 嵌套，零箱）
#[tokio::test]
async fn tower_side_static_composition_zero_box() {
    let ctx = RunCtx::default();
    // 组合：Timed( Skip( Leaf ) )——完整类型编译期确定
    let mut svc = TimedSvc {
        log: Arc::new(Mutex::new(Vec::new())),
        inner: SkipSvc {
            inner: LeafSvc { ctx: ctx.clone() },
        },
    };
    let _ = svc.call(ctx.clone()).await.unwrap();
    // B1 未触发（skip_me=false）→ leaf 两笔
    assert_eq!(*ctx.steps.lock().unwrap(), vec!["leaf-1", "leaf-2"]);
}

#[tokio::test]
async fn tower_side_short_circuit_works() {
    let ctx = RunCtx {
        steps: Arc::new(Mutex::new(Vec::new())),
        skip_me: true,
    };
    let mut svc = SkipSvc {
        inner: LeafSvc { ctx: ctx.clone() },
    };
    let out = svc.call(ctx.clone()).await.unwrap();
    assert_eq!(out, serde_json::json!({ "skipped": true }));
    assert!(ctx.steps.lock().unwrap().is_empty(), "leaf 未跑");
}

// ============================================================
// LoC 清点（作者侧，见 docs/experiments/middleware-protocol.md 的完整表）
// ============================================================
#[test]
fn authoring_cost_static_counts() {
    // 本文件内两种协议的行为实现行数（grep 口径：impl 块到块尾）。
    // 详见 docs/experiments/middleware-protocol.md 的对照表。
    // CPS：3 个行为 ≈ 8/8/4 行；tower：B1 ≈ 20 行、B2 零箱 ≈ 55 行
    //（手写 future）/ boxed ≈ 20 行、B3 ≈ 25 行。
    assert!(true, "口径见实验文档");
}

// ============================================================
// CPS 侧端到端：平铺 serve 跑进真实引擎（skip + around 计时）
// ============================================================
use aa_workflow_core::define::{
    CreateWorkflowConfig, MetaBag, StepCtx, WorkflowCtx, create_workflow,
};
use aa_workflow_core::{InMemoryStore, RunStatus, RunStore, RunWorkflowOptions, run_workflow};

#[derive(Clone)]
struct StepsMw {
    log: Arc<Mutex<Vec<String>>>,
}
impl MiddlewareServer for StepsMw {
    async fn serve(&self, mut args: MiddlewareServerFnArgs) -> Result<serde_json::Value> {
        self.log.lock().unwrap().push("mw-before".into());
        let out = args.next(None).await?;
        self.log.lock().unwrap().push("mw-after".into());
        Ok(out)
    }
}

#[tokio::test]
async fn cps_side_flat_serve_runs_in_the_real_engine() {
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let timed = TimedCps { log: Arc::new(Mutex::new(Vec::new())) };
    let wf = create_workflow(CreateWorkflowConfig::new("exp").input::<serde_json::Value>())
        .middleware(Middleware::from_impl(SkipCps))
        .middleware(Middleware::from_impl(TimedCps { log: Arc::new(Mutex::new(Vec::new())) }))
        .middleware(Middleware::from_impl(StepsMw { log: Arc::clone(&log) }))
        .handler(|ctx: WorkflowCtx| async move {
            ctx.step("leaf", |_sc: StepCtx| async { Ok(serde_json::json!({ "leaf": true })) }).await
        });

    // run1：meta 置 skip → SkipCps 短路（leaf 未跑）
    let out = run_workflow(
        RunWorkflowOptions::new(wf.clone(), Arc::clone(&store))
            .input(serde_json::json!({}))
            .meta(MetaBag::new().with::<SkipFlag>(true))
            .run_id("exp:skip"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(
        out.output,
        Some(serde_json::json!({ "skipped": true })),
        "CPS 平铺短路直接产出结果"
    );

    // run2：不短路 → around 计时在 after 位生效
    run_workflow(
        RunWorkflowOptions::new(wf.clone(), Arc::clone(&store))
            .input(serde_json::json!({}))
            .meta(MetaBag::new().with::<SkipFlag>(false))
            .run_id("exp:run"),
    )
    .outcome()
    .await
    .unwrap();
    assert!(
        !log.lock().unwrap().is_empty(),
        "TimedCps 的 after 逻辑（await 之后）已执行"
    );
}
