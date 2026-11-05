# 0016 — step 拦截改成包装层叠加

**状态**：**已接受并实现**（2026-11-03）。载体选定 **tower `Service` / `Layer`**。
**关联**：[0002](0002-rust-step-hooks.md)（现行 `set_step_interceptor`，将被取代）、
[0007](0007-middleware-compose-order.md)（组合顺序）、
[0015](0015-ext-namespaces.md)（命名空间扩展，同一批改造）

> 决策只有一条：**step 拦截从「单值槽位」改成「包装层叠加」**，
> 且包装层用 **tower 的 `Service` / `Layer`** 承载——这样 tower 生态里请求
> 类型无关的层（`timeout` / `retry` / `limit`）可以直接装在 `ctx.step` 上。

## 问题

现行实现是**单值槽位**（[0002](0002-rust-step-hooks.md)）：

```rust
pub type StepInterceptor = Arc<dyn Fn(&StepEvent<'_>) -> Option<StepAction> + Send + Sync>;
pub fn set_step_interceptor(&self, f: StepInterceptor);
```

`interim_goals`（泊车）与 `reseed`（复用）都通过它干预 step，但槽位只有一个：
**后装的静默覆盖先装的**。这不是能力不足，是「两个策略都想按 step 判断」时
必然有一方消失，且没有任何报错。

TS 侧同一个问题表现为 `ctx.step = wrapped` 被后一次赋值覆盖。需求是**叠加**，
不是更多种类的决定。

## 决策

**step 拦截 = 一层层 tower `Layer`**，先注册的在外层。

形状与 axum 的路由同构——这一点是整件事的关键：

```text
axum:  .route("/x", handler)  →  Router(持有 handler) → call(Request)
ours:  ctx.step("load", f)    →  Leaf(持有 f)          → call(StepRequest)
```

`f` 在**调用点**就确定了，所以每次 `ctx.step(...)` 就是「注册一个持有该闭包的
service，然后调用它」——与 axum 在注册期确定 handler 没有本质差别。

```rust
// 请求：纯数据（+ engine 句柄，供 skip / park 落盘）
pub struct StepRequest {
    pub step_id: String,
    pub run_id: String,
    pub run_meta: Option<Value>,
    pub completed: Arc<HashSet<String>>,
}

// 中间件里（都是真正的 tower Layer）
sc.wrap_step(tower::timeout::TimeoutLayer::new(Duration::from_secs(30)));
sc.wrap_step(tower::limit::ConcurrencyLimitLayer::new(4));
sc.wrap_step(MyReseedLayer::new(plan));
```

自定义层照常 `impl tower::Service<StepRequest>`；**短路 = 不调 `inner.call(req)`**，
这就是 tower 的标准做法，不需要 `next` 参数。

`ctx.step::<Order>()` 的类型推导保留在方法层（service 返回 `Value`，
方法里 `serde_json::from_value`）。

## 被否决的选项

### 方案 A：照搬 JS，把 `ctx.step` 变成 ctx 上的可赋值字段

```rust
pub struct BaseCtx<..> {
    pub step: StepFn,      // 属性 = 函数值，可改
}
```

**否决理由**：`step` 现在是泛型方法，`T` 由调用方的返回类型推导：

```rust
let order: Order = ctx.step("load", || load()).await?;
```

擦成 `Arc<dyn Fn(..) -> Result<Value>>` 之后 `T` 丢失，handler 要自己
`.downcast::<Order>()`。**中间件使用者优先于作者**，这是体验的净损失。

（「方法调属性、属性可改」本身不是问题——擦除层可以放 engine、`T` 留在
签名上。但那只是把下面「实现注记」里的细节换个外壳，不改变叠加语义。）

## 代价

- `set_step_interceptor` / `StepInterceptor` / `StepEvent` / `StepAction` 全部删除；
  `interim_goals` / `reseed` / `dag_policies` 三个策略中间件已重写。
- **错误类型从 `anyhow::Error` 换成 `Box<dyn Error + Send + Sync>`**（tower 的
  `BoxError`）。这是硬约束：`timeout` / `retry` / `limit` 都以它作输出错误类型。
  引擎内部的 `anyhow` 错误靠 `Into` 转换，不额外麻烦；但统一之后，
  `WorkflowParked` 的识别从 `anyhow::downcast_ref` 换成了标准 `downcast_ref`。
- 引入 `tower` 依赖（`aa_workflow_core` 与 `aa_workflow_middlewares`）。
- core 里多一个「step 级洋葱」的概念，与「drive 级洋葱」并列。这是必要复杂度：
  两级作用域不同（前者决定每个 step 怎么跑，后者决定整个 drive 怎么跑）。

## 实现

### 请求是纯数据

```rust
pub struct StepRequest {
    pub step_id: String,
    pub run_id: String,
    pub run_meta: Option<Value>,
    pub completed: Arc<HashSet<String>>,   // 本 drive 已成功的 step 快照
}
```

执行体**不在**请求里——它由当次的叶子 service 持有：

```rust
// 每次 ctx.step(id, f) 现建（= 注册一个 service）
pub(crate) struct StepLeaf { exec: Arc<dyn Fn() -> BoxFuture<...> + Send + Sync> }
impl Service<StepRequest> for StepLeaf { fn call(&mut self, _req) { (self.exec)() } }
```

### 链怎么组装

`Layer::layer(inner)` 需要内层实例，而叶子**每步都是新的**。所以存的是
「怎么包」而不是包好的链：

```rust
type StepLayer = Arc<dyn Fn(StepService) -> StepService + Send + Sync>;

// wrap_step：注册一层
self.step_layers.push(Arc::new(move |inner| StepService::new(layer.layer(inner))));

// exec_step：每次现建叶子 + 反向折叠（先注册的在外层）
let chain = layers.into_iter().rev().fold(leaf, |inner, f| f(inner));
```

`StepService = BoxCloneService<StepRequest, Value, BoxError>`——tower 官方的
运行期擦除载体，因为 `ServiceBuilder` 的层组合是编译期的，而 `wrap_step`
是运行期调用。

### 策略中间件

三个策略都是「看请求 → 放行 / 短路 / 泊车」，tower 的 `Service` 样板收在
`workflow_middlewares::step_layer`：

```rust
sc.wrap_step(step_layer(move |req: &StepRequest| {
    if reuse(req) { Decision::Skip(cached) } else { Decision::Pass }
}));
```

`Decision::Pass` 走标准 `inner.call(req)`；`Skip` / `Park` 调
`req.skip(..)` / `req.park(..)`（请求带着 engine 句柄）。

## 被否决的替代实现：自写闭包

分支 `experiment/step-onion` @ `bc609fd` 实现过一版 `StepWrapper` 闭包 +
`push_step_wrapper`。能工作（`step_onion.rs` 三条测试全过），但：

- core 里要手写 `fold` + `NextStep(Arc<Mutex<Option<Box<dyn FnOnce>>>>)`，
  是第二套洋葱执行器，与 `composeMiddlewares` 同构；
- **拿不到 tower 现成的层**——`timeout` / `retry` / `limit` 得各写一份私有版本。

否决的核心是第二点，且它是我一开始判断错的地方：我以为 tower 的层都面向
`http::Request`，实测只有 `tower-http` 才是；tower 本体的层对任意
`S: Service<Req>` 生效。

## 三个实测踩到的坑（与承载方式无关）

1. **引用环**：层常捕获 `ctx.engine`（泊车要调 pause 原语），而层存在
   `EngineRuntime` 上 → 永不 drop，publish sender 也不 drop，`drain()` 永久等待。
   drive 收尾清空。
2. **Park 被当失败**：`WorkflowParked` 从闭包内冒出时会落 `STEP_FAILED` 并进重试。
   原实现靠「Park 只发生在闭包外」的隐含假设，层打破它，需显式透传。
3. **锁跨 await**：`if !engine.step_layers.lock()...is_empty()` 的 guard
   生命周期覆盖整个 `if` 块，内部再取锁即死锁。

## 证据

- `packages/workflow_middlewares/tests/tower_layers.rs`（3 条）——
  `TimeoutLayer` / `ConcurrencyLimitLayer` 直接可用，且超时**真的生效**
- `packages/workflow_middlewares/tests/step_onion.rs`（3 条）——叠加顺序与短路
- `packages/workflow_middlewares/tests/completed_snapshot.rs`（3 条）——快照语义
- `cargo test --workspace`：290 passed；Postgres 侧 2 passed

## 修订

- 2026-11-03 起草时断言「现成的 layer 都面向 `http::Request`，生态复用接近零」
  ——**错**。tower 本体的层是请求类型无关的，见「被否决的替代实现」。
- 同日先实现了一版自写闭包（`bc609fd`），后改为 tower 承载。
