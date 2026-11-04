# 0016 — step 拦截改成包装层叠加

**状态**：**已接受**（决策层面，2026-11-03）；实现载体待选（见「待定」）。
**关联**：[0002](0002-rust-step-hooks.md)（现行 `set_step_interceptor`，将被取代）、
[0007](0007-middleware-compose-order.md)（组合顺序）、
[0015](0015-ext-namespaces.md)（命名空间扩展，同一批改造）

> 决策只有一条：**step 拦截从「单值槽位」改成「包装层叠加」**。
> 用什么承载这些包装层（自写闭包 / `tower::Service`）是实现细节，
> 不影响语义，见「实现注记」。

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

**step 拦截改成「包装层叠加」**：每个中间件可以往 step 的执行链上包一层，
先注册的在最外层，与中间件洋葱同构（[0007](0007-middleware-compose-order.md)）。

```rust
// 中间件里（可多次、可来自不同插件）
sc.wrap_step(w);

// 包装层
Box::pin(async move {
    if reuse { return call.skip(cached).await; }  // 短路 + 落 checkpoint
    if done  { return call.park(SIG, None).await; }  // 边界泊车
    call.next().await                              // 正常推进
})
```

控制流原语只有一种：**调不调 `next()`**。`skip` / `park` 是它的两个
受控快捷方式（分别对应原 `StepAction::Skip` / `Park`），不引入第二种信号。

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

- `set_step_interceptor` / `StepInterceptor` / `StepEvent` / `StepAction` 全部删除，
  `interim_goals` / `reseed` / `dag_policies` 三个策略中间件要重写（已实验过，改动可控）。
- 中间件作者少记一个枚举，多一个 `next`。
- core 里多一个「step 级洋葱」的概念，与「drive 级洋葱」并列。这是必要复杂度：
  两级作用域不同（前者决定每个 step 怎么跑，后者决定整个 drive 怎么跑）。

## 实现注记：包装层怎么承载

**这不是方案选择题。** 决策是「包装层叠加」，下面两种只是承载方式，
语义完全一致（不调下一层 = 短路）。差别只在「自己维护执行器」vs
「复用现成库」，属于依赖取舍。

### 自写闭包（已跑通）

```rust
pub type StepWrapper = Arc<dyn Fn(StepCall) -> BoxFuture<'static, Result<Value>> + Send + Sync>;
sc.push_step_wrapper(w);
```

实现见分支 `experiment/step-onion` @ `bc609fd`，叠加测试
`packages/workflow_middlewares/tests/step_onion.rs` 三条全过。

代价：core 里出现手写的 `fold` + `NextStep(Arc<Mutex<Option<Box<dyn FnOnce>>>>)`，
一个**自制的洋葱执行器**，和已有的 `composeMiddlewares` 是第二套同构物。
链的至多一次、错误传播都得自己维护。

### tower `Service`

```rust
pub struct StepRequest { pub step_id: String, pub run_meta: Option<Value>, /* 执行体 */ }
pub struct StepResponse(pub serde_json::Value);

impl Service<StepRequest> for StepLeaf { /* 跑闭包、落 STEP_FINISHED */ }

// 中间件里
sc.wrap_step(ReseedLayer::new(plan, load_results));
sc.wrap_step(InterimGoalLayer::new(goals));
```

好处：**不用在 core 里手写执行器**。版本绑定算不算坏处，取决于
`Service` / `Layer` 出不出现在公开签名里：

| | `Service`/`Layer` 在公开签名？ | 版本绑定 | 生态复用 |
| --- | --- | --- | --- |
| 只活在实现内部（对外仍是 `wrap_step(StepWrapper)`） | 否 | 不成立 | 无 |
| 对外接受 `tower::Layer` | 是 | **成立**：我们的 `tower` 版本升级等于用户的跟着动，用户写的 layer 必须实现同一版 `Service` | 有 |

第二行的收益是真实的——用户能复用 tower 生态的现成 layer、按生态习惯写层——
但**在本场景接近零**：那些 layer（`TimeoutLayer`、限流、`tower-http` 那一批）
都面向 `http::Request`，对我们自定义的 `StepRequest` 帮不上忙。本仓的 step 包装层
是**业务策略**（泊车、复用、DAG 调度），不是泛用横切。

所以这里不是「tower 一定是坏处」，而是：**要生态复用就得付版本绑定，
而这一步的生态复用没有实际收益**。若将来出现可复用的通用 step layer
（例如统一的超时 / 重试层），这个权衡要重新算。

### 两种承载都要面对的

1. **并发与策略状态**：step 会被并行分支并发调用。tower 习惯 `Service: Clone`、
   各持一份——而策略状态（`done` 集合、`parked` 标记）**不能分裂**。所以状态必须
   放在层外面、由 drive 持有，层只做判断。这与 `ctx.state` 并行分支的结论同源：
   跨分支共享的可变数据只能通过返回值，不能靠共享可变状态。
2. **`T` 的推导**：包装层是 `Value` 擦除的，`ctx.step::<Order>()` 的类型推导
   仍要留在方法签名上（与方案 A 的结论一致：擦除下沉，`T` 留在签名）。
3. **三个已踩到的坑**（实验期间实测，与承载方式无关）：
   - **引用环**：包装层常捕获 `ctx.engine`（泊车要调 pause 原语），而链存在
     `EngineRuntime` 上 → `engine → wrappers → engine` 永不 drop，publish sender
     也不 drop，`drain()` 永久等待。只能在 drive 收尾清空链。
   - **Park 被当失败**：`WorkflowParked` 从闭包内冒出时会落 `STEP_FAILED` 并进重试。
     原实现靠「Park 只发生在闭包外」的隐含假设，包装层打破了它，需显式透传。
   - **锁跨 await**：`if !engine.step_wrappers.lock()...is_empty()` 的 guard
     生命周期覆盖整个 `if` 块，内部再取锁即死锁。

## 待定

- 承载方式：自写还是 tower（上节）。
- `set_step_interceptor` 怎么下线：直接删，不留适配层——它本身就是洋葱的
  退化形式，留着等于永久维护两条语义不同的短路路径。

## 证据

- 方案 B 的分支：`experiment/step-onion` @ `bc609fd`
- 叠加测试：`packages/workflow_middlewares/tests/step_onion.rs`（3 条）
- 方案 B 期间的三个坑：见上文「实验中还踩到三个真实坑」
