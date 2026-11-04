# 0016 — step 拦截改成包装层叠加：自制 wrapper 还是 tower `Service`

**状态**：**待定**（2026-11-03 起）。结论未定，本文只记录问题与候选方案。
**关联**：[0002](0002-rust-step-hooks.md)（现行 `set_step_interceptor`）、
[0007](0007-middleware-compose-order.md)（组合顺序）、
[0015](0015-ext-namespaces.md)（命名空间扩展，同一批改造）

> **要定的是两件事**：① 要不要叠加（已定：要）；② 用什么承载包装层
> （自制 wrapper vs tower `Service`）。两者语义一致，见「方案 C」开头。
> 选定前不要把任何一条当结论引用。

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

## 方案 A（已否决）：照搬 JS，把 `ctx.step` 变成 ctx 上的可赋值字段

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
`.downcast::<Order>()`。中间件**使用者**优先于作者，这是体验的净损失。

要让「方法调属性、属性可改」也不难（擦除层放 engine，`T` 留在方法签名上），
但那只是把（B）的实现细节换了个外壳，不解决「谁决定嵌套顺序」。

## 方案 B（已实验，未采纳）：自制 `StepWrapper` 洋葱链

分支 `experiment/step-onion`（提交 `bc609fd`）实现过一版：

```rust
pub type StepWrapper = Arc<dyn Fn(StepCall) -> BoxFuture<'static, Result<Value>> + Send + Sync>;
// 中间件里
sc.push_step_wrapper(w);
// wrapper 里
call.skip(cached).await   // 短路 + 落 checkpoint
call.park(SIG, meta).await // 边界泊车
call.next().await          // 正常推进
```

**能工作**，测试 `packages/workflow_middlewares/tests/step_onion.rs` 三条验证了
叠加顺序与短路。但代价是核心（`engine/mod.rs`）里出现了手写的
`fold` + `NextStep(Arc<Mutex<Option<Box<dyn FnOnce>>>>)` — 一个**自制的洋葱执行器**，
和已有的 `composeMiddlewares` 是第二套同构物。链的语义、至多一次、错误传播
都得自己维护。

实验中还踩到三个真实坑（记录在此，任何方案都要面对）：

1. **引用环**：wrapper 常捕获 `ctx.engine`（泊车要调 pause 原语），而链存在
   `EngineRuntime` 上 → `engine → wrappers → engine` 永不 drop，publish sender
   也不 drop，`drain()` 永久等待。只能在 drive 收尾清空链。
2. **Park 被当失败**：`WorkflowParked` 从闭包内冒出时会落 `STEP_FAILED` 并进重试。
   原实现靠「Park 只发生在闭包外」的隐含假设，wrapper 打破它，需要显式透传。
3. **锁跨 await**：`if !engine.step_wrappers.lock()...is_empty()` 的 guard
   生命周期覆盖整个 `if` 块，内部再取锁即死锁。

这三条与「用不用 tower」无关——它们来自「step 变成一个可叠加的调用链」这件事本身。

## 方案 C（待评估，作者倾向）：`ctx.step` 是 tower 的 `Service`

**先说清楚 C 不是「另一种架构」，而是 B 的实现选型。** TS 的 `ctx.step = wrapped`
本身就是包装层，B 与它同构——差别只在包装层用什么承载：

| | 承载 | 叠加执行器 |
| --- | --- | --- |
| B | 自制 `StepWrapper` 闭包 | core 里手写 `fold` + `NextStep` |
| C | `tower::Service` | tower 的 `ServiceBuilder::layer` |

所以 C 与 B **语义完全一致**（不调下一层 = 短路），要决定的只是
「自己维护一个洋葱执行器，还是复用 tower 的」。

```rust
// step 的请求/响应
pub struct StepRequest { pub step_id: String, pub run_meta: Option<Value>, /* 执行体 */ }
pub struct StepResponse(pub serde_json::Value);

// 真正的执行是我方唯一要写的「叶子 service」，其余全部复用 tower
impl Service<StepRequest> for StepLeaf { /* 跑闭包、落 STEP_FINISHED */ }
```

中间件里给 step 加层，就是往 step 的 stack 上推一个 `Layer`：

```rust
sc.wrap_step(ReseedLayer::new(plan, load_results));   // 复用
sc.wrap_step(InterimGoalLayer::new(goals));           // 泊车
```

### 为什么可能优于 B 的实现

- **叠加、顺序、短路都是 tower 现成的**：`ServiceBuilder::layer` 的洋葱语义、
  `Service::call` 返回 future 的形状，都是生态共识，不需要在 core 里手写 `fold`。
- **中间件与 step 的分层更清楚**：中间件是 drive 级洋葱，它的职责之一是
  **决定给 step 装哪些层**；step 的 stack 是 step 级洋葱。B 里
  `push_step_wrapper` 两头都能改，职责是糊的。
- 与 [0007](0007-middleware-compose-order.md) 的约束天然吻合：策略层必须先包进
  调度之前，tower 的 `layer` 调用点就是「包进去」这个动作。

### 必须先回答的问题（否则不能采纳）

1. **`tower::Service::call(&mut self)` 的 `&mut`**：step 是并发调用的（并行分支），
   层需要 `Clone` 或内部加锁。tower 的惯例是 `Service` 可以 `Clone` 后各自持有，
   但我们的层持有策略状态（`done` 集合、`parked` 标记）——**状态该放在层里还是
   外面**？放外面就退化成（B）的 `Arc<Mutex<..>>`，放里面则 `Clone` 会分裂状态。
2. **`Layer` 的粒度**：一个 layer 包整个 step stack，还是每步一层？前者符合 tower，
   后者不符合。
3. **依赖代价**：core 是否愿意引入 `tower`（它本身不大，但会把 `Service` /
   `Layer` 的版本绑定变成对外的隐式契约）。
4. **`T` 的推导**：`Service` 是 `Value` 擦除的，`ctx.step::<Order>()` 的类型推导
   仍要保留在方法层（与方案 A 的结论一致：擦除下沉，`T` 留在签名）。

## 待定

要定的是**两件独立的事**：

1. **要不要叠加**（架构）——A 已否决，B/C 都同意「叠加」。这一条实际上已经定了：
   单值槽位在 `interim_goals + reseed` 同时使用时必然丢一个，必须改。
2. **用什么承载包装层**（实现选型）——自制 `StepWrapper`（B）还是 tower
   `Service`（C）。语义一致，差别在「自己维护执行器」vs「复用 tower」。

其他未定：

- `set_step_interceptor` 如何下线（直接删，不留适配层——它本身就是洋葱的退化形式）。
- 若选 C，先回答上文四个问题；问题 1（策略状态放层里还是外面）是唯一的硬约束，
  回答不了就用 B。

## 证据

- 方案 B 的分支：`experiment/step-onion` @ `bc609fd`
- 叠加测试：`packages/workflow_middlewares/tests/step_onion.rs`（3 条）
- 方案 B 期间的三个坑：见上文「实验中还踩到三个真实坑」
