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
pub struct StepRequest { pub step_id: String, pub run_meta: Option<Value> }
pub struct StepResponse(pub serde_json::Value);

// step 闭包就是叶子 service 的 call 体 —— 这正是 tower 的形状
impl Service<StepRequest> for StepLeaf { /* 跑闭包、落 STEP_FINISHED */ }

// 中间件里
sc.wrap_step(ReseedLayer::new(plan, load_results));
sc.wrap_step(TimeoutLayer::new(Duration::from_secs(30)));
```

## tower 值不值得上：先纠正一个错误论断

起草时写过「现成的 layer 都面向 `http::Request`，对 `StepRequest` 帮不上忙，
所以生态复用接近零」。**这是错的。**

只有 `tower-http` 那一批才绑定 HTTP。`tower` **本体**的层是请求类型无关的
——它对任意 `S: Service<Req>` 生效，不关心 `Req` 里装什么：

| layer | 对 `ctx.step` 是否有用 |
| --- | --- |
| `tower::timeout::TimeoutLayer` | 有用：统一的 step 超时（现在是每步 `StepOptions.timeout`） |
| `tower::retry::RetryLayer` | 有用：与现有 step 重试策略对位 |
| `tower::limit::ConcurrencyLimitLayer` | 有用：与现有 `gate` 对位 |
| `tower::limit::RateLimitLayer` | 有用 |
| `tower::tracing` / 自定义 metrics | 有用 |

而且 `Service::call` 里跑的就是 step 闭包本身——

```rust
fn call(&mut self, req: StepRequest) -> Self::Future {
    // 这里 await 用户闭包，落 STEP_FINISHED
}
```

——所以「闭包属于 service、请求只是数据」这个 tower 惯例，我们天然满足。

## 于是权衡变成

| | 收益 | 代价 |
| --- | --- | --- |
| 自写 `StepWrapper` | 无额外依赖；已跑通 | core 里多一套手写执行器；`timeout` / `retry` / `limit` 各写一份私有版本，互不兼容 |
| tower | 上表那批层直接可用；执行器是验证过的 | 版本绑定（若 `Layer` 进公开签名）；学习面稍大 |

**倾向 tower**，但有一个前置问题必须先回答（见下一节第 1 条）：策略状态放哪。
`
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

- **承载方式：已选定 tower 路线的前置条件已满足，待迁移。**
  关键前提是「策略状态不放在层里」——已落地（见下节「第一步已完成」）。
  tower 化的具体做法与验收标准见下节。
- `set_step_interceptor` 怎么下线：直接删，不留适配层——它本身就是洋葱的
  退化形式，留着等于永久维护两条语义不同的短路路径。

## 第一步已完成：策略无状态（2026-11-03）

把「策略自己累积 `done`」换成「读引擎给的快照」，这一步与用不用 tower 无关，
本身就该做：

```rust
pub struct StepCall {
    // 本 drive 内已成功的 step（含此前全部，不含本次）
    pub completed: Arc<HashSet<String>>,
}
```

三个策略的改造：

| 策略 | 原来 | 现在 |
| --- | --- | --- |
| `interim_goals` | `done: Mutex<HashSet>` + `parked: Mutex<bool>` | 只读 `call.completed` |
| `dag_policies` | `PlanState.done` + `parked: Mutex<HashSet<run_id>>` | 只读 `call.completed`；计划本身不可变 |
| `reseed` | `cache: Mutex<Option<HashMap>>` 懒加载 | drive 开始同步加载一次，不可变 |

**为什么可行**（两条事实，已核实）：

1. `STEP_FINISHED` 在**包装链返回之前**落盘，所以下一个 step 入口读到的快照
   必然包含此前全部完成的 step。
2. 泊车会让整条 drive 以 `WorkflowParked` 结束，**不可能在同一 drive 内
   触发第二次**，所以 `parked` 标记是多余的。

**为什么这是 tower 化的前提**：`Service: Clone` 会让层在并行分支间各持一份；
策略状态若在层里就会分裂。状态移到「请求携带的只读快照」之后，层变成纯函数，
`Clone` 不再有害。

测试：`packages/workflow_middlewares/tests/completed_snapshot.rs`（3 条）——
串行顺序、并行不含自身、跨 drive 累积。

## 下一步：tower 化

前置条件已满足，剩下的是一次机械替换：

```rust
// 叶子
impl Service<StepRequest> for StepLeaf { /* 跑闭包、落 STEP_FINISHED */ }
// 中间件
sc.wrap_step(TimeoutLayer::new(d));     // 现成的
sc.wrap_step(ReseedLayer::new(plan));   // 无状态，只读 request
```

验收标准：**现有测试原样通过**（`step_onion` 3 + `completed_snapshot` 3 +
策略 7 + 全 workspace）。删掉 `StepWrapper` / `StepCall` / `NextStep`。

## 证据

- 自写版实现（已合入 main，`040293f`）：`StepWrapper` + `push_step_wrapper`
- 叠加测试：`packages/workflow_middlewares/tests/step_onion.rs`（3 条）
- 快照测试：`packages/workflow_middlewares/tests/completed_snapshot.rs`（3 条）
- 三个坑的实测记录：见「两种承载都要面对的」第 3 条

## 修订

- 2026-11-03 起草时断言「现成的 layer 都面向 `http::Request`，生态复用接近零」
  ——**错**。tower 本体的层是请求类型无关的，见「先纠正一个错误论断」。
  更正后倾向从「自写」改为「tower」。
