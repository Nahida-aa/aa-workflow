# 0020 — 中间件 server 改为 scope-borrowed：`MiddlewareServer` trait

**状态**：已接受（2026-11-21）
**关联**：`docs/feature-requirements.md` F8、[0016](0016-step-onion.md)（step 洋葱）、
[0002](0002-rust-step-hooks.md)（单 server 形状）

## 背景

带参数的中间件（tower/axum 生态的常态形态：构造期捕获日志读取器、图形状、
配置、连接句柄）在旧合同 `Fn(Args) -> Fut + 'static` 下有固定仪式：

- `Fn`（中间件存进定义、跨 drive 复用，必须可重入）
- × `Fut: 'static`（boxed 存进洋葱，future 不能借闭包状态）
- × 非 Copy 捕获

三者联立 → 每个 `async move` 前手动 `let x = x.clone();` 一行。`workflow_middlewares`
里两个策略中间件（interim_goals / dag_policies）各有一行（前者还被用户点名难受）。

## 被否的选项（编译器实测）

**AsyncFn\* blanket（真异步闭包形态）**——语义上是解药（future 借 `&self` 状态），
但擦除位三道墙：

1. `dyn AsyncFnMut` / `dyn AsyncFn` **非 dyn compatible**（E0038）；
2. `dyn AsyncFnOnce(u32) -> u32` 因 `CallOnceFuture` 关联类型必须指定而写不出
   （E0191）——异构闭包没有公共 future 类型可点名；
3. 绕开 dyn、在 blanket impl 的 where 子句里写 `for<'a> F::CallRefFuture<'a>: Send`
   撞 **E0658**（`async_fn_traits` 库特性未稳定；糖式 bound `F: AsyncFn(..)` 稳定，
   但证明 future Send 必须点名 GAT 投影）。

结论：`server_async`（AsyncFn 形态构造器）**解禁后三行可加**，当前不实现。
另否决 `server` 方法名（与 `Fn::call` 在 blanket 场景歧义，E0034 风险）——trait
方法定为 `serve`。

**仅加 `with_state` 不动签名**（axum 式）——能收编闭包仪式，但 struct 形态
（tower 首选）的按引用使用仍然被 `'static` 挡住，不根治。

## 决策

1. 擦除位从「`Arc<dyn Fn(Args) -> BoxFuture<'static>>`」改为 trait
   [`MiddlewareServer`](../../packages/workflow_core/src/middleware/create_middleware.rs)：
   `fn serve<'a>(&'a self, args) -> BoxFuture<'a, Result<Value>>`——**返回 future
   借 `&self`**（lifetime-tied；无 GAT，dyn 安全）。
2. 三个构造形态：
   - `.server(f)`：旧闭包形态，blanket impl（`Fn(Args) -> 'static future`）直接
     覆盖，**现有调用点零改动**（「自有 ⊇ 借用」——自持数据对任意 `'a` 成立）；
   - `.server_state(state, f)`：axum `from_fn_with_state` 同款，框架每次调用
     clone 一份 state 传作闭包首参（Arc 场景=引用计数），仪式收进框架；
   - `Middleware::from_impl(m)`：struct 实现 trait——状态是**字段**，按引用使用，
     零克隆零仪式（tower `Service` 同款）。
3. `MiddlewareServerFn` 别名保留，类型改为 `Arc<dyn MiddlewareServer>`；
   公开字段 `pub server` 不变。

## 后果

- **兼容**：`workflow_middlewares` 的两个策略中间件重写为 struct 形态后，
  `interim_goals(get_events)` / `dag_policies(shape, get_events)` 的公开签名
  不变；define/mod.rs 的 struct 字面量测试（`Middleware { server: ext }`）经
  blanket coercion 零改动通过。
- **借用纪律**：`serve<'a>(&'a self)` 的 future 借 `&self`——中间件活在
  `WorkflowDefinition` 里、定义活过整条 drive，且引擎 inline 驱动（compose 在
  drive 内 await），借用范围天然成立。**future 要逃逸 drive（spawn 后台任务）
  时才需要 `'static`**——克隆回到语义上诚实的位置。
- **行为不变**：server 的调用时机/顺序/次数与旧合同完全一致（只是返回 future
  的生命周期从 `'static` 收窄为 `&'a self`）。
- **上游对照**：TS 侧闭包原生捕获、无此问题；`MiddlewareServer` 是 Rust 特有
  的擦除形态，对齐的是 tower 的 `Service`（`&self` + 关联 future 的 Rust 惯例）。

## 测试

- `parameterized_middleware_state_persists_across_drives`：struct 形态与
  server_state 形态各驱动两次，计数器=2（同一实例/state Arc 跨 drive 复用）；
- `middleware_forms_compose_together`：三种形态混装进同一条链；
- `workflow_middlewares` 的 park race 验收（跨 drive 泊车/续驱）在 struct
  形态下全绿。
