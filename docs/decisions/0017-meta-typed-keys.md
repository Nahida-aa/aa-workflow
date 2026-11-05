# 0017 — 插件的 meta 字段在编译期可见：`MetaKey` + 链式注册累积

**状态**：草案（2026-11-05），待评审
**关联**：[0011](0011-run-meta-and-plugins.md)（meta 通道的来源）、
[0013](0013-middleware-generics.md) / [0015](0015-ext-namespaces.md)
（两次否决「让使用者算类型」）、[0014](0014-middleware-takes-array.md)（收数组，本文偏离）

## 问题

`RunWorkflowOptions.meta` 现在是 `Option<serde_json::Value>`。插件消费其中的字段
（`__dagPlan`、`__reseed`…），但**使用方在写代码时看不到有哪些字段、各是什么类型**：
参数名写错、结构不对，都要等运行期反序列化失败才发现。

用户确定的验收标准：**编译期可见，写代码时就报错**（debug 时才发现不算解决）。

## 被否决的选项

### A. `RunWorkflowOptions<M>` 泛型参数由使用者填写

```rust
run_workflow::<MyMeta>(opts)
```

**否决**：这正是 [0013](0013-middleware-generics.md) / [0015](0015-ext-namespaces.md)
两次否决过的形状——「使用者必须自己算出类型」。插件可能来自多个外部开发者，
使用者既不知道也不该知道它们怎么拼成一个类型。

### B. 模块增强（TS 的做法）

```rust
// 插件包
impl MetaRegistry for ... { /* 全局注册 "__dagPlan" */ }
```

**否决**：Rust 没有 TS 的 `declare module`，全局注册只能靠 trait 一致性——
同一个 key 被两个 crate 注册就冲突，且**全局可见**（一个 workflow 注册的字段，
进程里所有 workflow 都看得到）。多 workflow 场景会互相污染。

### C. 让中间件在运行期控制 meta 的类型

**否决**：中间件在运行期执行，拿到的 ctx 是擦除的。类型必须在编译期确定，
而编译期能拿到插件信息的唯一位置是**构建 workflow 的那一行**。

## 决策

**类型由插件定义，在构建 workflow 时累积给使用方；使用者零注解。**

### 插件侧：一个 key 类型

```rust
pub trait MetaKey {
    const KEY: &'static str;
    type Value;
}

// 插件包里（外部 crate 自己定义，不必改 core）
pub struct DagPlanKey;
impl MetaKey for DagPlanKey {
    const KEY: &'static str = "__dagPlan";
    type Value = DagPlan;
}

pub fn dag(spec: DagSpec) -> TypedMiddleware<DagPlanKey> { .. }
```

`TypedMiddleware<K>` 与现有 `Middleware` 的关系：它包着一个 `Middleware`（策略逻辑
不变），额外携带 `K` 作为**类型标记**——运行期零开销（`PhantomData`）。

### 使用方：链式注册，类型自动累积

```rust
create_workflow(cfg)
    .m(dag(spec))         // Builder<Cons<DagPlan, Nil>>
    .m(reseed(plan))      // Builder<Cons<DagPlan, Cons<ReseedPlan, Nil>>>
    .handler(|ctx| async move {
        let plan: &DagPlan = ctx.meta.get::<DagPlanKey>();   // 强类型，无 unwrap
        ...
    })
    .run(opts)            // opts.meta: MetaOf<...> —— 类型不符则编译报错
```

`.m()` 逐个注册（**偏离 [0014](0014-middleware-takes-array.md) 的收数组**）：
数组要求元素同型，`TypedMiddleware<A>` 与 `TypedMiddleware<B>` 放不进同一个 `[T; N]`。
用户已明确此处不必对齐上游 TS。

### 累积机制

HList（`Cons<H, T>` / `Nil`）而不是 tuple：
- tuple 需要为每个长度写 `impl`，且 `(A, B)` 与 `(A, B, C)` 是不同类型，
  追加要在末尾做，标准库没有现成 trait；
- HList 只需两个 `impl`（`Nil` 与 `Cons`），追加是 O(1) 类型操作。

`ctx.meta` 类型是 `Meta<L>`：持有 HList 的值，`get::<K>()` 按 key 类型取字段，
**返回 `&K::Value` 而不是 `Option`**——编译期保证插件注册了就一定有值
（缺失值在 `.run(opts)` 处就报错了，见下）。

## 代价

1. **builder 的泛型逐层嵌套**：5 个插件 → `Builder<Cons<A, Cons<B, Cons<C, Cons<D, Cons<E, Nil>>>>>>`。
   编译期没问题，但**错误信息会很长**，且 `Builder` 不能存进 `Vec`（类型必须静态确定）。
   缓解：把嵌套藏在 `type` 别名后面；错误信息靠 `#[diagnostic::on_unimplemented]` 改善。
2. **偏离 0014**：`.middleware([...])` 保留（无类型中间件仍可批量注册），
   `.m()` 是新入口。两个入口并存，使用者要分清哪个携带类型。
3. **`TypedMiddleware<K>` 与 `Middleware` 是两个类型**：策略包的公开 API 要同时提供
   （例如 `dag()` 返回带类型的版本，`dag_erased()` 返回普通版本给不需要 meta 的场景）。
4. **缺失字段的检查在 `.run()` 处**：`opts.meta` 缺字段时，报错点是 `run_workflow`
   的调用处而非 `.handler()` 处。诊断信息要写清缺哪个 key。

## 证据

类型层可行已验证（spike：`.m(a).m(b).run()` 能编译，且累积出 HList 类型）。
生产实现与测试待补。

## 待定

- `ctx.meta.get::<K>()` 的名字与形状：`get` / `get_ref` / 直接字段访问？
- 中间件内部（非 handler）怎么读 meta：现在是 `sc.ctx.meta`（擦除的 `Option<Value>`），
  是否也换成 `Meta<L>`？若换，`MiddlewareServerFnArgs` 也要带泛型参数。
- 一个插件消费**多个** key 时，`TypedMiddleware<(K1, K2)>` 还是返回 `Meta<Cons<..>>`？
