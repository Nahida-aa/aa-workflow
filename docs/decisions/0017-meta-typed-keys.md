# 0017 — 中间件让 `RunWorkflowOptions.meta` 的类型在编译期可见

**状态**：草案（2026-11-05），待评审
**关联**：[0011](0011-run-meta-and-plugins.md)（meta 通道的来源）、
[0015](0015-ext-namespaces.md)（`ctx.ext` 的命名空间模式，本文复用）、
[0013](0013-middleware-generics.md)（否决「让使用者算类型」）、
[0014](0014-middleware-takes-array.md)（收数组，本文偏离）、
[0016](0016-step-onion.md)（钩子 = 中间件，因此不需要插件）

## 问题

`RunWorkflowOptions.meta` 现在是 `Option<serde_json::Value>`，中间件从中读策略参数
（`__dagPlan`、`__reseed`…）。但**使用方写代码时看不到有哪些字段、各是什么类型**：
参数名写错、结构不对，都要等运行期反序列化失败才发现。

验收标准（用户确定）：**编译期可见，写代码时就报错**。

## 先决条件：不需要插件系统

Rust 侧**目前没有插件**。需要检查它是否必要：

- 钩子已经都是中间件（[0016](0016-step-onion.md)：step 的包装就是 tower `Layer`，
  生命周期钩子只是挂在执行路径不同位置的层）；
- 中间件若能影响 `RunWorkflowOptions` 的类型，插件原本独有的能力（声明参数）也随之落位。

两条都成立，**插件没有剩余的必要性**。本文因此完全以中间件承载。

## 被否决的选项

### A. `RunWorkflowOptions<M>` 由使用者填类型

`run_workflow::<MyMeta>(opts)`——使用者必须自己算出 M。
[0013](0013-middleware-generics.md) / [0015](0015-ext-namespaces.md) 已两次否决。

### B. 模块增强（TS 的 `declare module`）

Rust 没有等价物；靠 trait 一致性模拟会与孤儿规则冲突，且注册是**全局**的
（一个 workflow 声明的字段，进程里所有 workflow 都看得到）。

### C. 从 handler 的 ctx 反推

**方向反了。** handler 默认写法是闭包：

```rust
.handler(|ctx| async move { let plan = ctx.meta.dag_plan(); .. })
```

闭包的参数类型是**推断**出来的，不是标注的。靠它反推要么要求使用者手写
`|ctx: Ctx<_, _, Meta<...>>|`（回到 A），要么改掉闭包用法。

类型必须**从中间件流向 handler**：中间件声明 → builder 累积 → handler 只需读取 →
options 必须满足。

### D. 中间件在运行期控制

中间件在运行期执行，ctx 是擦除的；类型必须在编译期确定，而编译期能拿到
中间件信息的唯一位置是**构建 workflow 的那一行**。

## 决策

### 类型由中间件定义（复用 `ctx.ext` 的模式）

与 [`ExtNamespace`](0015-ext-namespaces.md) 同构，**但多一层**——见下节「为什么不能只照抄」。

```rust
pub trait MetaKey {
    const KEY: &'static str;
    type Value: serde::de::DeserializeOwned;
}

// 中间件包（外部 crate 自己定义）
pub struct DagPlanKey;
impl MetaKey for DagPlanKey {
    const KEY: &'static str = "__dagPlan";
    type Value = DagPlan;
}

impl_meta_accessors!(DagPlanKey => DagPlanAccess, dag_plan, try_dag_plan);
// → ctx.meta.dag_plan()  /  ctx.meta.try_dag_plan()
```

读取侧与 `ctx.ext` **写法一致**（`MetaBag` ↔ `ExtBag`，`impl_meta_accessors!` ↔
`impl_ext_accessors!`），使用者学一套就够。

### 为什么不能只照抄 `ctx.ext`（重要）

`ctx.ext` 的类型支持**不是** `middleware()` 累积出来的——`middleware()` 的签名是
`fn middleware(&mut self, impl IntoIterator<Item = AnyMiddleware>) -> Self`，
返回 `Self`，类型不变。`ctx.ext` 的类型来自**扩展 trait**：

```rust
impl_ext_accessors!(AuthNs => AuthAccess, auth, try_auth);   // → impl AuthAccess for ExtBag
use auth_pkg::AuthAccess;                                     // 使用者 use 一下就能调
```

也就是说 `ctx.ext.auth()` 能否编译，只取决于 trait 在不在作用域里，与挂了哪些
中间件**完全无关**。代价是：**「这个扩展真的会被写入吗」编译期不知道**，
缺失时 panic（`ExtBag::get` 的文档明写了这点）。

meta 如果照抄，能得到的只有：
- ✅ 字段名拼错 → 编译报错
- ✅ 结构不对 → 编译报错
- ❌ **忘了传 `__dagPlan` → 运行期 panic**

用户的要求是「编译检查能报错就不要 debug 才发现」，第三条必须也在编译期。
要做到它，就必须让**声明**进入类型——这就是下面的 `MetaMiddleware<K>`。


### 中间件携带声明：`MetaMiddleware<K>`

```rust
pub fn dag(spec: DagSpec) -> MetaMiddleware<DagPlanKey>   // 包着 Middleware + 类型标记
```

`K` 是**纯类型标记**（`PhantomData`，运行期零开销）。一个中间件消费多个 key 时
用元组：`MetaMiddleware<(DagPlanKey, ReseedKey)>`——累积时展开成两个条目，
读取时各取各的。**不需要新机制**。

### 链式单注册（偏离 0014）

数组要求**元素完全同型**，`MetaMiddleware<A>` 与 `MetaMiddleware<B>` 放不进同一个
`[T; N]`。逐个注册是唯一不要求使用者写类型参数的办法：

```rust
create_workflow(cfg)
    .middleware(auth())        // 普通中间件，无 meta 声明
    .middleware(dag(spec))     // → 累积 DagPlanKey
    .middleware(reseed(plan))  // → 累积 ReseedKey
    .handler(|ctx| async move {
        let plan = ctx.meta.dag_plan();      // 强类型，无需 unwrap
        ...
    })
    .run(opts)                 // opts.meta 缺字段 → 编译报错
```

`.middleware()` 改为收**单个**中间件、可反复调用。`0014` 的「收数组」在这一点上
被偏离——用户已确认此处不必对齐上游 TS。

### 编译期检查落在哪

- `.middleware(..)`：累积「声明了哪些 key」的类型集合（类型层累加，值层不动）
- `run_workflow(wf, opts)`：`opts.meta` 的类型必须能覆盖该集合，缺则**编译报错**
- handler 侧：`ctx.meta.dag_plan()` 直接可用，无需 `Option`

## 代价

1. **builder 的泛型逐层嵌套**：N 个带声明的中间件 → N 层嵌套类型。编译期没问题，
   但错误信息会很长，且 builder 不能存进 `Vec`（类型必须静态确定）。
   缓解：用 `type` 别名隐藏；用 `#[diagnostic::on_unimplemented]` 改善报错。
2. **偏离 0014**：`.middleware()` 从收数组改为收单个。
3. **`MetaMiddleware<K>` 与 `Middleware` 是两个类型**：中间件包要同时提供
   （`dag()` 返回带声明的版本；不需要 meta 的场景用普通 `Middleware`）。
4. **中间件内部读 meta 走 `MetaBag`**（与 `ctx.ext` 对称）：

   ```rust
   sc.ctx.meta: MetaBag                       // 不再是 Option<Value>
   sc.ctx.meta.try_dag_plan()                 // Option<DagPlan>——中间件用这个
   ```

   **中间件必须用 `try_*`**：meta 是调用方给的，中间件阶段它可能没传
   （例如没传 `__dagPlan` 表示「不启用 DAG」）。而 handler 侧的 `dag_plan()`
   在编译期已保证有值，可以直接返回非 `Option`。
   这与 `ExtBag` 的 `get` / `try_get` 双入口一致，不需要新概念。

## 证据

### 已实现（第一阶段）：`MetaBag` + 访问器

- `packages/workflow_core/src/define/meta.rs`：`MetaKey` / `MetaBag` /
  `impl_meta_accessors!`，与 `ext.rs` 逐项对称。
- `BaseCtx.meta: MetaBag`（原 `Option<Value>`）。
- 目前提供：**字段名与结构错误的编译期检查**；缺失值仍是运行期 panic。

### 已验证（第二阶段设计）：从 workflow 提取 options 的类型

spike 跑通，形状如下（**使用者不写任何类型参数**）：

```rust
let wf = create_workflow().middleware(dag()).middleware(reseed()).handler();
//   wf: Workflow<Cons<DagPlanKey, Cons<ReseedKey, Nil>>>

let opts = RunOptions::of(&wf)                       // 类型从 wf 提取
    .set::<DagPlanKey>(plan)                         // 推进「已提供」集合
    .set::<ReseedKey>(rp);

run(wf, opts);   // 要求「已提供」== 「需要」
```

关键点：`RunOptions<L, F>` 携带**两个**列表——`L` 是 workflow 声明的需求，
`F` 是已经 `set` 过的；`run` 处要求 `F == L`。

漏 `set` 一个 key 时，编译器报：

```
expected `Cons<ReseedKey, Nil>`, found `Nil`
```

——缺哪个一眼可见。这是「忘了传值也编译报错」的实现方式。

`RunOptions::of(&wf) -> RunOptions<L>` 是**零注解**的关键：类型从 workflow 定义
提取，而不是使用者书写（后者是本文否决的方案 A）。

## 待定

- `.middleware()` 收单个还是保留批量入口。
- 第二阶段落地时，`RunWorkflowOptions` 与 `run_workflow` 的签名改动面
  （现有调用点全部要改成 `RunOptions::of(&wf)` 链式）。


## 已定

- 中间件内部读 meta：换成 `MetaBag`，用 `try_*` 取（见「代价」第 4 条）。
  与 `ctx.ext` 的 `ExtBag` 对称，且不需要给 `MiddlewareServerFnArgs` 加泛型。

