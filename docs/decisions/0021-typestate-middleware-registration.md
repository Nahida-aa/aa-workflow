# 0021 — 中间件注册 typestate 化：静态栈零箱、擦除只留给动态

**状态**：已接受（2026-11-21）
**关联**：[0020](0020-scope-borrowed-middleware-server.md)（scope-borrowed server）、
[0017](0017-meta-typed-keys.md)（meta 类型栈先例）、`docs/feature-requirements.md` F9

## 背景

中间件注册目前只有**列表形态**（`.middleware([a, b, c])`，元素为擦除的
`AnyMiddleware`），洋葱在定义期**逐层擦除**（每层一个 `Arc<dyn Fn>`）+ 每次
drive 的 server 调用再擦一次（`serve` 返回 `BoxFuture`）。

但注册的**常态是逐个**：`.middleware(a).middleware(b)`——传列表是少数需求
（低代码/运行时动态决定集合）。使用形态的频率分布恰好对准 tower 的两半架构：
`ServiceBuilder`（typestate 静态栈，零箱）vs `BoxService`（擦除，给动态）。

## 被否/受限的选项（编译器与结构实测）

1. **洋葱零箱的真正障碍是 `next(ext)` 协议本身**：pipeline 闭包（`NextFn`）
   按层异构，CPS（续延传递）形状决定了**每层每 drive 一次擦除是结构性的**——
   typestate 化消除的是定义期逐层箱（N+1 → 1）与 drive 期 serve-future 箱
   （N → 0），pipeline 的 N 次箱保留。
2. **零箱洋葱需要 Clone 纪律**（tower 同款）：递归组合要让 `'static` 闭包
   持有各层，`MiddlewareServer` 需要 `Clone` supertrait（我们的 struct 中间件
   全部持 `Arc` 字段，Clone = 引用计数）。不引入 Clone 的话，`serve<'a>` 的
   借用 future 无法进入 `'static` 链——这正是 0020 之前逐层擦除的根本原因。
3. **单件与元组不能共用一个 bound**：`impl MiddlewareStack for M where
   M: MiddlewareServer` 与 `impl<H, T> MiddlewareStack for (H, T)` 在下游
   为元组实现 `MiddlewareServer` 时构成 coherence 冲突——单件走
   `Append<M>`（type-level 追加），静态列表直接传元组（元组本身是栈）。
4. **AsyncFn 擦除墙**（承 0020）：`E0658`/`E0038`/`E0191` 三道墙未解，
   动态元素的擦除继续走 `DynMiddlewareServer`（现 `MiddlewareServer` 的
   dyn 形态改名保留）。

## 决策

1. **trait 拆分**：
   - `MiddlewareServer: Send + Sync + Clone + 'static`——静态门，
     `fn serve<'a>(&'a self, args) -> impl Future<Output = Result<Value>> + Send + 'a`
     （RPITIT，1.75+；具体中间件的 future 不再装箱）；
   - `DynMiddlewareServer: Send + Sync`——擦除门，`fn call<'a>(&'a self, args)
     -> BoxFuture<'a, Result<Value>>`（现 trait 改名保留）；blanket
     `impl<M: MiddlewareServer> DynMiddlewareServer for M`（任何静态中间件
     可按需擦除）。
2. **`MiddlewareStack`**（type-level 栈）：`()` 为终端；`(Head, Tail)` 递归；
   `fn serve(&self, inner: InnerFn) -> InnerFn`——定义期**泛型折叠**，
   洋葱组合为一个 `InnerFn`（Arc），定义只擦**一次**。`Append<M>` 做类型级
   追加（`()` / `(H, T)` 两个递归 impl）。
3. **builder typestate**：`WorkflowBuilder<.., TMw = ()>`；
   - `.middleware(m: impl MiddlewareServer)`——单件追加（常态），
     `TMw::Append<M>` 重键；
   - `.middleware_any(iter)`——擦除列表（低代码逃生门，元素为
     `Middleware`/擦除句柄，逐层箱 = 动态的固有税）；
   - `meta_middleware<K>` 保留（TMeta 重键不变，链元素暂为擦除形态——
     中间件自声明 meta 键的折叠留待后续）。
   静态长列表直接传元组 `.middleware((a, b, c))`（元组即栈），不需要宏；
   真要糖可后加（`middleware![]` 展开成链式）。
4. **drive 期的 future 箱从 2N 降为 N**（RPITIT serve 的 async 体不再
   装箱；每层链闭包的 body 箱与 pipeline 的 N 保留——后者是 CPS 协议的
   结构性成本）。定义期的组合 Arc 数不变（N 层闭包 + 1 个 innermost——
   递归折叠的形状决定；2026-11-21 实现后核账修正，初稿误写为「降为 1」）。

## 修订（2026-11-21，落地当轮）

- **擦除逃生门（`middleware_any` / `DynListMw`）暂缓**：用户指出「传列表是少数
  需求，低代码场景出现再加」。已知调用点全部迁移为静态链式（含测试里的
  `Vec<Middleware>` 参数改双参数——它们的 N 都是编写时已知的）。逃生门重新
  引入的成本 = 一个 `DynListMw` 栈元素（子洋葱折叠），需求出现时加。
- **`server_state` 移除**（同日二次修订）：用户裁决——中间件**提前定义**，
  **使用点代码应尽可能短**才是常态；axum 式（使用点传闭包/状态）方向反了。
  带状态中间件的规范形态 = struct + `impl MiddlewareServer`（状态即字段，
  定义点可长）+ 构造器函数；使用点 = `.middleware(interim_goals(ge))` 一行。
  独立 async fn + `server_state` 的「最短定义点」形态被否（见
  `docs/experiments/middleware-protocol.md` §7/§8）。
- **RPITIT serve 的 impl 可写 `async fn`**（编译器实测）：`impl MiddlewareServer
  for Mw { async fn serve(&self, args) -> Result<Value> { .. } }`——body 平铺、
  无 `async move` 包裹、无额外缩进（相对 tower `call` 的形状对齐）。两个策略
  中间件已改为此形态；trait 定义保持 `-> impl Future .. + Send + 'a`
  （调用方 Send 从签名可证）。
- 核账修正：定义期的组合 Arc 数**不变**（N 层闭包 + 1 innermost——递归折叠
  形状决定；初稿误写「N+1 → 1」）。降的是 drive 期 future 箱 2N → N。

## 后果

- **兼容**：`Middleware::server(f)` / `server_state(state, f)` /
  `from_impl` 的公开签名不变（内部适配 `DynMiddlewareServer`）；现有
  `.middleware([...])` 调用点迁移为 `.middleware(a).middleware(b)` 或
  `.middleware_any([...])`。
- **Clone 纪律**：`MiddlewareServer: Clone`——struct 中间件的字段必须是
  可克隆句柄（`Arc` = 引用计数，与 tower 的 service-clone 惯例一致）。
- **`serve` 返回 `impl Future`**：具体中间件的 async 体不再装箱；调用方
  （栈递归）保持泛型。tower 用户熟悉的「clone-per-call」保留在 pipeline
  与栈递归的最小必要处。
- **错误信息变长**：builder 类型随中间件数增长（tower ServiceBuilder 的
  已知税）——常态 2~3 个中间件下可接受。

## 测试

- 参数化中间件状态跨 drive 复用（struct + state 两形态）沿用；
- 新增：typestate 栈的注册顺序（洋葱外层=先注册）、擦除逃生门与静态栈
  混装、`Append` 的类型级追加；
- `workflow_middlewares` park race 验收在 RPITIT serve 下全绿。
