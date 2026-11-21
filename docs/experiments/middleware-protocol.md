# 实验记录：中间件协议对照——`next`（CPS）vs tower（`Service`/`Layer`）

**日期**：2026-11-21
**触发**：用户两轮质疑——①「你有专门进行实验吗」（此前结论是纸面推理）；
②「如果 next 协议真的更好，tower 怎么会不选择呢？tower 诞生时 next 协议
就已经流行了呀」（历史论点）。
**证据物**：`packages/workflow_middlewares/tests/protocol_experiment.rs`
（4 测试全绿——tower 侧静态组合实测、CPS 侧真实引擎端到端）。

---

## 1. 历史考证（crates.io 实证）

| 时点 | 事件 | 对 CPS 的影响 |
| --- | --- | --- |
| **2018-02-19** | `tower-service` crate 创建（0.0.0） | **Service 形状定型于 pre-async 时代** |
| 2018-08-09 | `tower-service` 0.1.0 | 关联 Future + poll_ready 的形状冻结 |
| 2019-08 | tower 官宣（interlude 系列） | 泛型 `Layer<S>` 静态组合确立 |
| 2019-11 | **stable async/await** | 此后平铺 CPS 才「语法上可写」 |
| 2019 | `async_trait` 宏 | CPS 可写，但每调用装箱 |
| 2023-12 | **RPITIT/AFIT（1.75）** | CPS 第一次**零箱 + 平铺**双全 |
| 2025-02 | async 闭包 + `AsyncFn*` | 擦除位仍有墙（E0658/E0038/E0191，见 ADR 0020） |

**对用户历史论点的回答**：「next 协议流行」在 **JS** 里对（express/koa 2011-）；
在 **Rust** 里，tower 定型时（2018）平铺 CPS **语法上不存在**——没有 async/await，
future 是 futures-0.1 组合子链。tower 选关联 future 是**当时唯一零箱的写法**，
不是对 CPS 的否定。CPS 在 Rust 里真正可写（boxed）要等 2019 async_trait，
可写且零箱要等 2023 RPITIT——** tower 没改道的主因：改 = 破坏整个生态的
impl**（每个 Service 实现都要迁移），且它还有 CPS 给不了的 `poll_ready`
背压语义。

## 2. 实验设置

同一组行为 × 两种协议 × 同一个 stub 内层（两笔「step」），行为等价断言：

- **B1 纯短路**（before-only）：meta 标志 → leaf 不跑；
- **B2 around**（before + after）：计时并记录——**分水岭**：要看执行结果之后再做事；
- **B3 扩展贡献**：给下游塞数据（CPS=next(Some)，tower=改写 request）。

CPS 侧用真实 `MiddlewareServer`（RPITIT）并跑进真实引擎端到端；tower 侧在
实验文件内定义 `Layer/Service`（tower::Service 原生 trait）手动组合驱动。
行数口径：含 struct/Layer 定义与 where 子句的完整 impl 块。

## 3. 实测对照（protocol_experiment.rs）

| 行为 | CPS（next） | tower boxed | tower 零箱 |
| --- | --- | --- | --- |
| B1 短路 | **~17 行** | ~50 行 | —（同 boxed） |
| B2 around | **~17 行** | ~35 行 | ~53 行（手写包装 future） |
| B3 扩展 | **~14 行** | ~34 行 | — |
| 装箱/驱动 | 每层每 drive 1 箱（pipeline，CPS 结构性） | 每请求 1 箱（boxed） | 零箱 |
| dyn 擦除 | ❌ RPITIT 不可 dyn（需 DynMiddlewareServer 门） | ✅ `dyn Service<.., Future = BoxFuture>` **关联类型可点名** | — |

## 4. 实验对上一轮结论的修正（三条）

1. **「around 逻辑 next 明显更顺」→ 修正为「boxed tower 同样平铺」**：
   tower 的 `Service::Future: Future<Output = Result<Response, Error>>`
   本身就是 Result 包装——`let out = inner.call(req).await?; after; Ok(out)`
   与我们的 `let out = args.next(None).await?; after; Ok(out)` **逐行等价**。
   差异收缩为：tower 需要显式 `Box::pin` 一行 + `poll_ready` 样板 +
   3 个关联类型；我们的 RPITIT impl 是一个 `async fn`（实测 impl 侧
   可直接写 `async fn serve(&self, args) -> Result<Value>`，body 平铺）。
2. **「纯 before 两者差不多」→ 修正为「tower 连纯 before 都有暗坑」**：
   提前返回（短路）与透传分支的类型不一致（装箱 future vs `S::Future`）→
   两个分支必须统一 → 都装箱或写 Either 枚举。CPS 无此问题（两分支都
   `return Ok(..)` 即可）。
3. **tower 的仪式清单实测**（每个 Service impl 都必须掌握）：
   ① `poll_ready` 样板；② `call(&mut self)` 与 async 组合的 mut-clone
   舞步；③ clone-per-request（关联 Future 无生命周期参数，`inner.call`
   返回的 future 借局部 `&mut inner` 会逃逸 → 必须 clone 后移进 future）；
   ④ 零箱 after 需要手写包装 future（~25 行）+ `S::Future: Unpin` 约束
   （真要做零 Pin 用 pin-project）。

## 5. tower 的真优势（实验确认，非纸面）

- **dyn 友好**：关联类型可点名（`Future = BoxFuture`）→ `Arc<dyn Service>`
  擦除门天然成立；RPITIT 不可 dyn，擦除需要独立 trait + 委托（ADR 0021）。
- **现成层生态**：tower-http 全家桶（但本仓的复用点在 step 级——已经 tower
  原生）。

## 6. 结论（回写 ADR 0022 草稿）

1. **处理器级维持 `next` 协议**：作者侧行数 1/3、无仪式清单、after 平铺——
   上游对齐之外的独立理由成立。
2. **tower 形态的定位**：step 级原生（已在用）+ 潜在的 run 级 dyn 门
   （tower 形态擦除比 RPITIT 顺——若未来需要「运行期动态洋葱」，tower
   形态是更自然的擦除语言，此时再加 `from_tower_layer` 桥）。
3. **Box::pin 总账不变**：CPS 的 N 箱（pipeline，结构性）+ tower 静态
   零箱/dyn 每请求一箱——两协议在「擦除 vs 静态」上的取舍与 ADR 0020/0021
   一致。


## 7. 追问（同日）：axum 会不会有更短的写法？

**有，且我们现行的 `server_state` 就是同款机制**——axum 的短不来自协议
（`from_fn` 底下仍是 tower Service），来自**「状态作参数」而非「状态作捕获」**：
独立 `async fn` 的签名里带上状态参数，捕获仪式从根上消失，连 struct/impl 块
都不需要。

实测（`protocol_experiment.rs` 的 `interim_goals_flat`，与 struct 形态
`InterimGoalsMw` 同逻辑）：

| 形态 | 作者侧行数 | 结构单元 | 注册 |
| --- | --- | --- | --- |
| struct + `impl MiddlewareServer`（RPITIT） | ~54 行 | struct + impl + 构造器 | `.middleware(interim_goals(ge))` |
| **独立 async fn + `server_state`** | **~36 行** | **一个 fn** | `.server_state(ge, fn)` |

```rust
// 最短形态：状态是首参，args 次参，body 平铺一层
async fn interim_goals_flat(get_events: Arc<GetEvents>, args: MiddlewareServerFnArgs)
    -> Result<Value> {
    let mut sc = args;
    let goals = sc.ctx.meta.try_get::<InterimGoalsKey>()?.clone();
    let finished = (get_events)(&sc.ctx.run_id)?;   // 状态参数直接用，无克隆仪式
    ...
    sc.next(None).await
}
// 注册：Middleware::new().server_state(get_events, interim_goals_flat)
```

**为什么可行**：`server_state` 的适配器每次调用 clone 一份 state 传作首参，
独立 `async fn` item 天然实现 `Fn((S, Args)) -> impl Future`——不需要闭包
捕获，因此没有 per-call clone 仪式。**这消灭了 struct 形态存在的最常见理由**
（「要带参数所以得写 struct」）。

**三分形态定稿**（谁该用哪个）：

| 需求 | 形态 |
| --- | --- |
| 简单策略（一个状态句柄 + meta 判定） | **独立 async fn + `server_state`**（最短） |
| 多状态字段 / 想单元测试的复杂中间件 | struct + `impl MiddlewareServer` |
| 运行时动态集合（低代码） | 暂缓（ADR 0021 修订注）——出现需求再加 |

**逻辑结论的最终修正**：写起来方便的总排序——`async fn + server_state`
（最短）≥ struct 形态 ≥ tower `Service` 手写 impl——而「协议」之争（next vs
tower）在这个维度上被「状态注入方式」之争取代了：**axum 式的短，本质是
「参数化状态」的短，与 CPS/tower 的协议选择正交**。我们的 next 协议 +
`server_state` 已经拿到这份短。


## 8. 裁决（同日，用户拍板）：`server_state` 移除——使用点最短原则

§7 的实测推翻了我推荐 server_state 的理由：36 行 vs 54 行的「短」发生在
**定义点**，而中间件的本命用法是**提前定义、使用点极短**（`.middleware(interim_goals(ge))`
一行）——定义点可长、使用点必须短。axum 式（使用点传闭包/状态）把定义逻辑
推到注册处，**方向反了，并不好**。

**裁决**：
- `server_state` / `StateServer` **移除**；
- **带状态中间件的规范形态 = struct + `impl MiddlewareServer`**（状态即字段，
  定义点随便长）；
- **使用点规范形态 = 构造器函数调用**（`.middleware(interim_goals(ge))`——
  一行，参数只是配置句柄）；
- 无状态速写闭包（`.server(f)`）保留。

三分形态表作废，替换为两形态：**struct（带状态，规范）/ 闭包（无状态，速写）**。


## 8.1 追加考证：run 记录的 workflowId「键漂移」不是 bug（上游同构）

dub_sf_ocr_dag 示例初版把 `deliver_signal` 的 resume 失败定性为
「FileExecutionStore 的潜在 bug」（注册键 `dub_sf_ocr` 被 run 记录里的
`dub-sf-ocr` 覆盖）。**查上游后撤回**：

- 上游 `run-workflow.ts:300`：`RunState.workflowId = workflow.id`——
  **定义自身的 id**，不是 startRun 的注册键；`setRunState` 落盘同样
  覆写 run 记录——键漂移行为**同构**；
- 上游不视之为问题，因为**惯例是注册键 == 定义 id**
  （`defineWorkflowRuntime({ workflows: { myWorkflow: .. } })` 的键与
  `createWorkflow({ id: 'myWorkflow' })` 写同一字符串）；
- 我们的偏差是**测试注册键与定义 id 不一致**——修正 = 注册键对齐
  定义 id（`"dub-sf-ocr"`），与上游惯例一致；
- 教训：定性为 bug 前先查上游——本例查证后从「store bug」变为
  「测试键名不一致 + 注册惯例未成文」（惯例已补进测试与文档）。
- **告警已落地**（runtime 层，core 无注册键）：`load_workflow` 在注册键 ≠
  定义 id 时发 stderr 告警（提示 resume 按 run.workflowId 加载将找不到
  注册）。**容错不拒绝**：首驱照常（mismatched_registration_key_still_drives_start
  测试钉住）——键一致是惯例不是硬约束，与上游零校验同款。
