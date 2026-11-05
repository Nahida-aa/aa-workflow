# 决策记录（Decisions）

本目录收录**架构与 API 决策**（ADR 风格）。每条记录：状态、背景、
决策、理由、后果。

**论证要写在这里**，不是只写结论。被否决的选项、代价、踩过的坑、
实测证据都属于 ADR 的正文——只写「决定 X」而读者无从判断为什么不是
Y 时，这条记录在未来就是不可复核的。别的文件（`feature/`、
`feature-requirements.md`）承担的是**需求与机制分析**，不是替代论证。

阅读顺序建议：先看 `feature-requirements.md`（需求与状态总表），需要知道
「为什么这样做」时再查对应 ADR。

| ADR | 标题 | 状态 |
| --- | --- | --- |
| [0001](0001-strategy-vs-mechanism.md) | 策略与机制分层：引擎不加业务参数，策略走中间件包 | 已接受 |
| [0002](0002-rust-step-hooks.md) | Rust 中间件 = 单 server 形状（对齐 TS）；`set_step_interceptor` 取代旧钩子 | 已接受（取代钩子版） |
| [0003](0003-event-taxonomy.md) | 事件分类按上游拆分：StepPaused/StepResume → 四类型 | 已接受 |
| [0004](0004-signal-inbox.md) | 信号收件箱：可选能力方法（A'），core 投递过闸 | 已接受 |
| [0005](0005-naming-reseedRun-redoSet.md) | 命名对齐：reseedRun / redoSet，不留 alias | 已接受 |
| [0006](0006-dag-dual-drive-and-middleware.md) | DAG 双驱动（core/runtime）+ DAG 作为中间件 | 已接受 |
| [0007](0007-middleware-compose-order.md) | 中间件组合顺序约束：调度器必须在最内层 | 已接受 |
| [0008](0008-drop-reference-generator.md) | 删除 docs/reference 生成器与生成物 | 已接受 |
| [0009](0009-no-implicit-new-run.md) | 隐式新开 run 默认禁止（四条放行标准） | 已接受 |
| [0010](0010-ts-middleware-capabilities.md) | TS 中间件的能力事实与参数载体（`input.__interimGoals`） | 已接受 |
| [0011](0011-run-meta-and-plugins.md) | 策略参数走 `RunWorkflowOptions.meta`；插件在构建期挂载 | 已接受 |
| [0013](0013-middleware-generics.md) | `Middleware<TCtxIn, TExtension>`：只有 `TCtxIn` 是实参数 | 已被 0015 取代 |
| [0014](0014-middleware-takes-array.md) | `.middleware()` 收数组（对齐上游），三处差异 | 已被 0015 取代 |
| [0015](0015-ext-namespaces.md) | `ctx.ext` 按命名空间存取：使用者不需要定义合并类型 | 已接受 |
| [0016](0016-step-onion.md) | step 拦截 = tower `Layer` 叠加（取代单值 `set_step_interceptor`） | 已接受 |
| [0017](0017-meta-typed-keys.md) | 插件的 meta 字段编译期可见：`MetaKey` + 链式注册累积 | **草案** |

## 什么时候写新 ADR

- 决定**采纳或拒绝**某个方案（含被否决的选项与理由）；
- 决定**有意偏离**上游（必须写偏离点与代价）；
- 决定**删除**某个已存在的东西。

## 一条 ADR 必须回答

1. **问题**：现在的做法哪里不够，具体是哪个场景坏的（不是「不够优雅」）；
2. **决策**：采纳什么，用代码片段写清形状；
3. **被否决的选项**：至少写一个，以及否决的确切理由——
   「语言不支持」要说明是哪条限制，「体验更差」要说清差在哪；
4. **代价**：这次改动让什么变难了、删掉了什么、需要谁配合迁移；
5. **证据**：测试名 / 实测输出 / 相关提交，让结论可复核。

缺 3 或 4 的，说明还没想清楚，先别写「已接受」。

不写：机制分析（→ `feature/`）、需求状态（→ `feature-requirements.md`）。
