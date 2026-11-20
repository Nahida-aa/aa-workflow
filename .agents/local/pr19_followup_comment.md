# PR #19 追加评论草稿（EN/CN 对照）

> 用途：贴到自己原评论（@Nahida-aa, Oct 3）下方的追加，收回「形成本 PR」的意愿，
> 声明 redo 一族不进 core。
> 原则：不删原评论（"edited" 保留 trail）、不提分支、不提 PoC、不替 tannerlinsley
> 决定主线（buffering/retries/compensation）。

## 英文版（贴到 GitHub 用这段）

**Update** on the `reexecuteFrom` / `interimGoals` sketch above: I've
re-evaluated and now lean toward **not** putting this family into core.

Rationale:

1. It's a local optimization for the "continue / invalidate / goal" use-case
   family — value concentrates on "re-run a few steps", not on generic engine
   semantics.
2. The cost of baking redo semantics into core is clearly higher than the
   payoff: fresh-only gating, invalidation-list closure, source-run
   existence checks — all of those have to be threaded through every drive
   entry. An orchestration layer can compose the equivalent without the
   engine endorsing it.
3. For the majority of cases, "start a fresh run + seed surviving
   checkpoints" (which `reexecuteFrom` already does) plus a
   caller-maintained `sourceRunId → newRunId` mapping already covers it.

So I'd keep core at its minimal surface (replay + checkpoints + primitives)
and leave redo / invalidation / interim-goal parking as drive parameters or
orchestration-level composition. Not planning to shape this into a PR.

## 中文版（存档 / 自己看）

**Update**：重新评估后，我倾向于**不**把 `reexecuteFrom` / `interimGoals`
这一族做进 core。

理由：

1. 它是「继续 / 失效 / 目标」这一使用场景族的局部优化——价值集中在
   「重跑某几步」，不是通用引擎语义。
2. 把 redo 语义焊进 core 的代价明显高于收益：fresh-only 判定、失效
   名单封闭性、源 run 存在性校验——这些都得散进每个 drive 入口。
   编排层完全可以组合出等价效果，不需要引擎背书。
3. 对大多数 case，「开新 run + 播种幸存 checkpoint」（`reexecuteFrom`
   本身已经做到的事）+ 调用方维护 `sourceRunId → newRunId` 映射，
   已经够用。

所以 core 保持最小面（replay + checkpoint + 原语），redo / 失效 /
interim-goal 泊车这族留在驱动参数或编排层做组合。不打算形成本 PR。

## 如果 tannerlinsley 回「那 buffering 主线呢」

- EN: "buffering / retries / compensation stay on PR #19's normal review
  track — my update only covers the `reexecuteFrom` / `interimGoals`
  sketch above."
- CN: buffering / retries / compensation 走 PR #19 正常 review，我这次
  update 只针对上面 `reexecuteFrom` / `interimGoals` 两段。

## 微调选项

- 第 2 条 "threaded through every drive entry" 若嫌太具体（暴露实现判断
  路径），可缩成 "amplifies engine complexity for a narrow use case"。
- "Not planning to shape this into a PR" 显式呼应原评论里
  "Happy to share the branch or shape it into a PR"，不留分支暗示。
