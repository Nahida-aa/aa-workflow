//! `dub_sf_ocr` 的 **DAG 声明式版**：整条流水线用 [`define_dag`] 声明成
//! `DagSpec`（节点闭包 = 服务调用 + 依赖取值），handler 由
//! [`dag_to_handler`] **生成**——用户不写编排体。形状（[`DagSpec::shape`]）
//! 是执行与策略（[`dag_policies`]）的**单一真相源**：没有「手写 try_join!
//! 与形状各编码一遍」的漂移面。原生 Rust 并行编排的教具在主例
//! [`super::dub_run`]（手写 try_join!）——两个世界各司其职。
//!
//! # 节点闭包的形状
//!
//! `DagRunFn(deps, input)` 全是 JSON：上游产物从 `deps[节点 id]` 反序列化
//! 成 typed 产物，run input 经 [`DubSfOcrState::from_input`] 重建 state
//! （与 builder 的 `initialize` 同一实现）。模型类节点经 [`DubEnv`] 调
//! 「外部服务」，本地 ffmpeg 类节点直接调自由函数——与主例同一套服务实现。
//!
//! env 的捕获仪式（[`DagRunFn`] 是 `Arc<dyn Fn>`，`'static` future 不许借
//! 用）：`dub_spec` 里**闭包外预克隆**一份 handle、闭包内**每次调用再克隆**
//! ——`DubEnv` 是 Arc 内核，克隆只是多一个指向同一环境的 handle。
//!
//! # 场景（`tests/dub_sf_ocr_dag.rs` 实测）
//!
//! 1. **全量首跑**：不传 meta → 中间件透明，10 步全真跑；
//! 2. **redo（世界变了）**：`DagPlan { source_run_id, from: ["translate"] }`
//!    ——shape 把 `translate` 的**下游闭包**展开成重跑名单（翻译轴 5 步真跑），
//!    人声轴 + OCR 链 5 步用源结果复用（`Decision::SkipImported` 落 checkpoint
//!    **带 imported_from 溯源**）；
//! 3. **临时目标**：`DagPlan { goals: ["translate"] }` → goals 全 checkpoint 后
//!    在 `split_audio` 入口泊车 → `deliver_signal(__interim-goal)` 续驱补齐；
//! 4. **步骤抛错 → reseedRun 恢复**（环境事件注入翻译服务故障）。
//!
//! # `get_events` 为什么由宿主注入
//!
//! 中间件读源 run 日志——store 是 runtime 侧的设施，workflow 定义不持有；
//! 使用者在组装 runtime 时把「读日志」的闭包连同中间件一起挂进定义
//! （见 `tests/dub_sf_ocr_dag.rs` 的 runtime 组装）。
use std::sync::Arc;

use aa_workflow_middlewares::{DagSpec, GetEvents, dag_policies, dag_to_handler, define_dag};
use serde_json::Value;

use super::{
    DubEnv, DubSfOcrInput, DubSfOcrState, FixedSubtitle, Keyframes, MixedAudio, RawSubtitle,
    Separated, Timings, TtsBatch, Translated, dub_builder, mix_audio, mix_video, sf_ocr_pre,
    split_audio,
};

/// 节点闭包里的 state 重建：run input（已序列化成 Value）→ typed input →
/// [`DubSfOcrState::from_input`]（与 builder 的 `initialize` 同一实现）。
fn st_of(input: &Value) -> anyhow::Result<DubSfOcrState> {
    let input: DubSfOcrInput = serde_json::from_value(input.clone())?;
    Ok(DubSfOcrState::from_input(&input))
}

/// dub 流水线的声明：10 个节点 + 依赖 + 执行体。
///
/// 模型类节点经 [`DubEnv`]（各闭包一份 handle 克隆）；本地 ffmpeg 类
/// 节点（抽帧 / 切分 / 混音 / 合成视频）直接调自由函数。
fn dub_spec(env: DubEnv) -> DagSpec {
    // env 的 per-node 预克隆（见模块文档的捕获仪式）。
    let env_sep = env.clone();
    let env_sep2 = env.clone();
    let env_ocr = env.clone();
    let env_fix = env.clone();
    let env_tr = env.clone();
    let env_tts = env.clone();

    DagSpec::new("dub-sf-ocr")
        // 分支 A：分离人声 + 伴奏。
        .node("separate", Vec::<&str>::new(), move |_deps, input| {
            let env = env_sep.clone();
            async move {
                let st = st_of(&input)?;
                let out = env.separate(&st).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("separate_after", ["separate"], move |deps, input| {
            let env = env_sep2.clone();
            async move {
                let st = st_of(&input)?;
                let prev: Separated = serde_json::from_value(deps["separate"].clone())?;
                let out = env.separate_after(&st, &prev).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        // 分支 B：抽关键帧 → OCR → 校正 → 翻译（抽帧是本地处理）。
        .node("sf_ocr_pre", Vec::<&str>::new(), move |_deps, input| {
            async move {
                let st = st_of(&input)?;
                let out = sf_ocr_pre(&st).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("sf_ocr", ["sf_ocr_pre"], move |deps, input| {
            let env = env_ocr.clone();
            async move {
                let st = st_of(&input)?;
                let frames: Keyframes = serde_json::from_value(deps["sf_ocr_pre"].clone())?;
                let out = env.sf_ocr(&st, &frames).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("sf_ocr_fix", ["sf_ocr"], move |deps, input| {
            let env = env_fix.clone();
            async move {
                let st = st_of(&input)?;
                let raw: RawSubtitle = serde_json::from_value(deps["sf_ocr"].clone())?;
                let out = env.sf_ocr_fix(&st, &raw).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("translate", ["sf_ocr_fix"], move |deps, input| {
            let env = env_tr.clone();
            async move {
                let st = st_of(&input)?;
                let fixed: FixedSubtitle = serde_json::from_value(deps["sf_ocr_fix"].clone())?;
                let out = env.translate(&st, &fixed).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        // 汇聚链（全是本地处理，直接吃两支的产物）。
        .node(
            "split_audio",
            ["separate_after", "translate"],
            move |deps, input| {
                async move {
                    let st = st_of(&input)?;
                    let vocals: Separated =
                        serde_json::from_value(deps["separate_after"].clone())?;
                    let translated: Translated =
                        serde_json::from_value(deps["translate"].clone())?;
                    let out = split_audio(&st, &vocals, &translated).await?;
                    Ok(serde_json::to_value(out)?)
                }
            },
        )
        .node("tts", ["split_audio"], move |deps, input| {
            let env = env_tts.clone();
            async move {
                let st = st_of(&input)?;
                let timings: Timings = serde_json::from_value(deps["split_audio"].clone())?;
                let out = env.tts(&st, &timings).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("mix_audio", ["separate_after", "tts"], move |deps, input| {
            async move {
                let st = st_of(&input)?;
                let vocals: Separated = serde_json::from_value(deps["separate_after"].clone())?;
                let batch: TtsBatch = serde_json::from_value(deps["tts"].clone())?;
                let out = mix_audio(&st, &vocals, &batch).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node(
            "mix_video",
            ["separate_after", "mix_audio"],
            move |deps, input| {
                async move {
                    let st = st_of(&input)?;
                    let separated: Separated =
                        serde_json::from_value(deps["separate_after"].clone())?;
                    let audio: MixedAudio = serde_json::from_value(deps["mix_audio"].clone())?;
                    let out = mix_video(&st, &separated, &audio).await?;
                    Ok(serde_json::to_value(out)?)
                }
            },
        )
}

/// DAG 策略版 dub 流水线：声明 spec → handler **生成** + [`dag_policies`]
/// 中间件（形状从 spec 派生——执行与策略同一张图）。
///
/// 环境由调用方注入（[`DubEnv`]）：runtime 的 `load` 闭包每次装配定义时
/// 克隆同一个 handle，测试侧持同一柄——「服务故障 / 恢复」跨 run 生效。
pub fn dub_sf_ocr_dag(
    get_events: Arc<GetEvents>,
    env: DubEnv,
) -> aa_workflow_core::WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    let spec = define_dag(dub_spec(env)).expect("dub spec 合法（无重复 / 未知依赖 / 环）");
    // 形状从 spec 派生——执行与策略共用同一张图（单一真相源）。
    let shape = Arc::new(spec.shape().expect("dub 形状无环"));
    dub_builder()
        .middleware(dag_policies(shape, get_events))
        .handler(dag_to_handler(spec))
}
