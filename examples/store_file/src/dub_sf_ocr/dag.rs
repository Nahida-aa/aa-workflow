//! `dub_sf_ocr` 的 **DAG 策略版**：同一条流水线 + [`dag_policies`] 中间件
//! （读 `RunWorkflowOptions.meta` 里的 [`DagPlan`][crate::dag::policies::DagPlan]）。
//!
//! # 三个场景（`tests/dub_sf_ocr_dag.rs` 实测）
//!
//! 1. **全量首跑**：不传 meta → 中间件透明，10 步全真跑；
//! 2. **redo（世界变了）**：`DagPlan { source_run_id, from: ["translate"] }`
//!    ——shape 把 `translate` 的**下游闭包**展开成重跑名单（翻译轴 5 步真跑），
//!    人声轴 + OCR 链 5 步用源结果复用（`Decision::SkipImported` 落 checkpoint
//!    **带 imported_from 溯源**）；
//! 3. **临时目标**：`DagPlan { goals: ["translate"] }` → goals 全 checkpoint 后
//!    在 `split_audio` 入口泊车 → `deliver_signal(__interim-goal)` 续驱补齐。
//!
//! # `get_events` 为什么由宿主注入
//!
//! 中间件读源 run 日志——store 是 runtime 侧的设施，workflow 定义不持有；
//! 使用者在组装 runtime 时把「读日志」的闭包连同中间件一起挂进定义
//! （见 `tests/dub_sf_ocr_dag.rs` 的 runtime 组装）。
use std::sync::Arc;

use aa_workflow_middlewares::{DagShape, GetEvents, dag_policies};

use super::{dub_builder, dub_run, DubSfOcrInput, DubSfOcrState};

/// `dub_sf_ocr` 的依赖形状：两条并行分支 + 汇聚链（10 步）。
pub fn dub_shape() -> Arc<DagShape> {
    Arc::new(
        DagShape::build(&[
            ("separate", &[]),
            ("separate_after", &["separate"]),
            ("sf_ocr_pre", &[]),
            ("sf_ocr", &["sf_ocr_pre"]),
            ("sf_ocr_fix", &["sf_ocr"]),
            ("translate", &["sf_ocr_fix"]),
            ("split_audio", &["separate_after", "translate"]),
            ("tts", &["split_audio"]),
            ("mix_audio", &["separate_after", "tts"]),
            ("mix_video", &["separate_after", "mix_audio"]),
        ])
        .expect("dub 形状无环"),
    )
}

/// DAG 策略版 dub 流水线：同一条 handler 链 + [`dag_policies`] 中间件。
pub fn dub_sf_ocr_dag(
    get_events: Arc<GetEvents>,
) -> aa_workflow_core::WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    dub_builder()
        .middleware(dag_policies(dub_shape(), get_events))
        .handler(dub_run)
}
