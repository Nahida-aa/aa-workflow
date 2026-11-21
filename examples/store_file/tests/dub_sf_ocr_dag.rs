//! `dub_sf_ocr` × **DAG 声明式调度 + 策略中间件**——真实 10 步流水线上的
//! redo（世界变了重跑翻译轴）与临时目标（泊车 + 续驱）。
//!
//! 定义在 `src/dub_sf_ocr/dag.rs`（`dub_sf_ocr_dag`：`define_dag` 声明 spec →
//! handler 由 `dag_to_handler` **生成**，形状从 spec 派生——执行与策略同一
//! 张图；服务调用经 `DubEnv` 环境边界）。store 是 **FileExecutionStore**
//! ——redo 的源日志跨 run 持久（run2 读 run1 的事件）。
//!
//! 观测口径：**真跑** = `STEP_STARTED`（skip 复用不发起 attempt）；
//! **复用** = `STEP_FINISHED.meta.imported_from`（`SkipImported` 带溯源）。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use aa_workflow_middlewares::{DagPlan, DagShape, GetEvents};
use aa_workflow_runtime::run_store_adapter::WorkflowExecutionStore;
use aa_workflow_core::{StepMeta, WorkflowEvent};
use aa_workflow_runtime::types::ReadEventsArgs;
use aa_workflow_runtime::{
    WorkflowRegistration, WorkflowRuntimeConfig, WorkflowRuntimeDefinition,
    WorkflowRuntimeRunResult, WorkflowRuntimeRunResultKind, WorkflowRuntimeStartRunArgs,
    create_run_store_adapter, define_workflow_runtime,
};
use example_store_file::dub_sf_ocr::dag::dub_sf_ocr_dag;
use example_store_file::dub_sf_ocr::{DubEnv, DubService};
use example_store_file::FileExecutionStore;

/// runtime 组装：FileExecutionStore + dag 策略版 dub 注册 + `get_events` 注入。
///
/// `get_events` 从同一把 store 读（adapter 升格到 core `RunStore` 面）——
/// `dag_policies` 靠它读 run1 的日志算 redo 名单。缺省环境（全健康）。
fn get_runtime(base: &PathBuf) -> (Arc<WorkflowRuntimeDefinition>, Arc<GetEvents>) {
    get_runtime_with_env(base, &DubEnv::default())
}

/// 同上，但环境由调用方注入：测试持一份 [`DubEnv`] handle，`load` 闭包克隆
/// 同一柄进定义——「服务故障 / 恢复」由测试改环境表达，对后续 run 生效。
fn get_runtime_with_env(
    base: &PathBuf,
    env: &DubEnv,
) -> (Arc<WorkflowRuntimeDefinition>, Arc<GetEvents>) {
    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(base));
    // `get_events` 闭包持有 adapter（Arc<dyn RunStore>，Clone ✓）
    let adapter = create_run_store_adapter(store.clone());
    let get_events: Arc<GetEvents> = Arc::new(move |run_id: &str| {
        adapter.get_events(run_id).map_err(Into::into)
    });
    let mut workflows = HashMap::new();
    workflows.insert(
        "dub-sf-ocr".to_string(),
        WorkflowRegistration {
            load: {
                let get_events = Arc::clone(&get_events);
                let env = env.clone();
                Arc::new(move || {
                    let get_events = Arc::clone(&get_events);
                    dub_sf_ocr_dag(get_events, env.clone()).into()
                })
            },
            previous_versions: HashMap::new(),
            version: None,
            schedules: vec![],
        },
    );
    (
        Arc::new(define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows)).unwrap()),
        get_events,
    )
}

async fn start(
    rt: &WorkflowRuntimeDefinition,
    run_id: &str,
    input: serde_json::Value,
    meta: Option<serde_json::Value>,
) -> WorkflowRuntimeRunResult {
    rt.start_run(WorkflowRuntimeStartRunArgs {
        workflow_id: "dub-sf-ocr".to_string(),
        run_id: run_id.to_string(),
        input,
        meta,
        ..Default::default()
    })
    .await
    .expect("start_run")
}

/// 某 run 的全部 `STEP_FINISHED`（真跑与复用都落这条 checkpoint——skip 复用
/// 同样落盘，所以日志里每步恰好一条）。
fn finished_events(rt: &WorkflowRuntimeDefinition, run_id: &str) -> Vec<(String, StepMeta)> {
    rt.config
        .store
        .read_events(ReadEventsArgs { run_id: run_id.to_string(), from_index: None })
        .expect("读事件")
        .into_iter()
        .filter_map(|stored| match stored.event {
            WorkflowEvent::StepFinished { step_id, meta, .. } => {
                Some((step_id, meta.unwrap_or_default()))
            }
            _ => None,
        })
        .collect()
}

/// **真跑**的 step：checkpoint 不带 `imported_from` 溯源。
fn executed_steps(rt: &WorkflowRuntimeDefinition, run_id: &str) -> Vec<String> {
    finished_events(rt, run_id)
        .into_iter()
        .filter(|(_, m)| m.imported_from.is_none())
        .map(|(id, _)| id)
        .collect()
}

/// **复用**的 step：checkpoint 带 `imported_from` 溯源（源 run + 下标）。
fn imported_steps(rt: &WorkflowRuntimeDefinition, run_id: &str) -> Vec<String> {
    finished_events(rt, run_id)
        .into_iter()
        .filter(|(_, m)| m.imported_from.is_some())
        .map(|(id, _)| id)
        .collect()
}

/// 场景 ①：全量首跑——无 meta，中间件透明，10 步全真跑。
#[tokio::test]
async fn full_first_run() {
    let (rt, _ge) = get_runtime(&temp_base("dag-full"));
    let out = start(&rt, "dag:r1", serde_json::json!({ "videoDir": "/w/exp" }), None).await;
    assert!(matches!(out.kind, WorkflowRuntimeRunResultKind::Completed));
    let executed = executed_steps(&rt, "dag:r1");
    assert_eq!(executed.len(), 10, "10 步全真跑");
}

/// 场景 ②：redo（世界变了重跑翻译轴）——`from: ["translate"]`，shape 展开
/// 下游闭包 → 翻译轴 5 步真跑；人声轴 + OCR 链 5 步复用（带溯源）。
#[tokio::test]
async fn redo_reruns_the_translation_axis() {
    let (rt, _ge) = get_runtime(&temp_base("dag-redo"));
    start(&rt, "dag:r1", serde_json::json!({ "videoDir": "/w/exp" }), None).await;

    let out = start(
        &rt,
        "dag:r2",
        serde_json::json!({ "videoDir": "/w/exp" }),
        Some(serde_json::json!({
            "__dagPlan": {
                "sourceRunId": "dag:r1",
                "from": ["translate"],
            }
        })),
    )
    .await;
    assert!(matches!(out.kind, WorkflowRuntimeRunResultKind::Completed));

    let executed = executed_steps(&rt, "dag:r2");
    for id in ["translate", "split_audio", "tts", "mix_audio", "mix_video"] {
        assert!(executed.contains(&id.to_string()), "{id} 应真跑");
    }
    for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
        assert!(!executed.contains(&id.to_string()), "{id} 应复用（不重跑）");
    }
    let imported = imported_steps(&rt, "dag:r2");
    assert_eq!(imported.len(), 5, "5 步复用且带溯源");
    for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
        assert!(imported.contains(&id.to_string()), "{id} 应带 imported_from");
    }
}

/// 场景 ③：临时目标——goals=["translate"]，translate 完成后在
/// `split_audio` 入口泊车；`deliver_signal(__interim-goal)` 续驱补齐。
#[tokio::test]
async fn interim_goal_parks_then_resumes() {
    let (rt, _ge) = get_runtime(&temp_base("dag-goal"));
    let out = start(
        &rt,
        "dag:g1",
        serde_json::json!({ "videoDir": "/w/exp" }),
        Some(serde_json::json!({ "__dagPlan": { "goals": ["translate"] } })),
    )
    .await;
    assert!(
        matches!(out.kind, WorkflowRuntimeRunResultKind::Paused),
        "goals 达成后泊车"
    );

    eprintln!("[exp] run 记录: {:?}", rt.config.store.load_run("dag:g1"));
    // 续驱：保留信号 `__interim-goal` 投递（runtime 原生 recover 路径）。
    let resumed = rt
        .deliver_signal(aa_workflow_runtime::WorkflowRuntimeDeliverSignalArgs {
            run_id: "dag:g1".into(),
            signal_id: "dag-goal-resume".into(),
            name: "__interim-goal".into(),
            payload: serde_json::json!({}),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(resumed.kind, WorkflowRuntimeRunResultKind::Completed));

    // 泊车前的步骤 + 续驱的步骤 = 全部 10 步。
    let executed = executed_steps(&rt, "dag:g1");
    assert_eq!(executed.len(), 10, "泊车前 6 步 + 续驱 4 步 = 10 步全真跑");
}

fn temp_base(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_dub_dag_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    dir
}

/// 注册键 ≠ 定义 id：**容错**（load 不拒绝、驱动照常）——但 resume 按
/// run.workflowId（= 定义 id）加载，注册键不一致会让「先 park 后 deliver」
/// 这类续驱找不到注册。告警在 load_workflow 发出（stderr）。
#[tokio::test]
async fn mismatched_registration_key_still_drives_start() {
    let base = temp_base("dag-mismatch");
    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(&base));
    let mut workflows = HashMap::new();
    // 注册键（下划线）≠ 定义 id（连字符，见 dub_builder 的 config）。
    workflows.insert(
        "mismatched_key".to_string(),
        WorkflowRegistration {
            load: {
                let get_events = get_events_of(store.clone());
                Arc::new(move || {
                    let get_events = Arc::clone(&get_events);
                    dub_sf_ocr_dag(get_events, DubEnv::default()).into()
                })
            },
            previous_versions: HashMap::new(),
            version: None,
            schedules: vec![],
        },
    );
    let rt = Arc::new(
        define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows)).unwrap(),
    );
    let out = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "mismatched_key".into(),
            run_id: "dag:mismatch".into(),
            input: serde_json::json!({ "videoDir": "/w/1" }),
            ..Default::default()
        })
        .await
        .unwrap();
    // 容错：首次驱动照常（键只在 resume 按 run.workflowId 加载时才炸）。
    assert!(matches!(out.kind, WorkflowRuntimeRunResultKind::Completed));
}

/// get_events（读日志闭包）从 store 组装的公共提取——避免与上面重复。
fn get_events_of(store: Arc<dyn WorkflowExecutionStore>) -> Arc<GetEvents> {
    let adapter = create_run_store_adapter(store);
    Arc::new(move |run_id: &str| adapter.get_events(run_id).map_err(Into::into))
}

/// 场景 ④：**步骤抛错 → reseed 型恢复（不指定目标的续驱）**——
/// run1：**环境**故障（`service_down(Translate)`，翻译服务 503）→
/// translate 抛错 → `Errored`（人声轴 + OCR 链 5 步成功在案，
/// translate `STEP_FAILED`）；
/// run2：环境恢复（`service_up(Translate)`）+ reseedRun **缺省种子**
/// （源日志未成功节点 = translate 及其下游 4 步）→ 翻译轴真跑、幸存 5 步
/// 导入 → `Finished`。
///
/// 故障是**环境状态**，不是 input：input 只有用户意图（videoDir），「服务
/// 挂了没有」由 [`DubEnv`] 回答——真实系统里用户从不声明服务健康度，
/// 世界变了改的是环境，不是 input。
///
/// 恢复型续驱（同 run 重驱）在这里是 rethrow 而非恢复（失败是终态）——
/// reseed 型才是跑错之后的恢复路（`errored_run_redrive_rethrows` 引擎测试
/// 钉过前半，本场景补真实流水线上的后半）。
#[tokio::test]
async fn errored_run_recovers_via_reseed_run() {
    let env = DubEnv::default();
    let (rt, _ge) = get_runtime_with_env(&temp_base("dag-err"), &env);

    // run1：翻译服务故障（环境事件）→ errored（5 步成功在案）
    env.service_down(DubService::Translate);
    let broken = start(
        &rt,
        "dag:e1",
        serde_json::json!({ "videoDir": "/w/exp" }),
        None,
    )
    .await;
    assert!(
        matches!(broken.kind, WorkflowRuntimeRunResultKind::Errored),
        "translate 抛错 → errored"
    );
    let executed_1 = executed_steps(&rt, "dag:e1");
    for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
        assert!(executed_1.contains(&id.to_string()), "{id} 成功在案");
    }
    assert!(
        !executed_1.contains(&"translate".to_string()),
        "translate 抛错（无成功 checkpoint）"
    );

    // run2：世界变了（服务恢复——环境事件，input 不动）——reseedRun 缺省
    // 种子 = run1 日志未成功节点（translate 及其下游 4 步）。
    env.service_up(DubService::Translate);
    let fixed = start(
        &rt,
        "dag:e2",
        serde_json::json!({ "videoDir": "/w/exp" }),
        Some(serde_json::json!({
            "__dagPlan": { "sourceRunId": "dag:e1" }
        })),
    )
    .await;
    assert!(matches!(fixed.kind, WorkflowRuntimeRunResultKind::Completed));

    let executed_2 = executed_steps(&rt, "dag:e2");
    for id in ["translate", "split_audio", "tts", "mix_audio", "mix_video"] {
        assert!(executed_2.contains(&id.to_string()), "{id} 真跑（未成功节点）");
    }
    for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
        assert!(
            !executed_2.contains(&id.to_string()),
            "{id} 复用（imported_from 溯源）"
        );
    }
}
