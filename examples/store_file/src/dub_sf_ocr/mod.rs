//! LocalDub `dub_sf_ocr` pipeline 的**声明式定义**（可复用的 workflow）。
//!
//! 放在 `src/` 而不是测试里，是因为它是一个**完整的示例 workflow**——别人可以
//! `use example_store_file::dub_sf_ocr` 拿去驱动自己的 store。测试在
//! `tests/dub_sf_ocr.rs`（引擎语义）与 `tests/dub_sf_ocr_dag.rs`（策略场景）。
//!
//! 从 `examples/shared` 搬过来的（那边已删除）。
//!
//! # 编排只有一个故事：DAG 声明 → handler 生成
//!
//! 整条流水线用 [`define_dag`] 声明成 `DagSpec`（[`dub_spec`]：10 个节点 +
//! 依赖 + 执行体），handler 由 [`dag_to_handler`] **生成**——本文件没有任何
//! 手写编排体。执行是 dataflow 调度：每个节点一个任务，**依赖一就绪就跑**，
//! 没有波次栅栏；失败即 `abort_all`。两个装配入口共用同一份 spec：
//!
//! 1. [`dub_sf_ocr`]——纯定义（健康环境、无中间件）；
//! 2. [`dub_sf_ocr_dag`]——+ [`dag_policies`] 中间件（redo / 临时目标 /
//!    reseedRun，走 `RunWorkflowOptions.meta`）。形状由 `DagSpec::shape()`
//!    派生——**执行与策略共用同一张图**，没有「编排与形状各编码一遍」的
//!    漂移面。
//!
//! # 形状
//!
//! ```text
//!              ┌─ separate ──► separate_after ─────────────────► (bgm)
//! import ──────┤                                                   │
//!              └─ sf_ocr_pre ► sf_ocr ► sf_ocr_fix ► translate ─┐  │
//!                                                               ▼  ▼
//!                                split_audio ◄──────────────────┘  │
//!                                    │                             │
//!                                tts ► mix_audio ► mix_video ◄─────┘
//! ```
//!
//! 两分支**零跨分支读**（A 只吃 `audio_source.wav`，B 只吃 `video_source.mp4`），
//! 所以是两条并行分支；分支内部靠依赖边串行（`separate_after` 要 `separate`
//! 的 stems）。不是 LocalDub 的接线，只是把它的依赖图形状搬过来。
//!
//! # 节点闭包的纪律（ctx-state）
//!
//! 节点闭包经 `sc.ctx.state` **只读** state（typed 快照，无任何写通道——
//! 值拷贝改不动引擎镜像），上游产物从 `deps[节点 id]` 反序列化。DAG 形态
//! 下 state 无写入者：一次 run 内恒定 ≡ `initialize` 产物，事件日志零
//! `STATE_DELTA`（场景 ⑤ 实测钉死）。对外观测走发送（emit/delta，不追加
//! 日志）；对内数据走返回值与依赖关系（进 checkpoint）——两个通道不共用
//! 任何载体（见 `docs/concepts/ctx-state.md`）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use aa_workflow_core::{CreateWorkflowConfig, WorkflowBuilder, WorkflowDefinition, create_workflow};
use aa_workflow_middlewares::{DagSpec, DagShape, GetEvents, dag_policies, dag_to_handler, define_dag};

// ========================================================================
// dub_sf_ocr 的定义
// ========================================================================

/// dub-sf-ocr 输入：示例层不落盘，`videoDir` 只用于拼产物路径。
///
/// 只有**用户意图**。翻译服务是否可用是**环境状态**（外部服务），不在这里
/// ——run 的 input 跨 resume 不可变（ADR 0022：run 是不可变事实），而环境
/// 随时会变，两者不同物。以前这里有个 `translateDown` 标志给 DAG 例子注入
/// 故障，那是拿「启动时的世界事实」给故障注入找的容身处；有了服务边界
/// （[`DubEnv`]），故障注入走环境，input 回归纯意图。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DubSfOcrInput {
    pub video_dir: String,
    /// 目标语言；缺省由 `initialize` 兜成 `"zh"`。
    #[serde(default)]
    pub target_lang: Option<String>,
}

/// dub 依赖的**外部服务**（生产里各是一个模型 / API 进程）。
///
/// 本地 ffmpeg 类处理（抽帧 / 切分 / 混音 / 合成视频）不算——同一台机器上
/// 的子进程，没有独立的健康度要注入，所以它们不走环境边界。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DubService {
    /// 人声 / 伴奏分离模型（`separate` 与 `separate_after` 两次调用共用）。
    Separate,
    /// subtitle-finder 关键帧 OCR。
    SfOcr,
    /// 字幕校正 LLM。
    SfOcrFix,
    /// 翻译模型。
    Translate,
    /// 逐段 TTS。
    Tts,
}

impl DubService {
    fn name(self) -> &'static str {
        match self {
            Self::Separate => "separate",
            Self::SfOcr => "sf_ocr",
            Self::SfOcrFix => "sf_ocr_fix",
            Self::Translate => "translate",
            Self::Tts => "tts",
        }
    }
}

/// 一个外部服务的可用性——可翻转的环境事实（缺省健康）。
///
/// ⚠️ `Default` 手写不派生：`AtomicBool::default()` 是 `false`，派生会把
/// 「缺省健康」变成「缺省全故障」。
#[derive(Clone)]
struct ServiceHealth(Arc<AtomicBool>);

impl Default for ServiceHealth {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}

/// dub 流水线的「环境」：**服务边界**——外部模型调用经由它发生。
///
/// 生产实现连真模型 / 真 API；本示例给可脚本化的假实现：每个服务一条
/// 可用性（[`DubService`]），测试经 [`DubEnv::service_down`] /
/// [`DubEnv::service_up`] 改环境表达「世界变了」——服务故障是一次上游
/// API 调用的返回码，用户从不声明它，所以它不属于 input（用户意图），
/// 属于环境。（以前这里有个 `translateDown` input 字段，是拿「启动时的
/// 世界事实」给故障注入找的容身处。）
///
/// `Arc` 内核：定义侧与测试侧各持一个 handle，共享同一个环境——「服务
/// 恢复」才能对后续 run 生效（dub 在同一进程里连跑 run1 / run2，环境状态
/// 自然延续；这正是真实世界的样子：服务健康度独立于任何 run 的 input 而
/// 变化）。
///
/// 服务实现（[`DubEnv::separate`] 等六个）是**假实现**：stub 计并发 / 记
/// 日志 / 模拟耗时，再过可用性闸门。它们被 [`dub_spec`] 的模型类节点闭包
/// 调用——[`dub_sf_ocr`] 与 [`dub_sf_ocr_dag`] 两个装配共用同一套服务。
#[derive(Clone, Default)]
pub struct DubEnv {
    separate: ServiceHealth,
    sf_ocr: ServiceHealth,
    sf_ocr_fix: ServiceHealth,
    translate: ServiceHealth,
    tts: ServiceHealth,
}

impl DubEnv {
    /// 环境事件：某外部服务故障——之后的调用返回 503。
    pub fn service_down(&self, svc: DubService) {
        self.health(svc).0.store(false, Ordering::SeqCst);
    }

    /// 环境事件：某外部服务恢复。
    pub fn service_up(&self, svc: DubService) {
        self.health(svc).0.store(true, Ordering::SeqCst);
    }

    fn health(&self, svc: DubService) -> &ServiceHealth {
        match svc {
            DubService::Separate => &self.separate,
            DubService::SfOcr => &self.sf_ocr,
            DubService::SfOcrFix => &self.sf_ocr_fix,
            DubService::Translate => &self.translate,
            DubService::Tts => &self.tts,
        }
    }

    /// 服务调用前的可用性闸门——不可用即 503（生产里这是一次上游 API
    /// 调用的返回码）。
    fn check(&self, svc: DubService) -> anyhow::Result<()> {
        if !self.health(svc).0.load(Ordering::SeqCst) {
            anyhow::bail!("{} service unavailable: upstream 503", svc.name());
        }
        Ok(())
    }

    // ── 外部服务实现（假）：stub 计并发 / 记日志 / 模拟耗时，再过闸门 ──

    /// 分离服务：无入参产物，产出人声 + 伴奏两条轨（分支 A 第 1 步调用）。
    async fn separate(&self, st: &DubSfOcrState) -> anyhow::Result<Separated> {
        stub(st, "separate", 50).await;
        self.check(DubService::Separate)?;
        Ok(Separated {
            vocals: format!("{}/separate/target_3_vocals.wav", st.video_dir),
            bgm: format!("{}/separate/target_bgm.wav", st.video_dir),
        })
    }

    /// 分离服务第二轮：**消费**上一轮的分轨（分支 A 第 2 步调用）。
    async fn separate_after(
        &self,
        st: &DubSfOcrState,
        prev: &Separated,
    ) -> anyhow::Result<Separated> {
        stub(st, "separate_after", 50).await;
        self.check(DubService::Separate)?;
        Ok(Separated {
            // 人声轨是上一轮的输入，所以这里能看到数据真的流过来了。
            vocals: prev.vocals.clone(),
            bgm: format!("{}/separate_after/target_bgm.wav", st.video_dir),
        })
    }

    /// OCR 服务：**消费**关键帧目录，产出字幕（分支 B 第 2 步调用）。
    async fn sf_ocr(&self, st: &DubSfOcrState, frames: &Keyframes) -> anyhow::Result<RawSubtitle> {
        stub(st, "sf_ocr", 50).await;
        self.check(DubService::SfOcr)?;
        Ok(RawSubtitle {
            srt: format!("{}/sf_ocr/raw.srt", st.video_dir),
            segments: frames.frames,
        })
    }

    /// 校正服务（LLM）：**消费**原始字幕，产出校正后的（分支 B 第 3 步调用）。
    async fn sf_ocr_fix(&self, st: &DubSfOcrState, raw: &RawSubtitle) -> anyhow::Result<FixedSubtitle> {
        stub(st, "sf_ocr_fix", 50).await;
        self.check(DubService::SfOcrFix)?;
        Ok(FixedSubtitle {
            srt: format!("{}/sf_ocr_fix/fixed.srt", st.video_dir),
            // 修复数不可能超过原始段数——`raw` 真的参与了计算。
            repaired: 2.min(raw.segments),
        })
    }

    /// 翻译服务：**消费**校正后的字幕 + 目标语言（分支 B 第 4 步调用）。
    async fn translate(&self, st: &DubSfOcrState, fixed: &FixedSubtitle) -> anyhow::Result<Translated> {
        stub(st, "translate", 50).await;
        self.check(DubService::Translate)?;
        Ok(Translated {
            path: format!("{}/translate/{}.json", st.video_dir, st.target_lang),
            target_lang: st.target_lang.clone(),
            segments: fixed.repaired + 1,
        })
    }

    /// TTS 服务：**消费**分段表，产出逐段 wav（汇聚链第 2 步调用）。
    async fn tts(&self, st: &DubSfOcrState, timings: &Timings) -> anyhow::Result<TtsBatch> {
        stub(st, "tts", 50).await;
        self.check(DubService::Tts)?;
        Ok(TtsBatch {
            dir: format!("{}/tts/wavs", st.video_dir),
            wavs: timings.segments,
        })
    }
}

/// dub-sf-ocr 的 **typed state**（Rust 版 zod `stateSchema`）。
///
/// state **不落盘**：每次 start / resume 都由 [`DubSfOcrState::from_input`]
/// 重建（builder 的 `initialize` 与节点闭包共用），handler 从头重跑
/// （与 TanStack 一致）。
///
/// ## 与 DAG 调度的交互
///
/// 节点闭包**拿不到** `ctx.state`（[`DagRunFn`]
/// 只收 deps + input）——state 的读路径只剩 `from_input` 一条，写路径为零
/// ：这正是「step 只读 state」的最强形态。需要跨节点可见的可变状态，走
/// step 的**耐久结果**（checkpoint 的 JSON）。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DubSfOcrState {
    pub video_dir: String,
    pub target_lang: String,
    pub output_mode: String,
    pub subtitle_source: String,
}

impl DubSfOcrState {
    /// `initialize` 的实现（builder 装配时安装）。state 全部由 input 派生
    /// （不落盘）；节点闭包经 `sc.ctx.state` 读的是**引擎镜像**（≡ 本函数
    /// 的产物，见模块文档的常量结论），不再各自重建。
    pub fn from_input(input: &DubSfOcrInput) -> Self {
        Self {
            video_dir: input.video_dir.clone(),
            target_lang: input
                .target_lang
                .clone()
                .unwrap_or_else(|| "zh".to_string()),
            output_mode: "dub".to_string(),
            subtitle_source: "sf_ocr".to_string(),
        }
    }
}

/// 并发探针：记录同时在飞的 step 数峰值。
///
/// 放在模块级是因为 step 闭包要 `Clone + Send + 'static`，没法把可变引用借进去。
pub mod dub_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    /// 进入一个 step。
    pub fn enter() {
        let n = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(n, Ordering::SeqCst);
    }

    /// 离开一个 step。
    pub fn leave() {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }

    /// 峰值并发数（两分支并行时应 ≥ 2）。
    pub fn peak() -> usize {
        PEAK.load(Ordering::SeqCst)
    }

    /// 复位（测试用）。
    pub fn reset() {
        IN_FLIGHT.store(0, Ordering::SeqCst);
        PEAK.store(0, Ordering::SeqCst);
    }
}

// ─── 每个节点一个产物类型 ──────────────────────────────────────────────────
//
// 之前 10 个 step 共用一个 `DubArtifact { artifact, target_lang, output_mode }`，
// 形状对每个 step 都是假的：`target_lang` / `output_mode` 只是把 state 回显一遍，
// 真正的产物差异全压在一个字符串字面量里。现在每个节点产出**它实际产出的东西**
// 的 typed 产物，序列化发生在 step 的耐久边界（checkpoint 存 JSON）。
//
// `separate` 和 `separate_after` 共用 `Separated` 是如实反映：两轮都产出
// 同样的一对音轨，没有必要硬造第二个形状相同的类型。

/// `separate` / `separate_after`：分离出的人声 + 伴奏两条轨。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Separated {
    vocals: String,
    bgm: String,
}

/// `sf_ocr_pre`：subtitle-finder 抽出的关键帧。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Keyframes {
    dir: String,
    frames: u32,
}

/// `sf_ocr`：关键帧识别出的原始字幕（未校正）。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSubtitle {
    srt: String,
    segments: u32,
}

/// `sf_ocr_fix`：校正后的字幕 + 实际改动数。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixedSubtitle {
    srt: String,
    repaired: u32,
}

/// `translate`：译文。`target_lang` 属于**这个**节点的结果（之前它出现在
/// 每个 step 的返回里，那是把 state 回显伪装成产物）。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Translated {
    path: String,
    target_lang: String,
    segments: u32,
}

/// `split_audio`：按字幕时间轴切分出的分段表。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Timings {
    path: String,
    segments: u32,
}

/// `tts`：逐段合成出的一批 wav。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TtsBatch {
    dir: String,
    wavs: u32,
}

/// `mix_audio`：人声轨被 TTS 替换后的成品音轨。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixedAudio {
    path: String,
    ms: u64,
}

/// `mix_video`：成品视频。多带一条 `audio`——记录 mux 进去的是哪条音轨，
/// 这不是凑形状：混音出问题时这是第一个要看的字段。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixedVideo {
    path: String,
    ms: u64,
    audio: String,
}

/// 外部服务桩的公共**行为**：计并发 → 记一行 → 模拟耗时。
///
/// 服务实现（[`DubEnv`] 的六个方法）与本地处理步骤（抽帧 / 切分 / 混音 /
/// 合成视频）都经它。
///
/// 只抽行为，不抽签名。每个节点的入参（消费上游产物）和返回类型各不相同
/// ——那是这个示例要展示的东西，压成一个统一签名就把产物形状抹平了。
///
/// 所有入参都以 `&DubSfOcrState` 打头且只取共享引用，于是「step 只读 state」
/// 这条规则由类型系统保证：`&DubSfOcrState` 没有 `DerefMut` 路径，写不进去。
/// 这与 `docs/concepts/ctx-state.md` 的结论一致——节点闭包唯一耐久的输出
/// 通道是返回值（checkpoint 的 JSON），state 不是那条通道。
async fn stub(st: &DubSfOcrState, name: &str, millis: u64) {
    dub_probe::enter();
    tracing::info!(
        target: "examples",
        "dub-sf-ocr: {name} (mode={}, source={}, lang={})",
        st.output_mode,
        st.subtitle_source,
        st.target_lang
    );
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    dub_probe::leave();
}

/// 抽关键帧（本地 ffmpeg 类处理，不走服务边界）。
async fn sf_ocr_pre(st: &DubSfOcrState) -> anyhow::Result<Keyframes> {
    stub(st, "sf_ocr_pre", 50).await;
    Ok(Keyframes {
        dir: format!("{}/sf_ocr_pre/frames", st.video_dir),
        frames: 12,
    })
}

/// 汇聚第 1 步：**同时消费两支**——分支 A 的人声轨 + 分支 B 的译文。
///
/// 之前这条注释写着「split_audio 吃 A 的人声 + B 的译文」，但签名里两者都没接。
async fn split_audio(
    st: &DubSfOcrState,
    vocals: &Separated,
    translated: &Translated,
) -> anyhow::Result<Timings> {
    stub(st, "split_audio", 50).await;
    let path = format!("{}/split_audio/timings.json", st.video_dir);
    tracing::debug!(
        target: "examples",
        "按 {} 段译文切分 {}", translated.segments, vocals.vocals
    );
    Ok(Timings {
        path,
        segments: translated.segments,
    })
}

/// 汇聚第 3 步：**消费**人声轨 + TTS 批次。
async fn mix_audio(
    st: &DubSfOcrState,
    vocals: &Separated,
    tts: &TtsBatch,
) -> anyhow::Result<MixedAudio> {
    stub(st, "mix_audio", 50).await;
    let path = format!("{}/mix_audio/audio_dubbing.wav", st.video_dir);
    tracing::debug!(target: "examples", "用 {} 覆盖 {} 段 TTS", vocals.vocals, tts.wavs);
    Ok(MixedAudio {
        path,
        ms: 1_000 + tts.wavs as u64 * 120,
    })
}

/// 汇聚第 4 步：**消费**伴奏轨 + 成品音轨，产出成品视频。
///
/// 之前这里写的是字面量 `"mix_video/dub_sf_ocr/{id}.mp4"`——那既不是
/// format string，也没有 `id` 这个变量，是个从 LocalDub 抄过来忘了改的壳。
async fn mix_video(
    st: &DubSfOcrState,
    separated: &Separated,
    audio: &MixedAudio,
) -> anyhow::Result<MixedVideo> {
    stub(st, "mix_video", 50).await;
    let path = format!("{}/mix_video/dub_sf_ocr.mp4", st.video_dir);
    tracing::debug!(target: "examples", "{} 叠 {} -> {}", separated.bgm, audio.path, path);
    Ok(MixedVideo {
        path,
        ms: audio.ms,
        audio: audio.path.clone(),
    })
}

/// builder 提取：config 到 handler 之前的链——两个装配入口（[`dub_sf_ocr`] /
/// [`dub_sf_ocr_dag`]）共用。
fn dub_builder() -> WorkflowBuilder<DubSfOcrInput, (), DubSfOcrState> {
    create_workflow(
        CreateWorkflowConfig::new("dub-sf-ocr")
            .input::<DubSfOcrInput>()
            .state::<DubSfOcrState>()
            // state 不落盘：每次 start / resume 都从这里重建。
            .initialize(|input: &DubSfOcrInput| Ok(DubSfOcrState::from_input(input))),
    )
}

/// dub 流水线的声明：10 个节点 + 依赖 + 执行体。**编排的全部真相都在这里**
/// ——handler 由 [`dag_to_handler`] 从它生成，形状由 [`DagSpec::shape`]
/// 从它派生，没有任何第二份编码。
///
/// 节点闭包的形状：`(sc, deps)`——`sc.ctx.state` 是 **typed** state 快照
/// （`RunCtx<DubSfOcrInput, DubSfOcrState>`：DAG 形态下 state 无写入者，
/// 一次 run 内恒定 ≡ `initialize` 产物），`deps[节点 id]` 反序列化上游产物
/// → 调「外部服务」（模型类节点经 [`DubEnv`]）或本地处理（ffmpeg 类节点
/// 直接调自由函数）→ typed 产物序列化进 checkpoint。
///
/// env 的捕获仪式（[`DagRunFn`][aa_workflow_middlewares::DagSpec] 是
/// `Arc<dyn Fn>`，`'static` future 不许借用）：闭包外预克隆一份 handle、
/// 闭包内每次调用再克隆——`DubEnv` 是 Arc 内核，克隆只是多一个指向同一
/// 环境的 handle。
fn dub_spec(env: DubEnv) -> DagSpec<DubSfOcrInput, DubSfOcrState> {
    // env 的 per-node 预克隆（见上文捕获仪式）。
    let env_sep = env.clone();
    let env_sep2 = env.clone();
    let env_ocr = env.clone();
    let env_fix = env.clone();
    let env_tr = env.clone();
    let env_tts = env.clone();

    DagSpec::new("dub-sf-ocr")
        // 分支 A：分离人声 + 伴奏。
        .node("separate", Vec::<&str>::new(), move |sc, _deps| {
            let env = env_sep.clone();
            async move {
                let st = &sc.ctx.state;
                let out = env.separate(&st).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("separate_after", ["separate"], move |sc, deps| {
            let env = env_sep2.clone();
            async move {
                let st = &sc.ctx.state;
                let prev: Separated = serde_json::from_value(deps["separate"].clone())?;
                let out = env.separate_after(&st, &prev).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        // 分支 B：抽关键帧 → OCR → 校正 → 翻译（抽帧是本地处理）。
        .node("sf_ocr_pre", Vec::<&str>::new(), move |sc, _deps| {
            async move {
                let st = &sc.ctx.state;
                let out = sf_ocr_pre(&st).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("sf_ocr", ["sf_ocr_pre"], move |sc, deps| {
            let env = env_ocr.clone();
            async move {
                let st = &sc.ctx.state;
                let frames: Keyframes = serde_json::from_value(deps["sf_ocr_pre"].clone())?;
                let out = env.sf_ocr(&st, &frames).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("sf_ocr_fix", ["sf_ocr"], move |sc, deps| {
            let env = env_fix.clone();
            async move {
                let st = &sc.ctx.state;
                let raw: RawSubtitle = serde_json::from_value(deps["sf_ocr"].clone())?;
                let out = env.sf_ocr_fix(&st, &raw).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("translate", ["sf_ocr_fix"], move |sc, deps| {
            let env = env_tr.clone();
            async move {
                let st = &sc.ctx.state;
                let fixed: FixedSubtitle = serde_json::from_value(deps["sf_ocr_fix"].clone())?;
                let out = env.translate(&st, &fixed).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        // 汇聚链（全是本地处理，直接吃两支的产物）。
        .node(
            "split_audio",
            ["separate_after", "translate"],
            move |sc, deps| {
                async move {
                    let st = &sc.ctx.state;
                    let vocals: Separated =
                        serde_json::from_value(deps["separate_after"].clone())?;
                    let translated: Translated =
                        serde_json::from_value(deps["translate"].clone())?;
                    let out = split_audio(&st, &vocals, &translated).await?;
                    Ok(serde_json::to_value(out)?)
                }
            },
        )
        .node("tts", ["split_audio"], move |sc, deps| {
            let env = env_tts.clone();
            async move {
                let st = &sc.ctx.state;
                let timings: Timings = serde_json::from_value(deps["split_audio"].clone())?;
                let out = env.tts(&st, &timings).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node("mix_audio", ["separate_after", "tts"], move |sc, deps| {
            async move {
                let st = &sc.ctx.state;
                let vocals: Separated = serde_json::from_value(deps["separate_after"].clone())?;
                let batch: TtsBatch = serde_json::from_value(deps["tts"].clone())?;
                let out = mix_audio(&st, &vocals, &batch).await?;
                Ok(serde_json::to_value(out)?)
            }
        })
        .node(
            "mix_video",
            ["separate_after", "mix_audio"],
            move |sc, deps| {
                async move {
                    let st = &sc.ctx.state;
                    let separated: Separated =
                        serde_json::from_value(deps["separate_after"].clone())?;
                    let audio: MixedAudio = serde_json::from_value(deps["mix_audio"].clone())?;
                    let out = mix_video(&st, &separated, &audio).await?;
                    Ok(serde_json::to_value(out)?)
                }
            },
        )
}

/// dub 流水线（纯定义）：spec → **生成的** handler，健康环境、无中间件。
///
/// 环境用缺省值（全部服务健康）；要脚本化故障，用 [`dub_sf_ocr_dag`] 传
/// 自己的 [`DubEnv`] handle。
pub fn dub_sf_ocr() -> WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    let spec = define_dag(dub_spec(DubEnv::default())).expect("dub spec 合法（无重复 / 未知依赖 / 环）");
    dub_builder().handler(dag_to_handler(spec))
}

/// DAG 策略版 dub 流水线：同一份 spec → **生成的** handler + [`dag_policies`]
/// 中间件（形状从 spec 派生——执行与策略同一张图）。
///
/// 环境由调用方注入（[`DubEnv`]）：runtime 的 `load` 闭包每次装配定义时
/// 克隆同一个 handle，测试侧持同一柄——「服务故障 / 恢复」跨 run 生效。
pub fn dub_sf_ocr_dag(
    get_events: Arc<GetEvents>,
    env: DubEnv,
) -> WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    let spec = define_dag(dub_spec(env)).expect("dub spec 合法（无重复 / 未知依赖 / 环）");
    let shape = Arc::new(spec.shape().expect("dub 形状无环"));
    dub_builder()
        .middleware(dag_policies(shape, get_events))
        .handler(dag_to_handler(spec))
}
