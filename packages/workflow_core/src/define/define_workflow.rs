//! WorkflowDefinition 的 authoring 层：怎么**声明**一个 workflow。
//!
//! 对齐 TanStack `define/define-workflow.ts`：`CreateWorkflowConfig`
//! （`createWorkflow(options)`）、`WorkflowBuilder`、`createWorkflow`。
//!
//! 与 [`define`](super) 的分界：`super` 装的是 handler **运行时**能拿到的东西
//! （`BaseCtx` / `StepCtx` / `StepOptions` / `RetryPolicy`）与 workflow 本体
//! [`WorkflowDefinition`](super::WorkflowDefinition)；这里装的是把它们拼起来的
//! 声明式入口。`createWorkflow(...).handler(...)` 产出的就是
//! `WorkflowDefinition<TInput, TOutput, TState>`。

use std::marker::PhantomData;
use std::sync::Arc;

use super::meta_spec::{Nil, Push};
use super::MetaKey;
use super::{AnyWorkflowDefinition, BaseCtx, InitializeFn, RetryPolicy, StateValidatorFn, WorkflowCtx, WorkflowDefinition, WorkflowHandler};
use crate::middleware::{AnyMiddleware, Append, Middleware, MiddlewareServer, MiddlewareStack};

/// The declaration config consumed by [`create_workflow`]. Mirrors TanStack's
/// `createWorkflow(options)`; only `id` is required (`initialize` defaults to
/// `{}`).
///
/// `TState` 缺省 `serde_json::Value`——对齐上游无 schema 时的
/// `Record<string, unknown>`：未声明 state 的工作流仍有**动态** state 可用
/// （handler 可把 `ctx.state` 当 JSON bag 读写）。要表达「无 state」就显式
/// `.state::<()>()` + `.initialize(|_| Ok(()))`（unit 序列化为 null，形状
/// 校验通过）。缺省不能改成 `()`：默认 `initialize` 产 `{}`，而 unit 只吃
/// null——未声明工作流会在 seed 时反序列化失败。 `input`/`state`/`output` re-type the config and install the matching
/// serde shape-checks (zod `.safeParse` counterparts); `version` is the string
/// this workflow's runs persist for version routing; `default_step_retry` is
/// the step retry fallback; `description` is metadata.
pub struct CreateWorkflowConfig<TInput = (), TOutput = (), TState = serde_json::Value> {
    pub id: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub initialize: InitializeFn,
    pub input_validator: Option<StateValidatorFn>,
    pub state_validator: Option<StateValidatorFn>,
    pub output_validator: Option<StateValidatorFn>,
    pub default_step_retry: Option<RetryPolicy>,
    pub handler: WorkflowHandler,
    pub _input: PhantomData<TInput>,
    pub _output: PhantomData<TOutput>,
    pub _state: PhantomData<TState>,
}

impl CreateWorkflowConfig<(), (), serde_json::Value> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            description: None,
            version: None,
            initialize: Arc::new(|_| Ok(serde_json::Value::Object(Default::default()))),
            input_validator: None,
            state_validator: None,
            output_validator: None,
            default_step_retry: None,
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }
}

impl<TInput, TOutput, TState> CreateWorkflowConfig<TInput, TOutput, TState> {
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// `initialize({ input })` — rebuild the per-invocation state on every
    /// start and resume (state is never persisted, see
    /// [`WorkflowDefinition::initialize`](WorkflowDefinition::initialize)).
    ///
    /// typed：闭包吃 `&TInput`、产 `TState`（对齐上游——`TState` 由
    /// `.state::<T>()` 声明、编译期钉死）。内部 serde 桥接到运行时的擦除
    /// 表示（`WorkflowDefinition` 持 `Fn(&Value) -> Result<Value>`）；input 反序列化
    /// 失败 / state 序列化失败都算 run 错误。
    pub fn initialize(
        mut self,
        f: impl Fn(&TInput) -> anyhow::Result<TState> + Send + Sync + 'static,
    ) -> Self
    where
        TInput: serde::de::DeserializeOwned,
        TState: serde::Serialize,
    {
        self.initialize = Arc::new(move |input: &serde_json::Value| {
            let typed_input: TInput = serde_json::from_value(input.clone())?;
            let typed_state = f(&typed_input)?;
            Ok(serde_json::to_value(typed_state)?)
        });
        self
    }

    /// Declare the state's *shape* for validation **without** re-typing the
    /// handler's view of `ctx.state`（保持 `Value`）——`.state::<T>()` 会把
    /// `TState` 重键为 `T`（handler 拿到强类型 state），而这里只安装同一份
    /// serde 校验（initialize 产物 / 重放重建的 state 必须能反序列化回
    /// `T`，否则 run 在声明点就报 validation）。handler 想以强类型读写
    /// state 时用 [`Self::state`]，想保持擦除态只做形状把关时用本方法。
    pub fn state_schema<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
        mut self,
    ) -> Self {
        self.state_validator = Some(Arc::new(|v| {
            serde_json::from_value::<T>(v.clone())?;
            Ok(())
        }));
        self
    }

    /// Fallback retry for steps that declare no retry of their own
    /// (see [`StepOptions`](super::StepOptions)).
    pub fn default_step_retry(mut self, retry: RetryPolicy) -> Self {
        self.default_step_retry = Some(retry);
        self
    }

    /// Declare the input schema as a serde type: re-types the config to
    /// `CreateWorkflowConfig<NewIn, TOutput, TState>` so `create_workflow` builds a
    /// [`WorkflowBuilder<NewIn, TOutput, TState>`](WorkflowBuilder). The handler's
    /// `ctx.input` is `NewIn`. Missing/mistyped input fields error the run
    /// (zod `inputSchema` `.safeParse` counterpart).
    pub fn input<NewIn>(self) -> CreateWorkflowConfig<NewIn, TOutput, TState>
    where
        NewIn: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            input_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewIn>(v.clone())?;
                Ok(())
            })),
            state_validator: self.state_validator,
            output_validator: self.output_validator,
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: PhantomData,
            _output: self._output,
            _state: self._state,
        }
    }

    /// Declare the state schema as a serde type: the state built by
    /// [`initialize`](Self::initialize) is shape-checked against `NewSt` on
    /// every invocation, and the handler's `ctx.state` is `NewSt`.
    pub fn state<NewSt>(self) -> CreateWorkflowConfig<TInput, TOutput, NewSt>
    where
        NewSt: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            input_validator: self.input_validator,
            state_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewSt>(v.clone())?;
                Ok(())
            })),
            output_validator: self.output_validator,
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: self._input,
            _output: self._output,
            _state: PhantomData,
        }
    }

    /// Declare the output schema as a serde type: re-types the config to
    /// `CreateWorkflowConfig<_, NewOut, _>`; the handler's `TOutput` is validated
    /// against `NewOut` (zod `.safeParse`) before the run finishes. Inferred
    /// from the handler closure otherwise, exactly like TanStack — the schema
    /// constrains but never declares.
    pub fn output<NewOut>(self) -> CreateWorkflowConfig<TInput, NewOut, TState>
    where
        NewOut: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            input_validator: self.input_validator,
            state_validator: self.state_validator,
            output_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewOut>(v.clone())?;
                Ok(())
            })),
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: self._input,
            _output: PhantomData,
            _state: self._state,
        }
    }
}

/// Define a workflow. Returns a builder chain:
///
/// ```rust,ignore
/// let onboard = create_workflow(
///     CreateWorkflowConfig::new("onboard").input::<OnboardInput>(),
/// )
/// .middleware(traced)
/// .handler(|ctx| async move {
///     let profile = ctx.step("load", |_| async { load_profile(&ctx.input.user_id) }).await?;
///     ctx.sleep("cooloff", std::time::Duration::from_secs(60)).await?;
///     let decision = ctx.approve("continue", "Continue?").await?;
///     Ok(serde_json::json!({ "ok": decision["approved"] }))
/// });
/// ```
///
/// The handler's `ctx` argument (a [`BaseCtx`]) carries everything: the
/// typed input, state, durable primitives ([`step`](BaseCtx::step),
/// [`sleep`](BaseCtx::sleep), [`approve`](BaseCtx::approve), ...), and any
/// fields added by registered middleware (`ctx.ext`). Helpers should accept
/// a typed `BaseCtx<...>` argument to compose cleanly.
///
/// `TInput`/`TOutput`/`TState` are pinned by the config's
/// [`input`](CreateWorkflowConfig::input) / [`output`](CreateWorkflowConfig::output)
/// / [`state`](CreateWorkflowConfig::state) methods; the handler's output type
/// is inferred from the closure return value.
pub fn create_workflow<TInput, TOutput, TState>(
    config: CreateWorkflowConfig<TInput, TOutput, TState>,
) -> WorkflowBuilder<TInput, TOutput, TState> {
    WorkflowBuilder {
        _meta: PhantomData,
        config,
        stack: (),
        previous: Vec::new(),
    }
}

/// The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
/// at build time.
///
/// 没有 `TCtxExt`：扩展走 [`ExtBag`](crate::define::ExtBag) 的命名空间获取，
/// 不参与类型层（见 `docs/decisions/0015`）。
/// 一个**声明了 meta 需求**的中间件。
///
/// 与普通 [`Middleware`] 的区别只有一个：类型参数 `K` 让 workflow 知道
/// 「我需要哪些 meta 字段」，于是 `RunWorkflowOptions.meta` 缺字段时
/// **编译报错**。运行期零开销（`K` 只在类型层）。
///
/// 一个中间件消费多个字段时用元组：`MetaMiddleware<(DagPlanKey, ReseedKey)>`。
pub struct MetaMiddleware<K> {
    /// 实际的中间件（与普通中间件完全一样）。
    pub middleware: Middleware,
    /// 类型标记：声明消费哪些 meta 字段。
    pub declares: PhantomData<K>,
}

impl<K> MetaMiddleware<K> {
    /// 用普通中间件 + 声明构造。
    pub fn new(middleware: Middleware) -> Self {
        Self {
            middleware,
            declares: PhantomData,
        }
    }
}

pub struct WorkflowBuilder<TInput, TOutput, TState, TMeta = Nil, TMw = ()> {
    config: CreateWorkflowConfig<TInput, TOutput, TState>,
    /// 中间件栈（类型层组合，[`MiddlewareStack`]）——常态逐个注册，
    /// 每加一个，`TMw` 长一格；洋葱在 [`handler`](Self::handler) 时折叠。
    stack: TMw,
    previous: Vec<AnyWorkflowDefinition>,
    _meta: PhantomData<TMeta>,
}

impl<TInput, TOutput, TState, TMeta, TMw: MiddlewareStack>
    WorkflowBuilder<TInput, TOutput, TState, TMeta, TMw>
{
    /// 注册一个中间件 —— **typestate**：`TMw` 长一格（tower `ServiceBuilder`
    /// 同款），洋葱在 [`handler`](Self::handler) 时以具体类型折叠——
    /// 逐层零箱，定义期只在终点擦一次。
    ///
    /// ```ignore
    /// let wf = create_workflow(cfg)
    ///     .middleware(agent_middleware())   // 逐个注册（常态）
    ///     .middleware(auth())
    ///     .handler(|ctx| async move {
    ///         let auth = ctx.ext.auth();    // 按命名空间取
    ///         ...
    ///     });
    /// ```
    ///
    /// 与上游一致：可多次调用叠加、顺序即注册顺序（先注册 = 外层）。
    /// **不需要写扩展类型**——扩展在运行期按命名空间存取
    /// （[`ExtBag`](crate::define::ExtBag)，见 `docs/decisions/0015`）。
    pub fn middleware<M>(self, m: M) -> WorkflowBuilder<TInput, TOutput, TState, TMeta, TMw::Out>
    where
        M: MiddlewareServer + 'static,
        TMw: Append<M>,
    {
        WorkflowBuilder {
            config: self.config,
            stack: self.stack.append(m),
            previous: self.previous,
            _meta: PhantomData,
        }
    }

    /// 注册一个**声明了 meta 需求**的中间件。
    ///
    /// 与 [`middleware`](Self::middleware) 的区别：它的 `K` 会累积进 workflow
    /// 的类型参数，于是 `RunWorkflowOptions.meta` 必须提供对应字段，
    /// **否则在构造 options 处编译报错**（见 `docs/decisions/0017`）。
    ///
    /// ```ignore
    /// create_workflow(cfg)
    ///     .middleware(auth())          // 不消费 meta
    ///     .meta_middleware(dag(spec))  // → 累积 DagPlanKey
    ///     .handler(|ctx| async move {
    ///         let plan = ctx.meta.dag_plan();     // 强类型
    ///     })
    /// ```
    ///
    /// 为什么单独一个方法而不是让 `middleware` 收 `MetaMiddleware`：数组要求
    /// 元素**完全同型**，`MetaMiddleware<A>` 与 `MetaMiddleware<B>` 装不进同一个
    /// `[T; N]`，而它们混装恰恰是最常见的用法。
    pub fn meta_middleware<K>(
        self,
        mw: MetaMiddleware<K>,
    ) -> WorkflowBuilder<TInput, TOutput, TState, TMeta::Out, TMw::Out>
    where
        K: MetaKey,
        TMeta: Push<K>,
        TMw: Append<Middleware>,
    {
        WorkflowBuilder {
            config: self.config,
            stack: self.stack.append(mw.middleware),
            previous: self.previous,
            _meta: PhantomData,
        }
    }

    /// Older versions of the same workflow to route resumed runs to (see
    /// [`select_workflow_version`](crate::engine::select_workflow_version)).
    pub fn previous_versions(
        mut self,
        v: impl IntoIterator<Item = impl Into<AnyWorkflowDefinition>>,
    ) -> Self {
        self.previous = v.into_iter().map(Into::into).collect();
        self
    }

    /// Finalize with the orchestrating closure. `AOut` is inferred from the
    /// handler's return value; the engine stores it serialized as JSON, so on
    /// resume the handler re-runs from scratch with re-parsed input/state and a
    /// freshly `produce`d extension (same as TanStack).
    pub fn handler<F, Fut, AOut>(
        self,
        handler: F,
    ) -> WorkflowDefinition<TInput, AOut, TState, TMeta>
    where
        TInput: serde::de::DeserializeOwned + Send + Sync + 'static,
        TState: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
        AOut: serde::Serialize + Send + Sync + 'static,
        F: Fn(BaseCtx<TInput, TState>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<AOut>> + Send + 'static,
    {
        let CreateWorkflowConfig {
            id,
            description,
            version,
            initialize,
            input_validator,
            state_validator,
            output_validator,
            default_step_retry,
            handler: _,
            _input: _,
            _output: _,
            _state: _,
        } = self.config;
        let handler = Arc::new(handler);
        let validator = output_validator.clone();
        // 中间件折叠（对齐 TS `composeMiddlewares` 的洋葱模型）：
        // 最内层 = typed ctx + handler；每个 middleware 的 `server` 包住内层。
        // server 经 `MiddlewareServerFnArgs` 拿到完整 ctx，可 `next(ext)` /
        // `wrap_step` / `take_over`。
        let innermost: WorkflowHandler = Arc::new(move |ctx: WorkflowCtx| {
            let handler = Arc::clone(&handler);
            let output_validator = output_validator.clone();
            Box::pin(async move {
                let input = serde_json::from_value(ctx.input.clone())?;
                let state = serde_json::from_value(ctx.state.snapshot())?;
                // 各中间件 server 里经 `next(ext)` 合并进 `engine.ext_extra`
                // （擦除层 Value；Object.assign 同义）。handler 侧按命名空间取。
                let ext = crate::define::ExtBag::from_value(
                    ctx.engine.ext_extra.lock().expect("ext lock").clone(),
                );
                let typed = BaseCtx {
                    run_id: ctx.run_id,
                    input,
                    meta: ctx.meta.clone(),
                    state: crate::define::StateHandle::new(
                        state,
                        ctx.state.mirror(),
                        Arc::new(|st: &TState| {
                            serde_json::to_value(st).unwrap_or(serde_json::Value::Null)
                        }),
                    ),
                    ext,
                    engine: ctx.engine.clone(),
                };
                let out = handler(typed).await?;
                let value = serde_json::to_value(out)?;
                if let Some(validate) = &output_validator {
                    validate(&value)?;
                }
                Ok(value)
            })
        });
        // 洋葱折叠（typestate 栈）：中间件栈在类型层组合，定义期一次擦除
        // （`MiddlewareStack::serve` 把栈折叠成一个 WorkflowHandler）。
        let engine_handler: WorkflowHandler = self.stack.compose(innermost);
        WorkflowDefinition {
            id,
            version,
            description,
            default_step_retry,
            previous_versions: self.previous,
            handler: engine_handler,
            initialize,
            input_validator,
            state_validator,
            output_validator: validator,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
            _meta: PhantomData,
        }
    }
}
