//! 示例 workflow 定义（handler 形态，代码即 DAG）。
//!
//! 输入用 `create_workflow(CreateWorkflowConfig::new(id).input::<T>())` 声明
//! （Rust 版 zod `inputSchema`：serde Deserialize 即运行时校验，缺字段/错类型
//! 在首次恢复时报 schema 错误）；handler 收到 `BaseCtx<TInput, TState>`，
//! `ctx.input` 字段直接是 `TInput`（借用即 `&TInput`）。
//! 输出类型从 handler 返回值自动推断（对齐 TanStack 的返回值推断，无 output 声明）。
//!
//! - [`fulfillment_saga`]（id `fulfillment-saga`）— 并行（`tokio::try_join!`）+ retry
//!   + 分支：演示 authoring 面；扣款等副作用**不注入**，走模块级 [`payment_gateway`]
//!     服务（对齐官方 pocs：workflow 无依赖参数，effect 写死在 handler 内）。
//!
//! - [`fulfillment`]（id `fulfillment`）— 对齐官方 `fulfillmentWorkflow` / wf-demo：
//!   `reserve-inventory` → `readyAt` 定时闸门（sleep）→ `payment-received` 事件等待 → `ship-order`。
//! - [`approval_review`] — 人工审批：`ctx.approve` 持久化一个等待点，
//!   外部用 [`RunWorkflowOptions::signal_delivery`](../engine/struct.RunWorkflowOptions.html) 交付决定后继续（signals 语义载体）。
//! - [`approval_order`]（id `approval-order`）— 对齐 wf-demo：金额阈值决定是否人工
//!   审批，拒绝分支；`email_digest` 是 resume 的行为载体。
//! - [`invoice`]（id `invoice`）— 对齐 wf-demo：连续两个 `sleep`（双定时器，引擎自动唤醒）。
//! - [`compliance`]（id `compliance`）— 对齐 wf-demo：连续两次事件等待后归档。
//! - [`refund`]（id `refund`）— 对齐 wf-demo 混合链：事件 → 定时闸门 → 审批 → 步骤。
//! - [`state_demo`]（id `state-demo`）— per-invocation state（对齐 TanStack
//!   `initialize` + `stateSchema`）：`.state::<T>()` 声明 typed state，
//!   `initialize(|input| ...)` 每次 start/resume 重建 state（**不落盘**），handler
//!   直接读写 `ctx.state` 字段（typed 时为 `T`，untyped 时为 `Value`）。step
//!   闭包**外**的 mutation 随重建重放，闭包**内**的被 replay 短路丢失
//!   （与 TanStack 0.0.4 一致）。

use std::time::Duration;
use aa_workflow_core::{
    Backoff, BaseCtx, CreateWorkflowConfig, RetryPolicy, StepOptions, WorkflowCtx,
    WorkflowDefinition, create_workflow,
};

// ========================================================================
// 输入类型 = workflow 的「schema」（Rust 版 zod `inputSchema`）：serde
// Deserialize 即运行时校验，`create_workflow(config.input::<TInput>())` 后 handler
// 拿到 `BaseCtx<TInput, ...>`，`ctx.input` 直接是 `TInput`，没有 `.get(...)` 链。
// ========================================================================

/// 履约 saga 输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FulfillmentSagaInput {
    pub order_id: String,
    #[serde(default)]
    pub expedited: bool,
}

/// 邮件 digest 输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EmailDigestInput {
    pub days: u64,
}

/// 履约流输入（对齐 wf-demo `FulfillmentInput`）。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FulfillmentInput {
    pub order_id: String,
    pub ready_at: u64,
}

/// 审批流输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalOrderInput {
    pub order_id: String,
    pub amount: u64,
    #[serde(default = "default_currency")]
    pub currency: String,
}

/// 发票输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceInput {
    pub order_id: String,
    #[serde(default = "default_t1")]
    pub t1: u64,
    #[serde(default = "default_t2")]
    pub t2: u64,
}

/// 合规归档输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceInput {
    pub subject_id: String,
}

/// 退款输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RefundInput {
    pub order_id: String,
    pub amount: u64,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub refund_at: Option<u64>,
}

fn default_currency() -> String {
    "USD".to_string()
}

fn default_t1() -> u64 {
    1_000
}

fn default_t2() -> u64 {
    3_000
}

/// 模拟"外部扣款"的模块级服务（对齐官方 pocs：workflow 不注入依赖，
/// 副作用就写成模块作用域里的服务函数）。用可配置的全局状态模拟支付网关
/// 的瞬时故障，好让 retry 测试可控。
pub mod payment_gateway {
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct State {
        attempts: usize,
        fail_first: usize,
        fail_always: bool,
    }

    static STATE: LazyLock<Mutex<State>> = LazyLock::new(Mutex::default);

    /// 扣款一次：前 `fail_first` 次（或 `fail_always` 置位时一直）返回瞬时
    /// 错误，由 workflow 步骤的 retry 策略吸收。
    pub fn charge(order_id: &str) -> anyhow::Result<()> {
        let mut st = STATE.lock().expect("payment gateway poisoned");
        st.attempts += 1;
        if st.fail_always || st.attempts <= st.fail_first {
            return Err(anyhow::anyhow!(
                "payment gateway down (attempt {})",
                st.attempts
            ));
        }
        tracing::info!(target: "examples", "charged {order_id}");
        Ok(())
    }

    /// 已尝试次数（含被 retry 吸收的失败）。
    pub fn attempts() -> usize {
        STATE.lock().expect("payment gateway poisoned").attempts
    }

    /// 让前 `n` 次扣款失败（测试用）。
    pub fn set_fail_first(n: usize) {
        STATE.lock().expect("payment gateway poisoned").fail_first = n;
    }

    /// 一直失败（测试用）。
    pub fn set_fail_always(yes: bool) {
        STATE.lock().expect("payment gateway poisoned").fail_always = yes;
    }

    /// 复位全局服务状态（测试用）。
    pub fn reset() {
        *STATE.lock().expect("payment gateway poisoned") = State::default();
    }
}

/// 履约 saga：`gen-pdf` 与 `charge` 并行（`try_join!`），扣款带重试
/// （瞬时失败经 [`payment_gateway`] 模拟）；`input.expedited` 为真时
/// 追加 `notify`（分支 = 普通 `if`）。输入 `orderId` 必填，缺失即
/// run 首次恢复时报 schema 错误（相当于 zod `.safeParse` 失败）。
pub fn fulfillment_saga() -> WorkflowDefinition<FulfillmentSagaInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment-saga").input::<FulfillmentSagaInput>())
        .handler(|ctx: BaseCtx<FulfillmentSagaInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let expedited = input.expedited;

            let (pdf, charged) = tokio::try_join!(
                {
                    let order_id = order_id.clone();
                    ctx.step("gen-pdf", move |_sc| {
                        let order_id = order_id.clone();
                        async move {
                            tracing::info!(target: "examples", "render pdf for {order_id}");
                            Ok(serde_json::json!({ "pdf": format!("{order_id}.pdf") }))
                        }
                    })
                },
                {
                    let order_id = order_id.clone();
                    ctx.step_with(
                        "charge",
                        StepOptions::new()
                            .retry(RetryPolicy::new(3, Backoff::Fixed { base_ms: 1 })),
                        move |_sc| {
                            let order_id = order_id.clone();
                            async move {
                                payment_gateway::charge(&order_id)?;
                                Ok(serde_json::json!({ "charged": true, "orderId": order_id }))
                            }
                        },
                    )
                },
            )?;

            if expedited {
                let order_id = order_id.clone();
                ctx.step("notify", move |_sc| {
                    let order_id = order_id.clone();
                    async move {
                        tracing::info!(target: "examples", "expedited notify for {order_id}");
                        Ok(serde_json::json!({ "notified": true }))
                    }
                })
                .await?;
            }

            Ok(serde_json::json!({
                "orderId": order_id,
                "pdf": pdf,
                "charge": charged,
            }))
        })
}

/// 邮件 digest：`scan-events` → `render` → `send` 三步链。
/// 本身无并行/分支，专门用来演示 resume 的检查点行为。
pub fn email_digest() -> WorkflowDefinition<EmailDigestInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("email-digest").input::<EmailDigestInput>()).handler(
        move |ctx: BaseCtx<EmailDigestInput>| async move {
            let days = ctx.input.days;

            let scanned = ctx
                .step("scan-events", move |_sc| async move {
                    tracing::info!(target: "examples", "scan events for {days} days");
                    Ok(serde_json::json!({ "count": days * 3 }))
                })
                .await?;

            let rendered = ctx
                .step("render", move |_sc| {
                    let scanned = scanned.clone();
                    async move {
                        tracing::info!(target: "examples", "render digest");
                        Ok(serde_json::json!({
                            "body": format!("{} events last {} days", scanned["count"], days),
                        }))
                    }
                })
                .await?;

            let sent = ctx
                .step("send", move |_sc| {
                    let rendered = rendered.clone();
                    async move {
                        tracing::info!(target: "examples", "send digest email");
                        Ok(serde_json::json!({ "to": "ops@example.com", "body": rendered["body"] }))
                    }
                })
                .await?;

            Ok(serde_json::json!({ "days": days, "sent": sent }))
        },
    )
}

/// 人工审批流：`draft` → `review`（`ctx.approve` 挂起，等外部决定）→ `publish`。
/// 审批决定（signal 的 payload）就是 `approve` 的返回值，最后作为 run 输出。
pub fn approval_review() -> WorkflowDefinition {
    create_workflow(CreateWorkflowConfig::new("approval-review").input::<serde_json::Value>()).handler(|ctx: WorkflowCtx| async move {
        ctx.step("draft", move |_sc| async move {
            tracing::info!(target: "examples", "draft the proposal");
            Ok(serde_json::json!({ "draft": true }))
        })
        .await?;

        let decision = ctx.approve("review", "这份提案是否放行发布？").await?;
        let decision_out = decision.clone();

        ctx.step("publish", move |_sc| {
            let decision = decision.clone();
            async move {
                tracing::info!(target: "examples", "publish with decision");
                Ok(decision.clone())
            }
        })
        .await?;

        Ok(decision_out)
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 履约流（对齐官方 `fulfillmentWorkflow` / wf-demo `fulfillment`）：
/// `reserve-inventory` → 若 `readyAt` 在未来先 `sleep` 闸门 → 等
/// `payment-received` 事件 → `ship-order`。
pub fn fulfillment() -> WorkflowDefinition<FulfillmentInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment").input::<FulfillmentInput>()).handler(
        |ctx: BaseCtx<FulfillmentInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let ready_at = input.ready_at;

            let order_id_reserve = order_id.clone();
            let order_id_ship = order_id.clone();

            let reservation = ctx
                .step("reserve-inventory", move |_sc| {
                    let order_id = order_id_reserve;
                    async move {
                        tracing::info!(target: "examples", "reserve inventory for {order_id}");
                        Ok(serde_json::json!({
                            "reservationId": format!("res_{order_id}"),
                            "reservedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            if ready_at > now_ms() {
                ctx.sleep("ready", Duration::from_millis((ready_at - now_ms()) as u64))
                    .await?;
            }

            let payment = ctx
                .approve("payment", "waiting for payment-received event")
                .await?;

            let shipment = ctx
                .step("ship-order", move |_sc| {
                    let order_id = order_id_ship;
                    async move {
                        tracing::info!(target: "examples", "ship {order_id}");
                        Ok(serde_json::json!({
                            "shipmentId": format!("ship_{order_id}"),
                            "shippedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "reservation": reservation,
                "payment": payment,
                "shipment": shipment,
            }))
        },
    )
}

const APPROVAL_THRESHOLD: u64 = 1_000;

/// 审批流（对齐 wf-demo `approval-order`）：`check-order` → 金额超过阈值才
/// `approve`（拒绝走 `notify-rejected` 分支）→ `charge-payment`。
pub fn approval_order() -> WorkflowDefinition<ApprovalOrderInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("approval-order").input::<ApprovalOrderInput>())
        .handler(|ctx: BaseCtx<ApprovalOrderInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let amount = input.amount;
            let currency = input.currency.clone();

            let order_id_check = order_id.clone();
            let currency_check = currency.clone();

            ctx.step("check-order", move |_sc| {
                let order_id = order_id_check;
                let currency = currency_check;
                async move { Ok(serde_json::json!({ "orderId": order_id, "currency": currency })) }
            })
            .await?;

            if amount > APPROVAL_THRESHOLD {
                let decision = ctx
                    .approve("review", "amount exceeds no-approval threshold")
                    .await?;
                let approved = decision
                    .get("approved")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !approved {
                    let reason = decision
                        .get("feedback")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .unwrap_or_else(|| "declined".to_string());
                    ctx.step("notify-rejected", move |_sc| async move {
                        Ok(serde_json::json!({ "notifiedAt": now_ms() }))
                    })
                    .await?;
                    return Ok(serde_json::json!({
                        "orderId": order_id,
                        "status": "rejected",
                        "amount": amount,
                        "currency": currency,
                        "reason": reason,
                    }));
                }
            }

            let order_id_charge = order_id.clone();
            ctx.step("charge-payment", move |_sc| {
                let order_id = order_id_charge;
                async move {
                    tracing::info!(target: "examples", "charge {order_id}");
                    payment_gateway::charge(&order_id)?;
                    Ok(serde_json::json!({ "chargedAt": now_ms() }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "status": "approved",
                "amount": amount,
                "currency": currency,
            }))
        })
}

/// 发票（对齐 wf-demo `invoice`）：`prepare-invoice` → 连续两个 `sleep`
/// （双定时器等待，引擎内自动唤醒）→ `settle-invoice`。
pub fn invoice() -> WorkflowDefinition<InvoiceInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("invoice").input::<InvoiceInput>()).handler(
        |ctx: BaseCtx<InvoiceInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let t1 = input.t1;
            let t2 = input.t2;

            let order_id_prepare = order_id.clone();
            let order_id_settle = order_id.clone();

            ctx.step("prepare-invoice", move |_sc| {
                let order_id = order_id_prepare;
                async move {
                    Ok(serde_json::json!({ "orderId": order_id, "preparedAt": now_ms() }))
                }
            })
            .await?;

            ctx.sleep("settle-due-1", Duration::from_millis(t1)).await?;
            ctx.sleep("settle-due-2", Duration::from_millis(t2)).await?;

            ctx.step("settle-invoice", move |_sc| {
                let order_id = order_id_settle;
                async move { Ok(serde_json::json!({ "orderId": order_id, "settledAt": now_ms() })) }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "settled": true,
                "settledAt": now_ms(),
            }))
        },
    )
}

/// 合规归档（对齐 wf-demo `compliance`）：连续两次事件等待
/// （`legal-review-done` → `compliance-signed`）→ `archive-record`。
pub fn compliance() -> WorkflowDefinition<ComplianceInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("compliance").input::<ComplianceInput>()).handler(
        |ctx: BaseCtx<ComplianceInput>| async move {
            let subject_id = ctx.input.subject_id.clone();

            ctx.approve("legal-review", "waiting for legal-review-done")
                .await?;
            ctx.approve("compliance-sign", "waiting for compliance-signed")
                .await?;

            let subject_id_archive = subject_id.clone();
            let record = ctx
                .step("archive-record", move |_sc| {
                    let subject_id = subject_id_archive;
                    async move {
                        Ok(serde_json::json!({
                            "subjectId": subject_id,
                            "archivedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            Ok(serde_json::json!({
                "subjectId": subject_id,
                "archived": true,
                "record": record,
            }))
        },
    )
}

/// 退款（对齐 wf-demo `refund`）混合链：`chargeback-filed` 事件 → 定时闸门 → 审批
/// （拒绝走 `declined`，无 disburse step）→ `disburse-refund`。
pub fn refund() -> WorkflowDefinition<RefundInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("refund").input::<RefundInput>()).handler(
        |ctx: BaseCtx<RefundInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let amount = input.amount;
            let currency = input.currency.clone();
            let refund_at = input.refund_at;

            ctx.approve("chargeback", "waiting for chargeback-filed")
                .await?;

            if let Some(ts) = refund_at
                && ts > now_ms()
            {
                ctx.sleep("refund-wait", Duration::from_millis((ts - now_ms()) as u64))
                    .await?;
            }

            let decision = ctx
                .approve("refund-policy", "refund needs manager approval")
                .await?;
            let approved = decision
                .get("approved")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !approved {
                return Ok(serde_json::json!({
                    "orderId": order_id,
                    "status": "declined",
                    "amount": amount,
                    "currency": currency,
                }));
            }

            let order_id_disburse = order_id.clone();
            ctx.step("disburse-refund", move |_sc| {
                let order_id = order_id_disburse;
                async move {
                    tracing::info!(target: "examples", "disburse refund for {order_id}");
                    payment_gateway::charge(&order_id)?;
                    Ok(serde_json::json!({ "disbursedAt": now_ms() }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "status": "refunded",
                "amount": amount,
                "currency": currency,
            }))
        },
    )
}

/// 状态机 demo 输入。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StateDemoInput {
    pub order_id: String,
    pub amount: u64,
}

/// typed state 的形状（Rust 版 zod `stateSchema`）：`.state::<T>()`
/// 声明契约，`initialize` 每次调用从输入重建，handler 的 `ctx.state` 就是
/// typed 字段 `CounterState`，直接读写。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CounterState {
    pub total: u64,
    pub settle_events: Vec<String>,
}

/// per-invocation state（对齐 TanStack `initialize`）：state **不落盘**，每次
/// start/resume 都从 `initialize(input)` 重建，然后 handler 从头重跑。因此
/// step 闭包**之外**的 mutation 天然可重放：gate 之后对 state 的修改在
/// resume 时会再次执行，最终结果与一次跑到底完全一致。
pub fn state_demo() -> WorkflowDefinition<StateDemoInput, serde_json::Value, CounterState> {
    create_workflow(
        CreateWorkflowConfig::new("state-demo")
            .input::<StateDemoInput>()
            .state::<CounterState>()
            .initialize(|input: &StateDemoInput| {
                Ok(CounterState {
                    total: input.amount,
                    settle_events: Vec::new(),
                })
            }),
    )
    .handler(
        |mut ctx: BaseCtx<StateDemoInput, CounterState>| async move {
            let order_id = ctx.input.order_id.clone();
            let amount = ctx.input.amount;

            // step 闭包之外的 mutation —— resume 时随 initialize 重建+重跑。
            ctx.state.total += 1;

            let order_id_prepare = order_id.clone();
            let prepared = ctx
                .step("prepare", move |_sc| {
                    let order_id = order_id_prepare;
                    async move {
                        Ok(serde_json::json!({ "orderId": order_id, "preparedAt": now_ms() }))
                    }
                })
                .await?;

            // 人工放行 gate：跨调用边界，验证 resume 后 state 重建+重放。
            ctx.approve("manual-release", "gate pending human release")
                .await?;

            ctx.state.settle_events.push("released".into());
            ctx.state.total += amount;

            Ok(serde_json::json!({
                "orderId": order_id,
                "total": ctx.state.total,
                "settleEvents": ctx.state.settle_events,
                "preparedAt": prepared["preparedAt"],
                "settled": true,
            }))
        },
    )
}

/// 事件闸门 demo（对齐 TanStack `waitForEvent` / `sleepUntil` / `emit`）：
/// 1. `ctx.emit` 广播可观测事件（不进日志，publisher 可见）；
/// 2. `ctx.wait_for_event` 按事件名挂起，外部 `deliver_signal` 按名投递；
/// 3. `ctx.sleep_until` 等绝对时间点（predictedAt 靠后成立，否则立即放行）。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventGateInput {
    pub market: String,
    pub predicted_at: u64,
}

pub fn event_gate() -> WorkflowDefinition<EventGateInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("event-gate").input::<EventGateInput>()).handler(
        |ctx: BaseCtx<EventGateInput>| async move {
            let market = ctx.input.market.clone();
            let predicted_at = ctx.input.predicted_at;

            ctx.emit(
                "gate-opened",
                serde_json::json!({ "market": market, "predictedAt": predicted_at }),
            );

            // 按名挂起：host 只认识 "price-settled" 事件，不需要知道 pause key。
            let settlement = ctx.wait_for_event("price-wait", "price-settled").await?;

            // 绝对时间闸门：预测时间未到则睡到那一刻。
            ctx.sleep_until("settle-wait", predicted_at).await?;

            let market_finalize = market.clone();
            ctx.step("finalize", move |_sc| {
                let market = market_finalize;
                async move { Ok(serde_json::json!({ "market": market })) }
            })
            .await?;

            Ok(serde_json::json!({
                "market": market,
                "settlement": settlement,
                "finalized": true,
            }))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;
    use aa_workflow_core::{
        AnyWorkflowDefinition, ApprovalDecision, InMemoryStore, RunStatus, RunStore,
        RunWorkflowOptions, SignalDelivery, WorkflowEvent, WorkflowDefinition, run_workflow,
    };

    /// 序列化共享全局网关状态的测试（tokio 各 test 默认并行跑）。
    static GATEWAY_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn finished_count(events: &[WorkflowEvent], step: &str) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::StepFinished { step_id, .. } if step_id == step))
            .count()
    }

    fn sf_ts(events: &[WorkflowEvent], step: &str) -> u64 {
        events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFinished { step_id, ts, .. } if step_id == step => Some(*ts),
                _ => None,
            })
            .expect("应有 StepFinished")
    }

    /// run 是否停在挂起点（paused 没有终态事件，靠挂起 checkpoint 判断）。
    fn is_paused(events: &[WorkflowEvent]) -> bool {
        events.iter().any(|e| {
            matches!(
                e,
                WorkflowEvent::SignalAwaited { .. } | WorkflowEvent::ApprovalRequested { .. }
            )
        })
    }

    /// 从事件流里取终态 output（`RunFinished.output`）。
    fn finished_output(events: &[WorkflowEvent]) -> Option<serde_json::Value> {
        events.iter().find_map(|e| match e {
            WorkflowEvent::RunFinished { output, .. } => output.clone(),
            _ => None,
        })
    }

    /// 从事件流里取 run_id（任意带 run_id 的事件）。
    fn event_run_id(events: &[WorkflowEvent]) -> Option<String> {
        events.iter().find_map(|e| match e {
            WorkflowEvent::RunStarted { run_id, .. } => Some(run_id.clone()),
            _ => None,
        })
    }

    /// 从事件流里取终态错误（`RunErrored.error`）。
    fn errored_message(events: &[WorkflowEvent]) -> Option<String> {
        events.iter().find_map(|e| match e {
            WorkflowEvent::RunErrored { error, .. } => Some(error.message.clone()),
            _ => None,
        })
    }

    /// 驱动到**挂起**并返回事件流：`run_workflow` 在挂起点立刻撒手（不等外部
    /// 投递）。这就是运行时（runtime）要替我们做的事——core 只负责「跑到挂起点
    /// 就撒手」。
    ///
    /// `opts` 是构造引擎入参的闭包——每个 drive 都要新建一份（deadline /
    /// yield_resume_at 是 per-drive 字段）。闭包收 `(workflow, store)`，因为
    /// [`RunWorkflowOptions`] 把这两项做成了**必填项**（对齐上游）。
    async fn drive_to_pause<TInput, TOutput, TState>(
        store: &Arc<dyn RunStore>,
        wf: &WorkflowDefinition<TInput, TOutput, TState>,
        opts: impl Fn(AnyWorkflowDefinition, Arc<dyn RunStore>) -> RunWorkflowOptions,
    ) -> Vec<WorkflowEvent> {
        run_workflow(opts(wf.clone().into(), store.clone()))
            .collect().await
    }

    /// 投递外部 signal 后再驱动一次，返回最终结果。
    async fn drive_after_signal<TInput, TOutput, TState>(
        store: &Arc<dyn RunStore>,
        wf: &WorkflowDefinition<TInput, TOutput, TState>,
        opts: impl Fn(AnyWorkflowDefinition, Arc<dyn RunStore>) -> RunWorkflowOptions,
        step_id: &str,
        payload: serde_json::Value,
    ) -> Vec<WorkflowEvent> {
        let run_id = opts(wf.clone().into(), store.clone())
            .run_id
            .clone()
            .expect("测试应显式指定 run_id");
        // 按挂起类型分派：审批是结构化决定（无 payload），信号有 payload。
        // 载荷**随 drive 一起给**（`approval` / `signal_delivery`），引擎在重放
        // handler 之前把它落成事件——与 runtime 的投递路径同形。
        let is_approval = store.get_events(&run_id).await.unwrap().iter().any(|e| {
            matches!(e, WorkflowEvent::ApprovalRequested { step_id: id, .. } if id == step_id)
        });
        let options = opts(wf.clone().into(), store.clone());
        let options = if is_approval {
            // 测试 payload 用 \`ok\` 表示批准与否；也认对齐后的 \`approved\`。
            let approved = payload
                .get("approved")
                .or_else(|| payload.get("ok"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let feedback = payload
                .get("feedback")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            options.approval(ApprovalDecision {
                approval_id: step_id.to_string(),
                approved,
                feedback,
            })
        } else {
            options.signal_delivery(SignalDelivery {
                signal_id: None,
                step_id: Some(step_id.to_string()),
                name: String::new(),
                payload,
                meta: None,
            })
        };
        run_workflow(options).collect().await
    }

    /// 一次投递载荷：信号或审批。`drive_through_timers` 的首次 drive 用。
    enum FirstDelivery {
        Signal(SignalDelivery),
        Approval(ApprovalDecision),
    }

    /// 反复 drive，替**计时器**投递（睡到 `deadline` 再投），直到 run 不再挂在
    /// `__timer` 上。等价于 runtime 的 `sweep` 里的 timer 认领逻辑——放在这里
    /// 是为了让示例测试不必手写「等 sleep → 投递 → 再 drive」的循环。
    ///
    /// 非 timer 的挂起（approve / wait_for_event）原样返回，由调用方投递。
    ///
    /// `first` 是**第一次 drive** 要带的载荷（调用方手里那个非 timer 的等待点）。
    /// 之后的轮次只投 `__timer`——载荷逐轮换，不能一直带同一个（run 已经在等
    /// 别的，会被判 delivery lost）。
    async fn drive_through_timers<TInput, TOutput, TState>(
        store: &Arc<dyn RunStore>,
        wf: &WorkflowDefinition<TInput, TOutput, TState>,
        opts: impl Fn(AnyWorkflowDefinition, Arc<dyn RunStore>) -> RunWorkflowOptions,
        first: Option<FirstDelivery>,
    ) -> Vec<WorkflowEvent> {
        let mut pending = first;
        loop {
            let run_id = opts(wf.clone().into(), store.clone())
                .run_id
                .clone()
                .expect("测试应显式指定 run_id");
            // 上一轮 timer 到点时攒下的载荷在这里交给 drive。
            let mut options = opts(wf.clone().into(), store.clone());
            match pending.take() {
                Some(FirstDelivery::Signal(d)) => options = options.signal_delivery(d),
                Some(FirstDelivery::Approval(a)) => options = options.approval(a),
                None => {}
            }
            let out = run_workflow(options).collect().await;
            if !is_paused(&out) {
                return out;
            }
            // 挂起投影有两种：signal（含 sleep 的 `__timer`）走 `waiting_for`，
            // 审批走 `pending_approval`。只有前者是计时器。
            let st = store.get_run_state(&run_id).await.unwrap().unwrap();
            let Some(w) = st.waiting_for else {
                return out;
            };
            if w.signal_name != "__timer" {
                return out;
            }
            // 睡到 deadline（已过期则立即）。
            if let Some(due) = w.deadline
                && due > now_ms()
            {
                tokio::time::sleep(Duration::from_millis((due - now_ms()) as u64)).await;
            }
            // timer 到点：攒进 pending，下一轮 drive 带上（引擎落成
            // SignalResolved，停在 `__timer` 上的等待点就放行）。
            pending = Some(FirstDelivery::Signal(SignalDelivery {
                signal_id: None,
                step_id: Some(
                    w.step_id
                        .as_deref()
                        .expect("timer 挂起应有 step_id")
                        .to_string(),
                ),
                name: String::new(),
                payload: serde_json::Value::Null,
                meta: None,
            }));
        }
    }

    #[tokio::test]
    async fn fulfillment_parallel_retry_then_success() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        payment_gateway::set_fail_first(2);

        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = fulfillment_saga();

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({ "orderId": "o-1", "expedited": true })),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "run 应正常结束"
        );

        // charge 重试 3 次成功（前 2 次失败）
        assert_eq!(payment_gateway::attempts(), 3);
        let run_id = event_run_id(&out).expect("应有 RunStarted/RunFinished");
        let events = store.get_events(&run_id).await.unwrap();
        let charge_fin = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFinished {
                    step_id, attempts, ..
                } if step_id == "charge" => Some(attempts),
                _ => None,
            })
            .expect("charge 应 StepFinished");
        assert_eq!(charge_fin.len(), 3, "切开看的 attempts 应为 3 次");
        // try_join! 并行：gen-pdf 与 charge 都只跑一次；分支 notify 也执行
        assert_eq!(finished_count(&events, "gen-pdf"), 1);
        assert_eq!(finished_count(&events, "charge"), 1);
        assert_eq!(finished_count(&events, "notify"), 1);
    }

    #[tokio::test]
    async fn approval_pauses_until_signal_then_publishes() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = approval_review();
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store)
                .input(serde_json::json!({}))
                .run_id("approve:r")
        };

        assert!(
            is_paused(&drive_to_pause(&store, &wf, opts).await),
            "挂起时 run 应停在 review 等待点"
        );

        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "review",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "投递后 run 应正常结束"
        );
        assert_eq!(
            finished_output(&out),
            Some(serde_json::json!({ "approved": true, "feedback": null }))
        );

        let evs = store.get_events("approve:r").await.unwrap();
        assert_eq!(finished_count(&evs, "draft"), 1);
        assert_eq!(finished_count(&evs, "publish"), 1);
        assert_eq!(
            evs.iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::SignalAwaited { step_id, .. } | WorkflowEvent::ApprovalRequested { step_id, .. } if step_id == "review")
                )
                .count(),
            1
        );
        assert_eq!(
            evs.iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::SignalResolved { step_id, .. } | WorkflowEvent::ApprovalResolved { step_id, .. } if step_id == "review")
                )
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fulfillment_sleep_gate_then_wait_event() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = fulfillment();
        // `ready` 是 sleep（由 drive_through_timers 自动投递），
        // `payment` 是 approve（人工投递）。
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "orderId": "o-1", "readyAt": now_ms() + 10 }))
                .run_id("fulfill:p")
        };

        let out = drive_through_timers(&store, &wf, opts, None).await;
        assert!(is_paused(&out), "应停在 payment 等审批");
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "payment",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "投递后 run 应正常结束"
        );

        let events = store.get_events("fulfill:p").await.unwrap();
        assert_eq!(finished_count(&events, "reserve-inventory"), 1);
        assert_eq!(finished_count(&events, "ship-order"), 1);
        let ready_sleep = events
            .iter()
            .filter(
                |e| matches!(e, WorkflowEvent::SignalAwaited { step_id, .. } | WorkflowEvent::ApprovalRequested { step_id, .. } if step_id == "ready"),
            )
            .count();
        assert_eq!(ready_sleep, 1, "readyAt 定时闸门睡过一次");
    }

    #[tokio::test]
    async fn approval_order_below_threshold_completes_immediately() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            RunWorkflowOptions::new(
                approval_order(),
                store.clone(),
            )
            .input(serde_json::json!({ "orderId": "o-1", "amount": 500 })),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "未超阈值应立即结束"
        );
        assert_eq!(
            finished_output(&out),
            Some(serde_json::json!({
                "orderId": "o-1", "status": "approved", "amount": 500, "currency": "USD"
            }))
        );
        let run_id = event_run_id(&out).expect("应有 RunStarted/RunFinished");
        assert_eq!(
            finished_count(&store.get_events(&run_id).await.unwrap(), "charge-payment"),
            1
        );
        let events = store.get_events(&run_id).await.unwrap();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::SignalAwaited { .. } | WorkflowEvent::ApprovalRequested { .. })),
            "未超阈值不应有暂停"
        );
    }

    #[tokio::test]
    async fn approval_order_above_threshold_await_decision() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = approval_order();
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "orderId": "o-2", "amount": 5000 }))
                .run_id("approve:o")
        };

        assert!(is_paused(&drive_to_pause(&store, &wf, opts).await));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "review",
            serde_json::json!({ "ok": false, "feedback": "budget overrun" }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "投递后 run 应正常结束"
        );
        let output = finished_output(&out).expect("RunFinished 应带 output");
        assert_eq!(output["status"], "rejected");
        assert_eq!(output["reason"], "budget overrun");
        let events = store.get_events("approve:o").await.unwrap();
        assert_eq!(finished_count(&events, "notify-rejected"), 1);
        assert_eq!(finished_count(&events, "charge-payment"), 0);
    }

    /// 两处 sleep：run 挂起 → 外部投递 → 挂起 → 外部投递 → Finished。
    /// 一次 `run_workflow` 不再跑完（D3），所以这里要驱动三次。
    #[tokio::test]
    async fn invoice_double_sleep_resumes_per_delivery() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = invoice();
        let out = drive_through_timers(
            &store,
            &wf,
            |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
                RunWorkflowOptions::new(wf, store)
                    .input(serde_json::json!({ "orderId": "i-1", "t1": 10, "t2": 25 }))
                    .run_id("invoice:r")
            },
            None,
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "两次 timer 投递后 run 应正常结束"
        );
        assert_eq!(finished_output(&out).unwrap()["settled"], true);

        let run_id = event_run_id(&out).expect("应有 RunStarted/RunFinished");
        let events = store.get_events(&run_id).await.unwrap();
        assert_eq!(finished_count(&events, "settle-invoice"), 1);
        for step in ["settle-due-1", "settle-due-2"] {
            let paused = events
                .iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::SignalAwaited { step_id, .. } | WorkflowEvent::ApprovalRequested { step_id, .. } if step_id == step),
                )
                .count();
            let resumed = events
                .iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::SignalResolved { step_id, .. } | WorkflowEvent::ApprovalResolved { step_id, .. } if step_id == step),
                )
                .count();
            assert_eq!(paused, 1, "{step} 暂停一次");
            assert_eq!(resumed, 1, "{step} 自动恢复一次");
        }
    }

    /// 两次顺序 wait_for_event：每次挂起都要外部投递后才继续。
    #[tokio::test]
    async fn compliance_two_sequential_events_then_archive() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = compliance();
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "subjectId": "s-1" })).run_id("comp:r")
        };

        let out = drive_to_pause(&store, &wf, opts).await;
        assert!(is_paused(&out));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "legal-review",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(is_paused(&out), "应停在第二个事件上");
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "compliance-sign",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "两次投递后 run 应正常结束"
        );
        assert_eq!(finished_output(&out).unwrap()["archived"], true);

        let events = store.get_events("comp:r").await.unwrap();
        assert_eq!(finished_count(&events, "archive-record"), 1);
    }

    #[tokio::test]
    async fn refund_declined_on_approval() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = refund();
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "orderId": "r-1", "amount": 200 }))
                .run_id("refund:d")
        };

        let out = drive_to_pause(&store, &wf, opts).await;
        assert!(is_paused(&out));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "chargeback",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(is_paused(&out));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "refund-policy",
            serde_json::json!({ "ok": false }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "拒绝后 run 应正常结束（走 declined 分支）"
        );
        assert_eq!(
            finished_output(&out).expect("RunFinished 应带 output")["status"],
            "declined"
        );
        let events = store.get_events("refund:d").await.unwrap();
        assert_eq!(finished_count(&events, "disburse-refund"), 0);
    }

    #[tokio::test]
    async fn refund_approved_disburses_after_timer() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = refund();
        let refund_at = now_ms() + 500;
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({
                "orderId": "r-2",
                "amount": 200,
                "refundAt": refund_at,
            }))
            .run_id("refund:q")
        };

        // 两次 approve → 中间的 sleep 由 `drive_through_timers` 自动投递。
        let out = drive_to_pause(&store, &wf, opts).await;
        assert!(is_paused(&out));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "chargeback",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(is_paused(&out));
        // 此刻泊在 `refund-wait`（sleep）上——审批还没轮到，**不能提前投**
        // （core 的 deliver_approval 过闸：没泊在该审批上就报错）。先把 timer
        // 驱动到泊上 `refund-policy`。
        let out = drive_through_timers(&store, &wf, opts, None).await;
        assert!(is_paused(&out), "timer 后应泊在 refund-policy");
        // 审批载荷随这一次 drive 给。
        let out = drive_through_timers(
            &store,
            &wf,
            opts,
            Some(FirstDelivery::Approval(ApprovalDecision {
                approval_id: "refund-policy".to_string(),
                approved: true,
                feedback: None,
            })),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "审批通过后 run 应正常结束"
        );
        assert_eq!(
            finished_output(&out).expect("RunFinished 应带 output")["status"],
            "refunded"
        );

        let events = store.get_events("refund:q").await.unwrap();
        assert_eq!(finished_count(&events, "disburse-refund"), 1);
        let timer_sleep = events
            .iter()
            .filter(
                |e| matches!(e, WorkflowEvent::SignalAwaited { step_id, .. } | WorkflowEvent::ApprovalRequested { step_id, .. } if step_id == "refund-wait"),
            )
            .count();
        assert_eq!(timer_sleep, 1, "定时闸门睡过一次");
    }

    /// 静态类型保留：`create_workflow(config.input::<TInput>())` 的 TInput 显式声明，
    /// TOutput 从 handler 返回类型自动推断（对齐 TanStack 的返回值推断，无需
    /// output schema 声明）。
    #[test]
    fn typed_workflow_keeps_static_types() {
        let _saga: WorkflowDefinition<FulfillmentSagaInput, serde_json::Value> = fulfillment_saga();
        let _wf: WorkflowDefinition<FulfillmentInput, serde_json::Value> = fulfillment();
    }

    /// zod 语义：缺失必填字段 → 首次恢复即 schema 错误，run 直接 Errored。
    #[tokio::test]
    async fn typed_input_rejects_missing_field() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = fulfillment();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store)
                .input(serde_json::json!({ "orderId": "o-1" }))
                .run_id("fulfill:bad"),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunErrored { .. })),
            "缺 readyAt 应 schema 报错"
        );
        let msg = errored_message(&out).expect("RunErrored 应带 error");
        assert!(
            msg.contains("ready_at") || msg.contains("missing field"),
            "错误应指向字段路径: {msg}"
        );
    }

    /// zod `stateSchema` 的 Rust 版：initialize 产物形状不符 `state_schema::<T>()`
    /// → 每次调用（start/resume）都把 run 标 Errored（与 inputSchema 语义一致）。
    #[tokio::test]
    async fn state_validation_rejects_init_shape() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("state-bad-shape")
                .input::<serde_json::Value>()
                .state_schema::<CounterState>()
                .initialize(|_input| Ok(serde_json::json!({ "bogus": 1 }))),
        )
        .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!({})) });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunErrored { .. })),
            "initialize 产物不符 state_schema 应报错"
        );
        let err = errored_message(&out).expect("RunErrored 应带 error");
        assert!(
            err.contains("missing field") || err.contains("total") || err.contains("settle_events"),
            "错误应指向 state 形状: {err}"
        );
        let run_id = event_run_id(&out).expect("应有 RunStarted/RunErrored");
        let st = store.get_run_state(&run_id).await.unwrap().unwrap();
        assert_eq!(
            st.status,
            RunStatus::Errored,
            "run state 也应持久化为 Errored"
        );
    }

    /// `state::<TState>()` 让 builder 重命名泛型：handler 拿到
    /// `BaseCtx<TInput, TState>`，`ctx.state` 是 typed 字段直接读写；durable 边界
    /// （`step`）把工作副本 flush 回引擎镜像。
    #[tokio::test]
    async fn state_typed_field_roundtrip() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("state-roundtrip")
                .input::<serde_json::Value>()
                .state::<CounterState>()
                .initialize(|_input: &serde_json::Value| {
                    Ok(CounterState {
                        total: 7,
                        settle_events: vec!["seed".to_string()],
                    })
                }),
        )
        .handler(
            |mut ctx: BaseCtx<serde_json::Value, CounterState>| async move {
                assert_eq!(
                    (ctx.state.total, ctx.state.settle_events.len()),
                    (7, 1),
                    "initialize 已注入"
                );
                ctx.state.total = 99;
                ctx.state.settle_events.push("mutated".into());
                // durable 边界：flush 执行（镜像写回，per-invocation 语义）。
                ctx.step("flush-check", |_sc| async move {
                    Ok(serde_json::json!({ "flushed": true }))
                })
                .await?;
                Ok(serde_json::json!(ctx.state.total))
            },
        );
        let out = run_workflow(
            RunWorkflowOptions::new(wf.clone(), store.clone())
                .input(serde_json::json!({})),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "run 应正常结束"
        );
        assert_eq!(finished_output(&out), Some(serde_json::json!(99)));
    }

    /// state 每次调用（start/resume）都从 initialize(input) 重建、step 闭包之外的
    /// mutation 重放——所以崩溃重启（abort → 重跑）后的输出与一次跑完完全一致。
    /// total = amount(initialize) + 1(前置) + amount(gate 后) = 2*amount + 1。
    #[tokio::test]
    async fn state_rebuilt_each_invocation() {
        let inp = || serde_json::json!({ "orderId": "s-1", "amount": 10 });

        // run A：drive 到 gate 挂起 → 投递 → 再 drive（不重建 state）。
        let store_a: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf_a = state_demo();
        let opts_a = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| RunWorkflowOptions::new(wf, store).input(inp()).run_id("state:burst");
        assert!(is_paused(&drive_to_pause(&store_a, &wf_a, opts_a).await));
        let out_burst = drive_after_signal(
            &store_a,
            &wf_a,
            opts_a,
            "manual-release",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out_burst
                .iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "run A 应正常结束"
        );

        // run B：挂起在 gate（模拟进程退出——挂起时确实没人在跑），投递后重跑
        // 同 run_id → 引擎从头重建 state + 重放 handler，最终输出应与 A 一致。
        let store_b: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = state_demo();
        let opts_b = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| RunWorkflowOptions::new(wf, store).input(inp()).run_id("state:gated");
        assert!(is_paused(&drive_to_pause(&store_b, &wf, opts_b).await));
        let paused_b = store_b.get_run_state("state:gated").await.unwrap().unwrap();
        assert_eq!(paused_b.status, RunStatus::Paused, "gate 应把 run 挂起");
        let out_b = drive_after_signal(
            &store_b,
            &wf,
            opts_b,
            "manual-release",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out_b
                .iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "run B 应正常结束"
        );

        // 跳过 preparedAt（每次取 now_ms）比较其余全部字段。
        let project = |o: &serde_json::Value| {
            serde_json::json!({
                "orderId": o["orderId"],
                "total": o["total"],
                "settleEvents": o["settleEvents"],
                "settled": o["settled"],
            })
        };
        let expected = serde_json::json!({
            "orderId": "s-1",
            "total": 21,
            "settleEvents": ["released"],
            "settled": true,
        });
        assert_eq!(
            project(&finished_output(&out_burst).expect("RunFinished 应带 output")),
            expected,
            "一次跑完的输出"
        );
        assert_eq!(
            project(&finished_output(&out_b).expect("RunFinished 应带 output")),
            expected,
            "崩溃重启后输出应一致（state 重建+重放）"
        );
    }

    /// 并行 step（`tokio::try_join!`）各自持 `ctx.clone()` 时的 state 快照：
    /// 互不共享、不持锁跨 `.await`（与 TanStack 的 in-place 共享对象不同——
    /// 克隆即分裂，见 PARITY.md）。driver 位点的 mutation 在 durable 边界
    /// `approve` 时 flush 回引擎镜像。
    #[tokio::test]
    async fn state_parallel_steps_snapshot_then_driver_flush() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("state-concurrency")
                .input::<serde_json::Value>()
                .state_schema::<CounterState>()
                .initialize(|_| Ok(serde_json::json!({ "total": 0, "settleEvents": [] }))),
        )
        .handler(|mut ctx: WorkflowCtx| async move {
                // clone 发生在 step 创建时 = state 快照；闭包内看到的都与 driver
                // 无关，3 个并发等待互不锁（state 是字段，不经过 RwLock）。
                let qa = ctx.clone();
                let fa = ctx.step("a", move |_sc| {
                    let qa = qa;
                    async move { Ok(qa.state["total"].clone()) }
                });
                let qb = ctx.clone();
                let fb = ctx.step("b", move |_sc| {
                    let qb = qb;
                    async move { Ok(qb.state["total"].clone()) }
                });
                let qc = ctx.clone();
                let fc = ctx.step("c", move |_sc| {
                    let qc = qc;
                    async move { Ok(qc.state["total"].clone()) }
                });
                let (ra, rb, rc): (serde_json::Value, serde_json::Value, serde_json::Value) =
                    tokio::try_join!(fa, fb, fc)?;
                assert_eq!(
                    (ra, rb, rc),
                    (
                        serde_json::json!(0),
                        serde_json::json!(0),
                        serde_json::json!(0)
                    ),
                    "快照分裂：各看 clone 时 total=0"
                );

                // driver 位点 mutation → durable 边界 flush 回引擎镜像。
                ctx.state["total"] = serde_json::json!(1);
                ctx.approve("flush-gate", "driver mutation flushed here")
                    .await?;
                Ok(serde_json::json!({ "total": ctx.state["total"] }))
            });
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| RunWorkflowOptions::new(wf, store).input(serde_json::json!({})).run_id("state:flush");
        assert!(is_paused(&drive_to_pause(&store, &wf, opts).await));
        let out = drive_after_signal(
            &store,
            &wf,
            opts,
            "flush-gate",
            serde_json::json!({ "ok": true }),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "投递后 run 应正常结束"
        );
        assert_eq!(finished_output(&out), Some(serde_json::json!({ "total": 1 })));
    }

    /// TanStack 0.0.4 同款 sharp edge：重启重放（crash → re-run handler）时，
    /// 已被 checkpoint 记录的 step 闭包**不会**再次调用，因此闭包内对 state 的
    /// 修改丢失——只有 step 闭包**之外**的 mutation 才是可重放的。
    #[tokio::test]
    async fn state_step_closure_mutation_lost_on_resume() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("state-sharp-edge")
                .input::<serde_json::Value>()
                .state_schema::<CounterState>()
                .initialize(|input| {
                    Ok(serde_json::json!({
                        "total": input["amount"].as_u64().unwrap_or(0),
                        "settleEvents": [],
                    }))
                }),
        )
        .handler(|mut ctx: WorkflowCtx| async move {
                // mutation 在 step 闭包内：重放时整个闭包被跳过 → 修改丢失；
                // 且闭包持 clone 快照，首个 drive 内也不写 driver 的字段。
                let inner = ctx.clone();
                ctx.step("bump-inside", move |_sc| {
                    let mut ctx = inner;
                    async move {
                        let mut st: CounterState = serde_json::from_value(ctx.state.snapshot())?;
                        st.total += 1;
                        *ctx.state = serde_json::to_value(st)?;
                        let bumped = ctx.state["total"].clone();
                        Ok(serde_json::json!({ "bumped": true, "total": bumped }))
                    }
                })
                .await?;

                ctx.approve("gate", "sharp edge gate").await?;

                // step 闭包之外的重放 mutation：重跑后仍会执行。
                let mut st: CounterState = serde_json::from_value(ctx.state.snapshot())?;
                st.settle_events.push("outside".into());
                *ctx.state = serde_json::to_value(st)?;

                let final_state: CounterState = serde_json::from_value(ctx.state.snapshot())?;
                Ok(serde_json::json!({
                    "total": final_state.total,
                    "settleEvents": final_state.settle_events,
                }))
            });

        // 进程 A：跑到 gate 挂起（挂起即没人执行——等价于进程退出，不写终态）。
        assert!(is_paused(
            &drive_to_pause(&store, &wf, |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
                RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "amount": 5 })).run_id("state:edge")
            })
            .await
        ));

        // 进程 B：同 run_id 重跑 → state 从 initialize 重建为 5，`bump-inside`
        // 已被 checkpoint 记录 → 闭包跳过（+1 丢失），只剩闭包之外的 mutation。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({ "amount": 5 }))
                .approval(ApprovalDecision {
                    approval_id: "gate".to_string(),
                    approved: true,
                    feedback: None,
                })
                .run_id("state:edge"),
        )
        .collect().await
        ;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "重跑后 run 应正常结束"
        );
        let o = finished_output(&out).expect("RunFinished 应带 output");
        assert_eq!(
            o["total"].as_u64().unwrap(),
            5,
            "bump-inside 的 +1 在重启重放时丢失（闭包被短路），只保留 initialize 的 base"
        );
        assert_eq!(
            o["settleEvents"],
            serde_json::json!(["outside"]),
            "闭包之外的 mutation 重放"
        );
    }

    /// Typed ctx 的完整基元组合：emit（publisher 可见、日志无）→ wait_for_event
    /// （按名挂起 + deliver_signal 按名投递）→ sleep_until（绝对时间点）。
    /// `emit` → `wait_for_event` 挂起 → 投递 → `sleep_until`（过去时间戳）。
    #[tokio::test]
    async fn event_gate_emit_wait_then_sleep_until() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let wf = event_gate();
        // 过去的时间戳 → `sleep_until` 挂了之后由 timer 立刻认领。
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "market": "btc", "predictedAt": 0 }))
                .run_id("event:gate")
        };

        // Drive #1：挂起在 price-wait。
        let first = run_workflow(
            opts(wf.clone().into(), store.clone()).publish(Some(Arc::new(
                move |_run_id: &aa_workflow_core::RunId, e: WorkflowEvent| sink.lock().unwrap().push(e.clone()),
            ))),
        )
        .collect().await
        ;
        assert!(is_paused(&first));

        // 挂起投影：waiting_for.signal_name = 事件通道（≠ pause key）。
        let st = store.get_run_state("event:gate").await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let w = st.waiting_for.as_ref().unwrap();
        assert_eq!(w.step_id.as_deref(), Some("price-wait"));
        assert_eq!(w.signal_name, "price-settled");
        assert!(st.pending_approval.is_none());

        // Drive #2 起：信号载荷随首次 drive 给；`sleep_until(0)` 会挂起，
        // 之后由 timer 投递推进。
        let out = drive_through_timers(
            &store,
            &wf,
            opts,
            Some(FirstDelivery::Signal(SignalDelivery {
                signal_id: None,
                step_id: None,
                name: "price-settled".to_string(),
                payload: serde_json::json!({ "px": 62000 }),
                meta: None,
            })),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "timer 投递后 run 应正常结束"
        );
        let output = finished_output(&out).expect("RunFinished 应带 output");
        assert_eq!(output["settlement"], serde_json::json!({ "px": 62000 }));
        assert_eq!(output["finalized"], serde_json::json!(true));

        // emit 进 publisher 但不进日志。
        assert!(seen.lock().unwrap().iter().any(|e| matches!(
            e,
            WorkflowEvent::Custom { name, .. } if name == "gate-opened"
        )));
        assert!(
            store
                .get_events("event:gate")
                .await.unwrap()
                .iter()
                .all(|e| !matches!(e, WorkflowEvent::Custom { .. }))
        );
    }

    /// wait_for_event 挂起（等价于崩溃——挂起时进程确实没在跑）→ 投递后同
    /// run_id 重跑：SignalResolved 已在日志，按名信号重放即可完成，不重新挂起。
    #[tokio::test]
    async fn event_gate_named_signal_replays_after_crash() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = event_gate();
        let opts = |wf: AnyWorkflowDefinition, store: Arc<dyn RunStore>| {
            RunWorkflowOptions::new(wf, store).input(serde_json::json!({ "market": "eth", "predictedAt": 0 }))
                .run_id("event:crash")
        };
        assert!(is_paused(&drive_to_pause(&store, &wf, opts).await));

        // 信号载荷随首次 drive 给；`sleep_until(0)` 也会挂起，之后由 timer 推进。
        let out = drive_through_timers(
            &store,
            &wf,
            opts,
            Some(FirstDelivery::Signal(SignalDelivery {
                signal_id: None,
                step_id: None,
                name: "price-settled".to_string(),
                payload: serde_json::json!({ "px": 3000 }),
                meta: None,
            })),
        )
        .await;
        assert!(
            out.iter()
                .any(|e| matches!(e, WorkflowEvent::RunFinished { .. })),
            "重跑后 run 应正常结束"
        );
        assert_eq!(
            finished_output(&out).expect("RunFinished 应带 output")["settlement"],
            serde_json::json!({ "px": 3000 })
        );
        assert_eq!(
            store
                .get_events("event:crash")
                .await.unwrap()
                .iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::SignalAwaited { step_id, .. } | WorkflowEvent::ApprovalRequested { step_id, .. } if step_id == "price-wait")
                )
                .count(),
            1,
            "重跑不重 append pause checkpoint"
        );
    }

}
