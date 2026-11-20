//! 内存 [`RunStore`] 实现：契约的最小完整实现，用来在接入真实持久化后端之前
//! 跑通引擎与测试。
//!
//! 三个 map 分别对应契约的两个面：`runs` 是元数据面，`logs` 是 append-only
//! 事件日志面，`subs` 只服务 `subscribe` fan-out。并发靠一把 `Mutex` 加
//! `append_event` 的 CAS 索引；不做跨进程协调。

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, Sender};

use crate::error::StoreError;
use crate::event::WorkflowEvent;

use super::{
    DeliverApprovalArgs, DeliverSignalArgs, DeliveryOutcome, DeleteReason, RunState, RunStore,
};

#[derive(Default)]
struct Inner {
    runs: HashMap<String, RunState>,
    logs: HashMap<String, Vec<WorkflowEvent>>,
    subs: HashMap<String, Vec<Sender<WorkflowEvent>>>,
}

/// In-memory [`RunStore`] for phase 0. Provides the full contract surface
/// (CAS append + subscribe) so the engine can be exercised with property
/// tests before a real filesystem store is written.
#[derive(Default)]
pub struct InMemoryStore {
    inner: Mutex<Inner>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RunStore for InMemoryStore {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .runs
            .get(run_id)
            .cloned())
    }

    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError> {
        self.inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .runs
            .insert(run_id.to_string(), state.clone());
        Ok(())
    }

    fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> Result<(), StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
        inner.runs.remove(run_id);
        inner.logs.remove(run_id);
        inner.subs.remove(run_id);
        Ok(())
    }

    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &WorkflowEvent,
    ) -> Result<(), StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
        let log = inner.logs.entry(run_id.to_string()).or_default();
        let actual = log.len();
        if actual != expected_next_index {
            return Err(StoreError::Conflict {
                run_id: run_id.to_string(),
                expected: expected_next_index,
                actual,
            });
        }
        log.push(event.clone());
        if let Some(senders) = inner.subs.get_mut(run_id) {
            senders.retain(|s| s.send(event.clone()).is_ok());
        }
        Ok(())
    }

    fn get_events(&self, run_id: &str) -> Result<Vec<WorkflowEvent>, StoreError> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .logs
            .get(run_id)
            .cloned()
            .unwrap_or_default())
    }


    fn subscribe(&self, run_id: &str) -> Option<Receiver<WorkflowEvent>> {
        let (tx, rx) = mpsc::channel();
        let mut inner = self.inner.lock().ok()?;
        inner.subs.entry(run_id.to_string()).or_default().push(tx);
        Some(rx)
    }

    /// 原子投递：一把锁里完成「查泊车 → 去重 → 追加」。
    fn deliver_signal(&self, args: DeliverSignalArgs) -> Result<DeliveryOutcome, StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
        if !inner.runs.contains_key(&args.run_id) {
            return Ok(DeliveryOutcome::NotFound);
        }
        let log = inner.logs.entry(args.run_id.clone()).or_default();

        // 幂等：同一 signal_id 已投递过就不重复写。
        if let Some(sid) = &args.delivery.signal_id
            && log.iter().any(|ev| {
                matches!(ev, WorkflowEvent::SignalResolved { signal_id: Some(x), .. } if x == sid)
            })
        {
            return Ok(DeliveryOutcome::Duplicate);
        }

        // 定位目标 step：显式给了 step_id 就按它，否则按等待的 name 找。
        let target = match &args.delivery.step_id {
            // 按 step 定位：name 从日志里取（调用方不必知道 name）。
            Some(step_id) => log.iter().rev().find_map(|ev| match ev {
                WorkflowEvent::SignalAwaited { step_id: id, name, .. } if id == step_id => {
                    Some((id.clone(), name.clone()))
                }
                _ => None,
            }),
            // 按 name 定位：找当前泊在这个名字上的 step。
            None => log.iter().rev().find_map(|ev| match ev {
                WorkflowEvent::SignalAwaited { step_id, name: n, .. } if n == &args.delivery.name => {
                    Some((step_id.clone(), n.clone()))
                }
                _ => None,
            }),
        };
        let Some((step_id, name)) = target else {
            return Ok(DeliveryOutcome::NotWaiting);
        };

        let ev = WorkflowEvent::SignalResolved {
            ts: args.now,
            run_id: args.run_id.clone(),
            step_id,
            name,
            signal_id: args.delivery.signal_id.clone(),
            payload: args.delivery.payload.clone(),
            meta: args.delivery.meta.clone(),
        };
        log.push(ev.clone());
        if let Some(senders) = inner.subs.get_mut(&args.run_id) {
            senders.retain(|s| s.send(ev.clone()).is_ok());
        }
        Ok(DeliveryOutcome::Delivered)
    }

    /// 原子投递审批结果。
    fn deliver_approval(
        &self,
        args: DeliverApprovalArgs,
    ) -> Result<DeliveryOutcome, StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
        if !inner.runs.contains_key(&args.run_id) {
            return Ok(DeliveryOutcome::NotFound);
        }
        let log = inner.logs.entry(args.run_id.clone()).or_default();

        let parked = log.iter().rev().any(|ev| matches!(
            ev,
            WorkflowEvent::ApprovalRequested { approval_id: id, .. } if id == &args.approval_id
        ));
        if !parked {
            return Ok(DeliveryOutcome::NotWaiting);
        }

        let ev = WorkflowEvent::ApprovalResolved {
            ts: args.now,
            run_id: args.run_id.clone(),
            step_id: args.approval_id.clone(),
            approval_id: args.approval_id.clone(),
            approved: args.approved,
            feedback: args.feedback.clone(),
            meta: None,
        };
        log.push(ev.clone());
        if let Some(senders) = inner.subs.get_mut(&args.run_id) {
            senders.retain(|s| s.send(ev.clone()).is_ok());
        }
        Ok(DeliveryOutcome::Delivered)
    }
}
