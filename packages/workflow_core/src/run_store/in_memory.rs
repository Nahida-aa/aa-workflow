//! 内存 [`RunStore`] 实现：契约的最小完整实现，用来在接入真实持久化后端之前
//! 跑通引擎与测试。
//!
//! 并发模型：**单写者 actor 串行化所有读写**（对齐上游 JS 事件循环的语义），
//! 不用 `Mutex`。`InMemoryStore` 只持有一个命令发送端；actor 任务独占 `Inner`
//! （runs / logs / subs），逐条处理命令。`append_event` 的 CAS 索引在 actor 内
//! 校验——因为命令串行处理，check + push 天然原子。

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::error::StoreError;
use crate::event::WorkflowEvent;

use super::{DeleteReason, RunState, RunStore};

#[derive(Default)]
struct Inner {
    runs: HashMap<String, RunState>,
    logs: HashMap<String, Vec<WorkflowEvent>>,
    subs: HashMap<String, Vec<mpsc::UnboundedSender<WorkflowEvent>>>,
}

type Reply<T> = oneshot::Sender<Result<T, StoreError>>;

enum Cmd {
    GetState {
        run_id: String,
        reply: Reply<Option<RunState>>,
    },
    SetState {
        run_id: String,
        state: RunState,
        reply: Reply<()>,
    },
    DeleteRun {
        run_id: String,
        reply: Reply<()>,
    },
    AppendEvent {
        run_id: String,
        expected_next_index: usize,
        event: WorkflowEvent,
        reply: Reply<()>,
    },
    GetEvents {
        run_id: String,
        reply: Reply<Vec<WorkflowEvent>>,
    },
    ConsumeBufferedSignal {
        run_id: String,
        step_id: String,
        name: String,
        reply: Reply<Option<WorkflowEvent>>,
    },
    Subscribe {
        run_id: String,
        tx: mpsc::UnboundedSender<WorkflowEvent>,
    },
}

/// In-memory [`RunStore`] for phase 0. Provides the full contract surface
/// (CAS append + subscribe) so the engine can be exercised with property
/// tests before a real filesystem store is written.
///
/// 所有读写经单 actor 串行处理；`subscribe` 同步注册 Sender（用无界 channel
/// 向 actor 投递注册命令，不阻塞）。
pub struct InMemoryStore {
    cmd_tx: mpsc::UnboundedSender<Cmd>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Cmd>();
        tokio::spawn(async move {
            let mut inner = Inner::default();
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    Cmd::GetState { run_id, reply } => {
                        let _ = reply.send(Ok(inner.runs.get(&run_id).cloned()));
                    }
                    Cmd::SetState {
                        run_id,
                        state,
                        reply,
                    } => {
                        inner.runs.insert(run_id, state);
                        let _ = reply.send(Ok(()));
                    }
                    Cmd::DeleteRun { run_id, reply } => {
                        inner.runs.remove(&run_id);
                        inner.logs.remove(&run_id);
                        inner.subs.remove(&run_id);
                        let _ = reply.send(Ok(()));
                    }
                    Cmd::AppendEvent {
                        run_id,
                        expected_next_index,
                        event,
                        reply,
                    } => {
                        let log = inner.logs.entry(run_id.clone()).or_default();
                        let actual = log.len();
                        if actual != expected_next_index {
                            let _ = reply.send(Err(StoreError::Conflict {
                                run_id,
                                expected: expected_next_index,
                                actual,
                            }));
                            continue;
                        }
                        log.push(event.clone());
                        if let Some(subs) = inner.subs.get_mut(&run_id) {
                            subs.retain(|tx| tx.send(event.clone()).is_ok());
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Cmd::GetEvents { run_id, reply } => {
                        let events = inner.logs.get(&run_id).cloned().unwrap_or_default();
                        let _ = reply.send(Ok(events));
                    }
                    Cmd::ConsumeBufferedSignal {
                        run_id: _,
                        step_id: _,
                        name: _,
                        reply,
                    } => {
                        // 与旧实现一致：最小内存 store 不实现耐久收件箱，
                        // 调用方回落到 find_resume 全日志扫描。
                        let _ = reply.send(Ok(None));
                    }
                    Cmd::Subscribe { run_id, tx } => {
                        inner.subs.entry(run_id).or_default().push(tx);
                    }
                }
            }
        });
        Self { cmd_tx }
    }
}

#[async_trait::async_trait]
impl RunStore for InMemoryStore {
    async fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::GetState {
                run_id: run_id.to_string(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }

    async fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::SetState {
                run_id: run_id.to_string(),
                state: state.clone(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }

    async fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> Result<(), StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::DeleteRun {
                run_id: run_id.to_string(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }

    async fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &WorkflowEvent,
    ) -> Result<(), StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::AppendEvent {
                run_id: run_id.to_string(),
                expected_next_index,
                event: event.clone(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }

    async fn get_events(&self, run_id: &str) -> Result<Vec<WorkflowEvent>, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::GetEvents {
                run_id: run_id.to_string(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }

    async fn subscribe(&self, run_id: &str) -> Option<mpsc::UnboundedReceiver<WorkflowEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();
        // 同步注册：无界 channel 的 send 不阻塞，actor 随后处理。
        if self
            .cmd_tx
            .send(Cmd::Subscribe {
                run_id: run_id.to_string(),
                tx,
            })
            .is_err()
        {
            return None;
        }
        Some(rx)
    }

    async fn consume_buffered_signal(
        &self,
        run_id: &str,
        step_id: &str,
        name: &str,
    ) -> Result<Option<WorkflowEvent>, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::ConsumeBufferedSignal {
                run_id: run_id.to_string(),
                step_id: step_id.to_string(),
                name: name.to_string(),
                reply,
            })
            .map_err(|e| StoreError::Io(e.to_string()))?;
        rx.await.map_err(|e| StoreError::Io(e.to_string()))?
    }
}
