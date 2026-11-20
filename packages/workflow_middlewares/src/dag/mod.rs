//! `dag` 模块命名空间：核心（`core`）= 从 `aa_workflow_dag` 复制的名单计算与调度。
//!
//! 策略层 `dag_policies` 是平级文件，通过 `crate::dag::core` 使用这里的共享核心；
//! 调度入口是 [`dag_to_handler`](crate::dag_to_handler)（作为 handler 用，天然最内层）。
pub mod core;
