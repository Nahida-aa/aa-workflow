//! `dag` 模块命名空间：核心（`core`）= 从 `aa_workflow_dag` 复制的名单计算与调度。
//!
//! 策略层 `dag_policies` 与调度中间件 `dag_scheduler` 目前是平级文件，
//! 它们通过 `crate::dag::core` 使用这里的共享核心。
pub mod core;
