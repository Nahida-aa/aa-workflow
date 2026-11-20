//! `dag` 模块命名空间：DAG 相关代码集中在此。
//!
//! - [`core`]：从 `aa_workflow_dag` 复制的名单计算与调度（shape / declare / scheduler）
//! - [`policies`]：策略层中间件 `dag_policies`（redo 复用 + 目标泊车）
//!
//! 调度入口是 [`dag_to_handler`](crate::dag_to_handler)（作为 handler 用，天然最内层）。
pub mod core;
pub mod policies;
