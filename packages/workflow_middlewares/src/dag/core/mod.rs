//! DAG 核心：名单计算（`shape`）、声明与校验（`declare`）、调度（`scheduler`）。
//!
//! 从 `aa_workflow_dag` 复制而来，使 `workflow_middlewares` 自包含、不再依赖
//! 那个包。原包暂留（供其自身测试），删除是后续事项。分三块：
//!
//! | 块 | 角色 |
//! | --- | --- |
//! | [`DagShape`] / [`RedoPlan`] | 名单计算：`reseed` 的失效闭包，不发起 step |
//! | [`DagSpec`] / [`define_dag`] | 声明与校验：图长什么样、声明是否合法 |
//! | [`run_dag`] | 调度：按依赖关系自动并发地发起 `ctx.step` |
pub mod declare;
pub mod scheduler;
pub mod shape;

pub use declare::{DagNode, DagRunFn, DagSpec, define_dag};
pub use scheduler::{dag_to_handler, run_dag};
pub use shape::{DagShape, RedoPlan, ShapeError};
