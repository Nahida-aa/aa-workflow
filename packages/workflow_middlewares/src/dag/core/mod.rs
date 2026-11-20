//! DAG 核心：名单计算（`shape`）与声明式调度（`declare`）。
//!
//! 从 `aa_workflow_dag` 复制而来，使 `workflow_middlewares` 自包含、不再依赖
//! 那个包。原包暂留（供其自身测试），删除是后续事项。分两块：
//!
//! | 块 | 角色 |
//! | --- | --- |
//! | [`DagShape`] / [`RedoPlan`] | 名单计算：`reseed` 的失效闭包，不发起 step |
//! | [`DagSpec`] / [`run_dag`] | 声明式调度：按依赖偏序发起 `ctx.step` |
pub mod declare;
pub mod shape;

pub use declare::{DagNode, DagRunFn, DagSpec, dag_to_handler, define_dag, run_dag};
pub use shape::{DagShape, RedoPlan, ShapeError};
