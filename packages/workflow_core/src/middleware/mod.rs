//! Middleware 层：对齐 TanStack `src/middleware/`。

mod create_middleware;
pub(crate) use create_middleware::NextStep;
pub use create_middleware::{
    AnyMiddleware, Middleware, MiddlewareServerFn, MiddlewareServerFnArgs, NextFn, StepCall,
    StepWrapper,
};
