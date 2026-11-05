//! meta 声明在类型层的累积（HList）：记录一个 workflow **需要**哪些 meta 字段。
//!
//! # 为什么需要
//!
//! [`MetaBag`](super::MetaBag) 让「字段怎么读」有类型（拼错报错），但编译器不知道
//! 「这个 run 会不会提供」。要让「忘了传」也编译报错，就让中间件的**声明**
//! 进入 workflow 的类型：
//!
//! ```text
//! create_workflow(cfg)
//!     .middleware(dag(spec))      // → WorkflowDefinition<.., TMeta = Cons<DagPlanKey, Nil>>
//!     .handler(..)
//! ```
//!
//! 然后 `RunWorkflowOptions<W>` 的 `meta` 字段类型是 `<W as WorkflowLike>::Meta`，
//! **从 workflow 字段推断**——不需要另加参数，也不需要「提供 vs 需要」的比对：
//! 类型不一，构造处就报错。
//!
//! # 为什么是 HList 而不是 tuple
//!
//! tuple 要为每个长度各写一份 `impl`，而且追加发生在**末尾**，标准库没有现成
//! trait。HList 只需两个 impl，追加 O(1)。
use std::marker::PhantomData;

use super::MetaKey;

/// 空列表：这个 workflow 不需要任何 meta 字段。
#[derive(Clone, Copy, Debug, Default)]
pub struct Nil;

/// 头 + 尾。
#[derive(Clone, Copy, Debug, Default)]
pub struct Cons<H, T>(PhantomData<(H, T)>);

/// 类型层追加：`L::Out` 是加上 `K` 之后的新列表。
pub trait Push<K: MetaKey> {
    /// 追加之后的新类型。
    type Out;
}

impl<K: MetaKey> Push<K> for Nil {
    type Out = Cons<K, Nil>;
}

impl<K: MetaKey, H, T: Push<K>> Push<K> for Cons<H, T> {
    type Out = Cons<H, T::Out>;
}
