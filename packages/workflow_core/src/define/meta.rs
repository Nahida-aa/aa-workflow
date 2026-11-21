//! `ctx.meta` —— run 级参数的类型化读取（对齐 `ctx.ext` 的命名空间模式）。
//!
//! # 与 `ctx.ext` 的关系
//!
//! 两者形状相同：键与类型由**中间件包**定义（`MetaKey` ↔ `ExtNamespace`），
//! 值存在擦除的 JSON 里，读取时按 key 反序列化（`MetaBag` ↔ `ExtBag`）。
//!
//! | | `ctx.ext` | `ctx.meta` |
//! | --- | --- | --- |
//! | 谁写 | 中间件（`next(Some(..))`），**运行期** | 调用方（`RunWorkflowOptions.meta`），**每次 run** |
//! | 谁读 | handler / 下游中间件 | 中间件（策略参数）/ handler |
//! | 缺失时 | panic（`ExtBag::get`） | 中间件用 `try_*`（可能没传）；handler 用 `*`（编译期保证有） |
//!
//! # 两条保证的区别（重要）
//!
//! **类型**是编译期保证的：`ctx.meta.dag_plan()` 能否写出来，只取决于
//! `DagPlanAccess` trait 在不在作用域里。
//!
//! **「这个值真的会被提供吗」** 则需要另一层机制——中间件把声明的 key 累积进
//! `WorkflowDefinition` 的类型参数，`run_workflow` 处校验 `opts.meta` 覆盖它。
//! 见 `docs/decisions/0017`。
//!
//! 所以：**handler 里可以不带 `Option`**（编译期已保证），而**中间件里必须用
//! `try_*`**——中间件在 run 开始时就执行，那时还没有「handler 已被保证」这回事。
use serde_json::Value;

use super::meta_spec::Nil;

impl MetaBag<crate::define::meta_spec::Nil> {
    /// 空袋（无任何 meta）。**构造 `RunWorkflowOptions.meta` 的起点**。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 从原始 JSON 构造（丢掉了「提供了哪些」的类型信息）。
    ///
    /// 只用于不需要编译期检查的场合（例如把已有 JSON 原样透传）。
    /// 新代码优先 [`set`](MetaBag::set)——它让键名与类型来自同一个 `MetaKey`。
    #[must_use]
    pub fn from_json(raw: Value) -> Self {
        Self::from_value(raw)
    }
}

/// 一个 meta 字段：`KEY` 是 JSON 里的键，`Value` 是读取时反序列化成的类型。
///
/// 中间件作者为每个消费的字段定义**一个零大小类型**实现它，于是声明与读取
/// 共享同一份键名与类型：
///
/// ```ignore
/// pub struct DagPlanKey;
/// impl MetaKey for DagPlanKey {
///     const KEY: &'static str = "__dagPlan";
///     type Value = DagPlan;
/// }
/// impl_meta_accessors!(DagPlanKey => DagPlanAccess, dag_plan, try_dag_plan);
/// ```
pub trait MetaKey {
    /// `ctx.meta` 里这个字段的键名。
    const KEY: &'static str;
    /// 字段的值类型。既要能读（反序列化）也要能写（构造 options 时序列化）。
    type Value: serde::de::DeserializeOwned + serde::Serialize;
}

/// 读 `ctx.meta` 的入口 —— 与 [`ExtBag`](crate::define::ExtBag) 同构。
///
/// 每个 key **独立**反序列化：一个解析失败只影响它自己。
///
/// # 类型参数 `P`：已经提供的 key 列表
///
/// 只在**构造**（`RunWorkflowOptions.meta`）时有用：`set::<K>()` 一次推进一位，
/// 于是「提供了哪些」是类型的一部分，`run_workflow` 才能校验「够不够」。
///
/// ```ignore
/// MetaBag::new()                          // MetaBag<Nil>
///     .set::<DagPlanKey>(plan)            // MetaBag<Cons<DagPlanKey, Nil>>
///     .set::<ReseedKey>(rp)               // MetaBag<Cons<DagPlanKey, Cons<ReseedKey, Nil>>>
/// ```
///
/// **读取侧不关心 `P`**：`ctx.meta` 是 `MetaBag`（默认 `P = Nil`），按 key 取值即可。
/// 之所以能这样分：写侧要证明「齐了」，读侧只需要「有什么读什么」。
#[derive(Debug)]
pub struct MetaBag<P = Nil> {
    raw: Value,
    _provided: std::marker::PhantomData<P>,
}

impl<P: Clone> Clone for MetaBag<P> {
    fn clone(&self) -> Self {
        MetaBag {
            raw: self.raw.clone(),
            _provided: std::marker::PhantomData,
        }
    }
}

impl Default for MetaBag<Nil> {
    fn default() -> Self {
        Self {
            raw: Value::Object(Default::default()),
            _provided: std::marker::PhantomData,
        }
    }
}

impl<P> MetaBag<P> {

    /// 从引擎的擦除表示构造。非对象（`null` / 缺失）视为空袋。
    pub fn from_value(raw: Value) -> MetaBag<Nil> {
        MetaBag {
            raw: if raw.is_object() {
                raw
            } else {
                Value::Object(Default::default())
            },
            _provided: std::marker::PhantomData,
        }
    }

    /// 擦除形态（调试 / 透传用）。
    pub fn as_value(&self) -> &Value {
        &self.raw
    }

    /// 没有任何 meta 字段。
    pub fn is_empty(&self) -> bool {
        self.raw.as_object().is_none_or(|m| m.is_empty())
    }

    /// 取某个字段。
    ///
    /// # Panics
    ///
    /// 调用方没传该字段时 panic。**handler 用它**（那里编译期已保证有值）；
    /// 中间件用 [`try_get`](Self::try_get)——中间件执行时值可能还没到。
    pub fn get<K: MetaKey>(&self) -> K::Value {
        self.try_get::<K>().unwrap_or_else(|| {
            panic!(
                "ctx.meta.{}: 缺少这个字段 —— RunWorkflowOptions.meta 里没传 `{}`",
                K::KEY,
                K::KEY
            )
        })
    }

    /// 取某个字段；缺失或反序列化失败返回 `None`。
    pub fn try_get<K: MetaKey>(&self) -> Option<K::Value> {
        let v = self.raw.get(K::KEY)?.clone();
        serde_json::from_value(v).ok()
    }

    /// 写入一个字段（构造 `RunWorkflowOptions.meta` 时用）。
    ///
    /// 链式调用可以叠加多个字段：
    ///
    /// ```ignore
    /// meta: MetaBag::new()
    ///     .set::<DagPlanKey>(plan)
    ///     .set::<ReseedKey>(reseed_plan),
    /// ```
    ///
    /// # Panics
    ///
    /// 值无法序列化时 panic——调用点在构造 options 处，属于编程错误。
    #[must_use]
    pub fn set<K: MetaKey>(mut self, value: K::Value) -> MetaBag<P::Out>
    where
        P: super::meta_spec::Push<K>,
    {
        let serialized = serde_json::to_value(&value).unwrap_or_else(|e| {
            panic!("ctx.meta.{}: 值无法序列化为 JSON（{}）", K::KEY, e)
        });
        if !self.raw.is_object() {
            self.raw = Value::Object(Default::default());
        }
        self.raw
            .as_object_mut()
            .expect("just ensured object")
            .insert(K::KEY.to_string(), serialized);
        MetaBag {
            raw: self.raw,
            _provided: std::marker::PhantomData,
        }
    }

    /// Write a field **without** advancing the `provided` type parameter
    /// (returns the same `MetaBag<P>`).
    ///
    /// 与 [`set`](Self::set) 的分工：
    ///
    /// - `set` 用于 workflow **声明了** meta 需求的场景——插入后类型参数前进一位，
    ///   `run_workflow` 才能在编译期校验「够不够」。
    /// - `with` 用于调用方**自己知道**该给什么、但 workflow 没声明（因此
    ///   `RunWorkflowOptions.meta` 的类型是 `MetaBag<Nil>`）的场景：中间件在
    ///   运行期用 `try_get` 读，缺了也不会编译报错。
    ///
    /// 典型用法是策略中间件的 per-drive 参数，例如 `interim_goals` 的目标：
    ///
    /// ```ignore
    /// MetaBag::new().with::<InterimGoalsKey>(vec!["b".into()])
    /// ```
    ///
    /// # Panics
    ///
    /// 值无法序列化时 panic——调用点在构造 options 处，属于编程错误。
    #[must_use]
    pub fn with<K: MetaKey>(mut self, value: K::Value) -> MetaBag<P> {
        let serialized = serde_json::to_value(&value).unwrap_or_else(|e| {
            panic!("ctx.meta.{}: 值无法序列化为 JSON（{}）", K::KEY, e)
        });
        if !self.raw.is_object() {
            self.raw = Value::Object(Default::default());
        }
        self.raw
            .as_object_mut()
            .expect("just ensured object")
            .insert(K::KEY.to_string(), serialized);
        self
    }

}

/// 为 meta key 声明 `ctx.meta` 上的访问器 —— 对齐 `impl_ext_accessors!`。
///
/// ```ignore
/// // 中间件包
/// impl MetaKey for DagPlanKey { const KEY = "__dagPlan"; type Value = DagPlan; }
/// impl_meta_accessors!(DagPlanKey => DagPlanAccess, dag_plan, try_dag_plan);
///
/// // 使用者（只要 use 那个 trait）
/// use dag_pkg::DagPlanAccess;
/// let plan = ctx.meta.dag_plan();          // handler：缺失 panic（编译期已保证有）
/// let plan = sc.ctx.meta.try_dag_plan();   // 中间件：Option
/// ```
#[macro_export]
macro_rules! impl_meta_accessors {
    ($key:ty => $trait_name:ident, $getter:ident, $try_getter:ident) => {
        /// 由 `impl_meta_accessors!` 生成：`MetaBag` 上该字段的访问器 trait。
        pub trait $trait_name {
            #[doc = concat!("取 `ctx.meta.", stringify!($getter), "` 的值；缺失时 panic。")]
            fn $getter(&self) -> <$key as $crate::define::MetaKey>::Value;
            #[doc = "取该字段的值；缺失返回 `None`。"]
            fn $try_getter(&self) -> Option<<$key as $crate::define::MetaKey>::Value>;
        }

        impl<P> $trait_name for $crate::define::MetaBag<P> {
            fn $getter(&self) -> <$key as $crate::define::MetaKey>::Value {
                self.get::<$key>()
            }
            fn $try_getter(&self) -> Option<<$key as $crate::define::MetaKey>::Value> {
                self.try_get::<$key>()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(serde::Deserialize, Clone, serde::Serialize, Debug, PartialEq)]
    pub struct DagPlan {
        goals: Vec<String>,
    }

    pub struct DagPlanKey;
    impl MetaKey for DagPlanKey {
        const KEY: &'static str = "__dagPlan";
        type Value = DagPlan;
    }
    crate::impl_meta_accessors!(DagPlanKey => DagPlanAccess, dag_plan, try_dag_plan);

    #[test]
    fn reads_a_declared_field() {
        let bag = MetaBag::<Nil>::from_value(json!({ "__dagPlan": { "goals": ["a", "b"] } }));
        assert_eq!(
            bag.dag_plan(),
            DagPlan {
                goals: vec!["a".into(), "b".into()]
            }
        );
    }

    #[test]
    fn missing_field_is_none_from_try() {
        let bag = MetaBag::<Nil>::from_value(json!({}));
        assert_eq!(bag.try_dag_plan(), None);
    }

    #[test]
    #[should_panic(expected = "__dagPlan")]
    fn missing_field_panics_from_get() {
        // handler 侧用它——但那只有在「声明累积」保证过之后才安全；
        // 现在（第一阶段）它仍然是运行期 panic，测试锁定这个行为。
        let bag = MetaBag::<Nil>::from_value(json!({}));
        let _ = bag.dag_plan();
    }

    #[test]
    fn malformed_field_is_none_from_try() {
        // 结构不对 → try_get 返回 None（不是 panic）
        let bag = MetaBag::<Nil>::from_value(json!({ "__dagPlan": "not an object" }));
        assert_eq!(bag.try_dag_plan(), None);
    }

    #[test]
    fn non_object_meta_is_empty_bag() {
        // `null` / 缺失 / 标量都视为空袋（与 ExtBag 一致）
        for raw in [json!(null), json!("x"), json!(42)] {
            let bag = MetaBag::<Nil>::from_value(raw);
            assert!(bag.is_empty());
            assert_eq!(bag.try_dag_plan(), None);
        }
    }

    #[test]
    fn one_bad_field_does_not_affect_others() {
        let bag = MetaBag::<Nil>::from_value(json!({
            "__dagPlan": "broken",
            "other": 1
        }));
        assert_eq!(bag.try_dag_plan(), None);
        // 其余字段照常可读（按 key 独立反序列化）
        assert_eq!(bag.as_value()["other"], 1);
    }

    #[test]
    fn set_then_get_round_trips() {
        // `set`（写 options）与 `get`（读 ctx.meta）共用同一个 KEY，必须对称。
        let plan = DagPlan {
            goals: vec!["a".into(), "b".into()],
        };
        let bag = MetaBag::<Nil>::new().set::<DagPlanKey>(plan);
        assert_eq!(
            bag.dag_plan(),
            DagPlan {
                goals: vec!["a".into(), "b".into()]
            }
        );
    }

    #[test]
    fn set_chains_multiple_fields() {
        let bag = MetaBag::<Nil>::new()
            .set::<DagPlanKey>(DagPlan { goals: vec![] })
            .set::<OtherKey>(42u32);
        assert!(bag.try_dag_plan().is_some());
        assert_eq!(bag.try_other(), Some(42));
    }

    #[test]
    fn empty_bag_set_is_still_object() {
        // 空袋上 set 之后必须变成 JSON object，再读得到
        let bag = MetaBag::<Nil>::default().set::<OtherKey>(7u32);
        assert!(bag.as_value().is_object());
        assert_eq!(bag.try_other(), Some(7));
    }

    pub struct OtherKey;
    impl MetaKey for OtherKey {
        const KEY: &'static str = "__other";
        type Value = u32;
    }
    crate::impl_meta_accessors!(OtherKey => OtherAccess, other, try_other);
}

/// 能构造「空」的 meta —— `RunWorkflowOptions::new` 用它给默认值，
/// 那时 `P` 由 workflow 的类型决定，调用点写不出来。
pub trait EmptyMeta {
    /// 空的 meta（没有任何字段）。
    fn empty_meta() -> Self;
}

impl<P> EmptyMeta for MetaBag<P> {
    fn empty_meta() -> Self {
        MetaBag {
            raw: Value::Object(Default::default()),
            _provided: std::marker::PhantomData,
        }
    }
}
