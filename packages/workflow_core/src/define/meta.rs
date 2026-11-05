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
    /// 字段的值类型。
    type Value: serde::de::DeserializeOwned;
}

/// 读 `ctx.meta` 的入口 —— 与 [`ExtBag`](crate::define::ExtBag) 同构。
///
/// 每个 key **独立**反序列化：一个解析失败只影响它自己。
#[derive(Clone, Default, Debug)]
pub struct MetaBag {
    raw: Value,
}

impl MetaBag {
    /// 从引擎的擦除表示构造。非对象（`null` / 缺失）视为空袋。
    pub fn from_value(raw: Value) -> Self {
        Self {
            raw: if raw.is_object() {
                raw
            } else {
                Value::Object(Default::default())
            },
        }
    }

    /// 空袋（没有 meta）。
    pub fn empty() -> Self {
        Self::default()
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

        impl $trait_name for $crate::define::MetaBag {
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

    #[derive(serde::Deserialize, Debug, PartialEq)]
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
        let bag = MetaBag::from_value(json!({ "__dagPlan": { "goals": ["a", "b"] } }));
        assert_eq!(
            bag.dag_plan(),
            DagPlan {
                goals: vec!["a".into(), "b".into()]
            }
        );
    }

    #[test]
    fn missing_field_is_none_from_try() {
        let bag = MetaBag::from_value(json!({}));
        assert_eq!(bag.try_dag_plan(), None);
    }

    #[test]
    #[should_panic(expected = "__dagPlan")]
    fn missing_field_panics_from_get() {
        // handler 侧用它——但那只有在「声明累积」保证过之后才安全；
        // 现在（第一阶段）它仍然是运行期 panic，测试锁定这个行为。
        let bag = MetaBag::from_value(json!({}));
        let _ = bag.dag_plan();
    }

    #[test]
    fn malformed_field_is_none_from_try() {
        // 结构不对 → try_get 返回 None（不是 panic）
        let bag = MetaBag::from_value(json!({ "__dagPlan": "not an object" }));
        assert_eq!(bag.try_dag_plan(), None);
    }

    #[test]
    fn non_object_meta_is_empty_bag() {
        // `null` / 缺失 / 标量都视为空袋（与 ExtBag 一致）
        for raw in [json!(null), json!("x"), json!(42)] {
            let bag = MetaBag::from_value(raw);
            assert!(bag.is_empty());
            assert_eq!(bag.try_dag_plan(), None);
        }
    }

    #[test]
    fn one_bad_field_does_not_affect_others() {
        let bag = MetaBag::from_value(json!({
            "__dagPlan": "broken",
            "other": 1
        }));
        assert_eq!(bag.try_dag_plan(), None);
        // 其余字段照常可读（按 key 独立反序列化）
        assert_eq!(bag.as_value()["other"], 1);
    }
}
