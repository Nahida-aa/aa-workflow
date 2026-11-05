//! `trybuild` UI 测试：编译期错误的样子。
//!
//! `docs/decisions/0017` 承诺「声明了 meta 却没提供 → **编译报错**」。
//! 运行期测试锁不住编译失败，只能靠这里。
//!
//! 失败时看 `packages/workflow_core/tests/ui/*.stderr`——那是**期望**的错误
//! 输出。改动后如果报错文案变了，用 `TRYBUILD=overwrite` 重新生成。
#[test]
fn meta_checks_are_compile_time() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/meta_missing.rs");
}
