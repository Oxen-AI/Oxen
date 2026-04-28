//! Integration test for `oxen_macros::IntoOxen`'s `Box<T>` auto-unboxing.
//!
//! The proc-macro emits `impl crate::error::IntoOxenError for ...` against the
//! enclosing crate's `crate::error::IntoOxenError`, so this test stands up a
//! minimal `error` module whose trait shape matches the one in liboxen but
//! returns the test enum instead of `OxenError`. That lets us exercise the
//! macro standalone, away from liboxen's full error machinery.

/// Mock funnel trait, shaped exactly like liboxen's `IntoOxenError` except its
/// `into_oxen` returns the test enum (which is what the macro emits in the
/// impl method body — see the `quote!` block in `crates/oxen-macros/src/lib.rs`).
mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::TestErr;
    }
}

/// Test enum exercising the three `#[from_ox]` shapes:
/// - plain field (no `Box`) — single impl emitted;
/// - `Box<T>` with path-shaped `T` — both `Box<T>` and `T` impls emitted;
/// - `Box<dyn Trait>` — only the outer `Box<dyn Trait>` impl emitted (the
///   inner `dyn Trait` is `?Sized` and would not satisfy `into_oxen(self)`).
#[derive(Debug, oxen_macros::IntoOxen)]
#[allow(dead_code)] // variants exist solely to drive macro expansion
pub enum TestErr {
    Plain(#[from_ox] u32),
    Boxed(#[from_ox] Box<i64>),
    BoxedDyn(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
}

#[test]
fn plain_field_emits_outer_impl() {
    use error::IntoOxenError;
    let e: TestErr = 7u32.into_oxen();
    assert!(matches!(e, TestErr::Plain(7)));
}

#[test]
fn box_field_emits_outer_impl() {
    use error::IntoOxenError;
    // `IntoOxenError for Box<i64>` constructs the variant directly without
    // re-allocating.
    let boxed: Box<i64> = Box::new(42);
    let e: TestErr = boxed.into_oxen();
    match e {
        TestErr::Boxed(b) => assert_eq!(*b, 42),
        _ => panic!("expected TestErr::Boxed"),
    }
}

#[test]
fn box_field_also_emits_inner_unboxed_impl() {
    use error::IntoOxenError;
    // `IntoOxenError for i64` boxes `self` on construction. This is the new
    // capability — without it the call below would not compile.
    let e: TestErr = 99i64.into_oxen();
    match e {
        TestErr::Boxed(b) => assert_eq!(*b, 99),
        _ => panic!("expected TestErr::Boxed"),
    }
}

#[test]
fn box_dyn_field_emits_outer_only() {
    use error::IntoOxenError;
    // The macro must NOT emit `IntoOxenError for dyn Error` (it's `?Sized`),
    // but the outer `IntoOxenError for Box<dyn Error + Send + Sync>` must
    // still work.
    let inner: Box<dyn std::error::Error + Send + Sync> = Box::new(std::io::Error::other("boom"));
    let e: TestErr = inner.into_oxen();
    assert!(matches!(e, TestErr::BoxedDyn(_)));
}
