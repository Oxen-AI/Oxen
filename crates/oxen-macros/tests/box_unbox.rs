//! Integration test for `oxen_macros::from_ox`'s `Box<T>` auto-unboxing.
//!
//! The proc-macro emits `impl crate::error::IntoOxenError for ...` against the
//! enclosing crate's `crate::error::IntoOxenError`, so this test stands up a
//! minimal `error` module whose trait shape matches the one in liboxen but
//! returns the test enum instead of `OxenError`. That lets us exercise the
//! macro standalone, away from liboxen's full error machinery.
//!
//! All `#[from_ox]`-marked field types implement `std::error::Error` because
//! the macro injects `#[source]` next to each `#[from_ox]`, and thiserror's
//! `#[source]` requires the field type to be an `Error`. This matches the
//! intended real-world usage on `OxenError`.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::TestErr;
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Plain({0})")]
pub struct Plain(pub String);

#[derive(thiserror::Error, Debug)]
#[error("Inner({0})")]
pub struct Inner(pub i64);

/// Test enum exercising the three `#[from_ox]` shapes:
/// - plain field (no `Box`) — single impl emitted;
/// - `Box<T>` with path-shaped `T` — both `Box<T>` and `T` impls emitted;
/// - `Box<dyn Trait>` — only the outer `Box<dyn Trait>` impl emitted (the
///   inner `dyn Trait` is `?Sized` and would not satisfy `into_oxen(self)`).
#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)] // variants exist solely to drive macro expansion
pub enum TestErr {
    #[error("plain: {0}")]
    PlainV(#[from_ox] Plain),
    #[error("boxed: {0}")]
    Boxed(#[from_ox] Box<Inner>),
    #[error("boxed dyn: {0}")]
    BoxedDyn(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
}

#[test]
fn plain_field_emits_outer_impl() {
    use error::IntoOxenError;
    let e: TestErr = Plain("hello".into()).into_oxen();
    assert!(matches!(e, TestErr::PlainV(Plain(ref s)) if s == "hello"));
}

#[test]
fn box_field_emits_outer_impl() {
    use error::IntoOxenError;
    // `IntoOxenError for Box<Inner>` constructs the variant directly without
    // re-allocating.
    let boxed: Box<Inner> = Box::new(Inner(42));
    let e: TestErr = boxed.into_oxen();
    match e {
        TestErr::Boxed(b) => assert_eq!(b.0, 42),
        _ => panic!("expected TestErr::Boxed"),
    }
}

#[test]
fn box_field_also_emits_inner_unboxed_impl() {
    use error::IntoOxenError;
    // `IntoOxenError for Inner` boxes `self` on construction. This is the new
    // capability — without it the call below would not compile.
    let e: TestErr = Inner(99).into_oxen();
    match e {
        TestErr::Boxed(b) => assert_eq!(b.0, 99),
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
