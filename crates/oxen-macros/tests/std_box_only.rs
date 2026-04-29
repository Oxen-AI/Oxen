//! Regression for the `unbox_inner_path_type` detector: it must accept *only*
//! the standard library's `Box` (bare, `std::boxed::Box`, `core::boxed::Box`,
//! `alloc::boxed::Box`, with optional leading `::`) and reject any other path
//! ending in an ident named `Box`.
//!
//! Before the fix, a user-defined `my_module::Box<T>` would be auto-unboxed
//! and the inner `T` would wrongly receive an `IntoOxenError` impl, which is
//! semantic nonsense (no relationship between the user's `Box` and
//! `std::boxed::Box`).

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::OnlyStdBox;
    }
}

/// Fake `Box` that lives in a non-std module. The macro must NOT detect this
/// as a `Box<T>` and therefore must NOT emit `IntoOxenError` for the inner
/// `Payload` type.
mod fake {
    #[derive(Debug)]
    #[allow(dead_code)]
    pub struct Box<T>(pub T);

    impl<T: std::fmt::Debug> std::fmt::Display for Box<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "fake::Box({:?})", self.0)
        }
    }
    impl<T: std::fmt::Debug> std::error::Error for Box<T> {}
}

#[derive(thiserror::Error, Debug)]
#[error("Payload({0})")]
pub struct Payload(pub u8);

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum OnlyStdBox {
    /// Real std `Box<Payload>` — auto-unboxing applies; both
    /// `IntoOxenError for Box<Payload>` and `IntoOxenError for Payload` are
    /// emitted.
    #[error("real_std: {0}")]
    RealStdBox(#[from_ox] Box<Payload>),
    /// User-defined `fake::Box<Payload>` — the path doesn't name std's `Box`,
    /// so only the outer `IntoOxenError for fake::Box<Payload>` is emitted.
    /// `Payload` MUST NOT pick up an `IntoOxenError` impl from this variant.
    #[error("fake: {0}")]
    FakeBox(#[from_ox] fake::Box<Payload>),
}

#[test]
fn real_std_box_emits_both_impls() {
    use error::IntoOxenError;
    // Outer: `Box<Payload>` constructs the variant directly.
    let e: OnlyStdBox = Box::new(Payload(7)).into_oxen();
    match e {
        OnlyStdBox::RealStdBox(b) => assert_eq!(b.0, 7),
        _ => panic!("expected OnlyStdBox::RealStdBox"),
    }
}

#[test]
fn fake_box_outer_impl_works() {
    use error::IntoOxenError;
    // The outer `fake::Box<Payload>` impl IS emitted — that's how the
    // variant is convertible at all.
    let e: OnlyStdBox = fake::Box(Payload(99)).into_oxen();
    match e {
        OnlyStdBox::FakeBox(fake::Box(p)) => assert_eq!(p.0, 99),
        _ => panic!("expected OnlyStdBox::FakeBox"),
    }
}

// CRITICAL negative check. There's exactly one `IntoOxenError for Payload`
// impl in scope here — the one emitted by the *real* `Box<Payload>` variant
// (`RealStdBox`). If the macro's detector wrongly accepted `fake::Box` as a
// `Box`, it would emit a SECOND `IntoOxenError for Payload`, and the
// resulting duplicate impl would fail to compile. The fact that THIS test
// crate compiles is itself proof that `fake::Box<Payload>` is NOT being
// auto-unboxed.
//
// We pin this with an explicit positive assertion that `Payload` does have
// `IntoOxenError` (from the real `Box<Payload>` variant), so a future
// refactor that accidentally drops the `RealStdBox` variant won't silently
// pass this test.
static_assertions::assert_impl_all!(Payload: error::IntoOxenError);
