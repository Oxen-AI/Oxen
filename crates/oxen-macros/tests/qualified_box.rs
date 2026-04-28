//! The macro's `Box<T>` detector matches the *last* path segment named `Box`,
//! so a fully-qualified `std::boxed::Box<T>` field must also trigger
//! auto-unboxing.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Qualified;
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Inner({0})")]
pub struct Inner(pub i32);

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum Qualified {
    /// Spell the field type with the full path.
    #[error("fully qualified: {0}")]
    FullyQualified(#[from_ox] std::boxed::Box<Inner>),
}

#[test]
fn fully_qualified_box_outer_impl_is_emitted() {
    use error::IntoOxenError;
    let q: Qualified = std::boxed::Box::new(Inner(7)).into_oxen();
    match q {
        Qualified::FullyQualified(b) => assert_eq!(b.0, 7),
    }
}

#[test]
fn fully_qualified_box_inner_impl_is_emitted() {
    use error::IntoOxenError;
    // Inner-type impl works only if the macro recognised `std::boxed::Box`
    // as a `Box`. If the detector were too strict (matching only on the bare
    // `Box` path with no leading segments), this test would not compile.
    let q: Qualified = Inner(99).into_oxen();
    match q {
        Qualified::FullyQualified(b) => assert_eq!(b.0, 99),
    }
}
