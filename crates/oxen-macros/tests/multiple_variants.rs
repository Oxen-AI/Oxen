//! Verifies that several `#[from_ox]` variants in one enum coexist cleanly:
//! the macro emits one set of impls per annotated variant, and they don't
//! interfere with each other or with non-annotated variants.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Multi;
    }
}

#[derive(thiserror::Error, Debug)]
#[error("PlainErr({0})")]
pub struct PlainErr(pub u32);

#[derive(thiserror::Error, Debug)]
#[error("IntErr({0})")]
pub struct IntErr(pub i64);

#[derive(thiserror::Error, Debug)]
#[error("StrErr({0})")]
pub struct StrErr(pub String);

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum Multi {
    /// Plain field — single impl.
    #[error("plain: {0}")]
    Plain(#[from_ox] PlainErr),
    /// Boxed concrete inner — both `Box<IntErr>` and `IntErr` impls.
    #[error("boxed: {0}")]
    Boxed(#[from_ox] Box<IntErr>),
    /// Boxed concrete inner with a different inner type — independent impls.
    #[error("string: {0}")]
    AnotherBoxed(#[from_ox] Box<StrErr>),
    /// Boxed dyn — only the outer impl (inner is `?Sized`).
    #[error("dyn: {0}")]
    DynBoxed(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
    /// No `#[from_ox]`: must NOT have an `IntoOxenError` impl generated.
    #[error("no conv: {0}")]
    NoConversion(bool),
    /// Unit variant — no field to annotate; macro must skip without error.
    #[error("empty")]
    Empty,
}

#[test]
fn plain_variant_works() {
    use error::IntoOxenError;
    let m: Multi = PlainErr(7).into_oxen();
    match m {
        Multi::Plain(PlainErr(7)) => {}
        _ => panic!("expected Multi::Plain(PlainErr(7))"),
    }
}

#[test]
fn first_box_variant_emits_both_impls() {
    use error::IntoOxenError;
    let m: Multi = Box::new(IntErr(42)).into_oxen();
    match m {
        Multi::Boxed(b) => assert_eq!(b.0, 42),
        _ => panic!("expected Multi::Boxed"),
    }
    let m: Multi = IntErr(100).into_oxen();
    match m {
        Multi::Boxed(b) => assert_eq!(b.0, 100),
        _ => panic!("expected Multi::Boxed"),
    }
}

#[test]
fn second_box_variant_emits_both_impls_independently() {
    use error::IntoOxenError;
    // Independent inner type (`StrErr`) — its own pair of impls, no collision
    // with the `IntErr` pair from `Boxed`.
    let m: Multi = Box::new(StrErr(String::from("alpha"))).into_oxen();
    match m {
        Multi::AnotherBoxed(b) => assert_eq!(b.0.as_str(), "alpha"),
        _ => panic!("expected Multi::AnotherBoxed"),
    }
    let m: Multi = StrErr(String::from("beta")).into_oxen();
    match m {
        Multi::AnotherBoxed(b) => assert_eq!(b.0.as_str(), "beta"),
        _ => panic!("expected Multi::AnotherBoxed"),
    }
}

#[test]
fn dyn_box_variant_outer_impl_only() {
    use error::IntoOxenError;
    let inner: Box<dyn std::error::Error + Send + Sync> = Box::new(std::io::Error::other("x"));
    let m: Multi = inner.into_oxen();
    assert!(matches!(m, Multi::DynBoxed(_)));
}

#[test]
fn unit_and_unannotated_variants_compile() {
    // The macro must skip both `NoConversion(bool)` and `Empty` cleanly: no
    // impl is emitted for either, and the enum can still be constructed
    // manually.
    let _ = Multi::NoConversion(true);
    let _ = Multi::Empty;
}
