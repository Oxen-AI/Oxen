//! Verifies that several `#[from_ox]` variants in one enum coexist cleanly:
//! the macro emits one set of impls per annotated variant, and they don't
//! interfere with each other or with non-annotated variants.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Multi;
    }
}

#[derive(Debug, oxen_macros::IntoOxen)]
#[allow(dead_code)]
pub enum Multi {
    /// Plain field — single impl.
    Plain(#[from_ox] u32),
    /// Boxed concrete inner — both Box<i64> and i64 impls.
    Boxed(#[from_ox] Box<i64>),
    /// Boxed concrete inner with a different inner type — independent impls.
    AnotherBoxed(#[from_ox] Box<String>),
    /// Boxed dyn — only the outer impl (inner is `?Sized`).
    DynBoxed(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
    /// No `#[from_ox]`: must NOT have an `IntoOxenError` impl generated.
    NoConversion(bool),
    /// Unit variant — no field to annotate; macro must skip without error.
    Empty,
}

#[test]
fn plain_variant_works() {
    use error::IntoOxenError;
    let m: Multi = 7u32.into_oxen();
    assert!(matches!(m, Multi::Plain(7)));
}

#[test]
fn first_box_variant_emits_both_impls() {
    use error::IntoOxenError;
    let m: Multi = Box::new(42i64).into_oxen();
    match m {
        Multi::Boxed(b) => assert_eq!(*b, 42),
        _ => panic!("expected Multi::Boxed"),
    }
    let m: Multi = 100i64.into_oxen();
    match m {
        Multi::Boxed(b) => assert_eq!(*b, 100),
        _ => panic!("expected Multi::Boxed"),
    }
}

#[test]
fn second_box_variant_emits_both_impls_independently() {
    use error::IntoOxenError;
    // Independent inner type (`String`) — its own pair of impls, no collision
    // with the `i64` pair from `Boxed`.
    let m: Multi = Box::new(String::from("alpha")).into_oxen();
    match m {
        Multi::AnotherBoxed(b) => assert_eq!(b.as_str(), "alpha"),
        _ => panic!("expected Multi::AnotherBoxed"),
    }
    let m: Multi = String::from("beta").into_oxen();
    match m {
        Multi::AnotherBoxed(b) => assert_eq!(b.as_str(), "beta"),
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
