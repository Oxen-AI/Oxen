//! `Box<T<U>>` where the inner `T<U>` is itself a generic type. The macro
//! must emit `IntoOxenError for T<U>` with the generic args preserved
//! verbatim. No special handling needed — the macro just clones the inner
//! `Type` token tree — but worth pinning down with a test.
//!
//! Both inner types here are user-defined generic newtypes that implement
//! `std::error::Error` (via `thiserror::Error`), so the `#[source]` injection
//! the macro performs is satisfied.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Generic;
    }
}

/// Single-arg generic that wraps a `Vec<u8>` payload.
#[derive(thiserror::Error, Debug)]
#[error("BytesErr({0:?})")]
pub struct BytesErr<T: std::fmt::Debug>(pub Vec<T>);

/// Two-arg generic that wraps a `(K, V)` pair, with both type parameters
/// preserved through the macro expansion.
#[derive(thiserror::Error, Debug)]
#[error("MapErr({key}, {val})")]
pub struct MapErr<K: std::fmt::Display, V: std::fmt::Display> {
    pub key: K,
    pub val: V,
}

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum Generic {
    /// Inner is `BytesErr<u8>`, a single-arg generic.
    #[error("bytes: {0}")]
    Bytes(#[from_ox] Box<BytesErr<u8>>),
    /// Inner is a two-arg generic with a multi-segment-equivalent shape.
    #[error("map: {0}")]
    Map(#[from_ox] Box<MapErr<String, i64>>),
}

#[test]
fn vec_inner_emits_both_impls() {
    use error::IntoOxenError;
    let g: Generic = Box::new(BytesErr(vec![1u8, 2, 3])).into_oxen();
    match g {
        Generic::Bytes(b) => assert_eq!(b.0, vec![1, 2, 3]),
        _ => panic!("expected Generic::Bytes"),
    }
    let g: Generic = BytesErr(vec![4u8, 5, 6]).into_oxen();
    match g {
        Generic::Bytes(b) => assert_eq!(b.0, vec![4, 5, 6]),
        _ => panic!("expected Generic::Bytes"),
    }
}

#[test]
fn two_arg_generic_inner_works() {
    use error::IntoOxenError;
    let m = MapErr {
        key: "k".to_string(),
        val: 1i64,
    };
    let g: Generic = Box::new(MapErr {
        key: m.key.clone(),
        val: m.val,
    })
    .into_oxen();
    match g {
        Generic::Map(b) => {
            assert_eq!(b.key, "k");
            assert_eq!(b.val, 1);
        }
        _ => panic!("expected Generic::Map"),
    }
    // Auto-unboxed call too:
    let g: Generic = m.into_oxen();
    match g {
        Generic::Map(b) => {
            assert_eq!(b.key, "k");
            assert_eq!(b.val, 1);
        }
        _ => panic!("expected Generic::Map"),
    }
}
