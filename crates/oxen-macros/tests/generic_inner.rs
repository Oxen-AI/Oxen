//! `Box<T<U>>` where the inner `T<U>` is itself a generic type. The macro
//! must emit `IntoOxenError for T<U>` with the generic args preserved
//! verbatim. No special handling needed — the macro just clones the inner
//! `Type` token tree — but worth pinning down with a test.

use std::collections::HashMap;

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Generic;
    }
}

#[derive(Debug, oxen_macros::IntoOxen)]
#[allow(dead_code)]
pub enum Generic {
    /// Inner is `Vec<u8>`, a single-arg generic.
    Bytes(#[from_ox] Box<Vec<u8>>),
    /// Inner is a two-arg generic with a multi-segment path.
    Map(#[from_ox] Box<HashMap<String, i64>>),
}

#[test]
fn vec_inner_emits_both_impls() {
    use error::IntoOxenError;
    let g: Generic = Box::new(vec![1u8, 2, 3]).into_oxen();
    match g {
        Generic::Bytes(b) => assert_eq!(*b, vec![1, 2, 3]),
        _ => panic!("expected Generic::Bytes"),
    }
    let g: Generic = vec![4u8, 5, 6].into_oxen();
    match g {
        Generic::Bytes(b) => assert_eq!(*b, vec![4, 5, 6]),
        _ => panic!("expected Generic::Bytes"),
    }
}

#[test]
fn hashmap_inner_with_qualified_path_works() {
    use error::IntoOxenError;
    let mut m = HashMap::new();
    m.insert("k".to_string(), 1i64);
    let g: Generic = Box::new(m.clone()).into_oxen();
    match g {
        Generic::Map(b) => assert_eq!(b.get("k"), Some(&1)),
        _ => panic!("expected Generic::Map"),
    }
    // Auto-unboxed call too:
    let g: Generic = m.into_oxen();
    match g {
        Generic::Map(b) => assert_eq!(b.get("k"), Some(&1)),
        _ => panic!("expected Generic::Map"),
    }
}
