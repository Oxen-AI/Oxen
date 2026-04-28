//! Edge case: an enum that derives `IntoOxen` but has zero `#[from_ox]`
//! annotations should compile cleanly and emit zero impls. The derive must
//! not fail, ICE, or warn — it should produce an empty token stream.
//!
//! We can't directly assert "no impl exists for type X" inside the test, but
//! we can confirm:
//! 1. The crate compiles (the derive didn't error out).
//! 2. The enum constructs and matches normally.
//! 3. Calling `IntoOxenError::into_oxen` on an unrelated type that *does*
//!    have a hand-written impl still works — proving the trait import
//!    machinery is intact.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::None_;
    }
}

#[derive(Debug, oxen_macros::IntoOxen)]
#[allow(dead_code, non_camel_case_types)]
pub enum None_ {
    Unit,
    Tuple(u32),
    Pair(u32, u32),
    Named { x: u32 },
}

// Hand-rolled impl, unrelated to the derive — used to verify the trait
// itself is still callable in the absence of any derive-emitted impls.
struct Marker;
impl error::IntoOxenError for Marker {
    fn into_oxen(self) -> None_ {
        None_::Unit
    }
}

#[test]
fn enum_constructs_normally() {
    let _ = None_::Unit;
    let _ = None_::Tuple(1);
    let _ = None_::Pair(1, 2);
    let _ = None_::Named { x: 1 };
}

#[test]
fn hand_rolled_impl_still_works() {
    use error::IntoOxenError;
    let n: None_ = Marker.into_oxen();
    assert!(matches!(n, None_::Unit));
}
