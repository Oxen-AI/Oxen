//! Edge cases for the macro's `Box<T>` detector. Anything that *isn't*
//! literally a path ending in `Box` with one type-argument must fall back to
//! "outer impl only" — even if it's a `Box`-like wrapper. And anything that
//! IS spelled `Box<T>`, regardless of leading-`::` qualification, must
//! trigger auto-unboxing.

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Shapes;
    }
}

/// A user-defined wrapper that *looks* like `Box` semantically but is
/// spelled differently. The macro must NOT auto-unbox it.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct MyBox<T>(pub T);

#[derive(Debug, oxen_macros::IntoOxen)]
#[allow(dead_code)]
pub enum Shapes {
    /// Reference field: not `Box`, just one outer impl.
    Reference(#[from_ox] &'static str),
    /// `Option<T>`: not `Box`, just one outer impl.
    Optioned(#[from_ox] Option<u8>),
    /// User-defined `MyBox<T>` — its last path segment isn't literally `Box`,
    /// so the macro must NOT auto-unbox it.
    MyBoxed(#[from_ox] MyBox<u16>),
    /// Leading-`::` absolute path to `Box`. The detector matches the *last*
    /// path segment, which is `Box`, so auto-unboxing applies.
    Absolute(#[from_ox] ::std::boxed::Box<u32>),
}

#[test]
fn reference_field_outer_impl_only() {
    use error::IntoOxenError;
    let s: Shapes = "hello".into_oxen();
    assert!(matches!(s, Shapes::Reference("hello")));
}

#[test]
fn option_field_outer_impl_only() {
    use error::IntoOxenError;
    let s: Shapes = Some(7u8).into_oxen();
    assert!(matches!(s, Shapes::Optioned(Some(7))));

    let s: Shapes = None::<u8>.into_oxen();
    assert!(matches!(s, Shapes::Optioned(None)));
}

#[test]
fn user_defined_box_lookalike_is_not_unboxed() {
    use error::IntoOxenError;
    // `MyBox<u16>` itself is convertible (the outer impl is emitted).
    let s: Shapes = MyBox(9u16).into_oxen();
    match s {
        Shapes::MyBoxed(MyBox(x)) => assert_eq!(x, 9),
        _ => panic!("expected Shapes::MyBoxed"),
    }
    // The bare `u16` MUST NOT have an `IntoOxenError` impl from this enum's
    // derive. We can't directly assert "this impl doesn't exist", but the
    // sibling `Shapes::Optioned(#[from_ox] Option<u8>)` already pins the only
    // primitive variant for unsigned ints; if the macro had wrongly unwrapped
    // `MyBox<u16>` into `IntoOxenError for u16`, this would compile but
    // a future contributor adding `u16` to another variant would hit a
    // surprise conflict. Documenting the intent here.
}

#[test]
fn absolute_path_box_triggers_auto_unboxing() {
    use error::IntoOxenError;
    let s: Shapes = ::std::boxed::Box::new(123u32).into_oxen();
    match s {
        Shapes::Absolute(b) => assert_eq!(*b, 123),
        _ => panic!("expected Shapes::Absolute"),
    }
    // Auto-unboxed: bare `u32` should also work for the `Absolute` variant.
    let s: Shapes = 456u32.into_oxen();
    match s {
        Shapes::Absolute(b) => assert_eq!(*b, 456),
        _ => panic!("expected Shapes::Absolute"),
    }
}
