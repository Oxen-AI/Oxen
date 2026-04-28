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

impl<T: std::fmt::Debug> std::fmt::Display for MyBox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyBox({:?})", self.0)
    }
}
impl<T: std::fmt::Debug> std::error::Error for MyBox<T> {}

/// `Option`-like wrapper with an inner Error-implementing payload, used so
/// the field type itself is `Error` (required by the `#[source]` attribute
/// that the macro now injects).
#[derive(thiserror::Error, Debug)]
#[error("InnerErr({0})")]
pub struct InnerErr(pub u8);

/// `&'static str` is `?Sized`-adjacent for source purposes (it doesn't impl
/// Error). To still test the "reference field, no Box detection" code path,
/// wrap it in a thin newtype that does impl Error.
#[derive(thiserror::Error, Debug)]
#[error("RefErr(&'static str: {0})")]
pub struct RefErr(pub &'static str);

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum Shapes {
    /// Newtype around `&'static str`: not `Box`, just one outer impl.
    #[error("ref: {0}")]
    Reference(#[from_ox] RefErr),
    /// `Option<InnerErr>`: not `Box`, just one outer impl. Wrapped in an
    /// outer `Option` because we want a field type that is *itself* the
    /// option, but the option must impl `Error`. We use a transparent
    /// wrapper to make that happen.
    #[error("opt: {0}")]
    Optioned(#[from_ox] OptErr),
    /// User-defined `MyBox<T>` — its last path segment isn't literally `Box`,
    /// so the macro must NOT auto-unbox it.
    #[error("my_box: {0}")]
    MyBoxed(#[from_ox] MyBox<u16>),
    /// Leading-`::` absolute path to `Box`. The detector matches the *last*
    /// path segment, which is `Box`, so auto-unboxing applies.
    #[error("absolute: {0}")]
    Absolute(#[from_ox] ::std::boxed::Box<InnerErr>),
}

#[derive(thiserror::Error, Debug)]
#[error("OptErr({0:?})")]
pub struct OptErr(pub Option<u8>);

#[test]
fn reference_field_outer_impl_only() {
    use error::IntoOxenError;
    let s: Shapes = RefErr("hello").into_oxen();
    match s {
        Shapes::Reference(RefErr(v)) => assert_eq!(v, "hello"),
        _ => panic!("expected Shapes::Reference"),
    }
}

#[test]
fn option_field_outer_impl_only() {
    use error::IntoOxenError;
    let s: Shapes = OptErr(Some(7)).into_oxen();
    match s {
        Shapes::Optioned(OptErr(Some(7))) => {}
        _ => panic!("expected Shapes::Optioned(OptErr(Some(7)))"),
    }
    let s: Shapes = OptErr(None).into_oxen();
    match s {
        Shapes::Optioned(OptErr(None)) => {}
        _ => panic!("expected Shapes::Optioned(OptErr(None))"),
    }
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
    // macro. If the macro had wrongly unwrapped `MyBox<u16>` into
    // `IntoOxenError for u16`, a future contributor adding `u16` to another
    // variant would hit a surprise conflict.
}

#[test]
fn absolute_path_box_triggers_auto_unboxing() {
    use error::IntoOxenError;
    let s: Shapes = ::std::boxed::Box::new(InnerErr(123)).into_oxen();
    match s {
        Shapes::Absolute(b) => assert_eq!(b.0, 123),
        _ => panic!("expected Shapes::Absolute"),
    }
    // Auto-unboxed: bare `InnerErr` should also work for the `Absolute` variant.
    let s: Shapes = InnerErr(45).into_oxen();
    match s {
        Shapes::Absolute(b) => assert_eq!(b.0, 45),
        _ => panic!("expected Shapes::Absolute"),
    }
}
