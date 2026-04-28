//! Regression: `#[oxen_macros::from_ox]` injects `#[source]` next to each
//! `#[from_ox]`-marked field so that `Error::source()` chains to the wrapped
//! foreign error. This is the behaviour that thiserror's `#[from]` provides
//! implicitly; we must provide it ourselves because our marker is not
//! recognised by thiserror.
//!
//! Without the `#[source]` injection, `Error::source()` returns `None` and
//! libraries that walk error causes (e.g. `tracing`'s `error.cause_chain`,
//! `anyhow`'s root-cause display) lose visibility into the underlying error.

use std::error::Error as _;

mod error {
    pub trait IntoOxenError {
        fn into_oxen(self) -> super::Wrapper;
    }
}

#[oxen_macros::from_ox]
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum Wrapper {
    /// Plain (non-Box) wrapped foreign error.
    #[error("io: {0}")]
    Io(#[from_ox] std::io::Error),
    /// Boxed wrapped foreign error — `#[source]` must be injected on the
    /// `Box<...>` field too.
    #[error("custom: {0}")]
    Custom(#[from_ox] Box<MyError>),
}

#[derive(thiserror::Error, Debug)]
#[error("MyError({msg})")]
pub struct MyError {
    msg: String,
}

#[test]
fn plain_field_chains_source() {
    use error::IntoOxenError;
    let inner = std::io::Error::other("inner");
    let outer: Wrapper = inner.into_oxen();
    let chained = outer
        .source()
        .expect("Error::source() must return the wrapped io::Error");
    // The plain-field case stores `io::Error` directly, so downcasting the
    // dyn Error back to io::Error must succeed.
    let downcast = chained
        .downcast_ref::<std::io::Error>()
        .expect("source must be the io::Error we put in");
    assert_eq!(downcast.to_string(), "inner");
}

#[test]
fn boxed_field_chains_source() {
    use error::IntoOxenError;
    let inner = MyError {
        msg: "nested".into(),
    };
    let outer: Wrapper = inner.into_oxen();
    let chained = outer
        .source()
        .expect("Error::source() must return the wrapped MyError");
    // For a `Box<MyError>` field, the dyn Error thiserror returns may point
    // either at the `Box<MyError>` itself or at the inner `MyError`,
    // depending on thiserror's `AsDynError` impl details. Either way, its
    // Display must include the wrapped message — that's the user-visible
    // contract that `source()` chaining preserves.
    assert!(
        chained.to_string().contains("nested"),
        "source().to_string() should surface the inner error's message; got {:?}",
        chained.to_string()
    );
}

#[test]
fn boxed_field_chains_source_when_caller_already_boxed() {
    use error::IntoOxenError;
    let inner = Box::new(MyError {
        msg: "pre-boxed".into(),
    });
    let outer: Wrapper = inner.into_oxen();
    let chained = outer
        .source()
        .expect("Error::source() must return the wrapped Box<MyError>");
    assert!(
        chained.to_string().contains("pre-boxed"),
        "source().to_string() should surface the inner error's message; got {:?}",
        chained.to_string()
    );
}

#[test]
fn boxed_field_source_chain_terminates() {
    use error::IntoOxenError;
    // Walk the chain to make sure it terminates: we expect exactly one level
    // (Wrapper -> MyError) since MyError has no source of its own.
    let inner = MyError { msg: "leaf".into() };
    let outer: Wrapper = inner.into_oxen();
    let mut depth = 0;
    let mut current: Option<&dyn std::error::Error> = Some(&outer);
    while let Some(e) = current {
        depth += 1;
        current = e.source();
        if depth > 10 {
            panic!("source chain did not terminate");
        }
    }
    assert_eq!(
        depth, 2,
        "expected exactly two levels in the chain (Wrapper, then MyError)"
    );
}
