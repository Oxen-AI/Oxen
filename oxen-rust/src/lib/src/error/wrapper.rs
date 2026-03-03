/// Produces a `struct $typeError` that wraps a `$type` as a std::error::Error.
/// Provide the `$type` as the first argument.
///
/// If the wrapped value doesn't implement `std::fmt::Display`, then a second
/// argument can be provided that transforms a borrow of the wrapped value
/// into something that does implement `std::fmt::Display`.
macro_rules! error_wrapper {
    ($type:ident) => {
        base_error_wrapper!($type);

        paste::paste! {
          impl std::fmt::Display for [<$type Error>] {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
              write!(f, "{}", self.0)
            }
          }
        }
    };

    ($type:ident, $display:expr) => {
        base_error_wrapper!($type);

        paste::paste! {
          impl std::fmt::Display for [<$type Error>] {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
              let inner_borrow = &self.0;
              write!(f, "{}", $display(inner_borrow))
            }
          }
        }
    };
}

macro_rules! base_error_wrapper {
    ($type:ident) => {
        paste::paste! {
          /// Struct that wraps a `$type` and implements the Error trait.
          /// Provides conveniences for wrapping, unwrapping, and producing boxed wrapped values.
          #[derive(Debug)]
          pub struct [<$type Error>]($type);

          impl [<$type Error>] {
              /// Wrap a value of `$type` as a `[<$type Error>]`.
              pub fn new(value: $type) -> Self {
                  [<$type Error>](value)
              }

              /// Destroy the wrapper and return the inner `$type` value.
              pub fn unwrap(self) -> $type {
                self.0
              }

              /// Wraps then boxes the `$type` value.
              pub fn boxed(value: $type) -> Box<Self> {
                  Box::new([<$type Error>]::new(value))
              }
          }

          impl From<$type> for [<$type Error>] {
              /// Wraps a `$type` by calling the `new` constructor.
              fn from(p: $type) -> Self {
                  [<$type Error>]::new(p)
              }
          }

          impl std::error::Error for [<$type Error>] {}
        }
    };
}
