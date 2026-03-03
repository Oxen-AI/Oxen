//! # StringError
//!
//! Struct that wraps a string and implements the necessary traits for errors.
//!

// TODO: do we actually need to wrap values into something that implements Error?
// TODO: investigate if it's sufficient to use values directly

error_wrapper!(String);

impl From<&str> for StringError {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}
