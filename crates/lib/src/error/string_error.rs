//! # StringError
//!
//! Struct that wraps a string and implements the necessary traits for errors.
//!

use std::fmt;

pub struct StringError(String);

impl StringError {
    pub fn new(s: String) -> Self {
        StringError(s)
    }
}

impl From<&str> for StringError {
    fn from(s: &str) -> Self {
        StringError(s.to_string())
    }
}

impl From<String> for StringError {
    fn from(s: String) -> Self {
        StringError(s)
    }
}

impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StringError {}
