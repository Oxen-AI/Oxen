use std::error::Error;

/// Represents an operation that can have partial success.
///
/// Unlike a `Result`, which models complete success or complete failure, a `Complete` adds an
/// in-between state. Some operations are composed of many smaller operations. When most, or even
/// some, succeed, but some fail, it's useful to be able to collect the failures. Sometimes, it's
/// for reporting and logging. In other situations, it may be possible to retry the failed
/// operations.
#[derive(Debug)]
pub enum Complete<S, P, E: Error> {
  /// The operation was completely successful.
  Success(S),
  /// The operation was partially successful, with some errors.
  /// Information about the errors is stored in this variant.
  Partial(P),
  /// The operation failed with an error. It should be considered
  /// completely unsuccessful.
  Error(E),
}

impl<S, P, E: Error> Complete<S, P, E> {
  /// For when the operation was completely successful.
  pub fn success(value: S) -> Self {
    Self::Success(value)
  }

  /// For when the operation was partially successful.
  pub fn partial(value: P) -> Self {
    Self::Partial(value)
  }

  /// For when the operation failed with an error.
  pub fn error(value: E) -> Self {
    Self::Error(value)
  }
}
