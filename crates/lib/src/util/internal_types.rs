use std::collections::{HashMap, HashSet};

/// Indicates that the type has a length.
pub(crate) trait HasLen {
    fn len(&self) -> usize;
}

macro_rules! impl_has_len_simple {
    ($ty:ty) => {
        impl HasLen for $ty {
            fn len(&self) -> usize {
                self.len()
            }
        }
    };
}

macro_rules! impl_has_len_generic1 {
  ($($ty:ident),*) => {
    $(
      impl<T> HasLen for $ty<T> {
          fn len(&self) -> usize {
              self.len()
          }
      }
    )*
  };
}

macro_rules! impl_has_len_generic2 {
  ($($ty:ident),*) => {
    $(
      impl<K, V> HasLen for $ty<K, V> {
          fn len(&self) -> usize {
              self.len()
          }
      }
    )*
  };
}

impl_has_len_simple!(bytes::Bytes);
impl_has_len_simple!(String);
impl_has_len_simple!(str);

impl_has_len_generic1!(Vec);
impl_has_len_generic1!(HashSet);

impl_has_len_generic2!(HashMap);
