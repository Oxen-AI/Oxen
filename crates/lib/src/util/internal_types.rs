use crate::error::OxenError;
use std::collections::{HashMap, HashSet};
use url::Url;

/// Parsed URL components with port-aware hostname.
pub struct Hostname {
    pub host: String,
    pub port: Option<u16>,
    pub scheme: String,
}

impl Hostname {
    /// Returns `host:port` when a port is present, otherwise just `host`.
    pub fn hostname(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{}", self.host, port),
            None => self.host.clone(),
        }
    }

    /// Extract scheme, host, and port from a `Url`.
    pub fn from_url(url: &Url) -> Result<Self, OxenError> {
        let Some(host) = url.host_str() else {
            return Err(OxenError::NoHost(url.to_string().into()));
        };
        Ok(Self {
            host: host.to_string(),
            port: url.port(),
            scheme: url.scheme().to_string(),
        })
    }
}

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
