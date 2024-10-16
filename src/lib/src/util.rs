//! Various utility functions
//!

pub mod concurrency;
pub mod fs;
pub mod hasher;
pub mod image;
pub mod logging;
pub mod oxen_version;
pub mod paginate;
pub mod progress_bar;
pub mod read_progress;
pub mod str;

pub use crate::util::read_progress::ReadProgress;
pub use paginate::{paginate, paginate_with_total};

pub mod oxen_date_format {
    use chrono::{DateTime, Local};
    use serde::{Deserialize, Deserializer, Serializer};

    pub const FORMAT: &str = "%a, %d %b %Y %H:%M:%S %z";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_str(&s, FORMAT)
            .map(Into::into)
            .map_err(serde::de::Error::custom)
    }
}
