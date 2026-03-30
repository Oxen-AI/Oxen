use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::EnumIter;
use strum::IntoEnumIterator;

use crate::error::OxenError;

#[derive(Clone, Debug, Default, Display, Serialize, Deserialize, PartialEq, Eq, EnumIter)]
#[serde(rename_all = "lowercase")]
// TODO: When we upgrade to derive_more 2.0.0, use #[display(rename_all = "lowercase")]
pub enum SortBy {
    #[default]
    #[display("name")]
    Name,
    #[display("date")]
    Date,
}

impl SortBy {
    pub fn values() -> Vec<&'static str> {
        Self::iter().map(|sort_by| sort_by.as_str()).collect()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SortBy::Name => "name",
            SortBy::Date => "date",
        }
    }
}

impl FromStr for SortBy {
    type Err = OxenError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim().to_lowercase();
        SortBy::iter()
            .find(|sort_by| sort_by.as_str() == value)
            .ok_or_else(|| {
                let options = SortBy::values()
                    .into_iter()
                    .map(|value| format!("'{value}'"))
                    .collect::<Vec<String>>()
                    .join(" or ");
                OxenError::basic_str(format!("Invalid sort_by: {value}. Expected {options}."))
            })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SortOpts {
    pub sort_by: SortBy,
    pub reverse: bool,
}

impl SortOpts {
    pub fn from_query(sort_by: Option<&str>, reverse: bool) -> Result<Option<Self>, OxenError> {
        let parsed = sort_by
            .map(SortBy::from_str)
            .transpose()?
            .map(|sort_by| Self { sort_by, reverse });

        Ok(parsed.or_else(|| {
            reverse.then_some(Self {
                sort_by: SortBy::Name,
                reverse: true,
            })
        }))
    }
}
