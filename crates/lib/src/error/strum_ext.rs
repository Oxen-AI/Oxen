use std::{marker::PhantomData, str::FromStr};

#[derive(Debug, thiserror::Error)]
#[error(
    "Unknown {type_name}: {input} (expected one of {names})",
    type_name=std::any::type_name::<X>().rsplit("::").next().unwrap_or(""), // will actually always be non-empty
    names=<X as strum::VariantNames>::VARIANTS.join(", "),
)]
pub struct VariantAwareStrumError<X: strum::VariantNames> {
    input: String,
    #[source]
    e: strum::ParseError,
    _type: PhantomData<X>,
}

/// Parse the string into an X, but return a nice error message that lists what the values should have been.
pub fn parse<X>(value: &str) -> Result<X, VariantAwareStrumError<X>>
where
    X: strum::VariantNames,
    X: FromStr<Err = strum::ParseError>,
{
    value.parse().map_err(|e| VariantAwareStrumError {
        input: value.to_string(),
        e,
        _type: PhantomData,
    })
}
