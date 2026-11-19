//! The cells a JSON document fills when it is decoded into a data frame.

use std::fmt;

use polars::prelude::PlHashMap;
use serde::de::{DeserializeSeed, Deserializer, Error, MapAccess, SeqAccess, Visitor};

use crate::constants::MAX_JSON_CELLS;
use crate::error::OxenError;

/// Fails with `OxenError::JsonCellsOverLimit` when decoding `json` into a data frame would fill
/// more than `MAX_JSON_CELLS` cells, and with the parse error when it is not JSON.
pub(super) fn check(json: &[u8]) -> Result<(), OxenError> {
    let cells = count(json)?;
    if cells > MAX_JSON_CELLS {
        return Err(OxenError::JsonCellsOverLimit { cells });
    }
    Ok(())
}

/// The cells, values and nulls alike, that decoding `json` into a data frame fills. Each object
/// found at a place in the document's nesting fills a cell for every key found at that place, a
/// null where it lacks the key, so the count can far exceed the document's size.
fn count(json: &[u8]) -> Result<u64, serde_json::Error> {
    let mut root = Place::default();
    Record(&mut root).deserialize(&mut serde_json::Deserializer::from_slice(json))?;
    // A top-level object is one row, and a top-level array's elements are its rows.
    Ok(root.cells(1))
}

/// What a JSON document holds at one place in its nesting, across every value found there.
#[derive(Default)]
struct Place {
    /// The keys of the objects found here, each with what was found under it.
    keys: PlHashMap<String, Place>,
    /// What the elements of the arrays found here hold.
    elements: Option<Box<Place>>,
    /// The elements of every array found here.
    element_count: u64,
}

impl Place {
    /// The cells filled at and below this place for `rows` rows.
    fn cells(&self, rows: u64) -> u64 {
        let own = rows.saturating_mul(self.keys.len() as u64);
        let under_keys = self
            .keys
            .values()
            .map(|place| place.cells(rows))
            .fold(0, u64::saturating_add);
        let in_elements = self
            .elements
            .as_ref()
            .map_or(0, |elements| elements.cells(self.element_count));
        own.saturating_add(under_keys).saturating_add(in_elements)
    }
}

/// Reads one JSON value into the `Place` it was found at.
struct Record<'a>(&'a mut Place);

impl<'de> DeserializeSeed<'de> for Record<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Record<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(place) = map.next_key_seed(Key(&mut self.0.keys))? {
            map.next_value_seed(Record(place))?;
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let elements = self.0.elements.get_or_insert_with(Box::default);
        while seq.next_element_seed(Record(elements))?.is_some() {
            self.0.element_count += 1;
        }
        Ok(())
    }
}

/// Reads an object's key into the `Place` under it, keeping a copy of the key only the first time
/// it is found.
struct Key<'a>(&'a mut PlHashMap<String, Place>);

impl<'de, 'a> DeserializeSeed<'de> for Key<'a> {
    type Value = &'a mut Place;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de, 'a> Visitor<'de> for Key<'a> {
    type Value = &'a mut Place;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: Error>(self, key: &str) -> Result<Self::Value, E> {
        Ok(self.0.entry_ref(key).or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::count;
    use crate::error::OxenError;
    use polars::prelude::{DataType, JsonReader, SerReader, Series};
    use std::io::Cursor;

    /// The cells `series` fills below its own, in the fields of its structs and the elements of
    /// its lists.
    fn cells_below(series: &Series) -> Result<u64, OxenError> {
        Ok(match series.dtype() {
            DataType::Struct(_) => {
                let mut cells = 0;
                for field in series.struct_()?.fields_as_series() {
                    cells += series.len() as u64 + cells_below(&field)?;
                }
                cells
            }
            DataType::List(_) => cells_below(&series.list()?.get_inner())?,
            _ => 0,
        })
    }

    #[test]
    fn test_count_json_cells() -> Result<(), OxenError> {
        let cases = [
            ("[]", 0, "an empty array has no rows"),
            (r#"{"a": 1, "b": 2}"#, 2, "a top-level object is one row"),
            (
                r#"[{"a": 1}, {"b": 2}, {"c": 3}]"#,
                9,
                "every row fills every key",
            ),
            (
                r#"[{"a": [1, 2, 3]}, {"a": []}]"#,
                2,
                "an array of scalars is one cell",
            ),
            (
                r#"[{"a": {"x": 1}}, {"a": {"y": 2}}, {"a": null}, {}]"#,
                4 + 4 * 2,
                "a nested object's keys fill for every row, null or missing included",
            ),
            (
                r#"[{"a": [{"k0": 0}, {"k1": 0}, {"k2": 0}]}, {"a": [{"k3": 0}]}]"#,
                2 + 4 * 4,
                "an array of objects fills every key for every element across rows",
            ),
            (
                r#"[{"a": [[{"x": 1}], [{"y": 2}]]}]"#,
                1 + 2 * 2,
                "arrays nest like any other place",
            ),
        ];
        for (json, cells, why) in cases {
            assert_eq!(count(json.as_bytes())?, cells, "{why}: {json}");
            // Polars' own decode is the oracle for what the count models.
            let df = JsonReader::new(Cursor::new(json)).finish()?;
            let mut decoded = (df.height() * df.width()) as u64;
            for column in df.columns() {
                decoded += cells_below(column.as_materialized_series())?;
            }
            assert_eq!(decoded, cells, "polars fills as many cells: {json}");
        }
        assert!(count(b"[{\"a\": 1}").is_err(), "a truncated document fails");
        Ok(())
    }
}
