use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::constants::{
    DEFAULT_HOST, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, FILE_ROW_NUM_COL_NAME, ROW_HASH_COL_NAME,
    ROW_NUM_COL_NAME,
};
use crate::core::df::filter::{self, DFFilterExp};
use crate::error::OxenError;
use crate::model::Schema;
use crate::model::data_frame::schema::Field;
use utoipa::ToSchema;

use super::{EmbeddingQueryOpts, PaginateOpts};

#[derive(Debug)]
pub struct AddColVals {
    pub name: String,
    pub value: String,
    pub dtype: String,
}

#[derive(Clone, Debug)]
pub struct IndexedItem {
    pub col: String,
    pub index: usize,
}

/// A half-open row range, `start..end`. Constructing one enforces `0 <= start < end`, so a range
/// that selects nothing, or that counts backwards from the end of the frame, cannot reach the read
/// path. Parses from and displays as `"0..10"`, the form the `slice` query parameter travels in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SliceRange {
    pub start: i64,
    pub end: i64,
}

impl SliceRange {
    pub fn new(start: i64, end: i64) -> Result<Self, OxenError> {
        let invalid = |reason| {
            Err(OxenError::InvalidDataFrameParam {
                param: "slice",
                value: format!("{start}..{end}"),
                reason,
            })
        };
        if start < 0 {
            return invalid("start must not be negative");
        }
        if start >= end {
            return invalid("start must be less than end");
        }
        Ok(Self { start, end })
    }

    /// The rows on a 1-based page. A page or page size below one reads as the first page, and a
    /// page past what the arithmetic can express reads as the last page it can.
    pub fn for_page(page: usize, page_size: usize) -> Self {
        let page_size = i64::try_from(page_size).unwrap_or(i64::MAX).max(1);
        let page = i64::try_from(page).unwrap_or(i64::MAX).max(1);
        // Holding the start below `i64::MAX - page_size` keeps `start + page_size` from
        // overflowing, so `0 <= start < end` holds for every page.
        let start = page_size.saturating_mul(page - 1).min(i64::MAX - page_size);
        Self {
            start,
            end: start + page_size,
        }
    }

    /// The range covering a single row.
    pub fn for_row(row: usize) -> Result<Self, OxenError> {
        let too_large = || OxenError::InvalidDataFrameParam {
            param: "row",
            value: row.to_string(),
            reason: "is past the largest row a range can name",
        };
        let start = i64::try_from(row).map_err(|_| too_large())?;
        Self::new(start, start.checked_add(1).ok_or_else(too_large)?)
    }

    /// The number of rows the range covers.
    pub fn row_count(&self) -> u32 {
        // `0 <= start < end` holds by construction, so the difference is positive and cannot
        // overflow.
        (self.end - self.start) as u32
    }
}

impl FromStr for SliceRange {
    type Err = OxenError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let malformed = || OxenError::InvalidDataFrameParam {
            param: "slice",
            value: value.to_string(),
            reason: "expected two whole numbers separated by '..', as '0..10'",
        };
        let (start, end) = value.split_once("..").ok_or_else(malformed)?;
        Self::new(
            start.parse().map_err(|_| malformed())?,
            end.parse().map_err(|_| malformed())?,
        )
    }
}

impl fmt::Display for SliceRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

#[derive(Clone, Debug, ToSchema)]
pub struct DFOpts {
    pub add_col: Option<String>,
    pub add_row: Option<String>,
    pub rename_col: Option<String>,
    pub at: Option<usize>,
    pub columns: Option<String>,
    pub delete_row: Option<String>,
    pub delimiter: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub find_embedding_where: Option<String>,
    pub filter: Option<String>,
    pub head: Option<usize>,
    pub host: Option<String>,
    #[schema(value_type = Option<String>)]
    pub output: Option<PathBuf>,
    pub output_column: Option<String>,
    pub page_size: Option<usize>,
    pub page: Option<usize>,
    #[schema(value_type = Option<String>)]
    pub path: Option<PathBuf>,
    pub row: Option<usize>,
    pub item: Option<String>,
    pub quote_char: Option<String>,
    #[schema(value_type = Option<String>)]
    pub repo_dir: Option<PathBuf>,
    pub should_randomize: bool,
    pub should_reverse: bool,
    pub should_page: bool,
    #[schema(value_type = Option<String>)]
    pub slice: Option<SliceRange>,
    pub sort_by: Option<String>,
    pub sort_by_similarity_to: Option<String>,
    pub sql: Option<String>,
    pub text2sql: Option<String>,
    pub tail: Option<usize>,
    pub take: Option<String>,
    pub unique: Option<String>,
    pub unique_count: Option<String>,
    #[schema(value_type = Option<Vec<String>>)]
    pub vstack: Option<Vec<PathBuf>>,
    #[schema(value_type = Option<String>)]
    pub write: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DFOptsView {
    pub opts: Vec<DFOptView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DFOptView {
    pub name: String,
    pub value: serde_json::Value,
}

impl DFOpts {
    pub fn empty() -> DFOpts {
        DFOpts {
            add_col: None,
            add_row: None,
            rename_col: None,
            at: None,
            columns: None,
            delete_row: None,
            delimiter: None,
            embedding: None,
            find_embedding_where: None,
            filter: None,
            head: None,
            host: None,
            item: None,
            output: None,
            output_column: None,
            page: None,
            page_size: None,
            path: None,
            row: None,
            quote_char: None,
            repo_dir: None,
            should_page: false,
            should_randomize: false,
            should_reverse: false,
            slice: None,
            sort_by: None,
            sort_by_similarity_to: None,
            sql: None,
            tail: None,
            take: None,
            text2sql: None,
            unique: None,
            unique_count: None,
            vstack: None,
            write: None,
        }
    }

    pub fn from_unique(fields_str: &str) -> Self {
        let mut opts = DFOpts::empty();
        opts.unique = Some(String::from(fields_str));
        opts
    }

    pub fn from_schema_columns(schema: &Schema) -> Self {
        DFOpts::from_columns(schema.fields.clone())
    }

    pub fn from_schema_columns_exclude_hidden(schema: &Schema) -> Self {
        let fields: Vec<Field> = schema
            .fields
            .clone()
            .into_iter()
            .filter(|f| {
                f.name != ROW_HASH_COL_NAME
                    && f.name != ROW_NUM_COL_NAME
                    && f.name != FILE_ROW_NUM_COL_NAME
            })
            .collect();
        DFOpts::from_columns(fields)
    }

    pub fn from_columns(fields: Vec<Field>) -> Self {
        let str_fields: Vec<String> = fields.iter().map(|f| f.name.to_owned()).collect();
        DFOpts::from_column_names(str_fields)
    }

    pub fn from_column_names(names: Vec<String>) -> Self {
        let mut opts = DFOpts::empty();
        opts.columns = Some(names.join(","));
        opts
    }

    pub fn has_filter_transform(&self) -> bool {
        self.sql.is_some()
            || self.text2sql.is_some()
            || self.unique.is_some()
            || self.unique_count.is_some()
            || self.filter.is_some()
    }

    pub fn has_transform(&self) -> bool {
        self.add_col.is_some()
            || self.add_row.is_some()
            || self.rename_col.is_some()
            || self.item.is_some()
            || self.columns.is_some()
            || self.filter.is_some()
            || self.head.is_some()
            || self.page_size.is_some()
            || self.page.is_some()
            || self.row.is_some()
            || self.should_randomize
            || self.should_reverse
            || self.sort_by.is_some()
            || self.sort_by_similarity_to.is_some()
            || self.slice.is_some()
            || self.sql.is_some()
            || self.tail.is_some()
            || self.take.is_some()
            || self.text2sql.is_some()
            || self.unique.is_some()
            || self.unique_count.is_some()
            || self.vstack.is_some()
    }

    /// The 1-based page and page size the options request, clamped to the domain the reader can
    /// serve. A page or page size below one reads as the first page.
    pub fn page_bounds(&self) -> (usize, usize) {
        (
            self.page.unwrap_or(DEFAULT_PAGE_NUM).max(1),
            self.page_size.unwrap_or(DEFAULT_PAGE_SIZE).max(1),
        )
    }

    /// The rows the options select, from either an explicit `slice` or a single `row`. `None`
    /// means every row.
    pub fn slice_indices(&self) -> Result<Option<SliceRange>, OxenError> {
        if let Some(range) = self.slice {
            return Ok(Some(range));
        }
        if let Some(row) = self.row {
            return Ok(Some(SliceRange::for_row(row)?));
        }
        Ok(None)
    }

    pub fn take_indices(&self) -> Result<Option<Vec<u32>>, OxenError> {
        if let Some(take) = self.take.clone() {
            let split = take
                .split(',')
                .map(|v| {
                    v.parse::<u32>()
                        .map_err(|_| OxenError::InvalidDataFrameParam {
                            param: "take",
                            value: take.clone(),
                            reason: "expected whole numbers separated by ','",
                        })
                })
                .collect::<Result<Vec<u32>, OxenError>>()?;
            return Ok(Some(split));
        }
        Ok(None)
    }

    pub fn columns_names(&self) -> Option<Vec<String>> {
        if let Some(columns) = self.columns.clone() {
            let split = columns
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }

    pub fn unique_columns(&self) -> Option<Vec<String>> {
        if let Some(columns) = self.unique.clone() {
            let split = columns
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }

    pub fn unique_count_columns(&self) -> Option<Vec<String>> {
        if let Some(columns) = self.unique_count.clone() {
            let split = columns
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }

    pub fn get_filter(&self) -> Result<Option<DFFilterExp>, OxenError> {
        filter::parse(self.filter.clone())
    }

    pub fn get_sort_by_embedding_query(&self) -> Option<EmbeddingQueryOpts> {
        if let (Some(query), Some(column), Some(path)) = (
            self.find_embedding_where.clone(),
            self.sort_by_similarity_to.clone(),
            self.path.clone(),
        ) {
            Some(EmbeddingQueryOpts {
                path,
                column,
                query,
                name: "similarity".to_string(),
                pagination: PaginateOpts {
                    page_num: self.page.unwrap_or(DEFAULT_PAGE_NUM),
                    page_size: self.page_size.unwrap_or(DEFAULT_PAGE_SIZE),
                },
            })
        } else {
            None
        }
    }

    pub fn get_host(&self) -> String {
        match &self.host {
            Some(host) => host.to_owned(),
            None => String::from(DEFAULT_HOST),
        }
    }

    pub fn column_at(&self) -> Result<Option<IndexedItem>, OxenError> {
        if let Some(value) = self.item.clone() {
            let malformed = || OxenError::InvalidDataFrameParam {
                param: "item",
                value: value.clone(),
                reason: "expected a column name and a whole number, as 'col:index'",
            };
            let (col, index) = value.split_once(':').ok_or_else(malformed)?;
            if col.is_empty() {
                return Err(malformed());
            }
            return Ok(Some(IndexedItem {
                col: String::from(col),
                index: index.parse::<usize>().map_err(|_| malformed())?,
            }));
        }
        Ok(None)
    }

    pub fn add_col_vals(&self) -> Result<Option<AddColVals>, OxenError> {
        if let Some(add_col) = self.add_col.clone() {
            let split = add_col
                .split(':')
                .map(String::from)
                .collect::<Vec<String>>();
            if split.len() != 3 {
                return Err(OxenError::InvalidDataFrameParam {
                    param: "add-col",
                    value: add_col,
                    reason: "expected three parts, as 'name:value:dtype'",
                });
            }

            return Ok(Some(AddColVals {
                name: split[0].to_owned(),
                value: split[1].to_owned(),
                dtype: split[2].to_owned(),
            }));
        }
        Ok(None)
    }

    pub fn to_http_query_params(&self) -> String {
        let randomize = if self.should_randomize {
            Some(String::from("true"))
        } else {
            Some(String::from("false"))
        };
        let should_reverse = if self.should_reverse {
            Some(String::from("true"))
        } else {
            Some(String::from("false"))
        };
        let page = self.page.map(|p| format!("{}", p));
        let page_size = self.page_size.map(|ps| format!("{}", ps));

        let params = vec![
            ("item", self.item.clone()),
            ("columns", self.columns.clone()),
            ("page_size", page_size),
            ("page", page),
            ("randomize", randomize),
            ("reverse", should_reverse),
            ("filter", self.filter.clone()),
            ("slice", self.slice.map(|s| s.to_string())),
            ("sort_by", self.sort_by.clone()),
            ("sql", self.sql.clone()),
            ("take", self.take.clone()),
            ("unique", self.unique.clone()),
            ("unique_count", self.unique_count.clone()),
            (
                "output",
                self.output
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
            ),
            ("sort_by_similarity_to", self.sort_by_similarity_to.clone()),
            ("find_embedding_where", self.find_embedding_where.clone()),
        ];

        let mut query = String::new();
        for (i, (name, val)) in params.iter().enumerate() {
            if let Some(val) = val {
                query.push_str(&format!("{}={}", name, urlencoding::encode(val)));
                if i != params.len() - 1 {
                    query.push('&');
                }
            }
        }
        query
    }
}

impl DFOptView {
    pub fn from_opt<T: serde::Serialize>(name: &str, opt: &Option<T>) -> Self {
        let value = match opt {
            Some(v) => serde_json::to_value(v).unwrap_or(Value::Null),
            None => Value::Null,
        };

        DFOptView {
            name: name.to_string(),
            value,
        }
    }
}
// Eventually want to make this configurable and accept user input - deterministic for now
impl DFOptsView {
    pub fn from_df_opts(opts: &DFOpts) -> DFOptsView {
        let ordered_opts: Vec<DFOptView> = [
            DFOptView::from_opt("text2sql", &opts.text2sql),
            DFOptView::from_opt("sql", &opts.sql),
            DFOptView::from_opt("filter", &opts.filter),
            DFOptView::from_opt("unique", &opts.unique),
            DFOptView::from_opt(
                "should_randomize",
                &Some(serde_json::to_value(opts.should_randomize).unwrap()),
            ),
            DFOptView::from_opt("sort_by", &opts.sort_by),
            DFOptView::from_opt(
                "should_reverse",
                &Some(serde_json::to_value(opts.should_reverse).unwrap()),
            ),
            DFOptView::from_opt("take", &opts.take),
            DFOptView::from_opt("slice", &opts.slice.map(|s| s.to_string())),
            DFOptView::from_opt("head", &opts.head),
            DFOptView::from_opt("tail", &opts.tail),
        ]
        .to_vec();

        DFOptsView { opts: ordered_opts }
    }
}

#[cfg(test)]
mod tests {
    use super::{DFOpts, SliceRange};
    use crate::error::OxenError;

    #[test]
    fn test_slice_range_rejects_a_range_that_selects_nothing() {
        // A range whose start is not below its end reaches polars as a zero or negative length,
        // which panics the worker thread rather than answering the caller.
        assert!(SliceRange::new(0, 0).is_err());
        assert!(SliceRange::new(10, 3).is_err());
        assert!(SliceRange::new(3, 10).is_ok());
    }

    #[test]
    fn test_slice_range_rejects_a_negative_start() {
        // Polars reads a negative offset as counting back from the end of the frame, which is a
        // different operation than the one `start..end` names.
        assert!(SliceRange::new(-5, 3).is_err());
        assert!(SliceRange::new(i64::MIN, i64::MAX).is_err());
    }

    #[test]
    fn test_slice_range_round_trips_through_its_wire_form() -> Result<(), OxenError> {
        let range: SliceRange = "330..333".parse()?;
        assert_eq!(range, SliceRange::new(330, 333)?);
        assert_eq!(range.to_string(), "330..333");
        assert_eq!(range.row_count(), 3);
        Ok(())
    }

    #[test]
    fn test_a_slice_that_is_not_a_range_is_an_error() {
        // Each of these used to be silently ignored, which answered a request for a few rows with
        // the whole data frame.
        for value in ["5", "1..2..3", "a..b", "", "..", "0..", "..10"] {
            assert!(
                value.parse::<SliceRange>().is_err(),
                "expected {value:?} to be rejected"
            );
        }
    }

    #[test]
    fn test_a_page_covers_the_rows_it_names() -> Result<(), OxenError> {
        assert_eq!(SliceRange::for_page(1, 10), SliceRange::new(0, 10)?);
        assert_eq!(SliceRange::for_page(3, 10), SliceRange::new(20, 30)?);
        Ok(())
    }

    #[test]
    fn test_a_degenerate_page_reads_as_the_first_page() -> Result<(), OxenError> {
        // A page or page size of zero would otherwise underflow the `page - 1` subtraction and
        // derive a range that selects nothing.
        assert_eq!(SliceRange::for_page(0, 10), SliceRange::new(0, 10)?);
        assert_eq!(SliceRange::for_page(1, 0), SliceRange::new(0, 1)?);
        assert_eq!(SliceRange::for_page(0, 0), SliceRange::new(0, 1)?);
        Ok(())
    }

    #[test]
    fn test_an_absurd_page_still_names_a_usable_range() {
        // The multiplication cannot overflow into a range the reader rejects, however large the
        // caller's page number is.
        for (page, page_size) in [
            (usize::MAX, usize::MAX),
            (usize::MAX, 100),
            (100, usize::MAX),
            (1 << 40, 1 << 40),
        ] {
            let range = SliceRange::for_page(page, page_size);
            assert!(
                range.start >= 0 && range.start < range.end,
                "for_page({page}, {page_size}) gave {range}"
            );
        }
    }

    #[test]
    fn test_page_bounds_clamps_to_the_servable_domain() {
        let mut opts = DFOpts::empty();
        opts.page = Some(0);
        opts.page_size = Some(0);
        assert_eq!(opts.page_bounds(), (1, 1));

        opts.page = Some(4);
        opts.page_size = Some(25);
        assert_eq!(opts.page_bounds(), (4, 25));
    }

    #[test]
    fn test_a_row_selects_only_that_row() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.row = Some(7);
        assert_eq!(opts.slice_indices()?, Some(SliceRange::new(7, 8)?));

        // A row index too large to name a range used to read as no selection at all, which
        // answered a request for one row with every row.
        opts.row = Some(usize::MAX);
        assert!(opts.slice_indices().is_err());
        Ok(())
    }

    #[test]
    fn test_a_row_past_what_a_range_can_name_is_an_error() -> Result<(), OxenError> {
        assert_eq!(SliceRange::for_row(7)?, SliceRange::new(7, 8)?);

        // Both of these used to overflow while computing the exclusive end.
        assert!(SliceRange::for_row(usize::MAX).is_err());
        assert!(SliceRange::for_row(i64::MAX as usize).is_err());
        Ok(())
    }

    #[test]
    fn test_a_slice_wins_over_a_row() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.row = Some(7);
        opts.slice = Some(SliceRange::new(0, 3)?);
        assert_eq!(opts.slice_indices()?, Some(SliceRange::new(0, 3)?));
        Ok(())
    }

    #[test]
    fn test_an_item_that_is_not_a_column_and_index_is_an_error() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.item = Some("file:2".to_string());
        let item = opts.column_at()?.expect("item should parse");
        assert_eq!(item.col, "file");
        assert_eq!(item.index, 2);

        // A value with no delimiter used to be ignored, returning the whole frame, and one with
        // extra segments used to drop everything past the second.
        for value in ["file", "", "file:2:3", "file:", ":2", "file:abc"] {
            opts.item = Some(value.to_string());
            assert!(
                opts.column_at().is_err(),
                "expected {value:?} to be rejected"
            );
        }
        Ok(())
    }

    #[test]
    fn test_a_take_that_is_not_a_list_of_indices_is_an_error() {
        let mut opts = DFOpts::empty();
        opts.take = Some("1,2,3".to_string());
        assert_eq!(opts.take_indices().unwrap(), Some(vec![1, 2, 3]));

        for value in ["abc", "1,,2", "1,-2", ""] {
            opts.take = Some(value.to_string());
            assert!(
                opts.take_indices().is_err(),
                "expected {value:?} to be rejected"
            );
        }
    }
}
