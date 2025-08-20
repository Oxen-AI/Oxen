use std::path::PathBuf;

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::constants::{
    DEFAULT_HOST, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, FILE_ROW_NUM_COL_NAME, ROW_HASH_COL_NAME,
    ROW_NUM_COL_NAME,
};
use crate::core::df::filter::{self, DFFilterExp};
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::Schema;

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

#[derive(Clone, Debug)]
pub struct DFOpts {
    pub add_col: Option<String>,
    pub add_row: Option<String>,
    pub rename_col: Option<String>,
    pub at: Option<usize>,
    pub bearer_token: Option<String>,
    pub columns: Option<String>,
    pub delete_row: Option<String>,
    pub delimiter: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub find_embedding_where: Option<String>,
    pub filter: Option<String>,
    pub head: Option<usize>,
    pub host: Option<String>,
    pub output: Option<PathBuf>,
    pub output_column: Option<String>,
    pub page_size: Option<usize>,
    pub page: Option<usize>,
    pub path: Option<PathBuf>,
    pub row: Option<usize>,
    pub item: Option<String>,
    pub quote_char: Option<String>,
    pub repo_dir: Option<PathBuf>,
    pub should_randomize: bool,
    pub should_reverse: bool,
    pub should_page: bool,
    pub slice: Option<String>,
    pub sort_by: Option<String>,
    pub sort_by_similarity_to: Option<String>,
    pub sql: Option<String>,
    pub text2sql: Option<String>,
    pub tail: Option<usize>,
    pub take: Option<String>,
    pub unique: Option<String>,
    pub vstack: Option<Vec<PathBuf>>,
    pub write: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DFOptsView {
    pub opts: Vec<DFOptView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
            bearer_token: None,
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
            || self.vstack.is_some()
    }

    pub fn slice_indices(&self) -> Option<(i64, i64)> {
        if let Some(slice) = self.slice.clone() {
            let split = slice.split("..").collect::<Vec<&str>>();
            if split.len() == 2 {
                let start = split[0]
                    .parse::<i64>()
                    .expect("Start must be a valid integer.");
                let end = split[1]
                    .parse::<i64>()
                    .expect("End must be a valid integer.");
                return Some((start, end));
            } else {
                return None;
            }
        }
        if let Some(row) = self.row {
            let next_row = row + 1;
            return Some((row as i64, next_row as i64));
        }
        None
    }

    pub fn take_indices(&self) -> Option<Vec<u32>> {
        if let Some(take) = self.take.clone() {
            let split = take
                .split(',')
                .map(|v| v.parse::<u32>().expect("Values must be a valid u32."))
                .collect::<Vec<u32>>();
            return Some(split);
        }
        None
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

    pub fn column_at(&self) -> Option<IndexedItem> {
        if let Some(value) = self.item.clone() {
            // col:index
            // ie: file:2
            let delimiter = ":";
            if value.contains(delimiter) {
                let mut split = value.split(delimiter);
                return Some(IndexedItem {
                    col: String::from(split.next().unwrap()),
                    index: split
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .expect("Index must be usize"),
                });
            }
        }
        None
    }

    pub fn add_col_vals(&self) -> Option<AddColVals> {
        if let Some(add_col) = self.add_col.clone() {
            let split = add_col
                .split(':')
                .map(String::from)
                .collect::<Vec<String>>();
            if split.len() != 3 {
                panic!("Invalid input for col vals. Format: 'name:val:dtype'");
            }

            return Some(AddColVals {
                name: split[0].to_owned(),
                value: split[1].to_owned(),
                dtype: split[2].to_owned(),
            });
        }
        None
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
        let page = if self.page.is_some() {
            Some(format!("{}", self.page.unwrap()))
        } else {
            None
        };
        let page_size = if self.page_size.is_some() {
            Some(format!("{}", self.page_size.unwrap()))
        } else {
            None
        };

        let params = vec![
            ("item", self.item.clone()),
            ("columns", self.columns.clone()),
            ("page_size", page_size),
            ("page", page),
            ("randomize", randomize),
            ("reverse", should_reverse),
            ("filter", self.filter.clone()),
            ("slice", self.slice.clone()),
            ("sort_by", self.sort_by.clone()),
            ("sql", self.sql.clone()),
            ("take", self.take.clone()),
            ("unique", self.unique.clone()),
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
            Some(ref v) => serde_json::to_value(v).unwrap_or(Value::Null),
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
    pub fn empty() -> DFOptsView {
        DFOptsView { opts: vec![] }
    }
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
            DFOptView::from_opt("slice", &opts.slice),
            DFOptView::from_opt("head", &opts.head),
            DFOptView::from_opt("tail", &opts.tail),
        ]
        .to_vec();

        DFOptsView { opts: ordered_opts }
    }
}
