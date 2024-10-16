use polars::prelude::*;

use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::node::FileNode;
use crate::model::{CommitEntry, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::repositories;
use crate::view::{JsonDataFrame, JsonDataFrameView};

use crate::model::diff::tabular_diff_summary::{
    TabularDiffSummary, TabularDiffSummaryImpl, TabularDiffWrapper,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffView {
    pub tabular: TabularDiffViewImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffViewImpl {
    #[serde(flatten)]
    pub summary: TabularDiffSummary,

    pub base_schema: Option<Schema>,
    pub head_schema: Option<Schema>,
    pub added_rows: Option<JsonDataFrame>,
    pub added_rows_view: Option<JsonDataFrameView>,
    pub removed_rows: Option<JsonDataFrame>,
    pub removed_rows_view: Option<JsonDataFrameView>,
    pub added_cols: Option<JsonDataFrame>,
    pub added_cols_view: Option<JsonDataFrameView>,
    pub removed_cols: Option<JsonDataFrame>,
    pub removed_cols_view: Option<JsonDataFrameView>,
}

impl TabularDiffView {
    pub fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
        df_opts: DFOpts,
    ) -> TabularDiffView {
        let base_df = TabularDiffWrapper::maybe_get_df_from_commit_entry(repo, base_entry);
        let head_df = TabularDiffWrapper::maybe_get_df_from_commit_entry(repo, head_entry);

        TabularDiffView::from_data_frames(base_df, head_df, df_opts)
    }

    pub fn from_file_nodes(
        repo: &LocalRepository,
        base_entry: &Option<FileNode>,
        head_entry: &Option<FileNode>,
        df_opts: DFOpts,
    ) -> TabularDiffView {
        let base_df = TabularDiffWrapper::maybe_get_df_from_file_node(repo, base_entry);
        let head_df = TabularDiffWrapper::maybe_get_df_from_file_node(repo, head_entry);

        TabularDiffView::from_data_frames(base_df, head_df, df_opts)
    }

    pub fn from_data_frames(
        base_df: Option<DataFrame>,
        head_df: Option<DataFrame>,
        df_opts: DFOpts,
    ) -> TabularDiffView {
        let schema_has_changed = TabularDiffWrapper::schema_has_changed(&base_df, &head_df);

        let base_schema = TabularDiffView::maybe_get_schema(&base_df);
        let head_schema = TabularDiffView::maybe_get_schema(&head_df);

        if base_schema.is_some() && head_schema.is_some() {
            let base_schema = base_schema.clone().unwrap();
            let head_schema = head_schema.clone().unwrap();
            let base_df = base_df.unwrap();
            let head_df = head_df.unwrap();

            if schema_has_changed {
                // compute new columns
                let df_diff = repositories::diffs::compute_new_columns_from_dfs(
                    base_df.clone(),
                    head_df.clone(),
                    &base_schema,
                    &head_schema,
                )
                .unwrap();

                let added_cols = df_diff
                    .added_cols
                    .clone()
                    .map(|df| JsonDataFrame::from_df_opts(df, df_opts.clone()));
                let added_cols_view = df_diff
                    .added_cols
                    .map(|df| JsonDataFrameView::from_df_opts(df, head_schema.clone(), &df_opts));
                let removed_cols = df_diff
                    .removed_cols
                    .clone()
                    .map(|df| JsonDataFrame::from_df_opts(df, df_opts.clone()));
                let removed_cols_view = df_diff
                    .removed_cols
                    .map(|df| JsonDataFrameView::from_df_opts(df, head_schema.clone(), &df_opts));

                // If the schema is changed, in order to figure out the added rows and removed rows,
                // we need to make find the minimum common schema between the two dataframes
                // and then compute the diff between the two dataframes.
                let common_fields = base_schema.common_fields(&head_schema);
                let column_names = common_fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<String>>();
                log::debug!("COMMON FIELDS: {:?}", column_names);

                // Lol this is insanity, but pretty cool..., clean up later
                let (added_rows_view, removed_rows_view) = if column_names.is_empty() {
                    (None, None)
                } else {
                    let cols = column_names.iter().map(col).collect::<Vec<Expr>>();

                    let common_base_df = base_df.clone().lazy().select(&cols).collect().unwrap();
                    let common_head_df = head_df.clone().lazy().select(&cols).collect().unwrap();

                    log::debug!("common_base_df: {:?}", common_base_df);
                    log::debug!("common_head_df: {:?}", common_head_df);

                    let df_diff = repositories::diffs::compute_new_rows_proj(
                        &common_base_df,
                        &common_head_df,
                        &base_df,
                        &head_df,
                        &base_schema,
                        &head_schema,
                    )
                    .unwrap();

                    log::debug!("ADDED ROWS: {:?}", df_diff.added_rows);
                    log::debug!("REMOVED ROWS: {:?}", df_diff.removed_rows);

                    let added_rows_view = df_diff.added_rows.map(|df| {
                        JsonDataFrameView::from_df_opts(df, base_schema.clone(), &df_opts)
                    });
                    let removed_rows_view = df_diff.removed_rows.map(|df| {
                        JsonDataFrameView::from_df_opts(df, base_schema.clone(), &df_opts)
                    });
                    (added_rows_view, removed_rows_view)
                };

                let summary = TabularDiffSummary {
                    summary: TabularDiffSummaryImpl {
                        num_added_rows: added_rows_view
                            .as_ref()
                            .map(|df| df.size.height)
                            .unwrap_or(0),
                        num_added_cols: added_cols
                            .as_ref()
                            .map(|df| df.full_size.width)
                            .unwrap_or(0),
                        num_removed_rows: removed_rows_view
                            .as_ref()
                            .map(|df| df.size.height)
                            .unwrap_or(0),
                        num_removed_cols: removed_cols
                            .as_ref()
                            .map(|df| df.full_size.width)
                            .unwrap_or(0),
                        schema_has_changed,
                    },
                };

                log::debug!("summary: {:?}", summary);

                return TabularDiffView {
                    tabular: TabularDiffViewImpl {
                        summary,
                        base_schema: Some(base_schema),
                        head_schema: Some(head_schema),
                        added_rows: None,
                        added_rows_view,
                        removed_rows: None,
                        removed_rows_view,
                        added_cols,
                        added_cols_view,
                        removed_cols,
                        removed_cols_view,
                    },
                };
            } else {
                // schema has not changed
                // compute new rows
                let df_diff =
                    repositories::diffs::compute_new_rows(&base_df, &head_df, &base_schema)
                        .unwrap();

                let added_rows = df_diff
                    .added_rows
                    .clone()
                    .map(|df| JsonDataFrame::from_df_opts(df, df_opts.clone()));
                let added_rows_view = df_diff
                    .added_rows
                    .map(|df| JsonDataFrameView::from_df_opts(df, base_schema.clone(), &df_opts));
                let removed_rows = df_diff
                    .removed_rows
                    .clone()
                    .map(|df| JsonDataFrame::from_df_opts(df, df_opts.clone()));
                let removed_rows_view = df_diff
                    .removed_rows
                    .map(|df| JsonDataFrameView::from_df_opts(df, base_schema.clone(), &df_opts));

                let summary = TabularDiffSummary {
                    summary: TabularDiffSummaryImpl {
                        num_added_rows: added_rows
                            .as_ref()
                            .map(|df| df.full_size.height)
                            .unwrap_or(0),
                        num_added_cols: 0,
                        num_removed_rows: removed_rows
                            .as_ref()
                            .map(|df| df.full_size.height)
                            .unwrap_or(0),
                        num_removed_cols: 0,
                        schema_has_changed,
                    },
                };

                return TabularDiffView {
                    tabular: TabularDiffViewImpl {
                        summary,
                        base_schema: Some(base_schema),
                        head_schema: Some(head_schema),
                        added_rows,
                        added_rows_view,
                        removed_rows,
                        removed_rows_view,
                        added_cols: None,
                        added_cols_view: None,
                        removed_cols: None,
                        removed_cols_view: None,
                    },
                };
            }
        }

        if base_schema.is_none() && head_schema.is_some() {
            // we added the dataframe
            let head_schema = head_schema.clone().unwrap();
            let head_df = head_df.unwrap();
            let added_df = Some(JsonDataFrame::from_df_opts(
                head_df.clone(),
                df_opts.clone(),
            ));
            let added_df_view = Some(JsonDataFrameView::from_df_opts(
                head_df,
                head_schema.clone(),
                &df_opts,
            ));

            let summary = TabularDiffSummary {
                summary: TabularDiffSummaryImpl {
                    num_added_rows: added_df.as_ref().map(|df| df.full_size.height).unwrap_or(0),
                    num_added_cols: added_df.as_ref().map(|df| df.full_size.width).unwrap_or(0),
                    num_removed_rows: 0,
                    num_removed_cols: 0,
                    schema_has_changed,
                },
            };

            return TabularDiffView {
                tabular: TabularDiffViewImpl {
                    summary,
                    base_schema: None,
                    head_schema: Some(head_schema),
                    added_rows: added_df,
                    added_rows_view: added_df_view,
                    removed_rows: None,
                    removed_rows_view: None,
                    added_cols: None,
                    added_cols_view: None,
                    removed_cols: None,
                    removed_cols_view: None,
                },
            };
        }

        if base_schema.is_some() && head_schema.is_none() {
            // we removed the dataframe
            let base_schema = base_schema.clone().unwrap();
            let base_df = base_df.unwrap();
            let removed_df = Some(JsonDataFrame::from_df_opts(
                base_df.clone(),
                df_opts.clone(),
            ));
            let removed_df_view = Some(JsonDataFrameView::from_df_opts(
                base_df,
                base_schema.clone(),
                &df_opts,
            ));

            let summary = TabularDiffSummary {
                summary: TabularDiffSummaryImpl {
                    num_added_rows: 0,
                    num_added_cols: 0,
                    num_removed_rows: removed_df
                        .as_ref()
                        .map(|df| df.full_size.height)
                        .unwrap_or(0),
                    num_removed_cols: removed_df
                        .as_ref()
                        .map(|df| df.full_size.width)
                        .unwrap_or(0),
                    schema_has_changed,
                },
            };

            return TabularDiffView {
                tabular: TabularDiffViewImpl {
                    summary,
                    base_schema: Some(base_schema),
                    head_schema: None,
                    added_rows: None,
                    added_rows_view: None,
                    removed_rows: removed_df,
                    removed_rows_view: removed_df_view,
                    added_cols: None,
                    added_cols_view: None,
                    removed_cols: None,
                    removed_cols_view: None,
                },
            };
        }

        // schema has not changed
        let summary = TabularDiffSummary {
            summary: TabularDiffSummaryImpl {
                num_added_rows: 0,
                num_added_cols: 0,
                num_removed_rows: 0,
                num_removed_cols: 0,
                schema_has_changed,
            },
        };
        TabularDiffView {
            tabular: TabularDiffViewImpl {
                summary,
                base_schema,
                head_schema,
                added_rows: None,
                added_rows_view: None,
                removed_rows: None,
                removed_rows_view: None,
                added_cols: None,
                added_cols_view: None,
                removed_cols: None,
                removed_cols_view: None,
            },
        }
    }

    pub fn maybe_get_schema(df: &Option<DataFrame>) -> Option<Schema> {
        df.as_ref().map(|df| Schema::from_polars(&df.schema()))
    }
}
