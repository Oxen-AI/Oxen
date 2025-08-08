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
    pub async fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
        df_opts: DFOpts,
    ) -> TabularDiffView {
        let base_df = TabularDiffWrapper::maybe_get_df_from_commit_entry(repo, base_entry).await;
        let head_df = TabularDiffWrapper::maybe_get_df_from_commit_entry(repo, head_entry).await;

        TabularDiffView::from_data_frames(base_df, head_df, df_opts).await
    }

    pub async fn from_file_nodes(
        repo: &LocalRepository,
        base_entry: &Option<FileNode>,
        head_entry: &Option<FileNode>,
        df_opts: DFOpts,
    ) -> TabularDiffView {
        let base_df = TabularDiffWrapper::maybe_get_df_from_file_node(repo, base_entry).await;
        let head_df = TabularDiffWrapper::maybe_get_df_from_file_node(repo, head_entry).await;

        TabularDiffView::from_data_frames(base_df, head_df, df_opts).await
    }

    pub async fn from_data_frames(
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
                .await
                .unwrap();

                let added_cols = match df_diff.added_cols {
                    Some(df) => Some(JsonDataFrame::from_df_opts(df, df_opts.clone()).await),
                    None => None,
                };

                let removed_cols = match df_diff.removed_cols {
                    Some(df) => Some(JsonDataFrame::from_df_opts(df, df_opts.clone()).await),
                    None => None,
                };

                let (added_cols_view, num_added_cols) = match &added_cols {
                    Some(df) => (
                        Some(
                            JsonDataFrameView::from_df_opts(
                                df.to_df().await,
                                head_schema.clone(),
                                &df_opts,
                            )
                            .await,
                        ),
                        df.full_size.width,
                    ),
                    None => (None, 0),
                };

                let (removed_cols_view, num_removed_cols) = match &removed_cols {
                    Some(df) => (
                        Some(
                            JsonDataFrameView::from_df_opts(
                                df.to_df().await,
                                head_schema.clone(),
                                &df_opts,
                            )
                            .await,
                        ),
                        df.full_size.width,
                    ),
                    None => (None, 0),
                };

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
                    .await
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

                let added_rows = match added_rows_view {
                    Some(df) => Some(df.await),
                    None => None,
                };
                let removed_rows = match removed_rows_view {
                    Some(df) => Some(df.await),
                    None => None,
                };

                let num_added_rows = match &added_rows {
                    Some(df) => df.size.height,
                    None => 0,
                };

                let num_removed_rows = match &removed_rows {
                    Some(df) => df.size.height,
                    None => 0,
                };

                let summary = TabularDiffSummary {
                    summary: TabularDiffSummaryImpl {
                        num_added_rows,
                        num_added_cols,
                        num_removed_rows,
                        num_removed_cols,
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
                        added_rows_view: added_rows,
                        removed_rows: None,
                        removed_rows_view: removed_rows,
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
                        .await
                        .unwrap();

                let added_rows = match df_diff.added_rows {
                    Some(df) => Some(JsonDataFrame::from_df_opts(df, df_opts.clone()).await),
                    None => None,
                };

                let added_rows_view = match &added_rows {
                    Some(df) => Some(
                        JsonDataFrameView::from_df_opts(
                            df.to_df().await,
                            base_schema.clone(),
                            &df_opts,
                        )
                        .await,
                    ),
                    None => None,
                };

                let removed_rows = match df_diff.removed_rows {
                    Some(df) => Some(JsonDataFrame::from_df_opts(df, df_opts.clone()).await),
                    None => None,
                };

                let removed_rows_view = match &removed_rows {
                    Some(df) => Some(
                        JsonDataFrameView::from_df_opts(
                            df.to_df().await,
                            base_schema.clone(),
                            &df_opts,
                        )
                        .await,
                    ),
                    None => None,
                };

                let added_rows = match added_rows {
                    Some(df) => df,
                    None => JsonDataFrame::empty(&base_schema),
                };
                let removed_rows = match removed_rows {
                    Some(df) => df,
                    None => JsonDataFrame::empty(&base_schema),
                };

                let num_added_rows = added_rows.full_size.height;
                let num_removed_rows = removed_rows.full_size.height;

                let summary = TabularDiffSummary {
                    summary: TabularDiffSummaryImpl {
                        num_added_rows,
                        num_added_cols: 0,
                        num_removed_rows,
                        num_removed_cols: 0,
                        schema_has_changed,
                    },
                };

                return TabularDiffView {
                    tabular: TabularDiffViewImpl {
                        summary,
                        base_schema: Some(base_schema),
                        head_schema: Some(head_schema),
                        added_rows: Some(added_rows),
                        added_rows_view,
                        removed_rows: Some(removed_rows),
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
            let added_df =
                Some(JsonDataFrame::from_df_opts(head_df.clone(), df_opts.clone()).await);
            let added_df_view =
                Some(JsonDataFrameView::from_df_opts(head_df, head_schema.clone(), &df_opts).await);

            let added_df = match added_df {
                Some(df) => df,
                None => JsonDataFrame::empty(&head_schema),
            };

            let num_added_cols = added_df.full_size.width;

            let summary = TabularDiffSummary {
                summary: TabularDiffSummaryImpl {
                    num_added_rows: added_df.full_size.height,
                    num_added_cols,
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
                    added_rows: Some(added_df),
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
            let removed_df =
                Some(JsonDataFrame::from_df_opts(base_df.clone(), df_opts.clone()).await);
            let removed_df_view =
                Some(JsonDataFrameView::from_df_opts(base_df, base_schema.clone(), &df_opts).await);

            let removed_df = match removed_df {
                Some(df) => df,
                None => JsonDataFrame::empty(&base_schema),
            };

            let summary = TabularDiffSummary {
                summary: TabularDiffSummaryImpl {
                    num_added_rows: 0,
                    num_added_cols: 0,
                    num_removed_rows: removed_df.full_size.height,
                    num_removed_cols: removed_df.full_size.width,
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
                    removed_rows: Some(removed_df),
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
        df.as_ref().map(|df| Schema::from_polars(df.schema()))
    }
}
