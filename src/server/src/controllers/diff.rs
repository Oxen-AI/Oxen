use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::data_frame::DataFrameSchemaSize;
use liboxen::model::diff::diff_entry_status::DiffEntryStatus;
use liboxen::model::diff::dir_diff_summary::{DirDiffSummary, DirDiffSummaryImpl};
use liboxen::model::diff::generic_diff_summary::GenericDiffSummary;
use liboxen::model::diff::DiffResult;
use liboxen::model::{Commit, CommitEntry, DataFrameSize, LocalRepository, Schema};
use liboxen::opts::df_opts::DFOptsView;
use liboxen::opts::DFOpts;
use liboxen::view::compare::{
    CompareCommits, CompareCommitsResponse, CompareDupes, CompareEntries, CompareEntryResponse,
    CompareTabular, CompareTabularResponse,
};
use liboxen::view::compare::{TabularCompareBody, TabularCompareTargetBody};
use liboxen::view::diff::{DirDiffStatus, DirDiffTreeSummary, DirTreeDiffResponse};
use liboxen::view::json_data_frame_view::{DFResourceType, DerivedDFResource};
use liboxen::view::message::OxenMessage;
use liboxen::view::{
    CompareEntriesResponse, JsonDataFrame, JsonDataFrameView, JsonDataFrameViewResponse,
    JsonDataFrameViews, Pagination, StatusMessage,
};
use liboxen::{constants, repositories, util};

use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_base_head, path_param, resolve_base_head, DFOptsQuery,
    PageNumQuery,
};

pub async fn commits(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    let commits = repositories::commits::list_between(&repository, &base_commit, &head_commit)?;
    let (paginated, pagination) = util::paginate(commits, page, page_size);

    let compare = CompareCommits {
        base_commit,
        head_commit,
        commits: paginated,
    };

    let view = CompareCommitsResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}

// TODO: Depreciate
pub async fn entries(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    let entries_diff = repositories::diffs::list_diff_entries(
        &repository,
        &base_commit,
        &head_commit,
        PathBuf::from(""),
        page,
        page_size,
    )?;

    let entries = entries_diff.entries;
    let pagination = entries_diff.pagination;

    let compare = CompareEntries {
        base_commit,
        head_commit,
        counts: entries_diff.counts,
        entries,
        self_diff: None,
    };
    let view = CompareEntriesResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn dir_tree(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    let dir_diffs =
        repositories::diffs::list_changed_dirs(&repository, &base_commit, &head_commit)?;

    let dir_diff_tree = group_dir_diffs_by_dir(dir_diffs);

    let response = DirTreeDiffResponse {
        dirs: dir_diff_tree,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn dir_entries(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;
    let dir = path_param(&req, "dir")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;
    let dir = PathBuf::from(dir);

    let entries_diff = repositories::diffs::list_diff_entries(
        // let entries_diff = repositories::diffs::list_diff_entries_in_dir_top_level(
        &repository,
        &base_commit,
        &head_commit,
        dir.clone(),
        page,
        page_size,
    )?;

    // For this view, exclude anything that isn't a direct child of the directory in question
    let summary = GenericDiffSummary::DirDiffSummary(DirDiffSummary {
        dir: DirDiffSummaryImpl {
            file_counts: entries_diff.counts.clone(),
        },
    });

    // let filtered_diff = subset_diff_to_direct_children(entries_diff, dir.clone())?;

    let self_entry = repositories::diffs::get_dir_diff_entry_with_summary(
        &repository,
        dir,
        &base_commit,
        &head_commit,
        summary,
    )?;

    let compare = CompareEntries {
        base_commit,
        head_commit,
        counts: entries_diff.counts,
        entries: entries_diff.entries,
        self_diff: self_entry,
    };
    let view = CompareEntriesResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination: entries_diff.pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn file(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head/resource string
    // For Example)
    //   main..feature/add-data/path/to/file.txt
    let (base_commit, head_commit, resource) = parse_base_head_resource(&repository, &base_head)?;

    let base_entry = repositories::entries::get_file(&repository, &base_commit, &resource)?;
    let head_entry = repositories::entries::get_file(&repository, &head_commit, &resource)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;
    opts.slice = Some(format!("{}..{}", start, end));

    let diff = repositories::diffs::diff_entries(
        &repository,
        resource,
        base_entry,
        &base_commit,
        head_entry,
        &head_commit,
        opts,
    )?;

    let view = CompareEntryResponse {
        status: StatusMessage::resource_found(),
        compare: diff,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn create_df_diff(
    req: HttpRequest,
    _query: web::Query<DFOptsQuery>,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<TabularCompareBody, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!(
                "unable to parse tabular comparison data. Err: {}\n{}",
                err,
                body
            );
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let resource_1 = PathBuf::from(data.left.path);
    let resource_2 = PathBuf::from(data.right.path);
    let keys = data.keys;
    let targets = data.compare;
    let display = data.display;

    log::debug!("display is {:?}", display);

    let display_by_column = get_display_by_columns(display);

    log::debug!("display by col is {:?}", display_by_column);

    let compare_id = data.compare_id;

    let commit_1 = repositories::revisions::get(&repository, &data.left.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.left.version.into()))?;
    let commit_2 = repositories::revisions::get(&repository, &data.right.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.right.version.into()))?;

    let node_1 =
        repositories::entries::get_file(&repository, &commit_1, &resource_1)?.ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_1.display(), commit_1).into())
        })?;
    let node_2 =
        repositories::entries::get_file(&repository, &commit_1, &resource_2)?.ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_2.display(), commit_2).into())
        })?;

    // TODO: Remove the next two lines when we want to allow mapping
    // different keys and targets from left and right file.
    let keys = keys.iter().map(|k| k.left.clone()).collect();
    let targets = get_targets_from_req(targets);

    let diff_result = repositories::diffs::diff_tabular_file_nodes(
        &repository,
        &node_1,
        &node_2,
        keys,
        targets,
        display_by_column, // TODONOW: add display handling here
    )?;

    let view = match diff_result {
        DiffResult::Tabular(diff) => {
            // Cache the diff on the server
            let entry_1 = CommitEntry::from_file_node(&node_1);
            let entry_2 = CommitEntry::from_file_node(&node_2);
            repositories::diffs::cache_tabular_diff(
                &repository,
                &compare_id,
                entry_1,
                entry_2,
                &diff,
            )?;

            let mut messages: Vec<OxenMessage> = vec![];

            if diff.summary.dupes.left > 0 || diff.summary.dupes.right > 0 {
                let cdupes = CompareDupes::from_tabular_diff_dupes(&diff.summary.dupes);
                messages.push(cdupes.to_message());
            }

            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: CompareTabular::from(diff),
                messages,
            }
        }
        _ => Err(OxenError::basic_str("Create diff wrong comparison type"))?,
    };

    Ok(HttpResponse::Ok().json(view))
}

pub async fn update_df_diff(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let compare_id = path_param(&req, "compare_id")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<TabularCompareBody, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!(
                "unable to parse tabular comparison data. Err: {}\n{}",
                err,
                body
            );
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let resource_1 = PathBuf::from(data.left.path);
    let resource_2 = PathBuf::from(data.right.path);
    let keys = data.keys;
    let targets = data.compare;
    let display = data.display;

    log::debug!("display is {:?}", display);

    let display_by_column = get_display_by_columns(display);

    log::debug!("display by col is {:?}", display_by_column);

    let commit_1 = repositories::revisions::get(&repository, &data.left.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.left.version.into()))?;
    let commit_2 = repositories::revisions::get(&repository, &data.right.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.right.version.into()))?;

    let node_1 =
        repositories::entries::get_file(&repository, &commit_1, &resource_1)?.ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_1.display(), commit_1).into())
        })?;
    let node_2 =
        repositories::entries::get_file(&repository, &commit_1, &resource_2)?.ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_2.display(), commit_2).into())
        })?;

    // TODO: Remove the next two lines when we want to allow mapping
    // different keys and targets from left and right file.
    let keys = keys.iter().map(|k| k.left.clone()).collect();
    let targets = get_targets_from_req(targets);

    let diff_result = repositories::diffs::diff_tabular_file_nodes(
        &repository,
        &node_1,
        &node_2,
        keys,
        targets,
        display_by_column, // TODONOW: add display handling here
    )?;

    let view = match diff_result {
        DiffResult::Tabular(diff) => {
            let entry_1 = CommitEntry::from_file_node(&node_1);
            let entry_2 = CommitEntry::from_file_node(&node_2);
            // Cache the diff on the server
            repositories::diffs::cache_tabular_diff(
                &repository,
                &compare_id,
                entry_1,
                entry_2,
                &diff,
            )?;

            let mut messages: Vec<OxenMessage> = vec![];

            if diff.summary.dupes.left > 0 || diff.summary.dupes.right > 0 {
                let cdupes = CompareDupes::from_tabular_diff_dupes(&diff.summary.dupes);
                messages.push(cdupes.to_message());
            }

            // Get rid of the mutable borrow after done writing stuff

            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: CompareTabular::from(diff),
                messages,
            }
        }
        _ => Err(OxenError::basic_str("Update df wrong comparison type"))?,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn get_df_diff(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let compare_id = path_param(&req, "compare_id")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let base_head = path_param(&req, "base_head")?;

    let data: TabularCompareBody = serde_json::from_str(&body)?;

    let (left, right) = parse_base_head(&base_head)?;
    let (left_commit, right_commit) = resolve_base_head(&repository, &left, &right)?;

    let left_commit = left_commit.ok_or(OxenError::revision_not_found(left.into()))?;
    let right_commit = right_commit.ok_or(OxenError::revision_not_found(right.into()))?;

    let left_entry = repositories::entries::get_commit_entry(
        &repository,
        &left_commit,
        &PathBuf::from(data.left.path.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.left.path, left_commit).into())
    })?;
    let right_entry = repositories::entries::get_commit_entry(
        &repository,
        &right_commit,
        &PathBuf::from(data.right.path.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.right.path, right_commit).into())
    })?;

    let maybe_cached_diff = repositories::diffs::get_cached_diff(
        &repository,
        &compare_id,
        Some(left_entry.clone()),
        Some(right_entry.clone()),
    )?;

    if let Some(diff) = maybe_cached_diff {
        let mut messages: Vec<OxenMessage> = vec![];

        match diff {
            DiffResult::Tabular(diff) => {
                if diff.summary.dupes.left > 0 || diff.summary.dupes.right > 0 {
                    let cdupes = CompareDupes::from_tabular_diff_dupes(&diff.summary.dupes);
                    messages.push(cdupes.to_message());
                }

                let view = CompareTabularResponse {
                    status: StatusMessage::resource_found(),
                    dfs: CompareTabular::from(diff),
                    messages,
                };
                Ok(HttpResponse::Ok().json(view))
            }
            _ => Err(OxenHttpError::NotFound),
        }
    } else {
        Err(OxenHttpError::NotFound)
    }
}

pub async fn delete_df_diff(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let compare_id = path_param(&req, "compare_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    repositories::diffs::delete_df_diff(&repo, &compare_id)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}

pub async fn get_derived_df(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let compare_id = path_param(&req, "compare_id")?;
    // let base_head = path_param(&req, "base_head")?;

    let compare_dir = repositories::diffs::get_diff_dir(&repo, &compare_id);

    let derived_df_path = compare_dir.join("diff.parquet");

    // TODO: If this structure holds for diff + query, there is some amt of reusability with
    // controllers::df::get logic

    let df = tabular::read_df(derived_df_path, DFOpts::empty())?;
    let og_schema = Schema::from_polars(&df.schema());

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    log::debug!("get_derived_df got opts: {:?}", opts);

    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;
    let opts_view = DFOptsView::from_df_opts(&opts);

    // We have to run the query param transforms, then paginate separately
    match tabular::transform(df, opts) {
        Ok(view_df) => {
            log::debug!("View df {:?}", view_df);

            let view_width = view_df.width();
            let view_height = view_df.height();

            // Paginate after transform
            let mut paginate_opts = DFOpts::empty();
            paginate_opts.slice = Some(format!("{}..{}", start, end));
            let mut paginated_df = tabular::transform(view_df, paginate_opts)?;

            let total_pages = (view_height as f64 / page_size as f64).ceil() as usize;
            let source_size = DataFrameSize {
                width: full_width,
                height: full_height,
            };

            // Merge the metadata from the original schema
            let mut view_schema = Schema::from_polars(&paginated_df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", view_schema);
            view_schema.update_metadata_from_schema(&og_schema);

            log::debug!("View schema {:?}", view_schema);

            let df = JsonDataFrame::from_slice(
                &mut paginated_df,
                og_schema.clone(),
                source_size.clone(),
                view_schema.clone(),
            );

            let source_df = DataFrameSchemaSize {
                schema: og_schema,
                size: source_size,
            };

            let view_df = JsonDataFrameView {
                data: df.data,
                schema: view_schema,
                size: DataFrameSize {
                    width: view_width,
                    height: view_height,
                },
                pagination: {
                    Pagination {
                        page_number: page,
                        page_size,
                        total_pages,
                        total_entries: view_height,
                    }
                },
                opts: opts_view,
            };

            let derived_resource = DerivedDFResource {
                resource_type: DFResourceType::Compare,
                resource_id: compare_id.clone(),
                path: format!("/compare/data_frames/{}/diff", compare_id),
            };

            let response = JsonDataFrameViewResponse {
                status: StatusMessage::resource_found(),
                data_frame: JsonDataFrameViews {
                    source: source_df,
                    view: view_df,
                },
                commit: None,
                resource: None,
                derived_resource: Some(derived_resource),
            };

            Ok(HttpResponse::Ok().json(response))
        }
        Err(OxenError::SQLParseError(sql)) => {
            log::error!("Error parsing SQL: {}", sql);
            Err(OxenHttpError::SQLParseError(sql))
        }
        Err(e) => {
            log::error!("Error transforming df: {}", e);
            Err(OxenHttpError::InternalServerError)
        }
    }
}

fn parse_base_head_resource(
    repo: &LocalRepository,
    base_head: &str,
) -> Result<(Commit, Commit, PathBuf), OxenError> {
    log::debug!("Parsing base_head_resource: {}", base_head);

    let mut split = base_head.split("..");
    let base = split
        .next()
        .ok_or(OxenError::resource_not_found(base_head))?;
    let head = split
        .next()
        .ok_or(OxenError::resource_not_found(base_head))?;

    let base_commit = repositories::revisions::get(repo, base)?
        .ok_or(OxenError::revision_not_found(base.into()))?;

    // Split on / and find longest branch name
    let split_head = head.split('/');
    let mut longest_str = String::from("");
    let mut head_commit: Option<Commit> = None;
    let mut resource: Option<PathBuf> = None;

    for s in split_head {
        let maybe_revision = format!("{}{}", longest_str, s);
        log::debug!("Checking maybe head revision: {}", maybe_revision);
        let commit = repositories::revisions::get(repo, &maybe_revision)?;
        if commit.is_some() {
            head_commit = commit;
            let mut r_str = head.replace(&maybe_revision, "");
            // remove first char from r_str
            r_str.remove(0);
            resource = Some(PathBuf::from(r_str));
        }
        longest_str = format!("{}/", maybe_revision);
    }

    log::debug!("Got head_commit: {:?}", head_commit);
    log::debug!("Got resource: {:?}", resource);

    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;
    let resource = resource.ok_or(OxenError::revision_not_found(head.into()))?;

    Ok((base_commit, head_commit, resource))
}

fn get_display_by_columns(display: Vec<TabularCompareTargetBody>) -> Vec<String> {
    let mut display_by_column = vec![];
    for d in display {
        if let Some(left) = d.left {
            display_by_column.push(format!("{}.left", left));
        }
        if let Some(right) = d.right {
            display_by_column.push(format!("{}.right", right));
        }
    }
    display_by_column
}

fn get_targets_from_req(targets: Vec<TabularCompareTargetBody>) -> Vec<String> {
    let mut out_targets: Vec<String> = vec![];
    for t in targets {
        if let Some(left) = t.left {
            out_targets.push(left);
        } else if let Some(right) = t.right {
            out_targets.push(right);
        }
    }
    out_targets
}

fn group_dir_diffs_by_dir(dir_diffs: Vec<(PathBuf, DiffEntryStatus)>) -> Vec<DirDiffTreeSummary> {
    // For attaching status to parent in response
    let mut dir_status_map: HashMap<PathBuf, DiffEntryStatus> = HashMap::new();
    for (dir, status) in dir_diffs.iter() {
        dir_status_map.insert(dir.clone(), status.clone());
    }
    // Group by parent
    let mut dir_parent_map: HashMap<PathBuf, Vec<DirDiffStatus>> = HashMap::new();
    for (dir, status) in dir_diffs {
        // Root should not be mapped to itself as a parent, but should exist in the result
        // to handle top-level changes
        if dir == Path::new("") {
            if !dir_parent_map.contains_key(&dir) {
                dir_parent_map.insert(dir.clone(), vec![]);
            }
            continue;
        }
        let parent = dir.parent().unwrap_or(Path::new(""));
        let parent = parent.to_path_buf();
        if !dir_parent_map.contains_key(&parent) {
            dir_parent_map.insert(parent.clone(), vec![]);
        }
        dir_parent_map
            .get_mut(&parent)
            .unwrap()
            .push(DirDiffStatus {
                name: dir.clone(),
                status,
            });
    }

    // If we're over the entity display limit (defaults to 10), only send back 10
    let mut dir_tree: Vec<DirDiffTreeSummary> = vec![];
    for (dir, entries) in dir_parent_map {
        let num_subdirs = entries.len();
        let can_display = num_subdirs > constants::MAX_DISPLAY_DIRS;
        let status = dir_status_map
            .get(&dir)
            .unwrap_or(&DiffEntryStatus::Modified)
            .clone();
        let cropped_entries = if can_display {
            entries[..constants::MAX_DISPLAY_DIRS].to_vec()
        } else {
            entries
        };
        let summary = DirDiffTreeSummary {
            name: dir.clone(),
            status,
            num_subdirs,
            can_display,
            children: cropped_entries,
        };
        dir_tree.push(summary);
    }

    dir_tree
}

#[cfg(test)]
mod tests {
    use liboxen::{error::OxenError, repositories};

    use crate::test;

    #[actix_web::test]
    async fn test_controllers_compare_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namepsace = "testing-namespace";
        let repo_name = "testing-repo";

        let repo = test::create_local_repo(&sync_dir, namepsace, repo_name)?;

        let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2";
        let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

        let path1 = repo.path.join("file1.csv");
        let path2 = repo.path.join("file2.csv");

        liboxen::test::write_txt_file_to_path(path1, csv1)?;
        liboxen::test::write_txt_file_to_path(path2, csv2)?;

        repositories::add(&repo, &repo.path)?;

        repositories::status(&repo)?;

        repositories::commit(&repo, "commit 1")?;

        Ok(())
    }
}
