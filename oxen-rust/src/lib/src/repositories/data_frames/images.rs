use std::io::{BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::DFOpts;
use crate::{repositories, util};

pub const IMAGE_PATH_COLUMN: &str = "image_path";
pub const DEFAULT_IMAGES_DIR: &str = "images";

/// Resolve where an image should live in the repo. If the image is already under
/// the repo root, return its repo-relative path. If external, copy it into the
/// repo (at `dest` or a default location) and return the repo-relative path.
pub fn resolve_image_location(
    repo: &LocalRepository,
    image_path: impl AsRef<Path>,
    df_path: impl AsRef<Path>,
    dest: Option<&Path>,
) -> Result<PathBuf, OxenError> {
    let image_path = image_path.as_ref();
    let df_path = df_path.as_ref();

    let repo_root = repo.path.canonicalize().map_err(|e| {
        OxenError::basic_str(format!(
            "Could not canonicalize repo path {:?}: {e}",
            repo.path
        ))
    })?;

    let canonical_image = image_path.canonicalize().map_err(|e| {
        OxenError::basic_str(format!(
            "Could not canonicalize image path {image_path:?}: {e}"
        ))
    })?;

    // Check if image is already under repo root
    if canonical_image.starts_with(&repo_root) {
        return util::fs::path_relative_to_dir(&canonical_image, &repo_root);
    }

    // External image — compute destination
    let filename = image_path.file_name().ok_or_else(|| {
        OxenError::basic_str(format!("Image path has no filename: {image_path:?}"))
    })?;

    let dest_relative = if let Some(dest) = dest {
        let dest_str = dest.to_string_lossy();
        if dest_str.ends_with('/') || repo.path.join(dest).is_dir() {
            dest.join(filename)
        } else {
            dest.to_path_buf()
        }
    } else {
        // Default: <df_path parent>/images/<filename>
        let df_parent = df_path.parent().unwrap_or_else(|| Path::new(""));
        if df_parent == Path::new("") {
            PathBuf::from(DEFAULT_IMAGES_DIR).join(filename)
        } else {
            df_parent.join(DEFAULT_IMAGES_DIR).join(filename)
        }
    };

    let abs_dest = repo.path.join(&dest_relative);
    if let Some(parent) = abs_dest.parent() {
        util::fs::create_dir_all(parent)?;
    }
    std::fs::copy(image_path, &abs_dest).map_err(|e| {
        OxenError::basic_str(format!(
            "Could not copy image {image_path:?} to {abs_dest:?}: {e}"
        ))
    })?;

    Ok(dest_relative)
}

/// Append one or more image paths (repo-relative) as rows to a dataframe file.
pub async fn append_image_paths_to_df(
    abs_df_path: &Path,
    image_relative_paths: &[PathBuf],
    extension_override: Option<&str>,
) -> Result<(), OxenError> {
    let ext = extension_override
        .or_else(|| abs_df_path.extension().and_then(|e| e.to_str()))
        .ok_or_else(|| {
            OxenError::basic_str(format!(
                "Cannot determine format for dataframe: {abs_df_path:?}"
            ))
        })?;

    let supported = ["csv", "tsv", "ndjson", "jsonl", "parquet", "arrow", "json"];
    if !supported.contains(&ext) {
        return Err(OxenError::basic_str(format!(
            "Unsupported dataframe format: {ext}"
        )));
    }

    let path_strings: Vec<String> = image_relative_paths
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    if !abs_df_path.exists() {
        // Create new file
        if let Some(parent) = abs_df_path.parent() {
            util::fs::create_dir_all(parent)?;
        }
        create_new_df_with_image_paths(abs_df_path, &path_strings, ext)?;
        return Ok(());
    }

    // File exists — choose strategy based on format
    match ext {
        "csv" | "tsv" => {
            let delimiter = if ext == "tsv" { b'\t' } else { b',' };
            append_csv_fast_path(abs_df_path, &path_strings, delimiter).await?;
        }
        "ndjson" | "jsonl" => {
            append_ndjson_fast_path(abs_df_path, &path_strings)?;
        }
        "parquet" | "arrow" | "json" => {
            append_slow_path(abs_df_path, &path_strings, ext).await?;
        }
        _ => {
            return Err(OxenError::basic_str(format!(
                "Unsupported dataframe format: {ext}"
            )));
        }
    }

    Ok(())
}

/// Combined orchestrator: resolve images, append to df, stage all files.
pub async fn add_images_to_df(
    repo: &LocalRepository,
    df_path: impl AsRef<Path>,
    image_paths: &[PathBuf],
    dest: Option<&Path>,
    extension_override: Option<&str>,
) -> Result<Vec<PathBuf>, OxenError> {
    let df_path = df_path.as_ref();

    // Validate all image files exist
    for img in image_paths {
        if !img.exists() {
            return Err(OxenError::basic_str(format!(
                "Image file does not exist: {img:?}"
            )));
        }
    }

    // Validate df format
    let ext = extension_override.or_else(|| df_path.extension().and_then(|e| e.to_str()));
    if let Some(ext) = ext {
        let supported = ["csv", "tsv", "ndjson", "jsonl", "parquet", "arrow", "json"];
        if !supported.contains(&ext) {
            return Err(OxenError::basic_str(format!(
                "Unsupported dataframe format: {ext}"
            )));
        }
    } else {
        return Err(OxenError::basic_str(format!(
            "Cannot determine format for dataframe: {df_path:?}"
        )));
    }

    // Resolve each image location
    let mut relative_image_paths = Vec::with_capacity(image_paths.len());
    for img in image_paths {
        let rel = resolve_image_location(repo, img, df_path, dest)?;
        relative_image_paths.push(rel);
    }

    // Append all paths to the dataframe in a single batch
    let abs_df_path = repo.path.join(df_path);
    append_image_paths_to_df(&abs_df_path, &relative_image_paths, extension_override).await?;

    // Stage the dataframe
    repositories::add(repo, &abs_df_path).await?;

    // Stage each image
    for rel_path in &relative_image_paths {
        repositories::add(repo, repo.path.join(rel_path)).await?;
    }

    Ok(relative_image_paths)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_csv_header(path: &Path, delimiter: u8) -> Result<Vec<String>, OxenError> {
    let file = std::fs::File::open(path)
        .map_err(|e| OxenError::basic_str(format!("Could not open file {path:?}: {e}")))?;
    let mut reader = BufReader::new(file);
    let mut header_line = String::new();
    reader
        .read_line(&mut header_line)
        .map_err(|e| OxenError::basic_str(format!("Could not read header from {path:?}: {e}")))?;
    let header_line = header_line.trim_end_matches('\n').trim_end_matches('\r');
    let delim_char = delimiter as char;
    let columns: Vec<String> = header_line
        .split(delim_char)
        .map(|s| s.trim_matches('"').to_string())
        .collect();
    Ok(columns)
}

fn build_csv_append_line(columns: &[String], image_path_value: &str, delimiter: u8) -> String {
    let delim_char = delimiter as char;
    columns
        .iter()
        .map(|col| {
            if col == IMAGE_PATH_COLUMN {
                image_path_value.to_string()
            } else {
                String::new()
            }
        })
        .collect::<Vec<_>>()
        .join(&delim_char.to_string())
}

async fn append_csv_fast_path(
    path: &Path,
    image_paths: &[String],
    delimiter: u8,
) -> Result<(), OxenError> {
    let columns = read_csv_header(path, delimiter)?;

    if columns.iter().any(|c| c == IMAGE_PATH_COLUMN) {
        // Fast path: column exists, just append lines
        let mut payload = String::new();

        // Check if file ends with newline
        let needs_leading_newline = !file_ends_with_newline(path)?;

        for (i, img_path) in image_paths.iter().enumerate() {
            if i == 0 && needs_leading_newline {
                payload.push('\n');
            }
            payload.push_str(&build_csv_append_line(&columns, img_path, delimiter));
            payload.push('\n');
        }

        util::fs::append_to_file(path, &payload)?;
    } else {
        // Slow path: need to add the column (full rewrite)
        let ext = if delimiter == b'\t' { "tsv" } else { "csv" };
        append_slow_path(path, image_paths, ext).await?;
    }

    Ok(())
}

fn append_ndjson_fast_path(path: &Path, image_paths: &[String]) -> Result<(), OxenError> {
    let mut payload = String::new();
    let needs_leading_newline = !file_ends_with_newline(path)?;

    for (i, img_path) in image_paths.iter().enumerate() {
        if i == 0 && needs_leading_newline {
            payload.push('\n');
        }
        let json_line = serde_json::to_string(&serde_json::json!({ IMAGE_PATH_COLUMN: img_path }))
            .map_err(|e| OxenError::basic_str(format!("JSON serialize error: {e}")))?;
        payload.push_str(&json_line);
        payload.push('\n');
    }

    util::fs::append_to_file(path, &payload)?;
    Ok(())
}

async fn append_slow_path(path: &Path, image_paths: &[String], ext: &str) -> Result<(), OxenError> {
    let mut df = tabular::read_df(path, DFOpts::empty()).await?;

    // Add image_path column if missing
    let has_col = df
        .schema()
        .iter_names()
        .any(|n| n.as_str() == IMAGE_PATH_COLUMN);
    if !has_col {
        let null_series = Series::new_null(PlSmallStr::from_str(IMAGE_PATH_COLUMN), df.height());
        let null_col = null_series
            .cast(&DataType::String)
            .map_err(|e| OxenError::basic_str(format!("Cast error: {e}")))?;
        df.with_column(null_col)
            .map_err(|e| OxenError::basic_str(format!("Could not add column: {e}")))?;
    }

    // Build new rows matching schema
    let new_rows = build_new_rows_df(df.schema(), image_paths)?;
    df = df
        .vstack(&new_rows)
        .map_err(|e| OxenError::basic_str(format!("vstack error: {e}")))?;

    tabular::write_df_with_ext(&mut df, path, ext)?;
    Ok(())
}

fn create_new_df_with_image_paths(
    path: &Path,
    image_paths: &[String],
    ext: &str,
) -> Result<(), OxenError> {
    match ext {
        "csv" | "tsv" => {
            let delimiter = if ext == "tsv" { "\t" } else { "," };
            // Write header + rows as text directly
            let mut content = format!("{IMAGE_PATH_COLUMN}\n");
            for img_path in image_paths {
                content.push_str(img_path);
                content.push('\n');
            }
            // Replace delimiter in header if needed for consistency (single col, no effect)
            let _ = delimiter;
            std::fs::write(path, content)
                .map_err(|e| OxenError::basic_str(format!("Could not write file {path:?}: {e}")))?;
        }
        "ndjson" | "jsonl" => {
            let mut content = String::new();
            for img_path in image_paths {
                let json_line =
                    serde_json::to_string(&serde_json::json!({ IMAGE_PATH_COLUMN: img_path }))
                        .map_err(|e| OxenError::basic_str(format!("JSON serialize error: {e}")))?;
                content.push_str(&json_line);
                content.push('\n');
            }
            std::fs::write(path, content)
                .map_err(|e| OxenError::basic_str(format!("Could not write file {path:?}: {e}")))?;
        }
        _ => {
            // parquet, arrow, json — use polars
            let values: Vec<&str> = image_paths.iter().map(|s| s.as_str()).collect();
            let series = Series::new(PlSmallStr::from_str(IMAGE_PATH_COLUMN), &values);
            let mut df = DataFrame::new(vec![Column::Series(series.into())])
                .map_err(|e| OxenError::basic_str(format!("DataFrame error: {e}")))?;
            tabular::write_df_with_ext(&mut df, path, ext)?;
        }
    }
    Ok(())
}

fn build_new_rows_df(schema: &SchemaRef, image_paths: &[String]) -> Result<DataFrame, OxenError> {
    let n = image_paths.len();
    let mut columns: Vec<Column> = Vec::with_capacity(schema.len());

    for (name, dtype) in schema.iter() {
        if name.as_str() == IMAGE_PATH_COLUMN {
            let values: Vec<&str> = image_paths.iter().map(|s| s.as_str()).collect();
            let series = Series::new(name.clone(), &values);
            columns.push(Column::Series(series.into()));
        } else {
            let null_series = Series::new_null(name.clone(), n);
            let null_col = null_series
                .cast(dtype)
                .map_err(|e| OxenError::basic_str(format!("Cast error: {e}")))?;
            columns.push(Column::Series(null_col.into()));
        }
    }

    DataFrame::new(columns).map_err(|e| OxenError::basic_str(format!("DataFrame error: {e}")))
}

fn file_ends_with_newline(path: &Path) -> Result<bool, OxenError> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| OxenError::basic_str(format!("Could not open file {path:?}: {e}")))?;
    let metadata = file
        .metadata()
        .map_err(|e| OxenError::basic_str(format!("Could not read metadata {path:?}: {e}")))?;
    if metadata.len() == 0 {
        return Ok(true); // Empty file, treat as having newline
    }
    let mut buf = [0u8; 1];
    file.seek_relative(metadata.len() as i64 - 1)
        .map_err(|e| OxenError::basic_str(format!("Seek error: {e}")))?;
    file.read_exact(&mut buf)
        .map_err(|e| OxenError::basic_str(format!("Read error: {e}")))?;
    Ok(buf[0] == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_add_image_single_internal_csv_with_column() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create an image file inside the repo
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;
            let img_path = img_dir.join("cat.jpg");
            test::write_txt_file_to_path(&img_path, "fake image data")?;

            // Create a CSV with image_path column and existing data
            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path,label\nold.jpg,cat\n")?;

            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[img_path], None, None).await?;

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], PathBuf::from("images/cat.jpg"));

            // Verify the CSV content
            let content = std::fs::read_to_string(&csv_path)?;
            let lines: Vec<&str> = content.trim().lines().collect();
            assert_eq!(lines.len(), 3); // header + 2 rows
            assert_eq!(lines[0], "image_path,label");
            assert_eq!(lines[1], "old.jpg,cat");
            assert!(lines[2].starts_with("images/cat.jpg,"));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_single_internal_csv_without_column() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;
            let img_path = img_dir.join("cat.jpg");
            test::write_txt_file_to_path(&img_path, "fake image data")?;

            // CSV without image_path column
            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "label,score\ncat,0.9\n")?;

            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[img_path], None, None).await?;

            assert_eq!(result.len(), 1);

            // Verify column was added
            let df = tabular::read_df(&csv_path, DFOpts::empty()).await?;
            assert!(df
                .schema()
                .iter_names()
                .any(|n| n.as_str() == IMAGE_PATH_COLUMN));
            assert_eq!(df.height(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_multiple_images() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;

            let img1 = img_dir.join("cat1.jpg");
            let img2 = img_dir.join("cat2.jpg");
            let img3 = img_dir.join("dog1.jpg");
            test::write_txt_file_to_path(&img1, "fake1")?;
            test::write_txt_file_to_path(&img2, "fake2")?;
            test::write_txt_file_to_path(&img3, "fake3")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let result = add_images_to_df(
                &repo,
                Path::new("data.csv"),
                &[img1, img2, img3],
                None,
                None,
            )
            .await?;

            assert_eq!(result.len(), 3);

            let content = std::fs::read_to_string(&csv_path)?;
            let lines: Vec<&str> = content.trim().lines().collect();
            // header + 3 rows (empty first row from original + 3 new)
            // Original had "image_path\n" which means one header + empty content after
            assert!(lines.len() >= 4); // header + at least 3 new rows

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_external_default_dest() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create an image outside the repo
            let tmp_dir = tempfile::tempdir()?;
            let external_img = tmp_dir.path().join("external.jpg");
            test::write_txt_file_to_path(&external_img, "external image data")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[external_img], None, None).await?;

            assert_eq!(result.len(), 1);
            // Default dest for data.csv at root is images/external.jpg
            assert_eq!(result[0], PathBuf::from("images/external.jpg"));
            assert!(repo.path.join("images/external.jpg").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_external_with_dest_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let tmp_dir = tempfile::tempdir()?;
            let external_img = tmp_dir.path().join("photo.jpg");
            test::write_txt_file_to_path(&external_img, "photo data")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let result = add_images_to_df(
                &repo,
                Path::new("data.csv"),
                &[external_img],
                Some(Path::new("my_images/")),
                None,
            )
            .await?;

            assert_eq!(result[0], PathBuf::from("my_images/photo.jpg"));
            assert!(repo.path.join("my_images/photo.jpg").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_external_with_dest_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let tmp_dir = tempfile::tempdir()?;
            let external_img = tmp_dir.path().join("photo.jpg");
            test::write_txt_file_to_path(&external_img, "photo data")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let result = add_images_to_df(
                &repo,
                Path::new("data.csv"),
                &[external_img],
                Some(Path::new("pics/renamed.jpg")),
                None,
            )
            .await?;

            assert_eq!(result[0], PathBuf::from("pics/renamed.jpg"));
            assert!(repo.path.join("pics/renamed.jpg").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_multiple_external() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let tmp_dir = tempfile::tempdir()?;
            let img1 = tmp_dir.path().join("a.jpg");
            let img2 = tmp_dir.path().join("b.png");
            test::write_txt_file_to_path(&img1, "a")?;
            test::write_txt_file_to_path(&img2, "b")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let result = add_images_to_df(
                &repo,
                Path::new("data.csv"),
                &[img1, img2],
                Some(Path::new("photos/")),
                None,
            )
            .await?;

            assert_eq!(result.len(), 2);
            assert!(repo.path.join("photos/a.jpg").exists());
            assert!(repo.path.join("photos/b.png").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_new_csv() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;
            let img_path = img_dir.join("new.jpg");
            test::write_txt_file_to_path(&img_path, "new image")?;

            // data.csv does not exist yet
            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[img_path], None, None).await?;

            assert_eq!(result.len(), 1);
            let csv_path = repo.path.join("data.csv");
            assert!(csv_path.exists());

            let content = std::fs::read_to_string(&csv_path)?;
            let lines: Vec<&str> = content.trim().lines().collect();
            assert_eq!(lines[0], "image_path");
            assert_eq!(lines[1], "images/new.jpg");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_parquet() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;
            let img_path = img_dir.join("test.jpg");
            test::write_txt_file_to_path(&img_path, "test image")?;

            // Create a parquet file with image_path column
            let series = Series::new(PlSmallStr::from_str("image_path"), &["existing.jpg"]);
            let mut df = DataFrame::new(vec![Column::Series(series.into())]).unwrap();
            let parquet_path = repo.path.join("data.parquet");
            tabular::write_df_parquet(&mut df, &parquet_path)?;

            let result =
                add_images_to_df(&repo, Path::new("data.parquet"), &[img_path], None, None).await?;

            assert_eq!(result.len(), 1);

            let df = tabular::read_df(&parquet_path, DFOpts::empty()).await?;
            assert_eq!(df.height(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_extension_override() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let img_dir = repo.path.join("images");
            util::fs::create_dir_all(&img_dir)?;
            let img_path = img_dir.join("test.jpg");
            test::write_txt_file_to_path(&img_path, "test image")?;

            // Create a file with no standard extension but override to csv
            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[img_path], None, Some("csv"))
                    .await?;

            assert_eq!(result.len(), 1);
            assert!(repo.path.join("data.csv").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_nonexistent_image_error() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            let nonexistent = PathBuf::from("/tmp/definitely_not_a_real_image_12345.jpg");
            let result =
                add_images_to_df(&repo, Path::new("data.csv"), &[nonexistent], None, None).await;

            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_image_files_staged() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let tmp_dir = tempfile::tempdir()?;
            let img1 = tmp_dir.path().join("staged1.jpg");
            let img2 = tmp_dir.path().join("staged2.jpg");
            test::write_txt_file_to_path(&img1, "data1")?;
            test::write_txt_file_to_path(&img2, "data2")?;

            let csv_path = repo.path.join("data.csv");
            test::write_txt_file_to_path(&csv_path, "image_path\n")?;

            add_images_to_df(&repo, Path::new("data.csv"), &[img1, img2], None, None).await?;

            // Verify files are staged
            let status = repositories::status(&repo)?;
            let staged_files: Vec<String> = status
                .staged_files
                .iter()
                .map(|(p, _)| p.to_string_lossy().to_string())
                .collect();

            assert!(
                staged_files.iter().any(|p| p.contains("data.csv")),
                "data.csv should be staged, got: {staged_files:?}"
            );
            assert!(
                staged_files.iter().any(|p| p.contains("staged1.jpg")),
                "staged1.jpg should be staged, got: {staged_files:?}"
            );
            assert!(
                staged_files.iter().any(|p| p.contains("staged2.jpg")),
                "staged2.jpg should be staged, got: {staged_files:?}"
            );

            Ok(())
        })
        .await
    }
}
