use crate::error::OxenError;
use crate::model::diff::change_type::ChangeType;
use crate::model::diff::text_diff::LineDiff;
use crate::model::diff::text_diff::TextDiff;
use crate::util;

use difference::{Changeset, Difference};
use std::path::PathBuf;

/// Adds a slice of lines from a text block to the result vector with a given modification type.
fn add_lines_to_diff(
    result: &mut TextDiff,
    text_block: &str,
    modification: ChangeType,
    lines_to_take: Option<(usize, usize)>,
) {
    let lines: Vec<&str> = text_block.split('\n').collect();
    let (start, end) = lines_to_take.unwrap_or((0, lines.len()));

    // Ensure start and end are within bounds
    let start = start.min(lines.len());
    let end = end.min(lines.len());

    if start >= end {
        return;
    }

    for line in &lines[start..end] {
        result.lines.push(LineDiff {
            modification,
            text: line.to_string(),
        });
    }
}

pub fn diff(
    version_file_1: Option<PathBuf>,
    version_file_2: Option<PathBuf>,
) -> Result<TextDiff, OxenError> {
    let mut result = TextDiff {
        filename1: version_file_1
            .clone()
            .map(|p| p.to_string_lossy().to_string()),
        filename2: version_file_2
            .clone()
            .map(|p| p.to_string_lossy().to_string()),
        ..Default::default()
    };
    let original_data = util::fs::read_file(version_file_1)?;
    let compare_data = util::fs::read_file(version_file_2)?;
    let Changeset { diffs, .. } = Changeset::new(&original_data, &compare_data, "\n");
    log::debug!("Changeset created with {} diffs", diffs.len());

    // Find the indices of all Add or Rem changes
    let change_indices: Vec<usize> = diffs
        .iter()
        .enumerate()
        .filter(|(_, d)| !matches!(d, Difference::Same(_)))
        .map(|(i, _)| i)
        .collect();

    // If there are no changes, return an empty diff
    if change_indices.is_empty() {
        log::debug!("No changes detected, returning empty TextDiff.");
        return Ok(result);
    }

    let mut last_processed_diff_idx: i32 = -1;
    let mut post_context_lines_from_prev_chunk = 0;
    let mut is_first_chunk = true;

    for &change_idx in &change_indices {
        if (change_idx as i32) <= last_processed_diff_idx {
            continue;
        }
        log::debug!("Processing change at index: {}", change_idx);

        if !is_first_chunk {
            result.lines.push(LineDiff {
                modification: ChangeType::Unchanged,
                text: "...".to_string(),
            });
        }
        is_first_chunk = false;

        let context_diff_idx = change_idx.saturating_sub(1);
        let mut pre_context_lines_to_skip = 0;

        if (context_diff_idx as i32) == last_processed_diff_idx {
            pre_context_lines_to_skip = post_context_lines_from_prev_chunk;
        }

        if change_idx > 0 {
            if let Some(Difference::Same(text)) = diffs.get(context_diff_idx) {
                let lines: Vec<_> = text.split('\n').collect();
                let desired_start = lines.len().saturating_sub(3);
                let actual_start = desired_start.max(pre_context_lines_to_skip);
                log::debug!(
                    "Adding pre-context from diff [{}], lines [{}..]",
                    context_diff_idx,
                    actual_start
                );
                add_lines_to_diff(
                    &mut result,
                    text,
                    ChangeType::Unchanged,
                    Some((actual_start, lines.len())),
                );
            }
        }
        post_context_lines_from_prev_chunk = 0;

        let mut current_idx = change_idx;
        while let Some(diff) = diffs.get(current_idx) {
            match diff {
                Difference::Add(text) => {
                    log::debug!("Adding Added block at index {}", current_idx);
                    add_lines_to_diff(&mut result, text, ChangeType::Added, None);
                }
                Difference::Rem(text) => {
                    log::debug!("Adding Removed block at index {}", current_idx);
                    add_lines_to_diff(&mut result, text, ChangeType::Removed, None);
                }
                Difference::Same(_) => {
                    break;
                }
            }
            last_processed_diff_idx = current_idx as i32;
            current_idx += 1;
        }

        if let Some(Difference::Same(text)) = diffs.get(current_idx) {
            let lines: Vec<_> = text.split('\n').collect();
            let count = 2.min(lines.len());
            log::debug!(
                "Adding post-context from diff [{}], lines [..{}]",
                current_idx,
                count
            );
            add_lines_to_diff(&mut result, text, ChangeType::Unchanged, Some((0, count)));

            last_processed_diff_idx = current_idx as i32;
            post_context_lines_from_prev_chunk = count;
        }
    }

    log::debug!(
        "contextual_diff returning result with {} lines",
        result.lines.len()
    );
    Ok(result)
}
