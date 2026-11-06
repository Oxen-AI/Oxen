use crate::error::OxenError;
use crate::model::diff::change_type::ChangeType;
use crate::model::diff::text_diff::LineDiff;
use crate::model::diff::text_diff::TextDiff;

use similar::{Algorithm, DiffTag, capture_diff_slices_deadline};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How long a line diff searches for the fewest changed lines before it settles for a diff that
/// marks more lines as changed than it has to.
const DIFF_TIMEOUT: Duration = Duration::from_secs(1);

/// A run of lines the two versions share, or that the newer one removes or adds.
enum Block<'a> {
    Same(&'a [&'a str]),
    Add(&'a [&'a str]),
    Rem(&'a [&'a str]),
}

/// The lines of a version, split on `\n` so a trailing newline ends in an empty line. Empty text
/// has no lines.
fn split_lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        Vec::new()
    } else {
        text.split('\n').collect()
    }
}

/// The two versions' lines as runs of shared, removed, and added lines, with each change's removed
/// lines ahead of its added ones.
fn blocks<'a>(old: &'a [&'a str], new: &'a [&'a str]) -> Vec<Block<'a>> {
    let deadline = Some(Instant::now() + DIFF_TIMEOUT);
    let ops = capture_diff_slices_deadline(Algorithm::Myers, old, new, deadline);
    // The equal runs, then an empty one at the end of both versions, so the lines between one
    // run and the next are the change between them.
    let equal_runs = ops
        .iter()
        .filter(|op| op.tag() == DiffTag::Equal)
        .map(|op| (op.old_range(), op.new_range()))
        .chain([(old.len()..old.len(), new.len()..new.len())]);
    let mut blocks = Vec::new();
    let (mut old_pos, mut new_pos) = (0, 0);
    for (old_run, new_run) in equal_runs {
        if old_pos < old_run.start {
            blocks.push(Block::Rem(&old[old_pos..old_run.start]));
        }
        if new_pos < new_run.start {
            blocks.push(Block::Add(&new[new_pos..new_run.start]));
        }
        (old_pos, new_pos) = (old_run.end, new_run.end);
        if !old_run.is_empty() {
            blocks.push(Block::Same(&old[old_run]));
        }
    }
    blocks
}

/// Adds lines to the result with a given modification type.
fn add_lines_to_diff(result: &mut TextDiff, lines: &[&str], modification: ChangeType) {
    result.lines.extend(lines.iter().map(|line| LineDiff {
        modification,
        text: line.to_string(),
    }));
}

pub fn diff(
    original_data: Option<String>,
    version_file_1: Option<PathBuf>,
    compare_data: Option<String>,
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

    let original_data = original_data.unwrap_or_default();
    let compare_data = compare_data.unwrap_or_default();
    let original_lines = split_lines(&original_data);
    let compare_lines = split_lines(&compare_data);

    let diffs = blocks(&original_lines, &compare_lines);
    log::debug!("Line diff found {} blocks", diffs.len());

    // Find the indices of all Add or Rem changes
    let change_indices: Vec<usize> = diffs
        .iter()
        .enumerate()
        .filter(|(_, d)| !matches!(d, Block::Same(_)))
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
        log::debug!("Processing change at index: {change_idx}");

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

        if change_idx > 0
            && let Some(Block::Same(lines)) = diffs.get(context_diff_idx)
        {
            let desired_start = lines.len().saturating_sub(3);
            let actual_start = desired_start.max(pre_context_lines_to_skip);
            log::debug!(
                "Adding pre-context from diff [{context_diff_idx}], lines [{actual_start}..]"
            );
            add_lines_to_diff(
                &mut result,
                lines.get(actual_start..).unwrap_or_default(),
                ChangeType::Unchanged,
            );
        }
        post_context_lines_from_prev_chunk = 0;

        let mut current_idx = change_idx;
        while let Some(diff) = diffs.get(current_idx) {
            match diff {
                Block::Add(lines) => {
                    log::debug!("Adding Added block at index {current_idx}");
                    add_lines_to_diff(&mut result, lines, ChangeType::Added);
                }
                Block::Rem(lines) => {
                    log::debug!("Adding Removed block at index {current_idx}");
                    add_lines_to_diff(&mut result, lines, ChangeType::Removed);
                }
                Block::Same(_) => {
                    break;
                }
            }
            last_processed_diff_idx = current_idx as i32;
            current_idx += 1;
        }

        if let Some(Block::Same(lines)) = diffs.get(current_idx) {
            let count = 2.min(lines.len());
            log::debug!("Adding post-context from diff [{current_idx}], lines [..{count}]");
            add_lines_to_diff(&mut result, &lines[..count], ChangeType::Unchanged);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The diff's lines, each marked `+` when added, `-` when removed, and ` ` otherwise.
    fn render(old: Option<&str>, new: Option<&str>) -> Vec<String> {
        diff(old.map(String::from), None, new.map(String::from), None)
            .expect("a line diff never fails")
            .lines
            .iter()
            .map(|line| {
                let mark = match line.modification {
                    ChangeType::Added => '+',
                    ChangeType::Removed => '-',
                    _ => ' ',
                };
                format!("{mark}{}", line.text)
            })
            .collect()
    }

    /// Pins the chunks clients render: three lines of context before a change, two after, and `...`
    /// between chunks.
    #[test]
    fn test_diff_keeps_chunks_and_context() {
        let twelve = (1..=12)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let cases = vec![
            (
                "two chunks",
                Some(twelve.clone()),
                Some(twelve.replace("l5", "L5").replace("l11", "L11")),
                vec![
                    " l2", " l3", " l4", "-l5", "+L5", " l6", " l7", " ...", " l8", " l9", " l10",
                    "-l11", "+L11", " l12",
                ],
            ),
            (
                "one line between changes",
                Some(twelve.clone()),
                Some(twelve.replace("l5", "L5").replace("l7", "L7")),
                vec![
                    " l2", " l3", " l4", "-l5", "+L5", " l6", " ...", "-l7", "+L7", " l8", " l9",
                ],
            ),
            (
                "three lines between changes",
                Some(twelve.clone()),
                Some(twelve.replace("l3", "L3").replace("l7", "L7")),
                vec![
                    " l1", " l2", "-l3", "+L3", " l4", " l5", " ...", " l6", "-l7", "+L7", " l8",
                    " l9",
                ],
            ),
            (
                "first line",
                Some("a\nb\nc\nd".into()),
                Some("A\nb\nc\nd".into()),
                vec!["-a", "+A", " b", " c"],
            ),
            (
                "last line",
                Some("a\nb\nc\nd".into()),
                Some("a\nb\nc\nD".into()),
                vec![" a", " b", " c", "-d", "+D"],
            ),
            (
                "trailing newline added",
                Some("a\nb".into()),
                Some("a\nb\n".into()),
                vec![" a", " b", "+"],
            ),
            ("added file", None, Some("x\ny".into()), vec!["+x", "+y"]),
            ("removed file", Some("x\ny".into()), None, vec!["-x", "-y"]),
            (
                "identical",
                Some("a\nb\n".into()),
                Some("a\nb\n".into()),
                vec![],
            ),
            (
                "three lines replaced by one",
                Some("a\nb\nc\nd\ne\nf".into()),
                Some("a\nX\ne\nf".into()),
                vec![" a", "-b", "-c", "-d", "+X", " e", " f"],
            ),
            (
                "one line inserted",
                Some("a\nb\nc\nd\ne\nf\ng\nh".into()),
                Some("a\nb\nc\nd\nNEW\ne\nf\ng\nh".into()),
                vec![" b", " c", " d", "+NEW", " e", " f"],
            ),
        ];
        for (name, old, new, expected) in cases {
            assert_eq!(render(old.as_deref(), new.as_deref()), expected, "{name}");
        }
    }
}
