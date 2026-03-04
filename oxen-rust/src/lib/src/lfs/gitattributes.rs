use std::path::Path;

use crate::error::OxenError;

const GITATTRIBUTES: &str = ".gitattributes";

/// The filter/diff/merge attributes appended to each tracked pattern.
const ATTR_SUFFIX: &str = "filter=oxen diff=oxen merge=oxen -text";

/// Format a .gitattributes line for a given pattern.
fn format_line(pattern: &str) -> String {
    format!("{pattern} {ATTR_SUFFIX}")
}

/// Add a pattern to `.gitattributes` (idempotent — skips if already present).
pub fn track_pattern(repo_root: &Path, pattern: &str) -> Result<(), OxenError> {
    let ga_path = repo_root.join(GITATTRIBUTES);
    let line = format_line(pattern);

    let existing = if ga_path.exists() {
        std::fs::read_to_string(&ga_path)?
    } else {
        String::new()
    };

    // Already tracked?
    if existing.lines().any(|l| l.trim() == line.trim()) {
        return Ok(());
    }

    // Append (ensure trailing newline in existing content).
    let mut content = existing;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&line);
    content.push('\n');

    std::fs::write(&ga_path, content)?;
    Ok(())
}

/// Remove a pattern from `.gitattributes`.
pub fn untrack_pattern(repo_root: &Path, pattern: &str) -> Result<(), OxenError> {
    let ga_path = repo_root.join(GITATTRIBUTES);
    if !ga_path.exists() {
        return Ok(());
    }

    let line = format_line(pattern);
    let existing = std::fs::read_to_string(&ga_path)?;
    let filtered: Vec<&str> = existing
        .lines()
        .filter(|l| l.trim() != line.trim())
        .collect();

    let mut content = filtered.join("\n");
    if !content.is_empty() {
        content.push('\n');
    }

    std::fs::write(&ga_path, content)?;
    Ok(())
}

/// List all patterns currently tracked with the oxen filter.
pub fn list_tracked_patterns(repo_root: &Path) -> Result<Vec<String>, OxenError> {
    let ga_path = repo_root.join(GITATTRIBUTES);
    if !ga_path.exists() {
        return Ok(Vec::new());
    }

    let text = std::fs::read_to_string(&ga_path)?;
    let patterns = text
        .lines()
        .filter(|l| l.contains(ATTR_SUFFIX))
        .filter_map(|l| {
            let trimmed = l.trim();
            trimmed
                .strip_suffix(ATTR_SUFFIX)
                .map(|p| p.trim().to_string())
        })
        .collect();
    Ok(patterns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_track_creates_gitattributes() {
        let tmp = TempDir::new().unwrap();
        track_pattern(tmp.path(), "*.bin").unwrap();

        let content = std::fs::read_to_string(tmp.path().join(GITATTRIBUTES)).unwrap();
        assert!(content.contains("*.bin filter=oxen diff=oxen merge=oxen -text"));
    }

    #[test]
    fn test_track_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        track_pattern(tmp.path(), "*.bin").unwrap();
        track_pattern(tmp.path(), "*.bin").unwrap();

        let content = std::fs::read_to_string(tmp.path().join(GITATTRIBUTES)).unwrap();
        assert_eq!(
            content.matches("*.bin").count(),
            1,
            "pattern should appear only once"
        );
    }

    #[test]
    fn test_track_multiple_patterns() {
        let tmp = TempDir::new().unwrap();
        track_pattern(tmp.path(), "*.bin").unwrap();
        track_pattern(tmp.path(), "datasets/**").unwrap();

        let patterns = list_tracked_patterns(tmp.path()).unwrap();
        assert_eq!(patterns, vec!["*.bin", "datasets/**"]);
    }

    #[test]
    fn test_untrack_removes_pattern() {
        let tmp = TempDir::new().unwrap();
        track_pattern(tmp.path(), "*.bin").unwrap();
        track_pattern(tmp.path(), "*.pt").unwrap();
        untrack_pattern(tmp.path(), "*.bin").unwrap();

        let patterns = list_tracked_patterns(tmp.path()).unwrap();
        assert_eq!(patterns, vec!["*.pt"]);
    }

    #[test]
    fn test_untrack_noop_when_missing() {
        let tmp = TempDir::new().unwrap();
        // No error when file doesn't exist.
        untrack_pattern(tmp.path(), "*.bin").unwrap();
    }

    #[test]
    fn test_list_empty() {
        let tmp = TempDir::new().unwrap();
        let patterns = list_tracked_patterns(tmp.path()).unwrap();
        assert!(patterns.is_empty());
    }
}
