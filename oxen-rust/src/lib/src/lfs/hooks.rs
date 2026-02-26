use std::path::Path;

use crate::error::OxenError;

/// Marker comment used to identify our hook sections.
const HOOK_MARKER: &str = "# oxen lfs";

/// Install pre-push, post-checkout, and post-merge hooks into `.git/hooks/`.
///
/// Idempotent: checks for existing `oxen lfs` content before appending.
/// Respects existing hook scripts by appending rather than overwriting.
pub fn install_hooks(git_dir: &Path) -> Result<(), OxenError> {
    let hooks_dir = git_dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir)?;

    install_hook(
        &hooks_dir,
        "pre-push",
        &format!(
            r#"{HOOK_MARKER}
command -v oxen >/dev/null 2>&1 || {{ echo >&2 "oxen not found in PATH, skipping LFS pre-push hook"; exit 0; }}
oxen lfs push "$@"
"#
        ),
    )?;

    install_hook(
        &hooks_dir,
        "post-checkout",
        &format!(
            r#"{HOOK_MARKER}
command -v oxen >/dev/null 2>&1 || exit 0
oxen lfs pull --local
"#
        ),
    )?;

    install_hook(
        &hooks_dir,
        "post-merge",
        &format!(
            r#"{HOOK_MARKER}
command -v oxen >/dev/null 2>&1 || exit 0
oxen lfs pull --local
"#
        ),
    )?;

    Ok(())
}

fn install_hook(hooks_dir: &Path, name: &str, snippet: &str) -> Result<(), OxenError> {
    let hook_path = hooks_dir.join(name);

    let existing = if hook_path.exists() {
        std::fs::read_to_string(&hook_path)?
    } else {
        String::new()
    };

    // Already installed?
    if existing.contains(HOOK_MARKER) {
        return Ok(());
    }

    let mut content = if existing.is_empty() {
        "#!/bin/sh\n".to_string()
    } else {
        let mut s = existing;
        if !s.ends_with('\n') {
            s.push('\n');
        }
        s
    };

    content.push('\n');
    content.push_str(snippet);

    std::fs::write(&hook_path, &content)?;

    // Make executable on Unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&hook_path)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&hook_path, perms)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_install_hooks_creates_files() {
        let tmp = TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();

        install_hooks(&git_dir).unwrap();

        let hooks_dir = git_dir.join("hooks");
        assert!(hooks_dir.join("pre-push").exists());
        assert!(hooks_dir.join("post-checkout").exists());
        assert!(hooks_dir.join("post-merge").exists());

        // Check content.
        let pre_push = std::fs::read_to_string(hooks_dir.join("pre-push")).unwrap();
        assert!(pre_push.contains("oxen lfs push"));
        assert!(pre_push.starts_with("#!/bin/sh"));
    }

    #[test]
    fn test_install_hooks_idempotent() {
        let tmp = TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();

        install_hooks(&git_dir).unwrap();
        install_hooks(&git_dir).unwrap();

        let pre_push = std::fs::read_to_string(git_dir.join("hooks/pre-push")).unwrap();
        assert_eq!(
            pre_push.matches("oxen lfs push").count(),
            1,
            "should not duplicate hook content"
        );
    }

    #[test]
    fn test_install_hooks_preserves_existing() {
        let tmp = TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        let hooks_dir = git_dir.join("hooks");
        std::fs::create_dir_all(&hooks_dir).unwrap();

        // Pre-existing hook script.
        std::fs::write(
            hooks_dir.join("pre-push"),
            "#!/bin/sh\necho 'existing hook'\n",
        )
        .unwrap();

        install_hooks(&git_dir).unwrap();

        let content = std::fs::read_to_string(hooks_dir.join("pre-push")).unwrap();
        assert!(
            content.contains("existing hook"),
            "should preserve existing"
        );
        assert!(content.contains("oxen lfs push"), "should add our hook");
    }

    #[cfg(unix)]
    #[test]
    fn test_hooks_are_executable() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();

        install_hooks(&git_dir).unwrap();

        let meta = std::fs::metadata(git_dir.join("hooks/pre-push")).unwrap();
        let mode = meta.permissions().mode();
        assert!(mode & 0o111 != 0, "hook should be executable");
    }
}
