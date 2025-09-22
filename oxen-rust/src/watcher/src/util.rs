use std::path::{Path, PathBuf};

/// Convert an absolute path to a relative path from a base directory
pub fn path_relative_to_dir(path: &Path, base: &Path) -> Result<PathBuf, std::io::Error> {
    // Canonicalize the base directory
    let abs_base = base.canonicalize()?;

    // For the target path, try to canonicalize if it exists,
    // otherwise just make it absolute by cleaning it
    let abs_path = if path.exists() {
        path.canonicalize()?
    } else {
        // Path doesn't exist (e.g., for remove events), so we need to handle it differently
        // If it's already absolute, use it as-is, otherwise join with current dir
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        }
    };

    // Try to strip the base prefix
    abs_path
        .strip_prefix(&abs_base)
        .map(|p| p.to_path_buf())
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Path {:?} is not within base directory {:?}", path, base),
            )
        })
}

/// Check if a repository exists at the given path
pub fn is_repository(path: &Path) -> bool {
    path.join(".oxen").exists()
}
