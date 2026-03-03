//! # PathBufError
//!
//! Struct that wraps a PathBuf and implements the necessary traits for errors.
//!

use std::path::{Display, Path, PathBuf};

// TODO: do we actually need to wrap values into something that implements Error?
// TODO: investigate if it's sufficient to use values directly

error_wrapper!(PathBuf, {
    // We tried to have:
    //    |pb: &PathBuf| pb.display()
    //
    // But rustc can't unify the lifetime of `pb` with the lifetime of the returned Display<'_>
    // when it's a closure. It can do it when it's a function.
    //
    // This issue is documented as [rust-lang/rust#58052](https://github.com/rust-lang/rust/issues/58052).
    //

    fn display_path_buf<'a>(pb: &'a Path) -> Display<'a> {
        pb.display()
    }

    display_path_buf
});

impl From<&Path> for PathBufError {
    fn from(p: &Path) -> Self {
        PathBufError(p.to_path_buf())
    }
}
