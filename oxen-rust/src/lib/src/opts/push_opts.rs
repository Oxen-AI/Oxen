#[derive(Clone, Debug, Default)]
pub struct PushOpts {
    pub remote: String,
    pub branch: String,
    pub delete: bool,
    pub missing_files: bool,
    pub missing_files_commit_id: Option<String>,
    pub revalidate: bool,
}
