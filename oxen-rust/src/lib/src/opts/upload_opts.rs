use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct UploadOpts {
    pub paths: Vec<PathBuf>,
    pub dst: PathBuf,
    pub branch: Option<String>,
    pub message: String,
    pub host: String,
    pub scheme: String,
    pub remote: String,
    pub bearer_token: Option<String>,
}
