#[derive(Clone, Debug)]
pub struct S3Opts {
    pub bucket: String,
    pub prefix: Option<String>,
}
