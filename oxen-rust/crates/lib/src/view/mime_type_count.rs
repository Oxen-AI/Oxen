use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct MimeTypeCount {
    pub count: usize,
    pub mime_type: String,
}
