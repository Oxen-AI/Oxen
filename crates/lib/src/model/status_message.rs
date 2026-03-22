use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct StatusMessage {
    pub status: String,
    pub status_message: String,
}

impl StatusMessage {
    pub fn is_sucessful(&self) -> bool {
        self.status == "success"
    }
}
