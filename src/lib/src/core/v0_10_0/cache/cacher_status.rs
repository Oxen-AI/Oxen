use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CacherStatusType {
    Pending,
    Failed,
    Success,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct CacherStatus {
    pub status: CacherStatusType,
    pub status_message: String,
}

impl CacherStatus {
    pub fn pending() -> CacherStatus {
        CacherStatus {
            status: CacherStatusType::Pending,
            status_message: String::from(""),
        }
    }

    pub fn success() -> CacherStatus {
        CacherStatus {
            status: CacherStatusType::Success,
            status_message: String::from(""),
        }
    }

    pub fn failed(msg: &str) -> CacherStatus {
        CacherStatus {
            status: CacherStatusType::Failed,
            status_message: String::from(msg),
        }
    }
}
