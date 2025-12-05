use crate::constants::OXEN_VERSION;
use crate::view;
use serde::{Deserialize, Serialize};

use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct StatusMessage {
    pub status: String,
    pub status_message: String,
    pub oxen_version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusMessageDescription {
    pub status: String,
    pub status_message: String,
    pub oxen_version: Option<String>,
    pub status_description: String,
}

impl StatusMessageDescription {
    pub fn not_found(description: impl AsRef<str>) -> StatusMessageDescription {
        StatusMessageDescription {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_RESOURCE_NOT_FOUND),
            oxen_version: Some(OXEN_VERSION.to_string()),
            status_description: String::from(description.as_ref()),
        }
    }

    pub fn workspace_not_found(workspace_id: impl AsRef<str>) -> StatusMessageDescription {
        let workspace_id = workspace_id.as_ref();
        StatusMessageDescription::not_found(format!("Workspace not found: {workspace_id}"))
    }

    pub fn bad_request(description: impl AsRef<str>) -> StatusMessageDescription {
        StatusMessageDescription {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_BAD_REQUEST),
            oxen_version: Some(OXEN_VERSION.to_string()),
            status_description: String::from(description.as_ref()),
        }
    }
}

impl StatusMessage {
    pub fn success(msg: &str) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(msg),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn error(msg: impl AsRef<str>) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(msg.as_ref()),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn bad_request() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_BAD_REQUEST),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn resource_found() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_FOUND),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn resource_not_found() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_RESOURCE_NOT_FOUND),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn resource_created() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_CREATED),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn resource_updated() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_UPDATED),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn resource_deleted() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_DELETED),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn internal_server_error() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_INTERNAL_SERVER_ERROR),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn not_implemented() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_NOT_IMPLEMENTED),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }

    pub fn update_required() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_UPDATE_REQUIRED),
            oxen_version: Some(OXEN_VERSION.to_string()),
        }
    }
}
