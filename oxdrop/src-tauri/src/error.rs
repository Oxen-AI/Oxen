use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OxDropError {
    pub message: String,
    pub code: String,
}

impl std::fmt::Display for OxDropError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl From<liboxen::error::OxenError> for OxDropError {
    fn from(err: liboxen::error::OxenError) -> Self {
        let message = err.to_string();
        let code = if message.contains("401") || message.contains("Unauthorized") {
            "AUTH_EXPIRED".to_string()
        } else if message.contains("404") || message.contains("not found") {
            "NOT_FOUND".to_string()
        } else if message.contains("connection") || message.contains("timeout") {
            "CONNECTION_ERROR".to_string()
        } else {
            "UNKNOWN".to_string()
        };
        OxDropError { message, code }
    }
}

impl From<std::io::Error> for OxDropError {
    fn from(err: std::io::Error) -> Self {
        OxDropError {
            message: err.to_string(),
            code: "IO_ERROR".to_string(),
        }
    }
}

impl From<toml::de::Error> for OxDropError {
    fn from(err: toml::de::Error) -> Self {
        OxDropError {
            message: err.to_string(),
            code: "CONFIG_ERROR".to_string(),
        }
    }
}

impl From<toml::ser::Error> for OxDropError {
    fn from(err: toml::ser::Error) -> Self {
        OxDropError {
            message: err.to_string(),
            code: "CONFIG_ERROR".to_string(),
        }
    }
}
