#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    Windows,
    MacOS,
    Unknown,
}

impl Platform {
    pub fn from_os_identifier(s: &str) -> Self {
        match s {
            "linux" => Platform::Linux,
            "windows" => Platform::Windows,
            "macos" => Platform::MacOS,
            _ => Platform::Unknown,
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Platform::Linux => "Linux",
            Platform::Windows => "Windows",
            Platform::MacOS => "MacOS",
            Platform::Unknown => "Unknown",
        }
    }
}
