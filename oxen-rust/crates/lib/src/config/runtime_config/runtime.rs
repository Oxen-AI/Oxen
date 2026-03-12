#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runtime {
    CLI,
    Python,
}

impl Runtime {
    pub fn from_runtime_name(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "python" => Runtime::Python,
            _ => Runtime::CLI,
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Runtime::Python => "Python",
            Runtime::CLI => "CLI",
        }
    }
}
