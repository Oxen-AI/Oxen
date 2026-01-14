

pub type FrameworkResult<T> = Result<T, FrameworkError>;
pub type FrameworkError = Box<dyn std::error::Error + Send + Sync>;