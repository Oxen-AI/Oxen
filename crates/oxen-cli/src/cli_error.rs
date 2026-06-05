use thiserror::Error;

#[derive(Debug, Error)]
#[error("Unknown {parent} subcommand '{name}'")]
pub struct UnknownSubcommand {
    pub parent: &'static str,
    pub name: String,
}
