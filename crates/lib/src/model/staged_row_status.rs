use strum::{Display, EnumString, VariantNames};

/// The status of rows that are being staged in a commit.
#[derive(Debug, EnumString, VariantNames, Display)]
#[strum(serialize_all = "lowercase")]
pub enum StagedRowStatus {
    Added,
    Modified,
    Removed,
    Unchanged,
}
