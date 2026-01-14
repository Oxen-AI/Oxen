use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash, Copy, ToSchema)]
pub enum ChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}
