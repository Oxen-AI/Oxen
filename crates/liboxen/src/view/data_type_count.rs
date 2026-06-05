use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct DataTypeCount {
    pub count: usize,
    pub data_type: String,
}

impl std::fmt::Display for DataTypeCount {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({},{})", self.count, self.data_type)
    }
}
