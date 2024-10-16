use serde::{Deserialize, Serialize};

use crate::view::DataTypeCount;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDir {
    pub dir: MetadataDirImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDirImpl {
    pub data_types: Vec<DataTypeCount>,
}

impl MetadataDir {
    pub fn new(data_types: Vec<DataTypeCount>) -> Self {
        Self {
            dir: MetadataDirImpl { data_types },
        }
    }
}

impl std::fmt::Display for MetadataDir {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetadataDir(")?;
        for data_type in &self.dir.data_types {
            write!(f, "{}", data_type)?;
        }
        write!(f, ")")
    }
}
