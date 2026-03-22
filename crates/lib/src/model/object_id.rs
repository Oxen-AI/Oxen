/// Simple object to serialize and deserialize an object id
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ObjectID {
    pub id: String,
}
