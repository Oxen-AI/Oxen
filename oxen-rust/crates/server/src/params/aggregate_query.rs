use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct AggregateQuery {
    pub column: Option<String>,
}
