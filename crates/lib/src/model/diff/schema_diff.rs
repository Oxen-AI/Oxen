#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub added_cols: Vec<String>,
    pub removed_cols: Vec<String>,
    pub unchanged_cols: Vec<String>,
}
