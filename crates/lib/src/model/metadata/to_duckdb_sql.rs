use duckdb::types::ToSql;

pub trait ToDuckDBSql {
    fn to_sql(&self) -> Vec<&dyn ToSql>;
}
