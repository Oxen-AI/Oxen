/// The row id of a successfully applied batch-update entry. Batch update is
/// all-or-nothing (a single DuckDB UPDATE), so there is no per-row error case.
#[derive(Debug)]
pub struct UpdateResult(pub String);
