use crate::model::DiffEntry;
use crate::model::diff::AddRemoveModifyCounts;
use crate::view::Pagination;

#[derive(Debug)]
pub struct DiffEntriesCounts {
    pub entries: Vec<DiffEntry>,
    pub counts: AddRemoveModifyCounts,
    pub pagination: Pagination,
}
