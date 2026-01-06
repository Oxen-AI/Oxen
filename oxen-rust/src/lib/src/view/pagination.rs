use crate::opts::PaginateOpts;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Default, Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct Pagination {
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count_cached: Option<bool>,
}

// Add default values
impl Pagination {
    pub fn empty(paginate_opts: PaginateOpts) -> Self {
        Pagination {
            page_number: paginate_opts.page_num,
            page_size: paginate_opts.page_size,
            total_pages: 1,
            total_entries: 0,
            count_cached: None,
        }
    }
}
