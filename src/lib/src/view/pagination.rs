use crate::opts::PaginateOpts;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Pagination {
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

// Add default values
impl Pagination {
    pub fn empty(paginate_opts: PaginateOpts) -> Self {
        Pagination {
            page_number: paginate_opts.page_num,
            page_size: paginate_opts.page_size,
            total_pages: 1,
            total_entries: 0,
        }
    }
}
