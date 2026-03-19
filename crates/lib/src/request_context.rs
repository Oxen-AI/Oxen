use std::cell::RefCell;

tokio::task_local! {
    pub static REQUEST_ID: RefCell<Option<String>>;
}

pub fn get_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.borrow().clone()).ok().flatten()
}
