#[derive(Clone, Debug)]
pub struct PullOpts {
    pub should_update_head: bool,
    pub should_pull_all: bool,
}
