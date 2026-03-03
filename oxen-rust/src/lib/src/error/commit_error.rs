use crate::model::Commit;

// TODO: do we actually need to wrap values into something that implements Error?
// TODO: investigate if it's sufficient to use values directly

error_wrapper!(Commit);
