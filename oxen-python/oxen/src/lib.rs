use pyo3::prelude::*;

pub mod auth;
pub mod df_utils;
pub mod diff;
pub mod error;
pub mod py_branch;
pub mod py_commit;
pub mod py_dataset;
pub mod py_diff;
pub mod py_entry;
pub mod py_merge;
pub mod py_notebooks;
pub mod py_paginated_dir_entries;
pub mod py_pagination;
pub mod py_remote_data_frame;
pub mod py_remote_repo;
pub mod py_repo;
pub mod py_schema;
pub mod py_staged_data;
pub mod py_user;
pub mod py_workspace;
pub mod py_workspace_data_frame;
pub mod remote;
pub mod user;
pub mod util;

/// A Python module implemented in Rust.
#[pymodule]
fn oxen(m: Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_async_runtimes::tokio::init(builder);

    // Logger was causing deadlock unless we run with py.allow_threads in our
    // bridge I'd rather not do that, because it's less efficient, leaving this
    // commented out just in case you need to do further debugging and
    // enable the rust -> python logging bridge

    // You will need to add py.allow_threads(|| { ... }) around your code to
    // get it to work and ensure there is not a deadlock. In practice we
    // shouldn't be calling back to python to need to grab the GIL again,
    // and can just perform all the heavy work in rust.

    // println! also works to debug, make sure to remove it before committing

    // https://docs.rs/pyo3-log/latest/pyo3_log/#interaction-with-python-gil
    // pyo3_log::init();

    m.add_class::<diff::py_tabular_diff::PyTabularDiff>()?;
    m.add_class::<diff::py_text_diff::PyChangeType>()?;
    m.add_class::<diff::py_text_diff::PyLineDiff>()?;
    m.add_class::<diff::py_text_diff::PyTextDiff>()?;
    m.add_class::<py_branch::PyBranch>()?;
    m.add_class::<py_commit::PyCommit>()?;
    m.add_class::<py_dataset::PyDataset>()?;
    m.add_class::<py_diff::PyDiff>()?;
    m.add_class::<py_entry::PyEntry>()?;
    m.add_class::<py_merge::PyMergeable>()?;
    m.add_class::<py_remote_data_frame::PyRemoteDataFrame>()?;
    m.add_class::<py_remote_repo::PyRemoteRepo>()?;
    m.add_class::<py_repo::PyRepo>()?;
    m.add_class::<py_schema::PySchema>()?;
    m.add_class::<py_staged_data::PyStagedData>()?;
    m.add_class::<py_user::PyUser>()?;
    m.add_class::<py_workspace::PyWorkspace>()?;
    m.add_class::<py_workspace_data_frame::PyColumn>()?;
    m.add_class::<py_workspace_data_frame::PyWorkspaceDataFrame>()?;

    // Workspace
    let workspace_module = PyModule::new_bound(m.py(), "workspace")?;
    // Workspace Data Frame
    let workspace_dataset_module = PyModule::new_bound(workspace_module.py(), "dataset")?;
    workspace_dataset_module.add_function(wrap_pyfunction!(
        py_workspace_data_frame::index,
        &workspace_dataset_module
    )?)?;
    workspace_module.add_submodule(&workspace_dataset_module)?;
    m.add_submodule(&workspace_module)?;

    // Util Module
    let util_module = PyModule::new_bound(m.py(), "util")?;
    util_module.add_function(wrap_pyfunction!(util::is_tabular, &util_module)?)?;
    util_module.add_function(wrap_pyfunction!(util::read_df, &util_module)?)?;
    util_module.add_function(wrap_pyfunction!(util::get_oxen_config_dir, &util_module)?)?;
    m.add_submodule(&util_module)?;

    // Auth Module
    let auth_module = PyModule::new_bound(m.py(), "auth")?;
    auth_module.add_function(wrap_pyfunction!(auth::config_auth, &auth_module)?)?;
    m.add_submodule(&auth_module)?;

    // User Module
    let user_module = PyModule::new_bound(m.py(), "user")?;
    user_module.add_function(wrap_pyfunction!(user::config_user, &user_module)?)?;
    user_module.add_function(wrap_pyfunction!(user::current_user, &user_module)?)?;
    m.add_submodule(&user_module)?;

    // Remote Module
    let remote_module = PyModule::new_bound(m.py(), "remote")?;
    remote_module.add_function(wrap_pyfunction!(remote::get_repo, &remote_module)?)?;
    remote_module.add_function(wrap_pyfunction!(remote::create_repo, &remote_module)?)?;
    m.add_submodule(&remote_module)?;

    // Diff Module
    let diff_module = PyModule::new_bound(m.py(), "diff")?;
    diff_module.add_function(wrap_pyfunction!(diff::diff_paths, &diff_module)?)?;
    m.add_submodule(&diff_module)?;

    // DataFrame (df) Module
    let df_module = PyModule::new_bound(m.py(), "df_utils")?;
    df_module.add_function(wrap_pyfunction!(df_utils::save, &df_module)?)?;
    df_module.add_function(wrap_pyfunction!(df_utils::load, &df_module)?)?;
    m.add_submodule(&df_module)?;

    // Notebooks Module
    let notebooks_module = PyModule::new_bound(m.py(), "py_notebooks")?;
    notebooks_module.add_function(wrap_pyfunction!(
        py_notebooks::py_start_notebook,
        &notebooks_module
    )?)?;
    notebooks_module.add_function(wrap_pyfunction!(
        py_notebooks::py_stop_notebook,
        &notebooks_module
    )?)?;
    m.add_submodule(&notebooks_module)?;

    Ok(())
}
