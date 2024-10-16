use crate::core::df::tabular;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::opts::DFOpts;
use crate::util;

pub fn convert_to_arrow(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("running convert_to_arrow");
    let commit_entry_reader = CommitEntryReader::new(repo, commit)?;

    for entry in commit_entry_reader.list_entries()? {
        let version_path = util::fs::version_path(repo, &entry);
        let arrow_path = util::fs::df_version_path(repo, &entry);
        let is_already_arrow = util::fs::has_ext(&version_path, "arrow");
        if util::fs::is_tabular(&version_path) && !arrow_path.exists() && !is_already_arrow {
            log::debug!("convert_to_arrow converting {:?}", entry.path);
            let mut df = tabular::read_df(version_path, DFOpts::empty())?;
            tabular::write_df(&mut df, &arrow_path)?;
            log::debug!("convert_to_arrow wrote {:?}", arrow_path);
        }
    }

    Ok(())
}
