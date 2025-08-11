use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::{api, repositories};

use colored::Colorize;
use std::path::Path;
use uuid::Uuid;

pub async fn pull(
    repo: &mut LocalRepository,
    branch_name: &str,
) -> Result<(), OxenError> {

    // Fetch new branch from the remote
    let fetch_opts = FetchOpts::from_branch(&branch.name);
    repositories::fetch::fetch_branch(&repo, &fetch_opts).await?;

    // Instantiate workspace on new branch
    repositories::remote_mode::create_checkout_branch(repo, branch_name.as_ref()).await?;

    // Checkout branch
    repositories::remote_mode::checkout_remote_mode(repo, branch_name.as_ref()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;



}