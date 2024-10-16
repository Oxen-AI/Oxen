# Integrate Server and CLI

The Oxen Server and CLI communicate via HTTP, and we would like the interfaces for local and remote to be straight forward and easy to use.

We will continue to work off of the example of [listing branches](AddLibraryCode.md), but this time doing it on the remote server. Within the oxen library there is an `api` module that is responsible for interacting with our data models. The code for interacting with remote branches lives in [src/lib/src/api/remote/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/api/remote/branches.rs).

If you have followed the [Integrate Server Code](IntegrateServerCode.md) example, you should have an endpoint up and running that can list branches.

We use the [reqwest](https://docs.rs/reqwest/latest/reqwest/) library to make http requests. Here is an example of making a GET request to list the branches.

```rust
/// Take in a RemoteRepository and returns a Vec<Branch> unless there is an error
pub fn list(
    repository: &RemoteRepository,
) -> Result<Vec<Branch>, OxenError> {
    // Auth Config reads ~/.oxen/user_config.toml to get the user access token and other relevant info
    let config = UserConfig::default()?;

    // url_from_repo will prepend the repositories url to the uri you provide
    // Should look like: http://{REMOTE}/repositories/{REPO_NAME}/branches
    let url = api::endpoint::url_from_repo(repository, "/branches");

    // Create a http request
    let client = reqwest::Client::new();
    if let Ok(res) = client
        .get(url)
        // Grab the authentication token from UserConfig
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        // Make requeest
        .send()
    {
        // Serialize the json into the structure you would like
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListBranchesResponse, serde_json::Error> = serde_json::from_str(&body);
        // Handle Serialization Errors
        match response {
            Ok(j_res) => Ok(j_res.branches), // SUCCESS!
            Err(err) => {
                // Serialization failed
                log::debug!(
                    "remote::branches::list() Could not serialize response [{}] {}",
                    err,
                    body
                );
                Err(OxenError::basic_str("Failed to list remote branches: Serialization Error"))
            }
        }
    } else {
        // HTTP request failed
        let err = "Failed to list remote branches: HTTP Error";
        log::error!("remote::branches::list() err: {}", err);
        Err(OxenError::basic_str(&err))
    }
}
```

To test the communication between the CLI and the Server, currently the tests require that a server is up and running. See the [Server Setup Documentation](../examples/0_ServerSetup.md) for more info on how to setup the server.

Assuming you have a server running on the default host and port, the test for listing branches is very similar to testing listing local branches.

```rust
#[test]
fn test_list_remote_branches() -> Result<(), OxenError> {
    test::run_empty_remote_repo_test(|remote_repo| {
        api::client::branches::create(remote_repo, "branch-1", "main")?;
        api::client::branches::create(remote_repo, "branch-2", "main")?;

        let branches = api::client::branches::list(remote_repo)?;
        assert_eq!(branches.len(), 3);

        assert!(branches.iter().any(|b| b.name == "branch-1"));
        assert!(branches.iter().any(|b| b.name == "branch-2"));
        assert!(branches.iter().any(|b| b.name == "main"));

        Ok(())
    })
}
```

If you have gotten this far, congratulations! You have successfully implemented library, client and server code 🎉. If you have any feedback on the docs feel free to make a pull request with the changes and tag an engineer.
