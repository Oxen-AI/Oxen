# Integrate Library Code into Server

This doc assumes you have written some library code and want to expose it as an HTTP API. We will be working off of the `list_branches` example from [here](AddLibraryCode.md).

The entry point to the server is in [src/server/src/main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/main.rs). We use the [actix](https://actix.rs/) library for all of the server setup. Each route is defined in the main file and attached to a controller.

In order to list the branches via HTTP we will want to add an `index` method to the [src/server/src/controllers/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/controllers/branches.rs) module. It is already committed to this file if you want to follow along in the actual code.

A simple controller method signature will just look like

```rust
pub async fn index(req: HttpRequest) -> HttpResponse
```

Feel free to browse the other implementations for more complex data processing out of the HTTP body or parameters etc.

Below is a stripped down version of the server in [main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/main.rs) to make some concepts easier to see. We bind an `OxenAppData` object that contains the root path for all the repositories on disk. Then we add authentication to lock down access to the server. Finally an example get request for listing the branches on a repository.

```rust
let sync_dir = "/tmp/repositories";
let host = "0.0.0.0";
let port: u16 = 3000;
let data = app_data::OxenAppData::from(&sync_dir);

HttpServer::new(move || {
    App::new()
        // Bind app data so that controllers can access sync_dir
        .app_data(data.clone())
        // Add authentication
        .wrap(HttpAuthentication::bearer(auth::validator::validate))
        // Add an endpoint for listing branches
        .route(
            "/oxen/{namespace}/{repo_name}/branches",
            web::get().to(controllers::branches::index),
        )
})

.bind((host, port))?
.run()
.await
```

Back in the [controllers/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/controllers/branches.rs) module we can grab any global app data out of the request.

```rust
pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // ...
}
```

This contains the base directory looking for repositories in `app_data.path`. We can then look up the repository on disk with our `api::local::repositories` module.

```rust
// ...

// We can grab the `repo_name` parameter that was defined in main.rs when binding the server endpoint
let namespace: &str = req.match_info().get("namespace").unwrap();
let name: &str = req.match_info().get("repo_name").unwrap();
// Then given a base path and a name, find the repository on disk
match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {

// ...
```

If we found the repository, we can try to list the branches, otherwise we return 404 or 500 internal server error if appropriate. The `api::local::branches::list` is just a wrapper around the `command::list` method we made earlier, but makes it clear if we are listing the branch model remotely or locally.

```rust
// ...

Ok(Some(repository)) => match api::local::branches::list(&repository) {
    Ok(branches) => {
        let view = ListBranchesResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            branches,
        };
        HttpResponse::Ok().json(view)
    }
    Err(err) => {
        log::error!("Unable to list branches. Err: {}", err);
        HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
    }
},

// ...
```

Next up we need a unit test to make sure the functionality works. Actix

```rust
#[actix_web::test]
async fn test_branches_index_multiple_branches() -> Result<(), OxenError> {
    // Create unique name for sync directory so tests can run in parallel
    let sync_dir = test::get_sync_dir()?;

    // Repository Name
    let namespace = "Testing-Namespace";
    let name = "Testing-Branches-1";

    // Create a local repository in the sync dir
    let repo = test::create_local_repo(&sync_dir, namespace, name)?;
    api::local::branches::create(&repo, "branch-1")?;
    api::local::branches::create(&repo, "branch-2")?;

    // uri format where we will fill in repo_name
    let uri = format!("/oxen/{}/{}/branches", namespace, name);

    // Creates a actix_web::test::TestRequest with and fills in the URI param
    let req = test::repo_request(&sync_dir, &uri, namespace, name);

    // Call the controller function
    let resp = controllers::branches::index(req).await;

    // Test that response is what we want
    assert_eq!(resp.status(), http::StatusCode::OK);
    let body = to_bytes(resp.into_body()).await.unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    // Serialization should be successfull
    let list: ListBranchesResponse = serde_json::from_str(text)?;
    // Validate data
    // main + branch-1 + branch-2
    assert_eq!(list.branches.len(), 3);
    assert!(branches.iter().any(|b| b.name == "branch-1"));
    assert!(branches.iter().any(|b| b.name == "branch-2"));
    assert!(branches.iter().any(|b| b.name == "main"));

    // cleanup directories
    util::fs::remove_dir_all(sync_dir)?;

    Ok(())
}
```

If you would like to see the API with `curl` on the command line you can run the server and use this curl command:

```shell
`curl -H "Authorization: Bearer $TOKEN" "http://$SERVER/repositories/$REPO_NAME/branches"`

{
  "status": "success",
  "status_message": "resource_found",
  "branches": [
    {
      "name": "add-training-data",
      "commit_id": "7d6258e0-5956-4695-aa13-6844b3c73e6d",
    },
    {
      "name": "main",
      "commit_id": "11f6c5d5-f683-42b2-9d6e-a82172509eed",
    }
  ]
}
```

In order to get a valid auth token you can run add a user to the server via

```shell
$ ./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
$ cat user_config.toml | grep token
```

For more information on server setup look at the [Server Setup Documentation](../examples/0_ServerSetup.md)
