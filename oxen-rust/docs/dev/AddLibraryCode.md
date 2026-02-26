# Add Library Code

This guide is to help developers get up in running, as well as document some code paths in the repository.

The project is split into three components in the `src` directory

- `lib` A shared library that can be used in both the CLI and Server
- `cli` The oxen CLI binary
- `server` The oxen server binary

The `cli` and the `server` share the `lib` as the common code path between them. Hence it is good to start implementing any code that you could see shared between the CLI and the Server in the shared `lib`.

Let's use the `oxen branch -a` comand as an example, which lists all the local branches in the repository. A useful entry point for all the commands is the [src/lib/src/command.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/command.rs). If you search for the `list_branches` function in this file you will see an example implementation.

```rust
/// # List branches
pub fn list_branches(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    Ok(branches)
}
```

All of the functions in the command module have a similar signature that takes in a `LocalRepository` and any other params they will take in from the command line or server.

For example we will be using the `create_branch` command along with `list_branches` to test the functionality.

```rust
pub fn create_branch(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError>
```

This makes it easy to chain commands in our unit and integration tests, similar to how you would do it on the command line.

Let's look at the [RefReader](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/index/ref_reader.rs) class for an example of this. RefReader stands for "Reference Reader" and manages references to commit ids via branch names.

The RefReader has a local RocksDB that is a simple key value store of `branch_name` => `commit_id` and can read from this DB. There is also a [RefWriter](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/index/ref_writer.rs) which is in charge of writing to this database, but for simplicity sake we will not be diving into it, and just using our `command::create_branch` interface to write branches.

A good place to start is always with the unit test for the functionality you want to build. Built into the library are some useful unit test helps to instantiate unique directory names so that the tests can run in parallel. We can initialize repositories in these directories and then clean up as the tests are run.

An example of this is the `run_empty_local_repo_test` function that takes a closure with a repository as the parameter. This function takes care of creating a local repository, running the test, and cleaning up the repository on disk after the test has been run, even if it fails. There are more variations of these test closures [here](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/test.rs) that start with fully committed training data, etc, to get rid of boilerplate work.

Here is a unit an example unit test for testing the `list_branches` functionaility.

```rust
#[test]
fn test_ref_reader_list_branches() -> Result<(), OxenError> {
    crate::test::run_empty_local_repo_test(|repo| {
        command::create_branch(&repo, "feature/add-something")?;
        command::create_branch(&repo, "bug/something-is-broken")?;

        // Main functionality we are testing
        let ref_reader = RefReader::new(&repo)?;
        let branches = ref_reader.list_branches()?;

        // Now there should be be 3 branches.
        // We start with the main branch, then added the two above
        assert_eq!(branches.len(), 3);

        assert!(branches.iter().any(|b| b.name == "feature/add-something"));
        assert!(branches.iter().any(|b| b.name == "bug/something-is-broken"));
        assert!(branches.iter().any(|b| b.name == "main"));

        Ok(())
    })
}
```

We could also test it with an [integration test](https://github.com/Oxen-AI/Oxen/blob/main/tests/integration_test.rs) that is slightly cleaner, that just uses the `command` interface.

```rust
#[test]
fn test_ref_reader_list_branches() -> Result<(), OxenError> {
    crate::test::run_empty_local_repo_test(|repo| {
        command::create_branch(&repo, "feature/add-something")?;
        command::create_branch(&repo, "bug/something-is-broken")?;

        // Removed a few lines of internal code and just using the high level command interface
        let branches = command::list_branches(&repo)?;

        // Now there should be be 3 branches.
        // We start with the main branch, then added the two above
        assert_eq!(branches.len(), 3);

        assert!(branches.iter().any(|b| b.name == "feature/add-something"));
        assert!(branches.iter().any(|b| b.name == "bug/something-is-broken"));
        assert!(branches.iter().any(|b| b.name == "main"));

        Ok(())
    })
}
```

Feel free to browse the rest of the RefReader class to see the implementation, but most importantly we want to make sure we have high level tests for all of the functionality, and leave the implementation to you.

To run this specific unit test use the command

```shell
cargo test -p liboxen test_ref_reader_list_branches
```

To run all unit tests to make sure you haven't broken anything in the process just run

```shell
cargo test --workspace
```

If you want to see all the debug output for the commands you must set the `RUST_LOG` environment variable. In fish shell the an example command might look like

```shell
env RUST_LOG=liboxen=debug cargo test -p liboxen -- --nocapture test_respository_create
```

Once your code is tested with unit tests, or integration tests, you can integrate it into the command line or server binaries. An example of hooking into the cli is [here](IntegrateCLICode.md) and an example of hooking into the server is [here](IntegrateServerCode.md).
