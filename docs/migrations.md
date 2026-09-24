# Migrations

Steps to take on upgrading, for the local repositories you work with through the `oxen` CLI and for the repositories a self-hosted `oxen-server` holds. Repositories hosted on [Oxen.ai](https://oxen.ai) are migrated for you, so nothing here applies to them.

Find the rows whose versions include the release you are upgrading to. `TBD` means no release has dropped support for the step yet. Once one does, it is replaced with the last release that still supports it.

| Versions | Applies to | Step |
| --- | --- | --- |
| 0.51.4 to TBD | CLI, server | [Move a repository's Merkle nodes to LMDB](#move-a-repositorys-merkle-nodes-to-lmdb) |
| 0.58.0 to TBD | Server | [Record identity for repositories that predate it](#record-identity-for-repositories-that-predate-it) |
| 0.58.0 to TBD | Server | [Re-seed the name table after changing the sync directory by hand](#re-seed-the-name-table-after-changing-the-sync-directory-by-hand) |
| 0.59.0 to TBD | Server, S3 backend | [Copy an S3-backed repository's objects to its UUID prefix](#copy-an-s3-backed-repositorys-objects-to-its-uuid-prefix) |
| 0.59.0 to TBD | Server | [Stop starting the server with `-a`](#stop-starting-the-server-with--a) |

## Move a repository's Merkle nodes to LMDB

**Versions:** 0.51.4 to TBD. **Applies to:** CLI and server.

The `merkle_nodes_to_lmdb` migration moves a repository's Merkle nodes from the filesystem backend onto LMDB and keeps the filesystem copy as a backup. From 0.54.0 the CLI refuses to work in a local repository still on the filesystem backend, and prints the command to run from the repository's root:

```bash
oxen migrate up merkle_nodes_to_lmdb .
```

Before 0.54.0 the migration is optional, so add `--run-optional`, which works on every release in the range.

A self-hosted server holds its own copy of every repository and needs the same move. Stop the server and run it over every repository in the sync directory:

```bash
for repo in "$SYNC_DIR"/*/*/; do
  oxen migrate up merkle_nodes_to_lmdb --run-optional "$repo"
done
```

A repository already on LMDB is left unchanged, so the loop is safe to rerun.

## Record identity for repositories that predate it

**Versions:** 0.58.0 to TBD. **Applies to:** server.

`oxen-server` refuses to create a repository under a name another repository already holds, and it learns the names that exist from the `[identity]` section of each repository's config. A repository created before `oxen-server` recorded identity has no such section, so that refusal does not cover its name until the section is written, and the server walks every config and logs a warning on each start until every repository has one.

Stop the server, then run the optional `backfill_repo_identity` migration over every repository in the sync directory, using the `oxen` CLI from the same release:

```bash
for repo in "$SYNC_DIR"/*/*/; do
  oxen migrate up backfill_repo_identity --run-optional "$repo"
done
```

Start the server again. It indexes the recorded names at startup and stops warning once every repository is covered. A repository that already records its identity is left unchanged, so the loop is safe to rerun.

## Re-seed the name table after changing the sync directory by hand

**Versions:** 0.58.0 to TBD. **Applies to:** server.

After restoring the sync directory from a backup or otherwise changing it behind the server's back, stop the server and run:

```bash
oxen-server seed-name-table
```

It indexes the name every repository records and prints how many it covered. It only adds names, so a repository directory removed by hand keeps its name taken, and the command reports those names as `unclaimed`.

## Copy an S3-backed repository's objects to its UUID prefix

**Versions:** 0.59.0 to TBD. **Applies to:** server, S3 backend only.

> **Warning:** the S3 backend is not yet supported in open source, so do not use it yet. If you do, you use it at your own risk.

The server reads an S3-backed repository's objects from `repo/{repo_uuid}/` in its bucket rather than `{namespace}/{name}/`. With the server stopped, [record identity](#record-identity-for-repositories-that-predate-it) for any repository that lacks it, copy each S3-backed repository's objects from its old prefix to `repo/{repo_uuid}/`, using the UUID in its config, then start the server and delete the old prefixes once the repositories read correctly.

## Stop starting the server with `-a`

**Versions:** 0.59.0 to TBD. **Applies to:** server.

`oxen-server` no longer authenticates requests itself, so it refuses to start with `-a` and no longer has an `add-user` command. Before upgrading a server started with `-a`, put a reverse proxy or gateway in front of it that authenticates requests (see [Self Hosting](../SelfHosting.md)), and drop `-a` from the command that starts it.

The tokens `add-user` issued and the secret they were signed with stay in `$SYNC_DIR/.oxen/`, which nothing reads any more. Delete it:

```bash
rm -rf "$SYNC_DIR/.oxen"
```

Until it is gone, the `oxen` CLI run from a directory inside the sync directory that no repository contains takes the whole sync directory for a repository.

## Maintaining this page

When a release asks a person to run something, add a row here giving its range as `X.Y.Z to TBD`, where `X.Y.Z` is the first release it applies to, and open the step's section with the same range. Keep the rows ordered by that first release. When a later release stops supporting the step, the PR that drops it replaces `TBD` with the last release that still supports it, in both the row and the section. [`deprecations.md`](deprecations.md) tracks when code is removed and this file tracks what a person runs, so a deprecation that removes the code a step relies on also closes that step's range.
