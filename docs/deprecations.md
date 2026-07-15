# Deprecations and scheduled removals

Central registry of deprecated behavior and the release each item should be **removed** in. Code-level `// DEPRECATED` / `// TODO: remove` comments are easy to lose track of, so anything with a concrete removal target is recorded here.

**Workflow:** when you deprecate something in code with a removal target, add a row here; when you remove it, delete the row. A scheduled weekday check (see "Reminder" below) reads the **Remove in** column, compares it against the latest released version, and alerts before a removal release so the item can be dropped in a PR first.

## Pending removals

| Item | Deprecated in | Remove in | Location | Notes |
| --- | --- | --- | --- | --- |
| JSON workspace-staging endpoint — `POST /repos/{ns}/{name}/workspaces/{id}/versions/{dir}` (`add_version_files`): returns **426** for clients at/above the deprecation release; clients use the multipart `files::add` endpoint instead. | 0.51.0 | ~0.54.0 | `crates/server/src/controllers/workspaces/files.rs` (`add_version_files`); gate in `crates/server/src/params.rs` (`client_must_use_multipart_staging`, version literal inlined at the check) | Added in S3 17d. Remove the version-gated 426 and, if the JSON endpoint is then unused, the endpoint itself. |
| CLI `--workspace` flag (renamed to `--workspace-id`) | ≤ 0.50.4 (already shipped) | ~0.54.0 | `crates/cli/src/cmd/workspace/status.rs` | Update the "`--workspace` will be REMOVED in a future release" warning to name 0.54.0, then drop the flag at that release. |
| `commits` no-op echo endpoint | ≤ 0.50.4 (already shipped) | ~0.54.0 | `crates/server/src/controllers/commits.rs` | Update the "will be removed in a future release" doc to name 0.54.0, then remove the endpoint at that release. |

## Reminder

There is no automated scheduler for this list (a Claude session cron expires after 7 days — too short for these multi-release horizons — and a persistent CI job wasn't wanted yet). Instead, the workspace `CLAUDE.md` ("Wrapping up work") tells the agent to **offer**, when work wraps up, a quick scan that cross-checks the **Remove in** targets above against the current/upcoming release and flags anything due for removal. Keep this list current so that scan stays useful.
