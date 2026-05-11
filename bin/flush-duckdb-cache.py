#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
# ]
# ///

"""
Force a CHECKPOINT on every cached DuckDB connection on a running oxen-server,
*before* a deploy that doesn't yet have shutdown-flush logic.

Why
---
The cache is a `static LazyLock` in oxen-server's process; Rust does not drop
statics at exit, so a pre-flush server hit with SIGTERM still has WAL content
for active workspaces un-checkpointed. The recovery code that runs on the next
open is the dangerous one — it can delete db+WAL when the second-pass open
fails — so the safer move is to flush before the deploy starts.

How
---
The cache is bounded at DF_DB_CACHE_SIZE (100, see
`crates/lib/src/core/db/data_frames/df_db.rs`). Inserting that many fresh
entries forces every prior entry out via LRU; each eviction drops the
Connection's last Arc, which triggers DuckDB's default close-time CHECKPOINT.

`PUT /api/repos/{ns}/{repo}/workspaces/{ws}/data_frames/resource/{path}` with
body `{"is_indexed": false}` is a path-hash-only no-op (the controller calls
`is_indexed`, sees no `df` table at the hashed location, and short-circuits)
that triggers the cache insertion as a side effect — exactly what we need.

The host workspace is just a hashing namespace for the spurious paths; nothing
real gets indexed. The 100 fresh paths are dummied with a per-invocation UUID
so re-running the script always evicts (no cache hits on the dummy paths).

Side effect
-----------
Each run leaves up to ${batch-size} empty DuckDB files at
  <host-workspace>/.oxen/mods/duckdb/<hash>/db
on the server. They are harmless and small (~12KB each). Clean up server-side
after the deploy if desired.

Usage
-----
    OXEN_AUTH_TOKEN=... ./bin/flush-duckdb-cache.py \\
        --host https://hub.oxen.ai \\
        --namespace ox \\
        --repo MyRepo \\
        --workspace-id 11111111-2222-3333-4444-555555555555
"""

import argparse
import os
import sys
import uuid
from urllib.parse import quote

import requests

# Must match DF_DB_CACHE_SIZE in
# crates/lib/src/core/db/data_frames/df_db.rs. 100 fresh inserts saturates the
# LRU regardless of starting occupancy.
CACHE_SIZE = 100


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Force CHECKPOINT on cached oxen-server DuckDB connections via LRU eviction.",
    )
    parser.add_argument(
        "--host", required=True, help="OxenHub base URL, e.g. https://hub.oxen.ai"
    )
    parser.add_argument("--namespace", required=True, help="Namespace of the host repo")
    parser.add_argument("--repo", required=True, help="Name of the host repo")
    parser.add_argument(
        "--workspace-id",
        required=True,
        help="Workspace ID to use as the path-hash host (any existing workspace works)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("OXEN_AUTH_TOKEN"),
        help="Bearer token (defaults to OXEN_AUTH_TOKEN env var)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=CACHE_SIZE,
        help=f"Fresh paths to insert (default {CACHE_SIZE}, the LRU cap; "
        f"increase if cache cap has been bumped server-side)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the URLs that would be hit, send nothing",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.token:
        parser.error("missing --token or OXEN_AUTH_TOKEN env var")

    invocation = uuid.uuid4().hex[:8]
    host = args.host.rstrip("/")
    base_path = (
        f"/api/repos/{args.namespace}/{args.repo}"
        f"/workspaces/{args.workspace_id}/data_frames/resource"
    )
    headers = {"Content-Type": "application/json"}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"

    print(f"flushing oxen-server DuckDB cache via {host}")
    print(f"  host workspace: {args.namespace}/{args.repo} ws={args.workspace_id}")
    print(f"  fresh-path prefix: __flush_{invocation}_*")
    print(f"  inserts: {args.batch_size}")
    print()

    with requests.Session() as sess:
        # Sanity-check the workspace exists before firing 100 PUTs blindly.
        if not args.dry_run:
            ws_url = (
                f"{host}/api/repos/{args.namespace}/{args.repo}"
                f"/workspaces/{args.workspace_id}"
            )
            r = sess.get(ws_url, headers=headers, timeout=30)
            if r.status_code != 200:
                print(
                    f"workspace check failed: GET {ws_url} -> {r.status_code} {r.text}",
                    file=sys.stderr,
                )
                return 1

        for i in range(args.batch_size):
            fake_path = f"__flush_{invocation}_{i}"
            url = f"{host}{base_path}/{quote(fake_path, safe='')}"
            if args.dry_run:
                print(f"PUT {url}")
                continue
            r = sess.put(url, headers=headers, json={"is_indexed": False}, timeout=30)
            if r.status_code != 200:
                print(
                    f"insert {i + 1}/{args.batch_size} failed: "
                    f"{r.status_code} {r.text}",
                    file=sys.stderr,
                )
                return 1
            if (i + 1) % 10 == 0 or i == args.batch_size - 1:
                print(f"  {i + 1}/{args.batch_size} inserts complete")

    print()
    if args.dry_run:
        print("dry run complete. no requests sent.")
    else:
        print(
            "done. CHECKPOINT has fired on every previously-cached connection. "
            "safe to deploy now."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
