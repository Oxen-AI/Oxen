#!/usr/bin/env python3
"""Fixed benchmark for block-level dedup research.

Replays the prepared corpus (benchmark/dedup/corpus/, see prepare.py) as a
sequence of oxen commits against a fresh block-v1 repository, then measures, in
the guide's priority order:

  1. storage    — stored_bytes (all of .oxen/versions: payloads + manifests +
                  chunk index), storage_ratio, incremental bytes/ratio,
                  metadata_bytes
  2. reads      — restore_seconds / restore_mb_s (checkout + byte-verify every
                  commit), sequential_read_mb_s (re-materialize the largest
                  file from the store), random_read_ms (median single-file
                  restore from random historical commits)
  3. compression— compression_seconds / compression_mb_s (add+commit, all
                  versions), version_add_seconds, peak_memory_mb (max RSS)

Correctness is a hard gate: every file of every commit must reconstruct
byte-for-byte (sha256 vs the corpus manifest).

score = storage_ratio. The score deliberately reflects ONLY priority 1; read
and compression metrics are reported raw and compared per the guide's ordered
rules. Do not modify this file during a research run.

Usage:
    OXEN_BIN=target/debug/oxen python3 benchmark/dedup/benchmark.py

Prints the canonical metric block to stdout; all subprocess noise goes to
stderr. Exits non-zero on correctness failure.
"""

import hashlib
import json
import os
import random
import re
import resource
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# BENCH_CORPUS selects the corpus dir (default: the original corpus);
# each corpus is its own fixed benchmark with its own baseline.
CORPUS = os.path.join(HERE, os.environ.get("BENCH_CORPUS", "corpus"))
RUN_DIR = os.path.join(HERE, ".run-" + os.environ.get("BENCH_CORPUS", "corpus"))
OXEN = os.environ.get("OXEN_BIN", "oxen")


def sh(args, cwd, capture=False):
    return subprocess.run(args, cwd=cwd, check=True, text=True,
                          stdout=subprocess.PIPE if capture else sys.stderr,
                          stderr=sys.stderr)


def tree_bytes(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def child_peak_rss_mb():
    # macOS reports ru_maxrss in bytes; Linux in KiB.
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def main():
    with open(os.path.join(CORPUS, "manifest.json")) as f:
        manifest = json.load(f)
    commits = sorted(manifest)

    if os.path.exists(RUN_DIR):
        shutil.rmtree(RUN_DIR)
    repo = os.path.join(RUN_DIR, "repo")
    os.makedirs(repo)

    sh([OXEN, "init"], repo)
    cfg_path = os.path.join(repo, ".oxen", "config.toml")
    with open(cfg_path) as f:
        lines = [l for l in f.read().splitlines() if not l.startswith("min_version")]
    lines.insert(lines.index("[storage]") + 1, 'content_format = "block-v1"')
    with open(cfg_path, "w") as f:
        f.write('min_version = "0.53.0"\n' + "\n".join(lines).rstrip() + "\n")
    # Optional profile-mark overrides for measurement runs (never committed):
    extra = os.environ.get("BENCH_STORAGE_PROFILES")
    if extra:
        with open(cfg_path, "a") as f:
            f.write(extra)

    versions_dir = os.path.join(repo, ".oxen", "versions")

    # ---- ingest all commit snapshots
    source_bytes = 0
    first_commit_source = 0
    version_add_seconds = []
    commit_ids = {}
    t_compress = 0.0
    stored_after = {}
    for i, commit in enumerate(commits):
        for name, meta in manifest[commit].items():
            shutil.copyfile(os.path.join(CORPUS, commit, name),
                            os.path.join(repo, name))
            source_bytes += meta["bytes"]
            if i == 0:
                first_commit_source += meta["bytes"]
        t0 = time.time()
        sh([OXEN, "add", "."], repo)
        sh([OXEN, "commit", "-m", commit], repo)
        dt = time.time() - t0
        t_compress += dt
        version_add_seconds.append(round(dt, 2))
        out = sh([OXEN, "log"], repo, capture=True).stdout
        m = re.search(r"commit ([0-9a-f]+)", out)
        commit_ids[commit] = m.group(1)
        stored_after[commit] = tree_bytes(versions_dir)

    stored_bytes = stored_after[commits[-1]]
    incremental_stored = stored_bytes - stored_after[commits[0]]
    incremental_source = source_bytes - first_commit_source
    files_root = os.path.join(versions_dir, "files")
    blocks_b = tree_bytes(os.path.join(files_root, "blocks"))
    index_b = tree_bytes(os.path.join(files_root, "chunk_index"))
    metadata_bytes = stored_bytes - blocks_b  # manifests + index + small blobs

    # ---- correctness + restore: checkout every commit, verify every file
    restored_bytes = 0
    correctness = True
    t0 = time.time()
    for commit in reversed(commits):
        sh([OXEN, "checkout", commit_ids[commit]], repo)
        for name, meta in manifest[commit].items():
            digest = sha256_file(os.path.join(repo, name))
            restored_bytes += meta["bytes"]
            if digest != meta["sha256"]:
                correctness = False
                print(f"CORRECTNESS FAIL {commit}/{name}", file=sys.stderr)
    restore_seconds = time.time() - t0

    # ---- sequential read: re-materialize the largest file from the store
    sh([OXEN, "checkout", "main"], repo)
    last = commits[-1]
    big_name = max(manifest[last], key=lambda n: manifest[last][n]["bytes"])
    # find the newest commit that contains big_name
    big_commit = max(c for c in commits if big_name in manifest[c])
    big_bytes = manifest[big_commit][big_name]["bytes"]
    os.remove(os.path.join(repo, big_name))
    t0 = time.time()
    sh([OXEN, "restore", big_name], repo)
    seq_seconds = time.time() - t0
    if sha256_file(os.path.join(repo, big_name)) != manifest[big_commit][big_name]["sha256"]:
        correctness = False
        print(f"CORRECTNESS FAIL sequential-read {big_name}", file=sys.stderr)

    # ---- random reads: single files from random historical commits
    rrng = random.Random(7)
    samples = []
    pool = [(c, n) for c in commits for n in manifest[c]]
    for c, n in rrng.sample(pool, min(12, len(pool))):
        target = os.path.join(repo, n)
        if os.path.exists(target):
            os.remove(target)
        t0 = time.time()
        sh([OXEN, "restore", "--source", commit_ids[c], n], repo)
        samples.append((time.time() - t0) * 1000)
        if sha256_file(target) != manifest[c][n]["sha256"]:
            correctness = False
            print(f"CORRECTNESS FAIL random-read {c}/{n}", file=sys.stderr)
    samples.sort()
    random_read_ms = samples[len(samples) // 2]

    score = stored_bytes / source_bytes
    print("---")
    print(f"score:                     {score:.6f}")
    print(f"source_bytes:              {source_bytes}")
    print(f"stored_bytes:              {stored_bytes}")
    print(f"storage_ratio:             {score:.6f}")
    print(f"incremental_stored_bytes:  {incremental_stored}")
    print(f"incremental_ratio:         {incremental_stored / incremental_source:.6f}")
    print(f"metadata_bytes:            {metadata_bytes}")
    print(f"chunk_index_bytes:         {index_b}")
    print(f"stored_after_each:         {json.dumps({c: stored_after[c] for c in commits})}")
    print(f"restore_seconds:           {restore_seconds:.1f}")
    print(f"restore_mb_s:              {restored_bytes / restore_seconds / 1e6:.1f}")
    print(f"sequential_read_mb_s:      {big_bytes / seq_seconds / 1e6:.1f}")
    print(f"random_read_ms:            {random_read_ms:.1f}")
    print(f"compression_seconds:       {t_compress:.1f}")
    print(f"compression_mb_s:          {source_bytes / t_compress / 1e6:.1f}")
    print(f"version_add_seconds:       {version_add_seconds}")
    print(f"peak_memory_mb:            {child_peak_rss_mb():.1f}")
    print(f"correctness:               {'pass' if correctness else 'FAIL'}")
    sys.exit(0 if correctness else 1)


if __name__ == "__main__":
    main()
