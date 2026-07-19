#!/usr/bin/env python3
"""Prepare the prompt-cache-structured benchmark corpus (corpus v2).

Models the redundancy structure that makes provider-side prompt caching work
(strict tools -> system -> messages prefix hierarchy, exact-prefix matching,
cacheable prefixes of 2-40 KB), as it appears in stored agent-trace datasets:

- bigprompt_traces.jsonl  — session-per-row traces where every row of an agent
  config starts with that config's verbatim system-prompt + tool-definitions
  prefix (12-40 KB). Versions include appends, scattered growth, a config
  "rollout" that edits one config's prompt (the cache-invalidation analog),
  and a full row shuffle.
- request_log.jsonl       — request-per-row logging: each row is the full
  request snapshot (tools, system, message prefix) at one API call, so
  consecutive rows of a session share everything but the newest turns.
- real_sessions/*.jsonl   — 10 real Claude Code sessions from the public
  trace-commons/agent-traces dataset (CC-BY-4.0), versioned as live-logged
  append-only files: each commit extends every session's event stream and new
  sessions appear over time.

Deterministic (seed-pinned); the manifest pins sha256 of every file. Run:

    python3 benchmark/dedup/prepare_promptcache.py

Requires benchmark/dedup/corpus-v2-src/*.jsonl (the downloaded real sessions,
pinned in manifest). Output: benchmark/dedup/corpus-promptcache/. Do not modify
between experiments.
"""

import glob
import hashlib
import json
import os
import random
import shutil

SEED = 20260719
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "corpus-v2-src")
OUT = os.path.join(HERE, "corpus-promptcache")

rng = random.Random(SEED)

SYLLABLES = ["an", "ba", "co", "de", "en", "fi", "ga", "ho", "in", "ju", "ka",
             "lo", "ma", "ne", "or", "pa", "qui", "ra", "se", "ta", "un", "va",
             "wo", "xe", "yo", "zu", "ther", "ing", "tion", "ment", "able"]
_vr = random.Random(77)
VOCAB = ["".join(_vr.choice(SYLLABLES) for _ in range(_vr.randint(1, 3))) for _ in range(1200)]
COMMON = ["the", "a", "to", "of", "and", "is", "in", "it", "you", "that",
          "for", "on", "with", "as", "this", "can", "we", "be", "not", "are"]


def words(n):
    return " ".join(rng.choice(COMMON) if rng.random() < 0.45 else rng.choice(VOCAB)
                    for _ in range(n))


def sentence():
    s = words(rng.randint(6, 18))
    return s[0].upper() + s[1:] + "."


def paragraph(n):
    return " ".join(sentence() for _ in range(n))


# ---------------------------------------------------------------- tool corpus
TOOL_NAMES = ["read_file", "write_file", "edit_file", "bash", "grep_search",
              "list_dir", "web_fetch", "run_tests", "git_diff", "sql_query",
              "browser_click", "deploy_preview", "notebook_edit", "task_create"]


def tool_definition(name, detail_sentences):
    """A realistic verbose tool schema (~0.8-2.5 KB, like production agents)."""
    params = {}
    for _ in range(rng.randint(2, 5)):
        params[rng.choice(VOCAB)] = {
            "type": rng.choice(["string", "integer", "boolean"]),
            "description": paragraph(rng.randint(1, 2)),
        }
    return {
        "name": name,
        "description": paragraph(detail_sentences),
        "input_schema": {
            "type": "object",
            "properties": params,
            "required": sorted(params)[: rng.randint(1, len(params))],
        },
    }


def agent_config(idx):
    """One agent configuration: the verbatim cacheable prefix every session of
    this config carries (system prompt + tool definitions, 12-40 KB)."""
    n_tools = rng.randint(6, 12)
    tools = [tool_definition(name, rng.randint(6, 14))
             for name in rng.sample(TOOL_NAMES, n_tools)]
    prompt = (f"You are agent configuration {idx}, an autonomous software agent. "
              + paragraph(rng.randint(20, 45))
              + "\n\n## Operating rules\n" + paragraph(rng.randint(15, 30))
              + "\n\n## Available tools\n" + json.dumps(tools))
    return prompt


def turns(n):
    out = []
    for _ in range(n):
        out.append({"role": "user", "content": paragraph(rng.randint(1, 3))})
        if rng.random() < 0.5:
            out.append({"role": "assistant", "content": "",
                        "tool_calls": [{"name": rng.choice(TOOL_NAMES),
                                        "arguments": {"path": f"src/{rng.choice(VOCAB)}.py"}}]})
            out.append({"role": "tool", "content": paragraph(rng.randint(2, 8))})
        out.append({"role": "assistant", "content": paragraph(rng.randint(2, 6))})
    return out


def random_uuid():
    return "%08x-%04x-%04x-%04x-%012x" % (
        rng.getrandbits(32), rng.getrandbits(16), rng.getrandbits(16),
        rng.getrandbits(16), rng.getrandbits(48))


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    commits = {}

    def snap(name, commit, data):
        commits.setdefault(commit, {})[name] = data

    # ---- bigprompt_traces.jsonl: session-per-row, shared config prefixes
    configs = [agent_config(i) for i in range(4)]

    def new_bigprompt_row(config_idx=None):
        ci = config_idx if config_idx is not None else rng.randrange(len(configs))
        msgs = [{"role": "system", "content": configs[ci]}]
        msgs += turns(rng.randint(1, 4))
        return {"uuid": random_uuid(), "config": ci, "messages": msgs,
                "source": rng.choice(["web", "api", "batch"])}

    def bigprompt_bytes(rows):
        return ("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows)).encode()

    bigprompt = [new_bigprompt_row() for _ in range(1200)]

    # ---- request_log.jsonl: request-per-row (full snapshot per API call)
    req_tools = [tool_definition(n, rng.randint(4, 8)) for n in TOOL_NAMES[:8]]
    req_system = "You are a support agent. " + paragraph(18)

    class Conversation:
        def __init__(self):
            self.session = random_uuid()
            self.msgs = []
            self.seq = 0

        def request_rows(self, n_requests, log):
            for _ in range(n_requests):
                self.msgs += turns(1)
                self.seq += 1
                log.append({"session": self.session, "seq": self.seq,
                            "tools": req_tools, "system": req_system,
                            "messages": list(self.msgs)})

    request_log = []
    conversations = [Conversation() for _ in range(150)]
    for c in conversations:
        c.request_rows(rng.randint(1, 5), request_log)

    def request_bytes():
        return ("".join(json.dumps(r, separators=(",", ":")) + "\n"
                        for r in request_log)).encode()

    # ---- real sessions: live-logged growing prefixes
    session_files = sorted(glob.glob(os.path.join(SRC, "*.jsonl")))
    if len(session_files) < 10:
        raise SystemExit(f"expected 10 downloaded sessions in {SRC}")
    session_lines = {os.path.basename(p): open(p, "rb").read().splitlines(keepends=True)
                     for p in session_files}
    names = sorted(session_lines)

    def real_prefix(name, frac):
        lines = session_lines[name]
        return b"".join(lines[: max(1, int(len(lines) * frac))])

    # ---- commit lifecycle
    # c1: base snapshots; 6 real sessions live at 40%
    snap("bigprompt_traces.jsonl", "c1", bigprompt_bytes(bigprompt))
    snap("request_log.jsonl", "c1", request_bytes())
    for name in names[:6]:
        snap(f"real_{name}", "c1", real_prefix(name, 0.4))

    # c2: bigprompt append; real sessions grow to 65%
    bigprompt += [new_bigprompt_row() for _ in range(300)]
    snap("bigprompt_traces.jsonl", "c2", bigprompt_bytes(bigprompt))
    for name in names[:6]:
        snap(f"real_{name}", "c2", real_prefix(name, 0.65))

    # c3: request log: every conversation continues, new conversations appear;
    #     real sessions grow to 85% and two new sessions appear
    for c in conversations:
        if rng.random() < 0.8:
            c.request_rows(rng.randint(1, 3), request_log)
    fresh = [Conversation() for _ in range(40)]
    for c in fresh:
        c.request_rows(rng.randint(1, 4), request_log)
    conversations.extend(fresh)
    snap("request_log.jsonl", "c3", request_bytes())
    for name in names[:6]:
        snap(f"real_{name}", "c3", real_prefix(name, 0.85))
    for name in names[6:8]:
        snap(f"real_{name}", "c3", real_prefix(name, 0.5))

    # c4: bigprompt scattered growth; request log continues
    for r in bigprompt:
        if rng.random() < 0.15:
            r["messages"] += turns(rng.randint(1, 3))
    snap("bigprompt_traces.jsonl", "c4", bigprompt_bytes(bigprompt))
    for c in conversations:
        if rng.random() < 0.5:
            c.request_rows(rng.randint(1, 2), request_log)
    snap("request_log.jsonl", "c4", request_bytes())

    # c5: config-0 rollout — the cache-invalidation analog: every config-0 row's
    #     prefix changes; real sessions reach 100%
    configs[0] = configs[0].replace("## Operating rules",
                                    "## Operating rules (revision B)")
    for r in bigprompt:
        if r["config"] == 0:
            r["messages"][0]["content"] = configs[0]
    snap("bigprompt_traces.jsonl", "c5", bigprompt_bytes(bigprompt))
    for name in names[:8]:
        snap(f"real_{name}", "c5", real_prefix(name, 1.0))
    for name in names[8:]:
        snap(f"real_{name}", "c5", real_prefix(name, 0.7))

    # c6: bigprompt full shuffle + append; last sessions complete
    rng.shuffle(bigprompt)
    bigprompt += [new_bigprompt_row() for _ in range(250)]
    snap("bigprompt_traces.jsonl", "c6", bigprompt_bytes(bigprompt))
    for name in names[8:]:
        snap(f"real_{name}", "c6", real_prefix(name, 1.0))

    # ---- write corpus + manifest
    manifest = {}
    for commit in sorted(commits):
        cdir = os.path.join(OUT, commit)
        os.makedirs(cdir, exist_ok=True)
        for name, data in commits[commit].items():
            with open(os.path.join(cdir, name), "wb") as f:
                f.write(data)
            manifest.setdefault(commit, {})[name] = {
                "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)

    total = sum(v["bytes"] for c in manifest.values() for v in c.values())
    for commit in sorted(manifest):
        csize = sum(v["bytes"] for v in manifest[commit].values())
        print(f"  {commit}: {len(manifest[commit])} files, {csize/1e6:.1f} MB")
    print(f"total logical bytes: {total/1e6:.1f} MB")


if __name__ == "__main__":
    main()
