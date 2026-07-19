#!/usr/bin/env python3
"""Prepare the long-horizon benchmark corpus (corpus v3): one agent-trace table,
exported and committed daily for 60 days.

The purest version-over-time workload: a single `traces.jsonl` whose sessions
grow and accumulate the way a production trace table does. Daily mix (seeded,
deterministic): new sessions appended; a subset of recent sessions grow;
every 10th day one agent config's system prompt is revised (a rollout touching
every session of that config); day 30 re-exports in a shuffled order (an
unordered warehouse dump). Snapshot grows ~9 MB → ~45 MB; total logical
presented across 60 commits ≈ 1.6 GB.

    python3 benchmark/dedup/prepare_longhorizon.py

Output: benchmark/dedup/corpus-longhorizon/ with a sha256 manifest. Fixed once
generated — do not modify between measurements.
"""

import hashlib
import json
import os
import random
import shutil

SEED = 20260760
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "corpus-longhorizon")
DAYS = 60

rng = random.Random(SEED)

SYLLABLES = ["an", "ba", "co", "de", "en", "fi", "ga", "ho", "in", "ju", "ka",
             "lo", "ma", "ne", "or", "pa", "qui", "ra", "se", "ta", "un", "va",
             "wo", "xe", "yo", "zu", "ther", "ing", "tion", "ment", "able"]
_vr = random.Random(55)
VOCAB = ["".join(_vr.choice(SYLLABLES) for _ in range(_vr.randint(1, 3))) for _ in range(1400)]
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


TOOLS = [{"name": n, "description": paragraph(6),
          "input_schema": {"type": "object",
                           "properties": {w: {"type": "string", "description": paragraph(1)}
                                          for w in rng.sample(VOCAB, 3)}}}
         for n in ["read_file", "write_file", "bash", "search", "run_tests", "web_fetch"]]

CONFIGS = [
    ("You are a coding agent. " + paragraph(24) + "\n\nTools:\n" + json.dumps(rng.sample(TOOLS, 4)))
    for _ in range(3)
]


def turns(n):
    out = []
    for _ in range(n):
        out.append({"role": "user", "content": paragraph(rng.randint(1, 3))})
        if rng.random() < 0.45:
            out.append({"role": "assistant", "content": "",
                        "tool_calls": [{"name": rng.choice(TOOLS)["name"],
                                        "arguments": {"path": f"src/{rng.choice(VOCAB)}.py"}}]})
            out.append({"role": "tool", "content": paragraph(rng.randint(2, 6))})
        out.append({"role": "assistant", "content": paragraph(rng.randint(2, 6))})
    return out


def random_uuid():
    return "%08x-%04x-%04x-%04x-%012x" % (
        rng.getrandbits(32), rng.getrandbits(16), rng.getrandbits(16),
        rng.getrandbits(16), rng.getrandbits(48))


def new_session(day):
    ci = rng.randrange(len(CONFIGS))
    msgs = [{"role": "system", "content": CONFIGS[ci]}]
    msgs += turns(rng.randint(1, 4))
    return {"uuid": random_uuid(), "config": ci, "day": day,
            "messages": msgs, "source": rng.choice(["web", "api", "batch"])}


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    sessions = [new_session(0) for _ in range(3000)]
    manifest = {}
    for day in range(1, DAYS + 1):
        if day > 1:
            # New traffic: sessions appended at the end of the export.
            sessions += [new_session(day) for _ in range(rng.randint(60, 140))]
            # Recent sessions keep going (recency-weighted scatter).
            for s in sessions:
                age = day - s["day"]
                p = 0.25 if age <= 2 else (0.06 if age <= 10 else 0.01)
                if rng.random() < p:
                    s["messages"] += turns(rng.randint(1, 3))
            # Every 10th day: one config's prompt is revised — every session of
            # that config changes at its prefix (the rollout/invalidation event).
            if day % 10 == 0:
                ci = (day // 10 - 1) % len(CONFIGS)
                CONFIGS[ci] = CONFIGS[ci].replace(
                    "You are a coding agent.",
                    f"You are a coding agent (policy rev {day}).")
                for s in sessions:
                    if s["config"] == ci:
                        s["messages"][0]["content"] = CONFIGS[ci]
            # Day 30: the warehouse re-exports in a different order.
            if day == 30:
                rng.shuffle(sessions)

        data = ("".join(
            json.dumps({k: s[k] for k in ("uuid", "messages", "source")},
                       separators=(",", ":")) + "\n"
            for s in sessions)).encode()
        commit = f"c{day:02d}"
        cdir = os.path.join(OUT, commit)
        os.makedirs(cdir, exist_ok=True)
        with open(os.path.join(cdir, "traces.jsonl"), "wb") as f:
            f.write(data)
        manifest[commit] = {"traces.jsonl": {
            "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}}

    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
    sizes = [manifest[c]["traces.jsonl"]["bytes"] for c in sorted(manifest)]
    print(f"{DAYS} commits, snapshot {sizes[0]/1e6:.1f} -> {sizes[-1]/1e6:.1f} MB, "
          f"total logical {sum(sizes)/1e6:.0f} MB")


if __name__ == "__main__":
    main()
