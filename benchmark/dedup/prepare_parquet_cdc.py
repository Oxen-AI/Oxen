#!/usr/bin/env python3
"""Prepare paired Parquet corpora: the same evolving trace table written with and
without pyarrow's content-defined chunking (`use_content_defined_chunking`,
pyarrow >= 21).

Writer-side CDC makes data-page boundaries follow content, so unchanged row runs
produce byte-identical pages even under compression — the property that makes
Parquet deduplicable by chunk-level storage (see the Hugging Face parquet-cdc
write-up). This generator produces two corpora with identical logical content:

    corpus-pqcdc-on/   zstd Parquet, use_content_defined_chunking=True
    corpus-pqcdc-off/  zstd Parquet, default page layout (the control)

Six versions of localized evolution (how Parquet trace exports really change):
base, append, one contiguous backfill, one contiguous purge, growth
concentrated in recent sessions, append. Densely scattered row edits belong to
the JSONL profile — at page granularity they dirty every page in any layout.
Run each corpus through the fixed benchmark and compare.

    oxen-python/.venv/bin/python benchmark/dedup/prepare_parquet_cdc.py
"""

import hashlib
import json
import os
import random
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 20260721
HERE = os.path.dirname(os.path.abspath(__file__))

rng = random.Random(SEED)

SYLLABLES = ["an", "ba", "co", "de", "en", "fi", "ga", "ho", "in", "ju", "ka",
             "lo", "ma", "ne", "or", "pa", "qui", "ra", "se", "ta", "un", "va"]
_vr = random.Random(11)
VOCAB = ["".join(_vr.choice(SYLLABLES) for _ in range(_vr.randint(1, 3))) for _ in range(1200)]


def paragraph(n):
    out = []
    for _ in range(n):
        ws = [rng.choice(VOCAB) for _ in range(rng.randint(6, 16))]
        out.append(" ".join(ws).capitalize() + ".")
    return " ".join(out)


SYSTEM = "You are a coding agent. " + paragraph(20)


def random_uuid():
    return "%08x-%04x" % (rng.getrandbits(32), rng.getrandbits(16))


def new_session():
    msgs = [{"role": "system", "content": SYSTEM}]
    for _ in range(rng.randint(1, 4)):
        msgs.append({"role": "user", "content": paragraph(rng.randint(1, 3))})
        msgs.append({"role": "assistant", "content": paragraph(rng.randint(2, 6))})
    return {"uuid": random_uuid(), "messages": msgs, "source": rng.choice(["web", "api"])}


MESSAGE_TYPE = pa.struct([("role", pa.string()), ("content", pa.string())])
SCHEMA = pa.schema([("uuid", pa.string()),
                    ("messages", pa.list_(MESSAGE_TYPE)),
                    ("source", pa.string())])


def to_parquet_bytes(sessions, cdc):
    table = pa.Table.from_pylist(sessions, schema=SCHEMA)
    tmp = os.path.join(HERE, "_pqcdc_tmp.parquet")
    pq.write_table(table, tmp, compression="zstd",
                   use_content_defined_chunking=cdc)
    with open(tmp, "rb") as f:
        data = f.read()
    os.remove(tmp)
    return data


def main():
    # One deterministic logical evolution with LOCALIZED edits — how Parquet
    # exports of trace tables actually change (appends, a contiguous backfill,
    # a contiguous purge, growth concentrated in recent sessions). Densely
    # scattered row edits are the JSONL profile's territory: at page
    # granularity they invalidate every page in any layout (the same
    # rows-per-chunk amplification law measured for byte chunks).
    sessions = [new_session() for _ in range(8000)]
    versions = [[dict(s) for s in sessions]]

    sessions += [new_session() for _ in range(800)]                    # v2 append
    versions.append([dict(s) for s in sessions])

    backfill = [new_session() for _ in range(400)]                     # v3 backfill
    sessions = sessions[:4000] + backfill + sessions[4000:]
    versions.append([dict(s) for s in sessions])

    sessions = sessions[:1000] + sessions[1400:]                       # v4 purge
    versions.append([dict(s) for s in sessions])

    grown = []                                                         # v5 recent growth
    for i, s in enumerate(sessions):
        s2 = {"uuid": s["uuid"], "messages": list(s["messages"]), "source": s["source"]}
        if i >= len(sessions) - 900 and rng.random() < 0.5:
            s2["messages"] = s2["messages"] + [
                {"role": "user", "content": paragraph(2)},
                {"role": "assistant", "content": paragraph(3)}]
        grown.append(s2)
    sessions = grown
    versions.append([dict(s) for s in sessions])

    sessions += [new_session() for _ in range(600)]                    # v6 append
    versions.append([dict(s) for s in sessions])

    for variant, cdc in [("on", True), ("off", False)]:
        out = os.path.join(HERE, f"corpus-pqcdc-{variant}")
        if os.path.exists(out):
            shutil.rmtree(out)
        manifest = {}
        for i, snap in enumerate(versions, 1):
            data = to_parquet_bytes(snap, cdc)
            commit = f"c{i}"
            os.makedirs(os.path.join(out, commit), exist_ok=True)
            with open(os.path.join(out, commit, "traces.parquet"), "wb") as f:
                f.write(data)
            manifest[commit] = {"traces.parquet": {
                "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}}
        with open(os.path.join(out, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=1, sort_keys=True)
        sizes = [manifest[c]["traces.parquet"]["bytes"] for c in sorted(manifest)]
        print(f"pqcdc-{variant}: {len(sizes)} versions, "
              f"{sizes[0]/1e6:.1f} -> {sizes[-1]/1e6:.1f} MB, total {sum(sizes)/1e6:.1f} MB")


if __name__ == "__main__":
    main()
