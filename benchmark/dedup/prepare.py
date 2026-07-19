#!/usr/bin/env python3
"""Prepare the fixed benchmark corpus for block-level dedup research.

Generates a deterministic (seed-pinned) multi-version corpus of agent-trace and
fine-tuning datasets in JSONL, CSV, and Parquet, laid out as one directory per
oxen commit. `benchmark.py` replays these commits against a block-v1 oxen repo.

Run with the oxen-python venv (needs pyarrow for the Parquet mirrors):

    oxen-python/.venv/bin/python benchmark/dedup/prepare.py

The corpus lands in benchmark/dedup/corpus/ (untracked) with a manifest.json of
sha256 digests. The generator and seed are FIXED for a research run: do not
modify this file between experiments — storage results across experiments are
only comparable against the identical corpus.

Version lifecycle exercised (per the autoresearch guide):
  c1  base snapshots (traces.jsonl, finetune.jsonl, labels.csv)
  c2  traces: append-only growth (new sessions at EOF)
  c3  traces: scattered in-place session growth; finetune: append
  c4  traces: full row reorder + growth; labels: append
  c5  traces: shared system-prompt edit + row deletes + inserts; labels: scattered edits
  c6  traces: large append; finetune: filtered copy as a second file (cross-file
      dedup); labels: annotation column added (full rewrite)
  c7  parquet mirrors of traces c1 and c4 content (cross-format redundancy)
"""

import hashlib
import json
import os
import random
import shutil

SEED = 20260718
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpus")

rng = random.Random(SEED)

# ---------------------------------------------------------------- text corpus
SYLLABLES = [
    "an", "ba", "co", "de", "en", "fi", "ga", "ho", "in", "ju", "ka", "lo",
    "ma", "ne", "or", "pa", "qui", "ra", "se", "ta", "un", "va", "wo", "xe",
    "yo", "zu", "ther", "ing", "tion", "ment", "able", "ward", "ness", "ful",
]
_vr = random.Random(99)
VOCAB = ["".join(_vr.choice(SYLLABLES) for _ in range(_vr.randint(1, 3))) for _ in range(1500)]
COMMON = ["the", "a", "to", "of", "and", "is", "in", "it", "you", "that",
          "for", "on", "with", "as", "this", "can", "we", "be", "not", "are"]

SYSTEM_PROMPTS = [
    "You are a helpful coding agent. You have access to a set of tools for reading, "
    "writing, and executing code. Always explain your reasoning before acting, keep "
    "changes minimal, and run the test suite after every modification you make.",
    "You are a data analysis agent. Use the provided tools to load datasets, compute "
    "statistics, and generate plots. Prefer reproducible scripts over ad-hoc commands "
    "and always report the exact rows and columns you used for each conclusion.",
    "You are a customer support agent for a developer tools company. Be friendly and "
    "precise, cite documentation where possible, and escalate to a human when the "
    "user reports data loss, billing problems, or security concerns.",
    "You are a research assistant. Break every task into explicit steps, cite your "
    "sources, quantify uncertainty, and clearly separate observations from "
    "hypotheses in every summary you produce.",
]

TOOL_SCHEMAS = [
    {"name": "read_file", "description": "Read a file from the workspace",
     "parameters": {"type": "object", "properties": {"path": {"type": "string"}},
                    "required": ["path"]}},
    {"name": "write_file", "description": "Write content to a file",
     "parameters": {"type": "object", "properties": {"path": {"type": "string"},
                    "content": {"type": "string"}}, "required": ["path", "content"]}},
    {"name": "run_tests", "description": "Run the project test suite",
     "parameters": {"type": "object", "properties": {"filter": {"type": "string"}}}},
    {"name": "search_code", "description": "Search the codebase for a pattern",
     "parameters": {"type": "object", "properties": {"pattern": {"type": "string"},
                    "max_results": {"type": "integer"}}, "required": ["pattern"]}},
    {"name": "sql_query", "description": "Run a read-only SQL query",
     "parameters": {"type": "object", "properties": {"query": {"type": "string"}},
                    "required": ["query"]}},
]

CODE_TEMPLATES = [
    "def load_{n}(path):\n    with open(path) as f:\n        rows = [json.loads(l) for l in f]\n    return [r for r in rows if r.get('{k}')]\n",
    "class {N}Processor:\n    def __init__(self, config):\n        self.config = config\n        self.cache = {{}}\n\n    def process(self, batch):\n        return [self.transform(x) for x in batch]\n",
    "SELECT {k}, COUNT(*) as n\nFROM events\nWHERE created_at > NOW() - INTERVAL '7 days'\nGROUP BY {k}\nORDER BY n DESC\nLIMIT 20;\n",
    "for path in sorted(glob.glob('data/*.jsonl')):\n    with open(path) as f:\n        for line in f:\n            row = json.loads(line)\n            counts[row['{k}']] += 1\n",
]

STACK_TRACE = (
    "Traceback (most recent call last):\n"
    '  File "pipeline.py", line {ln}, in process_batch\n'
    "    result = transform(row['{k}'])\n"
    '  File "transform.py", line 88, in transform\n'
    "    return schema.validate(value)\n"
    "KeyError: '{k}'\n"
)

LOG_FRAGMENT = (
    "[2026-07-{d:02d}T10:{m:02d}:12Z] INFO worker-{w} processed batch={b} rows=1000 "
    "ok=998 failed=2 retry_queued=2 elapsed_ms={ms}\n"
)


def words(n):
    return " ".join(
        rng.choice(COMMON) if rng.random() < 0.45 else rng.choice(VOCAB) for _ in range(n)
    )


def sentence():
    s = words(rng.randint(5, 18))
    return s[0].upper() + s[1:] + rng.choice([".", ".", ".", "?", "!"])


def paragraph(n):
    return " ".join(sentence() for _ in range(n))


def code_block():
    t = rng.choice(CODE_TEMPLATES)
    return "```\n" + t.format(
        n=rng.randint(1, 30), k=rng.choice(VOCAB), N=rng.choice(VOCAB).capitalize(),
        ln=rng.randint(10, 400),
    ) + "```"


def tool_output():
    r = rng.random()
    if r < 0.3:
        return STACK_TRACE.format(ln=rng.randint(10, 400), k=rng.choice(VOCAB))
    if r < 0.6:
        return "".join(
            LOG_FRAGMENT.format(d=rng.randint(1, 18), m=rng.randint(0, 59),
                                w=rng.randint(1, 8), b=rng.randint(100, 999),
                                ms=rng.randint(20, 900))
            for _ in range(rng.randint(2, 6))
        )
    return json.dumps({"status": "ok", "rows": rng.randint(1, 5000),
                       "columns": rng.sample(VOCAB, rng.randint(2, 6))})


def random_uuid():
    return "%08x-%04x-%04x-%04x-%012x" % (
        rng.getrandbits(32), rng.getrandbits(16), rng.getrandbits(16),
        rng.getrandbits(16), rng.getrandbits(48))


def agent_turns(n_steps):
    msgs = []
    for _ in range(n_steps):
        msgs.append({"role": "user", "content": paragraph(rng.randint(1, 3))})
        if rng.random() < 0.5:
            tool = rng.choice(TOOL_SCHEMAS)
            msgs.append({"role": "assistant", "content": "",
                         "tool_calls": [{"name": tool["name"],
                                         "arguments": {"path": f"src/{rng.choice(VOCAB)}.py"}
                                         if "path" in str(tool) else {"query": sentence()}}]})
            msgs.append({"role": "tool", "content": tool_output()})
        content = paragraph(rng.randint(2, 6))
        if rng.random() < 0.35:
            content += "\n\n" + code_block()
        msgs.append({"role": "assistant", "content": content})
    return msgs


def new_trace(prompt_idx=None):
    idx = prompt_idx if prompt_idx is not None else rng.randrange(len(SYSTEM_PROMPTS))
    tools = rng.sample(TOOL_SCHEMAS, rng.randint(2, 3))
    system = SYSTEM_PROMPTS[idx] + "\n\nAvailable tools:\n" + json.dumps(tools)
    msgs = [{"role": "system", "content": system}]
    msgs += agent_turns(rng.randint(1, 5))
    return {"uuid": random_uuid(), "messages": msgs,
            "source": rng.choice(["web", "api", "cli", "batch"]),
            "_prompt_idx": idx}


def trace_row(t):
    return json.dumps({k: t[k] for k in ("uuid", "messages", "source")},
                      separators=(",", ":")) + "\n"


def write_traces(traces, path):
    with open(path, "w") as f:
        for t in traces:
            f.write(trace_row(t))


def finetune_row(i, quality=None):
    template = rng.randrange(6)
    prompt = (f"Given the following {rng.choice(VOCAB)} description, classify it into one "
              f"of the standard categories and explain your choice: {paragraph(1)}")
    row = {"prompt": prompt, "completion": paragraph(rng.randint(1, 3)),
           "meta": {"template": template, "lang": "en", "id": i}}
    if quality is not None:
        row["meta"]["quality"] = quality
    return row


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    commits = {}

    # ---- traces.jsonl lifecycle
    traces = [new_trace() for _ in range(3000)]
    commits["c1"] = {}
    snapshots = {}

    def snap(name, commit, content_bytes):
        commits.setdefault(commit, {})[name] = content_bytes

    def traces_bytes():
        return "".join(trace_row(t) for t in traces).encode()

    snap("traces.jsonl", "c1", traces_bytes())

    # finetune + labels base
    finetune = [finetune_row(i) for i in range(8000)]
    ft_bytes = ("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in finetune)).encode()
    snap("finetune.jsonl", "c1", ft_bytes)

    labels = [(f"images/img_{i:06d}.jpg", f"label_{i % 13}", f"{rng.random():.4f}")
              for i in range(100_000)]

    def labels_bytes(cols3=True):
        header = "file,label,confidence\n" if cols3 else "file,label,confidence,reviewer\n"
        return (header + "".join(",".join(r) + "\n" for r in labels)).encode()

    snap("labels.csv", "c1", labels_bytes())

    # c2: traces append-only
    traces += [new_trace() for _ in range(600)]
    snap("traces.jsonl", "c2", traces_bytes())

    # c3: scattered growth + finetune append
    for t in traces:
        if rng.random() < 0.15:
            t["messages"] += agent_turns(rng.randint(1, 3))
    snap("traces.jsonl", "c3", traces_bytes())
    finetune += [finetune_row(i) for i in range(8000, 12000)]
    ft_bytes = ("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in finetune)).encode()
    snap("finetune.jsonl", "c3", ft_bytes)

    # c4: reorder + growth; labels append. Keep a parquet mirror source.
    rng.shuffle(traces)
    for t in traces:
        if rng.random() < 0.10:
            t["messages"] += agent_turns(rng.randint(1, 2))
    c4_traces = traces_bytes()
    snap("traces.jsonl", "c4", c4_traces)
    labels += [(f"images/img_{i:06d}.jpg", f"label_{i % 13}", f"{rng.random():.4f}")
               for i in range(100_000, 120_000)]
    snap("labels.csv", "c4", labels_bytes())

    # c5: shared system-prompt edit (template 0 revised) + deletes + inserts;
    # labels scattered edits
    SYSTEM_PROMPTS[0] = SYSTEM_PROMPTS[0].replace(
        "run the test suite after every modification",
        "run the full test suite and the linter after every modification")
    for t in traces:
        if t["_prompt_idx"] == 0:
            tools_part = t["messages"][0]["content"].split("\n\nAvailable tools:\n", 1)[1]
            t["messages"][0]["content"] = SYSTEM_PROMPTS[0] + "\n\nAvailable tools:\n" + tools_part
    traces = [t for t in traces if rng.random() >= 0.02]
    insert_at = rng.sample(range(len(traces)), 100)
    for pos in sorted(insert_at, reverse=True):
        traces.insert(pos, new_trace())
    snap("traces.jsonl", "c5", traces_bytes())
    for i in rng.sample(range(len(labels)), 600):
        f_, _, c_ = labels[i]
        labels[i] = (f_, f"label_{rng.randrange(13)}", c_)
    snap("labels.csv", "c5", labels_bytes())

    # c6: big trace append; filtered finetune copy; labels annotation column
    traces += [new_trace() for _ in range(1500)]
    snap("traces.jsonl", "c6", traces_bytes())
    clean = [r for r in finetune if r["meta"]["template"] != 3]
    snap("finetune_clean.jsonl", "c6",
         ("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in clean)).encode())
    labels = [(f_, l_, c_, rng.choice(["alice", "bob", "carol", ""])) for f_, l_, c_ in labels]
    snap("labels.csv", "c6", labels_bytes(cols3=False))

    # c7: parquet mirrors (cross-format duplicates of trace content)
    import pyarrow as pa
    import pyarrow.parquet as pq
    msg_type = pa.struct([("role", pa.string()), ("content", pa.string())])
    schema = pa.schema([("uuid", pa.string()), ("messages", pa.list_(msg_type)),
                        ("source", pa.string())])

    def to_parquet_bytes(raw):
        rows = []
        for line in raw.decode().splitlines():
            r = json.loads(line)
            rows.append({"uuid": r["uuid"],
                         "messages": [{"role": m["role"], "content": m.get("content", "")}
                                      for m in r["messages"]],
                         "source": r["source"]})
        table = pa.Table.from_pylist(rows, schema=schema)
        tmp = os.path.join(OUT, "_tmp.parquet")
        pq.write_table(table, tmp, row_group_size=1000, compression="zstd")
        with open(tmp, "rb") as f:
            data = f.read()
        os.remove(tmp)
        return data

    os.makedirs(OUT, exist_ok=True)
    snap("traces_v1.parquet", "c7", to_parquet_bytes(commits["c1"]["traces.jsonl"]))
    snap("traces_v4.parquet", "c7", to_parquet_bytes(c4_traces))

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
    print(f"corpus written to {OUT}")
    for commit in sorted(manifest):
        csize = sum(v["bytes"] for v in manifest[commit].values())
        print(f"  {commit}: {len(manifest[commit])} files, {csize/1e6:.1f} MB")
    print(f"total logical bytes: {total/1e6:.1f} MB")


if __name__ == "__main__":
    main()
