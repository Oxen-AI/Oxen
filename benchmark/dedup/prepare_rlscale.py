#!/usr/bin/env python3
"""Prepare the RL-scale benchmark corpus: agent rollout data at the 10 GB range.

Models a reinforcement-learning fine-tuning pipeline committing its rollout
dataset once per training iteration: each iteration appends a large batch of
fresh rollouts under the current policy, relabels rewards on a recent window
when the reward model updates, and periodically drops low-reward rollouts
(dataset curation). Every rollout carries its policy version's config prompt
(the verbatim task + tool prefix), which changes when the policy version bumps.

80 iterations, snapshot ~60 MB → ~190 MB, ~10 GB logical total. Generation is
streaming (line-level transforms of the previous snapshot), so memory stays
bounded regardless of scale.

    python3 benchmark/dedup/prepare_rlscale.py

Output: benchmark/dedup/corpus-rlscale/ (~10 GB) with a sha256 manifest. The
corpus is disposable after measurement — this generator reproduces it exactly.
"""

import hashlib
import json
import os
import random
import re
import shutil

SEED = 20260780
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "corpus-rlscale")
ITERATIONS = 80

rng = random.Random(SEED)

SYLLABLES = ["an", "ba", "co", "de", "en", "fi", "ga", "ho", "in", "ju", "ka",
             "lo", "ma", "ne", "or", "pa", "qui", "ra", "se", "ta", "un", "va",
             "wo", "xe", "yo", "zu", "ther", "ing", "tion", "ment", "able"]
_vr = random.Random(99)
VOCAB = ["".join(_vr.choice(SYLLABLES) for _ in range(_vr.randint(1, 3))) for _ in range(1600)]


def words(n):
    return " ".join(rng.choice(VOCAB) for _ in range(n))


def paragraph(n):
    return " ".join((words(rng.randint(6, 16)).capitalize() + ".") for _ in range(n))


def policy_config(ver):
    """The verbatim config prefix every rollout of a policy version carries."""
    tools = [{"name": n, "description": paragraph(4)}
             for n in ["browse", "execute", "submit", "verify"]]
    return (f"policy ppo-v{ver}: You are an autonomous agent in a software "
            f"environment. " + paragraph(30) + " Tools: " + json.dumps(tools))


def rollout(policy_ver, config, iteration):
    steps = []
    for t in range(rng.randint(8, 26)):
        steps.append({"t": t,
                      "obs": paragraph(rng.randint(1, 4)),
                      "action": {"tool": rng.choice(["browse", "execute", "submit", "verify"]),
                                 "args": {"input": words(rng.randint(4, 20))}},
                      "reward": round(rng.random() * 0.2, 4)})
    total = round(sum(s["reward"] for s in steps) + rng.random(), 4)
    return {"rollout_id": "%016x" % rng.getrandbits(64),
            "policy": f"ppo-v{policy_ver}", "iteration": iteration,
            "config": config, "steps": steps, "total_reward": total}


REWARD_RE = re.compile(rb'"total_reward":[0-9.]+}$')


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    policy_ver = 1
    config = policy_config(policy_ver)
    manifest = {}
    prev_path = None
    # Mirrors file line order: iteration each line was appended at.
    line_iters = []

    for it in range(1, ITERATIONS + 1):
        cdir = os.path.join(OUT, f"c{it:02d}")
        os.makedirs(cdir, exist_ok=True)
        path = os.path.join(cdir, "rollouts.jsonl")

        # New policy version every 10 iterations: fresh config prefix for new
        # rollouts from here on (old rollouts keep the config they ran under).
        if it > 1 and it % 10 == 0:
            policy_ver += 1
            config = policy_config(policy_ver)

        # Reward-model update every 5th iteration: relabel ~2% of rollouts from
        # the last 10 iterations (streamed, line-local edits).
        relabel = set()
        if it > 1 and it % 5 == 0:
            recent = [i for i, li in enumerate(line_iters) if li >= it - 10]
            if recent:
                relabel = set(rng.sample(recent, max(1, len(recent) // 50)))

        # Curation every 15th iteration: drop 4% of rollouts older than 20
        # iterations (low-reward pruning), streamed by line index.
        drop = set()
        if it > 1 and it % 15 == 0:
            old = [i for i, li in enumerate(line_iters) if li < it - 20]
            if old:
                drop = set(rng.sample(old, max(1, len(old) // 25)))

        h = hashlib.sha256()
        size = 0
        with open(path, "wb") as out:
            def emit(line):
                nonlocal size
                out.write(line)
                h.update(line)
                size += len(line)

            new_line_iters = []
            if prev_path:
                with open(prev_path, "rb") as f:
                    for idx, line in enumerate(f):
                        if idx in drop:
                            continue
                        if idx in relabel:
                            new_total = str(round(rng.random() * 4, 4)).encode()
                            line = REWARD_RE.sub(b'"total_reward":' + new_total + b"}", line)
                        emit(line)
                        new_line_iters.append(line_iters[idx])
            # Fresh rollout batch for this iteration.
            batch = rng.randint(110, 190) if it > 1 else 4500
            for _ in range(batch):
                r = rollout(policy_ver, config, it)
                emit((json.dumps(r, separators=(",", ":")) + "\n").encode())
                new_line_iters.append(it)
            line_iters = new_line_iters

        manifest[f"c{it:02d}"] = {"rollouts.jsonl": {
            "sha256": h.hexdigest(), "bytes": size}}
        prev_path = path
        if it % 10 == 0 or it == 1:
            print(f"  c{it:02d}: snapshot {size/1e6:.0f} MB", flush=True)

    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
    sizes = [manifest[c]["rollouts.jsonl"]["bytes"] for c in sorted(manifest)]
    print(f"{ITERATIONS} iterations, snapshot {sizes[0]/1e6:.0f} -> {sizes[-1]/1e6:.0f} MB, "
          f"total logical {sum(sizes)/1e9:.2f} GB")


if __name__ == "__main__":
    main()
