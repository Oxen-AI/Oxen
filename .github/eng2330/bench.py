"""Time `oxen add` for each arm on each workload, alternating arms within every round.

Usage: bench.py <work dir> <rounds> <syncount dylib or "-"> <arm>=<oxen binary>[|<VAR>=<value>]...

Each trial copies a workload's files into a fresh repository, runs `oxen init` and a `sync` outside
the timed window, then times `oxen add files` alone, and checks every version file was stored. One
extra untimed trial per arm and workload counts sync calls through the interposer. Prints one JSON
line per trial."""
import json
import os
import shutil
import subprocess
import sys
import time

work, rounds, dylib = sys.argv[1], int(sys.argv[2]), sys.argv[3]
# An arm is a binary plus the environment variables it runs with
arms = {}
for arg in sys.argv[4:]:
    arm, spec = arg.split("=", 1)
    binary, *assignments = spec.split("|")
    arms[arm] = (binary, dict(a.split("=", 1) for a in assignments))

WORKLOADS = {
    # Oxen issue 1124: 500 files of 1 KiB in one directory
    "small-500": [("", 500, 1024)],
    # 500 files of 1 KiB spread over 50 directories
    "small-500-in-50-dirs": [(f"d{d:02d}", 10, 1024) for d in range(50)],
    # 8 files of 64 MiB, where copying the bytes dominates
    "large-8x64MiB": [("", 8, 64 * 1024 * 1024)],
}


def make_fixture(name, layout):
    root = os.path.join(work, "fixtures", name)
    if os.path.isdir(root):
        return root
    os.makedirs(root)
    n = 0
    for subdir, count, size in layout:
        d = os.path.join(root, subdir)
        os.makedirs(d, exist_ok=True)
        for _ in range(count):
            # Distinct contents per file, so every file gets its own version file
            block = (b"%016d" % n) * 64
            with open(os.path.join(d, f"f{n:05d}.bin"), "wb") as f:
                written = 0
                while written < size:
                    chunk = block[: size - written]
                    f.write(chunk)
                    written += len(chunk)
            n += 1
    return root


def trial(arm, spec, name, fixture, expected, count_syncs):
    binary, arm_env = spec
    repo = os.path.join(work, "trial")
    shutil.rmtree(repo, ignore_errors=True)
    os.makedirs(repo)
    shutil.copytree(fixture, os.path.join(repo, "files"))
    subprocess.run([binary, "init"], cwd=repo, check=True, capture_output=True)
    if hasattr(os, "sync"):
        os.sync()
    env = dict(os.environ, **arm_env)
    syncs_file = os.path.join(work, "syncs")
    command = [binary, "add", "files"]
    if count_syncs and dylib == "strace":
        calls = "trace=fsync,fdatasync,syncfs,sync,sync_file_range"
        command = ["strace", "-f", "-c", "-o", syncs_file, "-e", calls] + command
    elif count_syncs:
        env["DYLD_INSERT_LIBRARIES"] = dylib
        env["SYNCOUNT_FILE"] = syncs_file
    start = time.perf_counter()
    done = subprocess.run(command, cwd=repo, env=env, capture_output=True)
    seconds = time.perf_counter() - start
    stored = sum(
        1
        for _, _, names in os.walk(os.path.join(repo, ".oxen", "versions"))
        for f in names
        if f == "data"
    )
    row = {
        "arm": arm,
        "workload": name,
        "seconds": round(seconds, 4),
        "exit": done.returncode,
        "versions_stored": stored,
        "versions_expected": expected,
        "load1": round(os.getloadavg()[0], 2) if hasattr(os, "getloadavg") else None,
    }
    if count_syncs and dylib == "strace":
        # strace -c's table: "% time  seconds  usecs/call  calls  errors  syscall"
        counts = {}
        with open(syncs_file) as f:
            for line in f:
                parts = line.split()
                if parts and parts[-1] in calls.split("=")[1].split(","):
                    counts[parts[-1]] = int(parts[3])
        row["syncs"] = counts
        row["counted"] = True
    elif count_syncs:
        with open(syncs_file) as f:
            row["syncs_fsync_fdatasync_full_barrier"] = f.read().split()
        row["counted"] = True
    if done.returncode:
        row["stderr"] = done.stderr.decode(errors="replace")[-2000:]
    print(json.dumps(row), flush=True)


fixtures = {name: make_fixture(name, layout) for name, layout in WORKLOADS.items()}
expected = {name: sum(c for _, c, _ in layout) for name, layout in WORKLOADS.items()}
if dylib != "-":
    for arm, spec in arms.items():
        for name in WORKLOADS:
            trial(arm, spec, name, fixtures[name], expected[name], True)
order = list(arms.items())
for r in range(rounds):
    for arm, spec in order if r % 2 == 0 else order[::-1]:
        for name in WORKLOADS:
            trial(arm, spec, name, fixtures[name], expected[name], False)
shutil.rmtree(os.path.join(work, "trial"), ignore_errors=True)
