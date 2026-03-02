# Test Dataset Scripts

Utilities for generating and restructuring synthetic datasets to test Oxen at scale.

## `generate-dataset.py`

Generate datasets with configurable file count, sizes, types, and directory layout. Runs standalone via [uv](https://docs.astral.sh/uv/) (no prior install needed):

```bash
./scripts/generate-dataset.py --help
```

### Quick start

```bash
# 1 000 text files, ~5 KB each, uniform size
./scripts/generate-dataset.py --num-files 1000 --avg-file-size 5KB --target /tmp/dataset

# 10 000 text files with log-normal file sizes
./scripts/generate-dataset.py --num-files 10k --distribution lognorm \
  --dist-params s=0.5 scale=8000 --type text --target /tmp/dataset --seed 42
```

### Modes of operation

The script offers three mutually exclusive ways to control file sizes:

| Mode | Key flags | Description |
|------|-----------|-------------|
| **Fixed size** | `--num-files`, `--avg-file-size`, `--total-size` (any 2 of 3) | Every file gets the same size. |
| **Distribution** | `--distribution`, `--dist-params`, plus `--num-files` or `--total-size` | File sizes sampled from a single [scipy.stats](https://docs.scipy.org/doc/scipy/reference/stats.html) distribution. |
| **Tier** | `--tier` (repeatable) | Multiple independent distributions in one run, optionally mixing file types. |

### Multi-tier datasets

Real repositories rarely have a single file-size profile. A typical ML repo might contain thousands of small config/text files, hundreds of medium data files, and a handful of multi-GB artifacts. The `--tier` flag models this by letting you define multiple size tiers in a single invocation.

#### Syntax

```
--tier "num=N dist=NAME [type=TYPE] [param=val ...]"
```

Each `--tier` value is a space-separated string of `key=value` tokens:

| Token | Required | Description |
|-------|----------|-------------|
| `num=N` | yes | Number of files in this tier. Supports `k`/`M`/`B` suffixes (e.g. `num=10k`). |
| `dist=NAME` | yes | scipy.stats distribution name (e.g. `lognorm`, `gamma`, `expon`). |
| `type=TYPE` | no | `text` or `binary`. Defaults to the global `--type` (default: `text`). `image` is not allowed. |
| *anything else* | no | Passed as distribution parameters (e.g. `s=0.4 scale=12000`). |

#### Example: mixed ML-style repository

```bash
./scripts/generate-dataset.py --target /tmp/ml_repo --seed 42 \
  --tier "num=9700 dist=lognorm s=0.4 scale=12000 type=text" \
  --tier "num=250 dist=lognorm s=0.4 scale=300000000 type=binary" \
  --tier "num=50 dist=lognorm s=0.4 scale=2000000000 type=binary"
```

This creates ~10 000 files:
- **9 700 text files** averaging ~12 KB (configs, scripts, READMEs)
- **250 binary files** averaging ~300 MB (model checkpoints, parquet shards)
- **50 binary files** averaging ~2 GB (large artifacts)

Files from all tiers are shuffled together before being distributed across directories, so each directory gets a realistic mix.

#### Mutual exclusivity

`--tier` cannot be combined with `--num-files`, `--total-size`, `--avg-file-size`, `--distribution`, or `--dist-params`. Use one mode at a time.

#### Per-tier seed stability

Each tier receives a deterministic sub-seed derived from `--seed` (seed + tier_index), so adding or reordering tiers doesn't affect the file sizes of other tiers.

### Common options

| Flag | Default | Description |
|------|---------|-------------|
| `--target DIR` | `./test_dataset` | Output directory (created if missing). |
| `--type {text,image,binary}` | `text` | File type. Also the default for `--tier` entries without an explicit `type=`. |
| `--seed N` | random | Seed for reproducible generation. |
| `--num-dirs N` | 1 | Number of subdirectories to spread files across. |
| `--files-per-dir N` | (all) | Alternative to `--num-dirs`; calculates directory count automatically. |
| `--workers N` | CPU count x 4 | Worker thread/process count. |

### File types

- **text** -- Fake English prose via [Faker](https://faker.readthedocs.io/). A 10 MB text pool is pre-generated and reused.
- **image** -- 1024x1024 PNG mosaics (solid blocks + noise). Size is determined by content, not by the size parameter.
- **binary** -- Random bytes written in 1 MB chunks.

---

## `reorganize-dataset.py`

Restructure a flat directory of generated files into a realistic nested directory tree. Pairs with `generate-dataset.py`:

```bash
# Generate flat, then restructure
./scripts/generate-dataset.py --num-files 10k --avg-file-size 5KB --target /tmp/flat
./scripts/reorganize-dataset.py --source /tmp/flat --target /tmp/nested --seed 42
```

Run `./scripts/reorganize-dataset.py --help` for full options.
