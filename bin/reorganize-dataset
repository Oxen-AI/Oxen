#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker",
#     "numpy",
#     "scipy",
#     "tqdm",
# ]
# ///

"""
Reorganize a flat (or shallowly nested) directory of files into an organic,
deeply nested directory tree — the kind of structure you'd find in a real
project or data repository.

Pairs with generate-dataset.py: first generate a flat dataset, then use this
script to give it realistic directory structure.
"""

import argparse
import json
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import scipy.stats
from faker import Faker
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Helpers copied from generate-dataset.py (single-file uv scripts can't import
# each other, so we duplicate the small utilities we need).
# ---------------------------------------------------------------------------


def parse_dist_params(params_list: list[str] | None) -> dict[str, float]:
    """Parse key=value strings into a {str: float} dict for distribution parameters."""
    if not params_list:
        return {}
    result = {}
    for item in params_list:
        if "=" not in item:
            print(f"Error: Invalid --dist-params format: '{item}'. Expected key=value.")
            sys.exit(1)
        key, _, value = item.partition("=")
        try:
            result[key] = float(value)
        except ValueError:
            print(
                f"Error: Non-numeric value in --dist-params: '{item}'. Value must be a number."
            )
            sys.exit(1)
    return result


def validate_distribution(dist_name: str, dist_params: dict[str, float]):
    """
    Verify dist_name exists in scipy.stats, freeze it with params, and check support.
    Returns a frozen distribution object.
    """
    dist_obj = getattr(scipy.stats, dist_name, None)
    if dist_obj is None:
        print(
            f"Error: Unknown distribution '{dist_name}'. Must be a scipy.stats distribution."
        )
        sys.exit(1)
    if not isinstance(dist_obj, (scipy.stats.rv_continuous, scipy.stats.rv_discrete)):
        print(f"Error: '{dist_name}' is not a scipy.stats distribution.")
        sys.exit(1)
    try:
        frozen = dist_obj(**dist_params)
    except TypeError as e:
        print(f"Error: Bad parameters for '{dist_name}': {e}")
        sys.exit(1)
    lower, _ = frozen.support()
    if lower < 0:
        print(
            f"Error: Distribution '{dist_name}' with the given parameters can produce negative values."
        )
        print("Only distributions defined on non-negative numbers are allowed.")
        sys.exit(1)
    return frozen


def _make_rng(seed: int | None) -> np.random.Generator:
    return np.random.default_rng(seed)


def _clamp_positive(
    sizes: np.ndarray, frozen_dist, rng: np.random.Generator
) -> np.ndarray:
    """Resample any non-positive values in sizes (up to 100 attempts, then clamp to 1)."""
    for _ in range(100):
        bad_mask = sizes <= 0
        if not bad_mask.any():
            break
        resampled = frozen_dist.rvs(size=bad_mask.sum(), random_state=rng)
        sizes[bad_mask] = np.rint(resampled).astype(int)
    else:
        sizes[sizes <= 0] = 1
    return sizes


# ---------------------------------------------------------------------------
# Tree data structure
# ---------------------------------------------------------------------------


@dataclass
class DirNode:
    name: str
    files: list[Path] = field(default_factory=list)
    children: list["DirNode"] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Directory naming
# ---------------------------------------------------------------------------


def generate_dir_name(
    faker: Faker,
    rng: np.random.Generator,
    *,
    single_word: float = 0.6,
    compound_word: float = 0.8,
    validate: bool = True,
) -> str:
    """Generate a realistic directory name with mixed styles."""
    if validate:
        assert 0 <= single_word <= 1.0, f"{single_word=} must be between 0 and 1"
        assert 0 <= compound_word <= 1.0, f"{compound_word=} must be between 0 and 1"
        range_compound_word = compound_word - single_word
        assert 0 <= range_compound_word <= 1, (
            f"{range_compound_word=} must be between 0 and 1"
        )
        range_number = 1.0 - range_compound_word - single_word
        assert 0 <= range_number <= 1, f"{range_number=} must be between 0 and 1"
        total = range_number + range_compound_word + single_word
        assert np.isclose(total, 1.0), (
            f"Proportions for single_word={single_word}, compound_word={range_compound_word}, number_word={range_number} must be between 0 and 1 and all sum to 1"
        )

    roll = rng.random()
    if roll < single_word:
        # Single word (~single_word%)
        return faker.word()
    elif roll < compound_word:
        # Compound word_word (~compound_word-single_word)%)
        return f"{faker.word()}_{faker.word()}"
    else:
        # Numbered word_NNN (~(1-compound_word)%)
        return f"{faker.word()}_{rng.integers(1, 999):03d}"


def deduplicate_name(name: str, existing: set[str]) -> str:
    """Append a numeric suffix if name collides with existing names."""
    if name not in existing:
        return name
    i = 2
    while f"{name}_{i}" in existing:
        i += 1
    return f"{name}_{i}"


# ---------------------------------------------------------------------------
# Recursive tree builder
# ---------------------------------------------------------------------------


def build_tree(
    files: list[Path],
    depth: int,
    max_depth: int,
    frozen_dist,
    rng: np.random.Generator,
    faker: Faker,
    name: str = ".",
) -> DirNode:
    """
    Recursively partition *files* into an organic directory tree.

    At each node we:
    1. Sample how many files live directly in this directory.
    2. If remaining files exist and we haven't hit max_depth, create children
       with geometric branching and Dirichlet-allocated file counts.
    """
    node = DirNode(name=name)

    if not files:
        return node

    # 1. Sample n_here from the distribution
    n_here_raw = frozen_dist.rvs(size=1, random_state=rng)
    n_here = max(1, int(np.rint(n_here_raw[0])))
    n_here = min(n_here, len(files))

    node.files = files[:n_here]
    remaining = files[n_here:]

    # 2. If nothing left or at max depth, absorb everything
    if not remaining or depth >= max_depth:
        node.files.extend(remaining)
        return node

    # 3. Determine branching factor via geometric distribution
    n_children = int(rng.geometric(p=0.4))
    n_children = max(1, min(n_children, len(remaining)))

    # 4. Split remaining files among children using Dirichlet allocation
    if n_children == 1:
        proportions = np.array([1.0])
    else:
        alpha = np.ones(n_children)
        proportions = rng.dirichlet(alpha)

    # Convert proportions to integer counts, ensuring no files are lost
    raw_counts = proportions * len(remaining)
    child_counts = np.floor(raw_counts).astype(int)
    # Distribute the rounding remainder
    deficit = len(remaining) - child_counts.sum()
    # Give extra files to the children with the largest fractional parts
    fractional = raw_counts - child_counts
    top_indices = np.argsort(fractional)[::-1][:deficit]
    child_counts[top_indices] += 1

    # 5. Recurse for each child
    existing_names: set[str] = set()
    offset = 0
    for i in range(n_children):
        count = int(child_counts[i])
        if count == 0:
            continue
        child_files = remaining[offset : offset + count]
        offset += count

        child_name = generate_dir_name(
            faker,
            rng,
            single_word=0.6,
            compound_word=0.8,
            validate=False,
        )
        child_name = deduplicate_name(child_name, existing_names)
        existing_names.add(child_name)

        child_node = build_tree(
            child_files,
            depth + 1,
            max_depth,
            frozen_dist,
            rng,
            faker,
            child_name,
        )
        node.children.append(child_node)

    return node


# ---------------------------------------------------------------------------
# Tree utilities
# ---------------------------------------------------------------------------


def flatten_moves(
    node: DirNode,
    parent_path: Path,
    _reserved: dict[Path, set[str]] | None = None,
) -> list[tuple[Path, Path]]:
    """Convert the tree into a flat list of (old_path, new_path) pairs.

    Detects basename collisions within each target directory and appends a
    numeric disambiguator (``_2``, ``_3``, …) to the stem so that no two
    destination paths are identical.
    """
    if _reserved is None:
        _reserved = {}

    moves = []
    current = parent_path / node.name if node.name != "." else parent_path

    if current not in _reserved:
        _reserved[current] = set()
    used = _reserved[current]

    for old_file in node.files:
        base = old_file.name
        if base in used:
            # Disambiguate: append increasing suffix to the stem
            stem = old_file.stem
            suffix = old_file.suffix
            counter = 2
            while f"{stem}_{counter}{suffix}" in used:
                counter += 1
            base = f"{stem}_{counter}{suffix}"
        used.add(base)
        new_file = current / base
        if old_file != new_file:
            moves.append((old_file, new_file))

    for child in node.children:
        moves.extend(flatten_moves(child, current, _reserved))

    return moves


def print_tree(node: DirNode, prefix: str = "", is_last: bool = True) -> None:
    """Print the tree structure with file counts."""
    connector = "└── " if is_last else "├── "
    file_info = f"  ({len(node.files)} files)" if node.files else ""
    print(f"{prefix}{connector}{node.name}/{file_info}")

    extension = "    " if is_last else "│   "
    child_prefix = prefix + extension

    for i, child in enumerate(node.children):
        print_tree(child, child_prefix, i == len(node.children) - 1)


def collect_stats(node: DirNode) -> dict:
    """Collect summary statistics from the tree."""
    dir_file_counts: list[int] = []
    depth_counts: Counter[int] = Counter()

    def _walk(n: DirNode, depth: int):
        dir_file_counts.append(len(n.files))
        depth_counts[depth] += 1
        for child in n.children:
            _walk(child, depth + 1)

    _walk(node, 0)

    total_dirs = len(dir_file_counts)
    counts_arr = np.array(dir_file_counts)

    return {
        "total_dirs": total_dirs,
        "total_files": int(counts_arr.sum()),
        "depth_histogram": dict(sorted(depth_counts.items())),
        "min_files_per_dir": int(counts_arr.min()) if total_dirs else 0,
        "max_files_per_dir": int(counts_arr.max()) if total_dirs else 0,
        "mean_files_per_dir": float(counts_arr.mean()) if total_dirs else 0,
    }


# ---------------------------------------------------------------------------
# Plan serialization
# ---------------------------------------------------------------------------


def tree_to_plan(node: DirNode) -> dict:
    """Recursively convert a DirNode tree to a JSON-serializable plan dict."""
    return {
        "name": node.name,
        "n_files": len(node.files),
        "children": [tree_to_plan(child) for child in node.children],
    }


def _count_plan_files(plan: dict) -> int:
    """Recursively count total files in a plan."""
    return plan["n_files"] + sum(_count_plan_files(c) for c in plan["children"])


def plan_to_tree(plan: dict, files: list[Path]) -> DirNode:
    """
    Reconstruct a DirNode tree from a plan dict, distributing *files*
    proportionally based on the original file counts in the plan.

    Files should already be shuffled before calling this function.
    """
    node = DirNode(name=plan["name"])

    if not files:
        return node

    plan_total = _count_plan_files(plan)
    if plan_total == 0:
        # Degenerate plan with no files — put everything in root
        node.files = files
        return node

    child_plans = plan["children"]

    if not child_plans:
        # Leaf node — gets all files
        node.files = files
        return node

    # Collect all "slots": this node + each child subtree
    # Each slot's proportion is its recursive file count / plan_total
    slot_proportions = [plan["n_files"] / plan_total]
    for child_plan in child_plans:
        child_total = _count_plan_files(child_plan)
        slot_proportions.append(child_total / plan_total)

    # Allocate files to slots proportionally
    n_total = len(files)
    raw_counts = np.array(slot_proportions) * n_total
    slot_counts = np.floor(raw_counts).astype(int)

    # Distribute remainder to slots with largest fractional parts
    deficit = n_total - slot_counts.sum()
    fractional = raw_counts - slot_counts
    top_indices = np.argsort(fractional)[::-1][:deficit]
    slot_counts[top_indices] += 1

    # Assign files to this node
    n_here = int(slot_counts[0])
    node.files = files[:n_here]
    offset = n_here

    # Recurse into children
    for i, child_plan in enumerate(child_plans):
        count = int(slot_counts[i + 1])
        child_files = files[offset : offset + count]
        offset += count
        node.children.append(plan_to_tree(child_plan, child_files))

    return node


def remove_empty_dirs(root: Path) -> int:
    """Remove empty directories under root (bottom-up). Returns count removed."""
    removed = 0
    # Walk bottom-up so child dirs are removed before parents
    for dirpath in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()
            removed += 1
    return removed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(
    *,
    target: str,
    max_depth: int | None,
    distribution: str,
    dist_params: list[str] | None,
    seed: int | None,
    dry_run: bool,
    save_plan: str | None,
    from_plan: str | None,
) -> None:
    target_path = Path(target)
    if not target_path.is_dir():
        print(f"Error: '{target_path}' is not a directory or does not exist.")
        sys.exit(1)

    # Set up RNG and Faker
    rng = _make_rng(seed)
    if seed is not None:
        Faker.seed(seed)
    faker = Faker()

    # Scan for all files
    all_files = sorted([f for f in target_path.rglob("*") if f.is_file()])
    if not all_files:
        print("No files found in target directory.")
        sys.exit(0)

    # Shuffle for organic assignment (seeded)
    py_rng = rng.permutation(len(all_files))
    all_files = [all_files[i] for i in py_rng]

    print(f"Found {len(all_files)} files in '{target_path}'.\n")

    # Build the tree — either from a saved plan or by generating a new one
    if from_plan:
        plan_path = Path(from_plan)
        if not plan_path.is_file():
            print(f"Error: Plan file '{plan_path}' does not exist.")
            sys.exit(1)
        with open(plan_path) as f:
            plan = json.load(f)
        tree = plan_to_tree(plan, all_files)
    else:
        # Apply default dist-params when none given and distribution is the default
        DIST_DEFAULTS: dict[str, dict[str, float]] = {
            "lognorm": {"s": 0.5, "scale": 8},
        }
        parsed_dist_params = parse_dist_params(dist_params)
        if not parsed_dist_params and distribution in DIST_DEFAULTS:
            parsed_dist_params = DIST_DEFAULTS[distribution]

        frozen_dist = validate_distribution(distribution, parsed_dist_params)
        tree = build_tree(all_files, 0, max_depth, frozen_dist, rng, faker)

    # Save the plan if requested
    if save_plan:
        plan_out = tree_to_plan(tree)
        save_path = Path(save_plan)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(plan_out, f, indent=2)
        print(f"Plan saved to '{save_path}'.\n")

    # Print tree structure
    print("Planned tree structure:")
    print(f"└── {target_path.name}/  ({len(tree.files)} files)")
    for i, child in enumerate(tree.children):
        print_tree(child, "    ", i == len(tree.children) - 1)

    # Print summary stats
    stats = collect_stats(tree)
    print("\nSummary:")
    print(f"  Total directories: {stats['total_dirs']}")
    print(f"  Total files:       {stats['total_files']}")
    print(
        f"  Files per dir:     min={stats['min_files_per_dir']}, "
        f"max={stats['max_files_per_dir']}, "
        f"mean={stats['mean_files_per_dir']:.1f}"
    )
    print(f"  Depth histogram:   {stats['depth_histogram']}")

    if dry_run:
        print("\n(dry-run) No files were moved.")
        return

    # Build move plan
    moves = flatten_moves(tree, target_path)
    if not moves:
        print("\nNo moves needed — files are already in place.")
        return

    print(f"\nMoving {len(moves)} files...")
    start_time = time.time()

    # Create all needed directories first
    new_dirs: set[Path] = set()
    for _, new_path in moves:
        new_dirs.add(new_path.parent)
    for d in sorted(new_dirs):
        d.mkdir(parents=True, exist_ok=True)
    dirs_created = sum(1 for d in new_dirs if d != target_path)

    # Execute moves
    for old_path, new_path in tqdm(moves, desc="Moving files", unit="file"):
        shutil.move(str(old_path), str(new_path))

    # Clean up empty directories
    removed = remove_empty_dirs(target_path)

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  Files moved:         {len(moves)}")
    print(f"  Directories created: {dirs_created}")
    print(f"  Empty dirs removed:  {removed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reorganize a flat directory of files into an organic nested tree.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --target /tmp/dataset --max-depth 5 --seed 42
  %(prog)s --target /tmp/dataset --max-depth 5 \\
      --distribution lognorm --dist-params s=0.5 scale=10 --seed 42
  %(prog)s --target /tmp/dataset --max-depth 3 \\
      --distribution gamma --dist-params a=2.0 scale=5 --dry-run

  # Save a plan and apply it to another dataset:
  %(prog)s --target /tmp/dataset_a --max-depth 5 --seed 42 \\
      --save-plan /tmp/tree-plan.json --dry-run
  %(prog)s --target /tmp/dataset_b --from-plan /tmp/tree-plan.json --seed 99
        """,
    )

    parser.add_argument(
        "--target",
        type=str,
        required=True,
        help="Directory containing the files to reorganize",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Maximum nesting depth of the output tree (required unless --from-plan is used)",
    )
    parser.add_argument(
        "--distribution",
        type=str,
        default="lognorm",
        metavar="NAME",
        help="scipy.stats distribution name controlling files-per-directory (default: lognorm)",
    )
    parser.add_argument(
        "--dist-params",
        nargs="+",
        default=None,
        metavar="KEY=VALUE",
        help="Distribution parameters as key=value pairs (default for lognorm: s=0.5 scale=8)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned tree without moving anything",
    )
    parser.add_argument(
        "--save-plan",
        type=str,
        default=None,
        metavar="PATH",
        help="Save the tree structure as a JSON plan file (works with --dry-run)",
    )
    parser.add_argument(
        "--from-plan",
        type=str,
        default=None,
        metavar="PATH",
        help="Load a saved plan instead of generating a new tree "
        "(ignores --max-depth, --distribution, --dist-params)",
    )

    args = parser.parse_args()

    # Pull all arguments into named, typed variables
    arg_target: str = args.target
    arg_max_depth: int | None = args.max_depth
    arg_distribution: str = args.distribution
    arg_dist_params: list[str] | None = args.dist_params
    arg_seed: int | None = args.seed
    arg_dry_run: bool = args.dry_run
    arg_save_plan: str | None = args.save_plan
    arg_from_plan: str | None = args.from_plan

    # --max-depth is required unless --from-plan is given
    if arg_from_plan is None and arg_max_depth is None:
        parser.error("--max-depth is required when not using --from-plan")

    main(
        target=arg_target,
        max_depth=arg_max_depth,
        distribution=arg_distribution,
        dist_params=arg_dist_params,
        seed=arg_seed,
        dry_run=arg_dry_run,
        save_plan=arg_save_plan,
        from_plan=arg_from_plan,
    )
