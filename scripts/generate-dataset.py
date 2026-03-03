#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker",
#     "numpy",
#     "pillow",
#     "scipy",
#     "tqdm",
# ]
# ///

"""
Generate test datasets with configurable size, structure, and file types.
"""

import argparse
import os
import random
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import scipy.stats
from faker import Faker
from PIL import Image
from tqdm import tqdm


FileType = Literal["text", "image", "binary"]


@dataclass
class Tier:
    num: int
    dist_name: str
    dist_params: dict[str, float]
    file_type: str
    frozen_dist: object  # frozen scipy distribution


# Global text pool for reusing generated text (shared across threads)
TEXT_POOL = None


def positive_int(value: str) -> int:
    """Parse and validate a positive integer with support for k/K, m/M, b/B suffixes."""
    value = value.strip().upper()

    # Handle suffixes: k (thousand), M (million), B (billion)
    multipliers = {
        'K': 1_000,
        'M': 1_000_000,
        'B': 1_000_000_000,
    }

    for suffix, multiplier in multipliers.items():
        if value.endswith(suffix):
            try:
                numeric_part = float(value[:-1])
                ivalue = int(numeric_part * multiplier)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"Invalid format: {value}. Use formats like: 100, 10k, 2M, 1B"
                )
            if ivalue <= 0:
                raise argparse.ArgumentTypeError(f"{value} must be positive")
            return ivalue

    # No suffix, parse as plain integer
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid format: {value}. Use formats like: 100, 10k, 2M, 1B"
        )

    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} must be positive")
    return ivalue


def parse_size(size_str: str) -> int:
    """Parse human-readable size string to bytes."""
    size_str = size_str.strip().upper()

    # Check units from longest to shortest to avoid partial matches
    # (e.g., "GB" before "B" so "10GB" doesn't match "B" first)
    units = [
        ('TB', 1024 ** 4),
        ('GB', 1024 ** 3),
        ('MB', 1024 ** 2),
        ('KB', 1024),
        ('B', 1),
    ]

    # Try to find unit suffix
    for unit, multiplier in units:
        if size_str.endswith(unit):
            try:
                value = float(size_str[:-len(unit)])
                size_bytes = int(value * multiplier)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"Invalid size format: {size_str}. Use formats like: 1GB, 500MB, 1024"
                )
            if size_bytes <= 0:
                raise argparse.ArgumentTypeError("Size must be positive")
            return size_bytes

    # If no unit, assume bytes
    try:
        size_bytes = int(size_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid size format: {size_str}. Use formats like: 1GB, 500MB, 1024"
        )

    if size_bytes <= 0:
        raise argparse.ArgumentTypeError("Size must be positive")
    return size_bytes


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f}PB"


def init_text_pool(pool_size_mb: int = 10) -> list[tuple[str, int]]:
    """
    Generate a pool of text chunks to reuse across files.
    This avoids calling Faker for every file, speeding up text generation.
    Returns list of (text, byte_size) tuples to avoid repeated UTF-8 encoding.
    """
    fake = Faker()
    chunks = []
    total_size = 0
    target = pool_size_mb * 1024 * 1024

    while total_size < target:
        text = fake.text(max_nb_chars=1000) + "\n"
        byte_size = len(text.encode('utf-8'))
        chunks.append((text, byte_size))
        total_size += byte_size

    return chunks


def validate_parameters(total_size: int | None, num_files: int | None,
                       avg_file_size: int | None) -> tuple[int, int]:
    """
    Validate and calculate final parameters.
    Returns: (num_files, file_size)
    """
    params_provided = sum([
        total_size is not None,
        num_files is not None,
        avg_file_size is not None
    ])

    if params_provided < 2:
        print("Error: Must specify at least 2 of: --total-size, --num-files, --avg-file-size")
        sys.exit(1)

    # If all three provided, check consistency
    if params_provided == 3:
        expected_total = num_files * avg_file_size
        tolerance = 0.01  # 1% tolerance
        if abs(expected_total - total_size) / total_size > tolerance:
            print("Error: Conflicting parameters!")
            print(f"  Total size: {format_size(total_size)}")
            print(f"  Num files: {num_files}")
            print(f"  Avg file size: {format_size(avg_file_size)}")
            print(f"  Expected total: {format_size(expected_total)}")
            print("\nPlease adjust parameters so: total_size ≈ num_files × avg_file_size")
            sys.exit(1)
        # Use provided values
        return num_files, avg_file_size

    # Calculate missing parameter
    if total_size is None:
        total_size = num_files * avg_file_size
        print(f"Calculated total size: {format_size(total_size)}")
        return num_files, avg_file_size
    elif num_files is None:
        num_files = total_size // avg_file_size
        print(f"Calculated number of files: {num_files}")
        return num_files, avg_file_size
    else:  # avg_file_size is None
        avg_file_size = total_size // num_files
        print(f"Calculated average file size: {format_size(avg_file_size)}")
        return num_files, avg_file_size


def validate_directory_parameters(num_files: int, num_dirs: int | None,
                                  files_per_dir: int | None) -> int:
    """
    Validate and calculate directory parameters.
    Returns: num_dirs
    """
    if num_dirs is not None and files_per_dir is not None:
        # Both provided, check consistency
        expected_dirs = (num_files + files_per_dir - 1) // files_per_dir  # Ceiling division
        if expected_dirs != num_dirs:
            print("Error: Conflicting directory parameters!")
            print(f"  Num files: {num_files}")
            print(f"  Num dirs: {num_dirs}")
            print(f"  Files per dir: {files_per_dir}")
            print(f"  Expected dirs: {expected_dirs}")
            print("\nPlease adjust parameters so: num_dirs = ceil(num_files / files_per_dir)")
            sys.exit(1)
        return num_dirs
    elif num_dirs is not None:
        # Only num_dirs provided
        return num_dirs
    elif files_per_dir is not None:
        # Only files_per_dir provided, calculate num_dirs
        num_dirs = (num_files + files_per_dir - 1) // files_per_dir  # Ceiling division
        print(f"Calculated number of directories: {num_dirs}")
        return num_dirs
    else:
        # Neither provided, default to 1
        return 1


def parse_dist_params(params_list: list[str] | None) -> dict[str, float]:
    """Parse key=value strings into a {str: float} dict for distribution parameters."""
    if not params_list:
        return {}
    result = {}
    for item in params_list:
        if '=' not in item:
            print(f"Error: Invalid --dist-params format: '{item}'. Expected key=value.")
            sys.exit(1)
        key, _, value = item.partition('=')
        try:
            result[key] = float(value)
        except ValueError:
            print(f"Error: Non-numeric value in --dist-params: '{item}'. Value must be a number.")
            sys.exit(1)
    return result


def validate_distribution(dist_name: str, dist_params: dict[str, float]):
    """
    Verify dist_name exists in scipy.stats, freeze it with params, and check support.
    Returns a frozen distribution object.
    """
    dist_obj = getattr(scipy.stats, dist_name, None)
    if dist_obj is None:
        print(f"Error: Unknown distribution '{dist_name}'. Must be a scipy.stats distribution.")
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
        print(f"Error: Distribution '{dist_name}' with the given parameters can produce negative values.")
        print("Only distributions defined on non-negative numbers are allowed for file sizes.")
        sys.exit(1)
    return frozen


def parse_tier(tier_string: str, default_type: str) -> Tier:
    """Parse a --tier string like 'num=100 dist=lognorm s=0.4 scale=12000 type=text'."""
    tokens = tier_string.split()
    kv: dict[str, str] = {}
    for token in tokens:
        if '=' not in token:
            print(f"Error: Invalid token in --tier: '{token}'. Expected key=value.")
            sys.exit(1)
        key, _, value = token.partition('=')
        kv[key] = value

    if 'num' not in kv:
        print(f"Error: --tier requires 'num=N'. Got: '{tier_string}'")
        sys.exit(1)
    if 'dist' not in kv:
        print(f"Error: --tier requires 'dist=NAME'. Got: '{tier_string}'")
        sys.exit(1)

    num = positive_int(kv.pop('num'))
    dist_name = kv.pop('dist')
    file_type = kv.pop('type', default_type)

    if file_type == 'image':
        print("Error: type=image is not allowed in --tier (images can't use distributions).")
        sys.exit(1)
    if file_type not in ('text', 'binary'):
        print(f"Error: Invalid type '{file_type}' in --tier. Must be 'text' or 'binary'.")
        sys.exit(1)

    # Remaining keys are distribution parameters
    dist_params: dict[str, float] = {}
    for key, value in kv.items():
        try:
            dist_params[key] = float(value)
        except ValueError:
            print(f"Error: Non-numeric distribution parameter in --tier: '{key}={value}'.")
            sys.exit(1)

    frozen_dist = validate_distribution(dist_name, dist_params)

    return Tier(
        num=num,
        dist_name=dist_name,
        dist_params=dist_params,
        file_type=file_type,
        frozen_dist=frozen_dist,
    )


def _make_rng(seed: int | None) -> np.random.Generator:
    return np.random.default_rng(seed)


def _clamp_positive(sizes: np.ndarray, frozen_dist, rng: np.random.Generator) -> np.ndarray:
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


def sample_file_sizes(frozen_dist, num_files: int, seed: int | None) -> list[int]:
    """
    Sample exactly num_files file sizes from a frozen scipy distribution.
    Rounds to nearest integer, resamples any values <= 0.
    """
    rng = _make_rng(seed)
    sizes = np.rint(frozen_dist.rvs(size=num_files, random_state=rng)).astype(int)
    return _clamp_positive(sizes, frozen_dist, rng).tolist()


def sample_file_sizes_until(frozen_dist, total_size: int, seed: int | None) -> list[int]:
    """
    Sample file sizes from a frozen scipy distribution until their cumulative
    sum equals or exceeds total_size. Generates in batches for efficiency.
    """
    rng = _make_rng(seed)
    batch_size = 1024
    collected: list[int] = []
    remaining = total_size

    while remaining > 0:
        batch = np.rint(frozen_dist.rvs(size=batch_size, random_state=rng)).astype(int)
        batch = _clamp_positive(batch, frozen_dist, rng)
        for val in batch:
            collected.append(int(val))
            remaining -= int(val)
            if remaining <= 0:
                break

    return collected


def generate_text_file(path: Path, size: int) -> None:
    """Generate a text file with fake text content from pre-generated pool."""
    global TEXT_POOL

    # Build entire file content first, then write once (faster for small files)
    parts = []
    current_size = 0

    while current_size < size:
        remaining = size - current_size

        # Randomly select a chunk from the pre-generated pool (text, byte_size)
        chunk_text, chunk_size = random.choice(TEXT_POOL)

        # Trim chunk if it's too large for remaining space
        if chunk_size > remaining:
            # Approximate trim (may be slightly off due to UTF-8 variable length)
            chunk_text = chunk_text[:remaining]
            chunk_size = len(chunk_text.encode('utf-8'))

        parts.append(chunk_text)
        current_size += chunk_size

    # Single write operation
    with open(path, 'w') as f:
        f.write(''.join(parts))


def generate_image_file(path: Path, size: int) -> None:
    """
    Generate a PNG image file with a mosaic pattern (mix of solid blocks and noise).

    These are quick to generate, are visually distinct from one another, are
    compressable to a fairly realistic level, and still provide a high level of
    randomness.
    """
    # Fixed dimensions for now (1024x1024)
    width, height = 1024, 1024
    block_size = 32  # 32x32 blocks

    img = Image.new('RGB', (width, height))
    pixels = img.load()

    # Generate mosaic pattern
    for block_y in range(0, height, block_size):
        for block_x in range(0, width, block_size):
            # Randomly decide: solid color block or noise block
            # Use ~40% noise blocks to reduce compressibility
            is_noise_block = random.random() < 0.4

            if is_noise_block:
                # Fill block with random pixels (noise)
                for y in range(block_y, min(block_y + block_size, height)):
                    for x in range(block_x, min(block_x + block_size, width)):
                        pixels[x, y] = (
                            random.randint(0, 255),
                            random.randint(0, 255),
                            random.randint(0, 255)
                        )
            else:
                # Fill block with solid random color
                color = (
                    random.randint(0, 255),
                    random.randint(0, 255),
                    random.randint(0, 255)
                )
                for y in range(block_y, min(block_y + block_size, height)):
                    for x in range(block_x, min(block_x + block_size, width)):
                        pixels[x, y] = color

    # Save as PNG
    img.save(path, 'PNG')


HAS_DD = shutil.which("dd") is not None


def generate_binary_file_dd(path: Path, size: int) -> None:
    """Generate a binary file with random bytes using dd."""
    subprocess.run(
        ["dd", "if=/dev/random", f"of={path}", f"bs={size}", "count=1"],
        check=True,
        capture_output=True,
    )


def generate_binary_file_python(path: Path, size: int) -> None:
    """Generate a binary file with random bytes using Python."""
    with open(path, 'wb') as f:
        # Write in chunks to avoid memory issues with large files
        chunk_size = min(1024 * 1024, size)  # 1MB chunks or smaller
        remaining = size

        while remaining > 0:
            write_size = min(chunk_size, remaining)
            # Use random.randbytes() instead of os.urandom() for much faster generation
            f.write(random.randbytes(write_size))
            remaining -= write_size


def generate_binary_file(path: Path, size: int) -> None:
    """Generate a binary file with random bytes, using dd if available."""
    if HAS_DD and size > 400 * 1024 * 1024:
        generate_binary_file_dd(path, size)
    else:
        generate_binary_file_python(path, size)


def compute_avg_image_size(num_samples: int = 10) -> int:
    """
    Compute average image size by generating sample images.
    Returns the average size in bytes.
    """
    import tempfile

    sizes = []
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for i in range(num_samples):
            sample_file = tmp_path / f"sample_{i}.png"
            generate_image_file(sample_file, 0)  # Size parameter not used for images
            sizes.append(sample_file.stat().st_size)

    return sum(sizes) // len(sizes)


def generate_file(args: tuple) -> Path:
    """Generate a single file. Used for parallel execution."""
    path, file_type, size = args

    generators = {
        'text': generate_text_file,
        'image': generate_image_file,
        'binary': generate_binary_file,
    }

    generator = generators[file_type]
    generator(path, size)

    return path


def main():
    parser = argparse.ArgumentParser(
        description="Generate test datasets with configurable parameters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --total-size 1GB --num-files 100 --type text
  %(prog)s --num-files 50 --total-size 500MB --type image --num-dirs 5
  %(prog)s --total-size 500MB --avg-file-size 5MB --type binary --target ./test_data
  %(prog)s --num-files 10k --avg-file-size 1MB --files-per-dir 100 --type text
  %(prog)s --total-size 16GB --num-files 3M --files-per-dir 30k --seed 42 --type text

Distribution-based random file sizes (use --num-files or --total-size):
  %(prog)s --num-files 1000 --distribution lognorm --dist-params s=0.954 scale=5000 --type text
  %(prog)s --num-files 500 --distribution gamma --dist-params a=2.0 scale=10000 --type binary
  %(prog)s --total-size 1GB --distribution expon --dist-params scale=50000 --type binary
  %(prog)s --num-files 10k --distribution expon --dist-params scale=50000 --type text --seed 42

Multi-tier datasets (--tier is repeatable, mutually exclusive with --num-files/--total-size/etc.):
  %(prog)s --target /tmp/dataset --seed 42 \\
    --tier "num=9700 dist=lognorm s=0.4 scale=12000 type=text" \\
    --tier "num=250 dist=lognorm s=0.4 scale=300000000 type=binary" \\
    --tier "num=50 dist=lognorm s=0.4 scale=2000000000 type=binary"
        """
    )

    # Size parameters
    parser.add_argument('--total-size', type=parse_size,
                       help='Total size of generated dataset (e.g., 1GB, 500MB)')
    parser.add_argument('--num-files', type=positive_int,
                       help='Number of files to generate (e.g., 100, 10k, 3M)')
    parser.add_argument('--avg-file-size', type=parse_size,
                       help='Average size per file (e.g., 10MB, 1GB). Not valid for image type.')

    # Structure parameters
    parser.add_argument('--num-dirs', type=positive_int, default=None,
                       help='Number of directories to split files into (e.g., 10, 100, 5k)')
    parser.add_argument('--files-per-dir', type=positive_int, default=None,
                       help='Number of files per directory (e.g., 100, 10k) - alternative to --num-dirs')
    parser.add_argument('--target', type=str, default='./test_dataset',
                       help='Target directory for generated files (default: ./test_dataset)')

    # File type
    parser.add_argument('--type', type=str, choices=['text', 'image', 'binary'],
                       default='text', help='Type of files to generate (default: text)')

    # Parallelism
    parser.add_argument('--workers', type=positive_int, default=None,
                       help='Number of parallel worker threads (e.g., 10, 100) - default: CPU count × 4')

    # Other options
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducible generation')
    parser.add_argument('--dry-run', action='store_true',
                       help='Print the generation plan without creating any files')

    # Distribution-based file sizes
    parser.add_argument('--distribution', type=str, default=None, metavar='NAME',
                       help='scipy.stats distribution name for random file sizes (e.g., lognorm, gamma, expon)')
    parser.add_argument('--dist-params', nargs='+', default=None, metavar='KEY=VALUE',
                       help='Distribution parameters as key=value pairs (e.g., s=0.954 scale=5000)')

    # Tier-based generation
    parser.add_argument('--tier', action='append', default=None,
                       metavar='"num=N dist=NAME [type=TYPE] [param=val ...]"',
                       help='Define a file-size tier (repeatable). '
                            'Mutually exclusive with --num-files, --total-size, '
                            '--avg-file-size, --distribution, --dist-params.')

    args = parser.parse_args()

    # Validate --tier mutual exclusivity
    if args.tier is not None:
        tier_exclusive = {
            '--num-files': args.num_files,
            '--total-size': args.total_size,
            '--avg-file-size': args.avg_file_size,
            '--distribution': args.distribution,
            '--dist-params': args.dist_params,
        }
        conflicts = [name for name, val in tier_exclusive.items() if val is not None]
        if conflicts:
            parser.error(f"--tier cannot be used with {', '.join(conflicts)}")

    # Validate distribution arguments
    if args.dist_params is not None and args.distribution is None:
        parser.error("--dist-params requires --distribution")
    if args.distribution is not None:
        if args.num_files is None and args.total_size is None:
            parser.error("--distribution requires either --num-files or --total-size")
        if args.num_files is not None and args.total_size is not None:
            parser.error("--num-files and --total-size cannot both be used with --distribution")
        if args.avg_file_size is not None:
            parser.error("--avg-file-size cannot be used with --distribution")
        if args.type == 'image':
            parser.error("--type image cannot be used with --distribution")

    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    # Determine file sizes: tier mode, distribution mode, or fixed-size mode
    tiers: list[Tier] | None = None
    tier_tasks: list[tuple[str, int]] | None = None  # (file_type, size) per file

    if args.tier is not None:
        # Tier mode: parse each tier, sample sizes, combine and shuffle
        tiers = [parse_tier(t, args.type) for t in args.tier]
        tier_tasks = []
        for i, tier in enumerate(tiers):
            tier_seed = args.seed + i if args.seed is not None else None
            sizes = sample_file_sizes(tier.frozen_dist, tier.num, tier_seed)
            tier_tasks.extend((tier.file_type, s) for s in sizes)
        random.shuffle(tier_tasks)
        file_sizes = [s for _, s in tier_tasks]
        num_files = len(file_sizes)
    elif args.distribution is not None:
        dist_params = parse_dist_params(args.dist_params)
        frozen_dist = validate_distribution(args.distribution, dist_params)
        if args.num_files is not None:
            num_files = args.num_files
            file_sizes = sample_file_sizes(frozen_dist, num_files, args.seed)
        else:
            file_sizes = sample_file_sizes_until(frozen_dist, args.total_size, args.seed)
            num_files = len(file_sizes)
    else:
        # Special handling for image type
        if args.type == 'image':
            if args.avg_file_size is not None:
                print("Error: --avg-file-size cannot be specified for image type.")
                print("Image size is determined by the image dimensions, content, and compression.")
                sys.exit(1)

            # Compute average image size by generating test samples
            print("Computing average image size by generating test samples...")
            computed_avg_size = compute_avg_image_size(num_samples=10)
            print(f"Computed average image size: {format_size(computed_avg_size)}\n")
            args.avg_file_size = computed_avg_size

        # Validate and calculate parameters
        num_files, file_size = validate_parameters(
            args.total_size, args.num_files, args.avg_file_size
        )
        file_sizes = [file_size] * num_files

    # Validate and calculate directory parameters
    num_dirs = validate_directory_parameters(
        num_files, args.num_dirs, args.files_per_dir
    )

    # Create target directory structure
    target = Path(args.target)
    target.mkdir(parents=True, exist_ok=True)

    # Warn if target directory has existing files
    existing_files = list(target.rglob('*'))
    existing_files = [f for f in existing_files if f.is_file()]
    if existing_files:
        print(f"Warning: Target directory contains {len(existing_files)} existing files.")
        response = input("Continue and potentially overwrite files? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("Aborted.")
            sys.exit(0)

    # Extension lookup
    extensions = {
        'text': 'txt',
        'image': 'png',
        'binary': 'bin',
    }

    # Calculate files per directory
    files_per_dir = num_files // num_dirs
    extra_files = num_files % num_dirs

    # Determine padding width for file and directory names based on counts
    file_padding = len(str(num_files))
    dir_padding = len(str(num_dirs))

    # Build list of files to generate
    file_tasks = []
    file_counter = 0

    for dir_idx in range(num_dirs):
        if num_dirs > 1:
            dir_path = target / f"dir_{dir_idx + 1:0{dir_padding}d}"
            dir_path.mkdir(exist_ok=True)
        else:
            dir_path = target

        # Distribute extra files to first directories
        num_files_in_dir = files_per_dir + (1 if dir_idx < extra_files else 0)

        for file_idx in range(num_files_in_dir):
            if tier_tasks is not None:
                file_type, size = tier_tasks[file_counter]
            else:
                file_type = args.type
                size = file_sizes[file_counter]
            ext = extensions[file_type]
            file_name = f"file_{file_counter + 1:0{file_padding}d}.{ext}"
            file_path = dir_path / file_name
            file_tasks.append((file_path, file_type, size))
            file_counter += 1

    # Choose executor type and worker count based on file type
    # Tier mode disallows images, so always use threads
    if tiers is not None or args.type != 'image':
        executor_class = ThreadPoolExecutor
        default_workers = os.cpu_count() * 4
        executor_type = "threads"
    else:
        # Image generation is CPU-bound, use ProcessPoolExecutor
        executor_class = ProcessPoolExecutor
        default_workers = os.cpu_count()
        executor_type = "processes"

    max_workers = args.workers or default_workers

    # Display generation plan
    print("\nGeneration Plan:")
    print(f"  Target: {target}")
    if tiers is not None:
        all_sizes = np.array(file_sizes)
        for i, tier in enumerate(tiers):
            tier_sizes = np.array(
                sample_file_sizes(tier.frozen_dist, tier.num,
                                  args.seed + i if args.seed is not None else None)
            )
            print(f"  Tier {i + 1}: {tier.num} {tier.file_type} files")
            print(f"    Distribution: {tier.dist_name}({', '.join(f'{k}={v}' for k, v in tier.dist_params.items())})")
            print(f"    Size range: {format_size(int(tier_sizes.min()))} – {format_size(int(tier_sizes.max()))}")
            print(f"    Mean / median: {format_size(int(tier_sizes.mean()))} / {format_size(int(np.median(tier_sizes)))}")
        print(f"  Total files: {num_files}")
        print(f"  Total size: ~{format_size(int(all_sizes.sum()))}")
    elif args.distribution is not None:
        print(f"  File type: {args.type}")
        print(f"  Number of files: {num_files}")
        sizes_arr = np.array(file_sizes)
        print(f"  Distribution: {args.distribution}")
        print(f"  Dist params: {dist_params}")
        print(f"  File size min: {format_size(int(sizes_arr.min()))}")
        print(f"  File size max: {format_size(int(sizes_arr.max()))}")
        print(f"  File size mean: {format_size(int(sizes_arr.mean()))}")
        print(f"  File size median: {format_size(int(np.median(sizes_arr)))}")
        print(f"  File size std: {format_size(int(sizes_arr.std()))}")
        print(f"  Total size: ~{format_size(int(sizes_arr.sum()))}")
    else:
        print(f"  File type: {args.type}")
        print(f"  Number of files: {num_files}")
        print(f"  File size: {format_size(file_sizes[0])}")
        print(f"  Total size: ~{format_size(num_files * file_sizes[0])}")
    print(f"  Directories: {num_dirs}")
    print(f"  Files per directory: ~{num_files // num_dirs}")
    print(f"  Workers: {max_workers} {executor_type}")
    print()

    if args.dry_run:
        print("(dry-run) No files were generated.")
        return

    # Generate files in parallel using appropriate executor for workload type
    needs_text = args.type == 'text' or (
        tiers is not None and any(t.file_type == 'text' for t in tiers)
    )
    if needs_text:
        print("Initializing text pool...", flush=True)
        # Initialize text pool in main thread (threads share memory)
        global TEXT_POOL
        TEXT_POOL = init_text_pool(pool_size_mb=10)

    print("Starting generation...", flush=True)
    # Calculate chunksize with a reasonable cap to avoid excessive buffering
    chunksize = max(1, min(10000, num_files // (max_workers * 4)))

    start_time = time.time()
    with executor_class(max_workers=max_workers) as executor:
        with tqdm(total=num_files, desc="Generating files", unit="file",
                  unit_scale=False, smoothing=0.1) as pbar:
            for _ in executor.map(generate_file, file_tasks, chunksize=chunksize):
                pbar.update(1)
    elapsed_time = time.time() - start_time

    # Show completion statistics
    files_per_sec = num_files / elapsed_time if elapsed_time > 0 else 0
    print(f"\n✓ Dataset generated successfully in {target}")
    print(f"  Generated {num_files:,} files in {elapsed_time:.1f}s ({files_per_sec:.1f} files/sec)")


if __name__ == '__main__':
    main()
