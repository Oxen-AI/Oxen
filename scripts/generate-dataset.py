#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker",
#     "pillow",
#     "tqdm",
# ]
# ///

"""
Generate test datasets with configurable size, structure, and file types.
"""

import argparse
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

from faker import Faker
from PIL import Image
from tqdm import tqdm


FileType = Literal["text", "image", "binary"]

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


def generate_binary_file(path: Path, size: int) -> None:
    """Generate a binary file with random bytes."""
    with open(path, 'wb') as f:
        # Write in chunks to avoid memory issues with large files
        chunk_size = min(1024 * 1024, size)  # 1MB chunks or smaller
        remaining = size

        while remaining > 0:
            write_size = min(chunk_size, remaining)
            # Use random.randbytes() instead of os.urandom() for much faster generation
            f.write(random.randbytes(write_size))
            remaining -= write_size


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

    args = parser.parse_args()

    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

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

    # Determine file extension
    extensions = {
        'text': 'txt',
        'image': 'png',
        'binary': 'bin',
    }
    ext = extensions[args.type]

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
            file_name = f"file_{file_counter + 1:0{file_padding}d}.{ext}"
            file_path = dir_path / file_name
            file_tasks.append((file_path, args.type, file_size))
            file_counter += 1

    max_workers = args.workers or (os.cpu_count() * 4)

    # Display generation plan
    print("\nGeneration Plan:")
    print(f"  Target: {target}")
    print(f"  File type: {args.type}")
    print(f"  Number of files: {num_files}")
    print(f"  File size: {format_size(file_size)}")
    print(f"  Total size: ~{format_size(num_files * file_size)}")
    print(f"  Directories: {num_dirs}")
    print(f"  Files per directory: ~{num_files // num_dirs}")
    print(f"  Workers: {max_workers} threads")
    print()

    # Generate files in parallel using threads (faster for I/O-bound workloads)
    if args.type == 'text':
        print("Initializing text pool...", flush=True)
        # Initialize text pool in main thread (threads share memory)
        global TEXT_POOL
        TEXT_POOL = init_text_pool(pool_size_mb=10)

    print("Starting generation...", flush=True)
    # Calculate chunksize with a reasonable cap to avoid excessive buffering
    chunksize = max(1, min(10000, num_files // (max_workers * 4)))

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
