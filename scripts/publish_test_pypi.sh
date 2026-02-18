#!/usr/bin/env bash
# Build a .devN Python wheel for oxen-python and publish it to Test PyPI.
#
# Usage:
#   TEST_PYPI_API_TOKEN=pypi-... ./scripts/publish_test_pypi.sh <N>
#
# The published version will be <V>.dev<N>, where V is the version currently
# set in oxen-python/pyproject.toml.
#
# Example:
#   If pyproject.toml has version 0.44.1 and N=3, publishes 0.44.1.dev3
#
# Requirements:
#   - maturin   (pip install maturin)
#   - cargo
#   - TEST_PYPI_API_TOKEN env var set to a valid Test PyPI API token

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="$REPO_ROOT/oxen-python/pyproject.toml"
CARGO_TOML="$REPO_ROOT/oxen-python/Cargo.toml"
CARGO_LOCK="$REPO_ROOT/oxen-python/Cargo.lock"

# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------
if [[ $# -ne 1 ]] || ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 <N>" >&2
    echo "  N  - non-negative integer for the .devN suffix" >&2
    exit 1
fi

DEV_N="$1"

# ---------------------------------------------------------------------------
# Auth check
# ---------------------------------------------------------------------------
if [[ -z "${TEST_PYPI_API_TOKEN:-}" ]]; then
    echo "Error: TEST_PYPI_API_TOKEN env var is not set." >&2
    echo "Generate a token at https://test.pypi.org/manage/account/token/" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Read current version from pyproject.toml
# ---------------------------------------------------------------------------
CURRENT_VERSION=$(grep -m1 '^version' "$PYPROJECT" | sed 's/version *= *"\(.*\)"/\1/')
if [[ -z "$CURRENT_VERSION" ]]; then
    echo "Error: could not read version from $PYPROJECT" >&2
    exit 1
fi

PEP440_VERSION="${CURRENT_VERSION}.dev${DEV_N}"   # e.g. 0.44.1.dev3
CARGO_VERSION="${CURRENT_VERSION}-dev.${DEV_N}"    # e.g. 0.44.1-dev.3

echo "Current version : $CURRENT_VERSION"
echo "Test PyPI version: $PEP440_VERSION  (Cargo: $CARGO_VERSION)"
echo ""

# ---------------------------------------------------------------------------
# Backup originals & set up cleanup trap
# ---------------------------------------------------------------------------
cp "$PYPROJECT"  "$PYPROJECT.bak"
cp "$CARGO_TOML" "$CARGO_TOML.bak"
cp "$CARGO_LOCK" "$CARGO_LOCK.bak"

cleanup() {
    echo ""
    echo "Restoring original files..."
    mv -f "$PYPROJECT.bak"  "$PYPROJECT"
    mv -f "$CARGO_TOML.bak" "$CARGO_TOML"
    mv -f "$CARGO_LOCK.bak" "$CARGO_LOCK"
    echo "Done."
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Patch versions
# ---------------------------------------------------------------------------
"$REPO_ROOT/scripts/patch_dev_version.sh" "$PEP440_VERSION"

echo "Updating Cargo.lock..."
(cd "$REPO_ROOT/oxen-python" && cargo update -p oxen --quiet)

# ---------------------------------------------------------------------------
# Build wheel
# ---------------------------------------------------------------------------
echo ""
echo "Building wheel with maturin..."
(cd "$REPO_ROOT/oxen-python" && maturin build --release)

# ---------------------------------------------------------------------------
# Locate the built wheel(s)
# ---------------------------------------------------------------------------
WHEEL_DIR="$REPO_ROOT/oxen-python/target/wheels"
WHEELS=("$WHEEL_DIR"/*"${PEP440_VERSION}"*.whl)

if [[ ${#WHEELS[@]} -eq 0 ]]; then
    echo "Error: no wheels found matching version $PEP440_VERSION in $WHEEL_DIR" >&2
    exit 1
fi

echo ""
echo "Built wheel(s):"
printf '  %s\n' "${WHEELS[@]}"

# ---------------------------------------------------------------------------
# Upload to Test PyPI
# ---------------------------------------------------------------------------
echo ""
echo "Uploading to Test PyPI..."
MATURIN_PYPI_TOKEN="$TEST_PYPI_API_TOKEN" \
    maturin upload \
        --repository-url https://test.pypi.org/legacy/ \
        --skip-existing \
        "${WHEELS[@]}"

echo ""
echo "Published $PEP440_VERSION to Test PyPI."
echo "Install with:"
echo "  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ oxenai==$PEP440_VERSION"
