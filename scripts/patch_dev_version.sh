#!/usr/bin/env bash
# Patch oxen-python's pyproject.toml and Cargo.toml with a PEP 440 dev version.
#
# Usage: scripts/patch_dev_version.sh <PEP440_VERSION>
# Example: scripts/patch_dev_version.sh 0.44.1.dev3
#
# The PEP 440 version (e.g. 0.44.1.dev3) is written directly into pyproject.toml.
# For Cargo.toml, it is converted to a semver pre-release (e.g. 0.44.1-dev.3).
#
# This script is used by:
#   - scripts/publish_test_pypi.sh  (local dev builds)
#   - CI workflows (build_wheels_linux.yml, build_wheels_macos.yml)

set -euo pipefail

if [[ $# -ne 1 ]] || [[ -z "$1" ]]; then
    echo "Usage: $0 <PEP440_VERSION>" >&2
    echo "  e.g. $0 0.44.1.dev3" >&2
    exit 1
fi

PEP440_VERSION="$1"
CARGO_VERSION="${PEP440_VERSION/.dev/-dev.}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPROJECT="$SCRIPT_DIR/../oxen-python/pyproject.toml"
CARGO_TOML="$SCRIPT_DIR/../oxen-python/Cargo.toml"

# --- Patch pyproject.toml ---
echo "Patching pyproject.toml → $PEP440_VERSION"
if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' 's/^version = ".*"/version = "'"$PEP440_VERSION"'"/' "$PYPROJECT"
else
    sed -i  's/^version = ".*"/version = "'"$PEP440_VERSION"'"/' "$PYPROJECT"
fi

# --- Patch Cargo.toml (scoped to [package] section) ---
echo "Patching Cargo.toml    → $CARGO_VERSION"
if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' '/^\[package\]/,/^\[/ s/^version = ".*"/version = "'"$CARGO_VERSION"'"/' "$CARGO_TOML"
else
    sed -i  '/^\[package\]/,/^\[/ s/^version = ".*"/version = "'"$CARGO_VERSION"'"/' "$CARGO_TOML"
fi
