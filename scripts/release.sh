#!/bin/bash
# Release script to update version across Rust and Python codebases
# Usage: ./scripts/release.sh <version>
# Example: ./scripts/release.sh 0.39.0

set -e

VERSION=$1

# Validate version argument
if [[ -z "$VERSION" ]]; then
  echo "Error: Version argument required"
  echo "Usage: ./scripts/release.sh <version>"
  echo "Example: ./scripts/release.sh 0.39.0"
  exit 1
fi

# Validate version format (basic semver check)
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
  echo "Error: Version must be in semver format (e.g., 0.39.0 or 0.39.0-beta.1)"
  exit 1
fi

echo "Bumping version to $VERSION"
echo ""

# Update all Cargo.toml files (excluding target directories and dependencies sections)
echo "Updating Cargo.toml files..."
find . -name "Cargo.toml" -not -path "*/target/*" -not -path "*/experiments/*" | while read -r file; do
  # Only update the [package] version line, not dependency versions
  sed -i '' '/^\[package\]/,/^\[/ s/^version = ".*"/version = "'"$VERSION"'"/' "$file"
  echo "  ✓ $file"
done

# Update pyproject.toml
echo "Updating pyproject.toml..."
sed -i '' 's/^version = ".*"/version = "'"$VERSION"'"/' oxen-python/pyproject.toml
echo "  ✓ oxen-python/pyproject.toml"

# Update lock files
echo "Updating lock files..."
echo "  Updating oxen-rust/Cargo.lock..."
(cd oxen-rust && cargo update --workspace --quiet)
echo "  ✓ oxen-rust/Cargo.lock"

echo "  Updating oxen-python/Cargo.lock and uv.lock..."
(cd oxen-python && cargo update --quiet && uv lock --quiet)
echo "  ✓ oxen-python/Cargo.lock"
echo "  ✓ oxen-python/uv.lock"

echo ""
echo "Version updated to $VERSION"
echo ""
echo "Changed files:"
git status --short

echo ""
echo "Next steps:"
echo "  1. Review the changes: git diff"
echo "  2. Commit and tag: git add . && git commit -m 'Bump v$VERSION' && git tag 'v$VERSION'"
echo "  3. Push to remote: git push && git push --tags"
