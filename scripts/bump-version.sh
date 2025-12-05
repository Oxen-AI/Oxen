#!/bin/bash
# Version bumping script to update version across Rust and Python codebases
# Usage:
#   ./scripts/bump-version.sh <version>  - Set specific version
#   ./scripts/bump-version.sh major      - Bump major version (1.2.3 -> 2.0.0)
#   ./scripts/bump-version.sh minor      - Bump minor version (1.2.3 -> 1.3.0)
#   ./scripts/bump-version.sh patch      - Bump patch version (1.2.3 -> 1.2.4)
# Example: ./scripts/bump-version.sh 0.39.0

set -e

# Get current version from the main Cargo.toml
get_current_version() {
    grep -m 1 -oE '^version = "[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?"' oxen-rust/Cargo.toml | \
        grep -oE '[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?'
}

# Parse and validate semver format
parse_semver() {
    local version="$1"
    # Remove 'v' prefix if present
    version="${version#v}"

    if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
        echo "Error: Invalid semver format: $version" >&2
        return 1
    fi

    echo "$version"
}

# Bump a semver component (strips pre-release tags)
bump_version() {
    local version="$1"
    local component="$2"

    # Strip pre-release tag for bumping
    version="${version%%-*}"

    local major minor patch
    IFS='.' read -r major minor patch <<< "$version"

    case "$component" in
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
        *)
            echo "Error: Unknown component: $component" >&2
            return 1
            ;;
    esac

    echo "${major}.${minor}.${patch}"
}

ARG=$1

# Validate argument
if [[ -z "$ARG" ]]; then
  echo "Error: Version argument required" >&2
  echo "Usage: ./scripts/bump-version.sh <version|major|minor|patch>" >&2
  echo "Example: ./scripts/bump-version.sh 0.39.0" >&2
  echo "Example: ./scripts/bump-version.sh patch" >&2
  exit 1
fi

# Determine version based on argument
case "$ARG" in
    major|minor|patch)
        CURRENT_VERSION=$(get_current_version)
        if [[ -z "$CURRENT_VERSION" ]]; then
            echo "Error: Could not read current version from oxen-rust/Cargo.toml" >&2
            exit 1
        fi
        echo "Current version: $CURRENT_VERSION" >&2
        VERSION=$(bump_version "$CURRENT_VERSION" "$ARG")
        echo "Bumping $ARG version..." >&2
        ;;
    *)
        # Assume it's a version number
        VERSION=$(parse_semver "$ARG") || exit 1
        ;;
esac

echo "Bumping version to $VERSION" >&2
echo "" >&2

# Update all Cargo.toml files (excluding target directories and dependencies sections)
echo "Updating Cargo.toml files..." >&2
find . -name "Cargo.toml" -not -path "*/target/*" -not -path "*/experiments/*" | while read -r file; do
  # Only update the [package] version line, not dependency versions
  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' '/^\[package\]/,/^\[/ s/^version = ".*"/version = "'"$VERSION"'"/' "$file"
  else
    sed -i '/^\[package\]/,/^\[/ s/^version = ".*"/version = "'"$VERSION"'"/' "$file"
  fi
  echo "  ✓ $file" >&2
done

# Update pyproject.toml
echo "Updating pyproject.toml..." >&2
if [[ "$(uname)" == "Darwin" ]]; then
  sed -i '' 's/^version = ".*"/version = "'"$VERSION"'"/' oxen-python/pyproject.toml
else
  sed -i 's/^version = ".*"/version = "'"$VERSION"'"/' oxen-python/pyproject.toml
fi
echo "  ✓ oxen-python/pyproject.toml" >&2

# Update lock files (only workspace packages, not all dependencies)
echo "Updating lock files..." >&2
echo "  Updating oxen-rust/Cargo.lock..." >&2
(cd oxen-rust && cargo update -p liboxen -p oxen-cli -p oxen-server --quiet)
echo "  ✓ oxen-rust/Cargo.lock" >&2

echo "  Updating oxen-python/Cargo.lock and uv.lock..." >&2
(cd oxen-python && cargo update -p oxen --quiet && uv lock --quiet)
echo "  ✓ oxen-python/Cargo.lock" >&2
echo "  ✓ oxen-python/uv.lock" >&2

echo "" >&2
echo "Version updated to $VERSION" >&2
echo "" >&2
echo "Changed files:" >&2
git status --short >&2

# Commit changes and create tag
echo "" >&2
echo "Committing changes..." >&2
# Add only the files modified by this script
find . -name "Cargo.toml" -not -path "*/target/*" -not -path "*/experiments/*" -exec git add {} \;
git add oxen-python/pyproject.toml
git add oxen-rust/Cargo.lock
git add oxen-python/Cargo.lock
git add oxen-python/uv.lock
git commit -m "Bump v$VERSION"
echo "  ✓ Committed changes" >&2

echo "Creating tag v$VERSION..." >&2
git tag "v$VERSION"
echo "  ✓ Created tag v$VERSION" >&2

echo "" >&2
echo "Next steps:" >&2
echo "  Push to remote: git push && git push --tags" >&2

# Output the version to stdout for capture by automation
echo "v$VERSION"
